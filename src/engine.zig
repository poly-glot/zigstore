//! The comptime data plane: an app declares its store as a `Schema`, and `Engine(schema)`
//! generates the typed, on-disk paged storage layer: a `PAGE_SIZE` superblock `Header` with a
//! root/count slot per index and a slot per persisted counter, the named paged B+Trees
//! themselves, and the write memtables fronting the subset of indexes the schema marks.
//!
//! This generalizes the hand-rolled index table a concrete consumer would otherwise walk with
//! `inline for`: declare the indexes once, and the header layout, the trees, the memtables,
//! and the counters are all generated from that single source.
//!
//! The backing is the real paged B+Tree over a page cache, free list, and WAL. The engine owns
//! durability and recovery ordering but imports no application module: per-entry WAL decode and
//! the bootstrap of a fresh store are supplied by the caller through the `recover` hooks, and
//! background maintenance runs through the generic `spawnWorker` seam.

const std = @import("std");
const codec = @import("codec.zig");
const page = @import("page.zig");
const file_header = @import("file_header.zig");
const page_cache = @import("page_cache.zig");
const freelist = @import("freelist.zig");
const memtable = @import("memtable.zig");
const wal = @import("wal.zig");
const wal_replay = @import("wal_replay.zig");
const snapshot = @import("snapshot.zig");
const replication = @import("replication.zig");
const commit = @import("commit.zig");

/// The paged B+Tree backing one index (point ops + range scans over byte keys/values).
pub const BPlusTree = @import("btree/btree.zig").BPlusTree;

/// The sharded write memtable fronting an index until it drains into the index's B+Tree.
pub const MemTable = memtable.MemTable;

const MEMTABLE_SHARDS = memtable.NUM_SHARDS;

/// One replayed WAL entry handed to a `recover` hook: its sequence, op code, and payload bytes.
pub const ReplayEntry = wal_replay.ReplayEntry;

/// What `Store.healthStatus` reports — the engine-owned facts a consumer's health/ping op
/// serves for liveness and readiness probes. A replica's readiness is typically
/// `!read_only or (leader durable LSN - last_applied_lsn) <= lag budget`, with the leader
/// LSN taken from `replication.Receiver.status()`.
pub const HealthStatus = struct {
    read_only: bool,
    last_applied_lsn: u64,
    durable_lsn: u64,
};

const log = std.log.scoped(.engine);

/// How an index orders its keys. The kind is metadata for validation and client codegen;
/// the store compares the encoded key bytes either way, so big-endian encodings
/// (`codec.encodeU64`, `codec.CompositeKey`) sort numerically.
pub const KeyKind = union(enum) {
    /// A single big-endian `u64` key (see `codec.encodeU64`).
    u64,
    /// An opaque byte key compared lexically (e.g. a slug path).
    bytes,
    /// A multi-`u64` key whose components are listed for codegen (see `codec.CompositeKey`).
    composite: []const [:0]const u8,
};

/// One declared index: a name (used for the generated header slots and tree field) and
/// its key kind.
pub const IndexSpec = struct {
    name: [:0]const u8,
    key: KeyKind,
};

/// The normalized, comptime description of a store: its on-disk identity, its indexes, the
/// subset of indexes fronted by a write memtable, and its persisted monotonic counters.
pub const Schema = struct {
    magic: u32,
    format_version: u32,
    indexes: []const IndexSpec,
    memtable_indexes: []const [:0]const u8,
    counters: []const [:0]const u8,
};

/// Validate and normalize an anonymous schema literal into a `Schema`. Call at comptime and
/// feed the result to `Engine`.
///
/// Expects a struct with `magic`, `format_version`, `indexes` (a tuple of
/// `.{ .name, .key }`), `memtable_indexes`, and `counters`.
pub fn schema(comptime spec: anytype) Schema {
    comptime {
        var indexes: [spec.indexes.len]IndexSpec = undefined;
        for (spec.indexes, 0..) |entry, i| {
            indexes[i] = .{ .name = entry.name, .key = normalizeKey(entry.key) };
        }

        var memtables: [spec.memtable_indexes.len][:0]const u8 = undefined;
        for (spec.memtable_indexes, 0..) |name, i| memtables[i] = name;

        var counters: [spec.counters.len][:0]const u8 = undefined;
        for (spec.counters, 0..) |name, i| counters[i] = name;

        for (memtables) |name| {
            if (indexOfName(indexes[0..], name) == null)
                @compileError("memtable_index '" ++ name ++ "' is not a declared index");
        }

        const frozen_indexes = indexes;
        const frozen_memtables = memtables;
        const frozen_counters = counters;
        return .{
            .magic = spec.magic,
            .format_version = spec.format_version,
            .indexes = &frozen_indexes,
            .memtable_indexes = &frozen_memtables,
            .counters = &frozen_counters,
        };
    }
}

fn normalizeKey(comptime k: anytype) KeyKind {
    if (@typeInfo(@TypeOf(k)) == .@"struct") return .{ .composite = k.composite };
    if (k == .u64) return .u64;
    if (k == .bytes) return .bytes;
    @compileError("unsupported key kind in schema (expected .u64, .bytes, or .{ .composite = ... })");
}

fn indexOfName(comptime indexes: []const IndexSpec, comptime name: []const u8) ?usize {
    for (indexes, 0..) |idx, i| {
        if (std.mem.eql(u8, idx.name, name)) return i;
    }
    return null;
}

fn structField(comptime name: [:0]const u8, comptime T: type) std.builtin.Type.StructField {
    return .{
        .name = name,
        .type = T,
        .default_value_ptr = null,
        .is_comptime = false,
        .alignment = @alignOf(T),
    };
}

fn headerPrefixSize(comptime s: Schema) usize {
    comptime {
        var size: usize = 4 * @sizeOf(u32) + 3 * @sizeOf(u64);
        size += s.indexes.len * 2 * @sizeOf(u64);
        size += s.counters.len * @sizeOf(u64);
        return size;
    }
}

fn HeaderType(comptime s: Schema) type {
    comptime {
        if (headerPrefixSize(s) > page.PAGE_SIZE) @compileError("zigstore schema: header (indexes+counters) exceeds PAGE_SIZE; reduce indexes/counters");
        const reserved_len = page.PAGE_SIZE - headerPrefixSize(s);
        const generic = [_]std.builtin.Type.StructField{
            structField("magic", u32),
            structField("format_version", u32),
            structField("page_size", u32),
            structField("_pad", u32),
            structField("free_list_head", u64),
            structField("page_count", u64),
            structField("seq", u64),
        };

        var fields: [generic.len + s.indexes.len * 2 + s.counters.len + 1]std.builtin.Type.StructField = undefined;
        var n: usize = 0;
        for (generic) |f| {
            fields[n] = f;
            n += 1;
        }
        for (s.indexes) |idx| {
            fields[n] = structField(std.fmt.comptimePrint("{s}_root", .{idx.name}), u64);
            n += 1;
            fields[n] = structField(std.fmt.comptimePrint("{s}_count", .{idx.name}), u64);
            n += 1;
        }
        for (s.counters) |c| {
            fields[n] = structField(c, u64);
            n += 1;
        }
        fields[n] = structField("_reserved", [reserved_len]u8);
        n += 1;

        return @Type(.{ .@"struct" = .{
            .layout = .@"extern",
            .fields = fields[0..n],
            .decls = &.{},
            .is_tuple = false,
        } });
    }
}

fn TreesType(comptime s: Schema) type {
    comptime {
        var fields: [s.indexes.len]std.builtin.Type.StructField = undefined;
        for (s.indexes, 0..) |idx, i| {
            fields[i] = structField(idx.name, BPlusTree);
        }
        return @Type(.{ .@"struct" = .{
            .layout = .auto,
            .fields = &fields,
            .decls = &.{},
            .is_tuple = false,
        } });
    }
}

fn MemtablesType(comptime s: Schema) type {
    comptime {
        var fields: [s.memtable_indexes.len]std.builtin.Type.StructField = undefined;
        for (s.memtable_indexes, 0..) |name, i| {
            fields[i] = structField(name, MemTable);
        }
        return @Type(.{ .@"struct" = .{
            .layout = .auto,
            .fields = &fields,
            .decls = &.{},
            .is_tuple = false,
        } });
    }
}

/// A generic background worker spawned by `Store.spawnWorker`: a thread that fires the
/// supplied `tick` on a fixed interval until `stop` joins it cleanly.
pub const Worker = struct {
    thread: std.Thread,
    shutdown: std.atomic.Value(bool),
    cond: std.Thread.Condition,
    mutex: std.Thread.Mutex,
    interval_ns: u64,
    ctx: *anyopaque,
    tick: *const fn (ctx: *anyopaque) anyerror!void,
    allocator: std.mem.Allocator,

    /// Signal the worker to stop, wait for the current tick to finish, join the thread, and
    /// free the worker. Safe to call exactly once.
    pub fn stop(self: *Worker) void {
        self.mutex.lock();
        self.shutdown.store(true, .release);
        self.cond.signal();
        self.mutex.unlock();
        self.thread.join();
        self.allocator.destroy(self);
    }

    fn loop(self: *Worker) void {
        while (true) {
            self.mutex.lock();
            if (!self.shutdown.load(.acquire)) {
                self.cond.timedWait(&self.mutex, self.interval_ns) catch {};
            }
            const stopping = self.shutdown.load(.acquire);
            self.mutex.unlock();

            if (stopping) break;
            self.tick(self.ctx) catch |err| {
                log.warn("worker tick failed: {}", .{err});
            };
        }
    }
};

/// Generate the typed paged `Store` for a comptime `Schema`.
///
/// The returned type exposes `Header` (the generated superblock), `Config`, `init`/`deinit`,
/// per-index tree access via `tree(name)`, per-memtable access via `memtable(name)`,
/// persisted-counter access via `counter(name)` / `nextId(name)`, the atomic `checkpoint`
/// (and its pressure valve `flushPages`), memtable draining via `drainMemtables`, and the
/// `recover`/`spawnWorker`/`spawnCheckpointer` runtime seams. Tree, memtable, and counter
/// names are checked at comptime against the schema.
///
/// Durability model: a WAL append is acknowledged after `fdatasync`; tree pages are written
/// only by `checkpoint`/`flushPages` through `PageCache.checkpoint` (journal, then in place),
/// never by eviction; `header.seq` records the WAL sequence up to which every entry is fully
/// contained in the on-disk tree, and `checkpoint` truncates the WAL to that point.
///
/// Lock order, everywhere in the engine: `mt_drain_mutex` → `apply_mutex`. A consumer's
/// `apply_fn`/`apply_entry` runs under `apply_mutex` and must therefore never call
/// `drainMemtables`, `flushPages`, or `checkpoint`.
pub fn Engine(comptime s: Schema) type {
    return struct {
        const Store = @This();

        /// The generated superblock: the generic `magic`/`format_version`/`page_size`/
        /// `free_list_head`/`page_count`/`seq` fields, a `<name>_root`/`<name>_count` pair
        /// per index, a slot per declared counter, and a trailing `_reserved` pad to
        /// `PAGE_SIZE`. App-supplied `magic`; no schema field names are hardcoded.
        pub const Header = HeaderType(s);

        /// The compile-time schema this store was generated from.
        pub const schema_def = s;

        /// Open/create configuration: the on-disk data directory and the page-cache budget.
        pub const Config = struct {
            data_dir: []const u8,
            cache_size_mb: u32 = 64,
            wal_batch_size: u32 = 32,
            /// Open the WAL with `O_DIRECT` (the default) or force buffered I/O. Set `false` on a
            /// filesystem where `O_DIRECT` is unavailable or undesirable (NFS, some CSI volumes);
            /// the WAL still `fdatasync`s for durability, it just skips direct I/O and its
            /// block-aligned padding.
            wal_direct_io: bool = true,
        };

        const Trees = TreesType(s);
        const Memtables = MemtablesType(s);

        allocator: std.mem.Allocator,
        config: Config,
        file: std.fs.File,
        journal: std.fs.File,
        header: Header,
        cache: page_cache.PageCache,
        free_list: freelist.FreeList,
        wal_writer: ?wal.WalWriter,

        trees: Trees,
        memtables: Memtables,

        was_empty: bool,

        mt_drain_mutex: std.Thread.Mutex,

        header_lock: std.Thread.Mutex,

        apply_mutex: std.Thread.Mutex,
        apply_cond: std.Thread.Condition,
        last_applied_seq: u64,
        snapshot_in_progress: std.atomic.Value(bool),

        read_only: std.atomic.Value(bool),
        replica_streaming: std.atomic.Value(bool),
        commit_gate: std.atomic.Value(?*replication.CommitGate),
        sync_gate: replication.CommitGate,

        checkpoint_interval_s: u32,
        last_checkpoint_time: i64,

        /// Open (or create) the store under `config.data_dir`, returning a heap-allocated
        /// `*Store` at a stable address. A complete `checkpoint.journal` left by a crash is
        /// redone into `store.dat` first (an incomplete one is discarded); the journal then
        /// stays open, empty, for the store's checkpoints. On a fresh directory
        /// the header is formatted with the schema's identity and every index starts at an
        /// empty (lazily-allocated) root; on an existing one the header is loaded and validated
        /// and each tree is wired to the shared cache/free list at its persisted root and count.
        /// The WAL must open: a WAL that cannot be opened or scanned (including
        /// `error.WalCorrupt`) fails `init`. The store owns its own allocation: every tree's
        /// `cache`/`free_list` and the free list's `cache` are bound to the settled in-place
        /// pointers and the WAL flusher is started before returning, so the value never needs
        /// an external rebind. Release it with `deinit`.
        pub fn init(allocator: std.mem.Allocator, config: Config) !*Store {
            std.fs.makeDirAbsolute(config.data_dir) catch |err| switch (err) {
                error.PathAlreadyExists => {},
                else => return err,
            };

            const path = try std.fmt.allocPrint(allocator, "{s}/store.dat", .{config.data_dir});
            defer allocator.free(path);

            const file = try std.fs.createFileAbsolute(path, .{ .read = true, .truncate = false });
            errdefer file.close();

            const journal = try openJournal(allocator, config.data_dir, file);
            errdefer journal.close();

            const file_size = (try file.stat()).size;
            const is_new = file_size == 0;

            var header: Header = undefined;
            if (is_new) {
                header = std.mem.zeroes(Header);
                header.magic = s.magic;
                header.format_version = s.format_version;
                header.page_size = page.PAGE_SIZE;
                header.free_list_head = page.INVALID_PAGE;
                inline for (s.indexes) |idx| {
                    @field(header, idx.name ++ "_root") = page.INVALID_PAGE;
                }

                const header_bytes = file_header.serialize(header);
                try file.seekTo(0);
                try file.writeAll(&header_bytes);
                try file.sync();
            } else {
                try file.seekTo(0);
                var header_buf: [page.PAGE_SIZE]u8 = undefined;
                const bytes_read = try file.readAll(&header_buf);
                if (bytes_read < @sizeOf(Header)) return error.UnexpectedEof;
                header = file_header.deserialize(Header, &header_buf);
                try file_header.validate(header, s.magic, s.format_version);
            }

            const cache_pages = (config.cache_size_mb * 1024 * 1024) / page.PAGE_SIZE;
            var cache = try page_cache.PageCache.init(allocator, file, cache_pages);
            errdefer cache.deinit();

            const snapshot_base = snapshot.SnapshotManager.getWalSequence(config.data_dir) catch |err| blk: {
                log.warn("snapshot meta read failed: {} — WAL sequence resumes from 0", .{err});
                break :blk 0;
            };
            const wal_base = @max(header.seq, snapshot_base);
            var wal_writer: ?wal.WalWriter = try wal.WalWriter.init(allocator, config.data_dir, config.wal_batch_size, wal_base, config.wal_direct_io);
            errdefer if (wal_writer) |*w| w.deinit();

            const self = try allocator.create(Store);
            errdefer allocator.destroy(self);

            self.* = Store{
                .allocator = allocator,
                .config = config,
                .file = file,
                .journal = journal,
                .header = header,
                .cache = cache,
                .free_list = .{ .head = @intCast(header.free_list_head), .cache = undefined, .mutex = .{} },
                .wal_writer = wal_writer,
                .trees = undefined,
                .memtables = undefined,
                .was_empty = is_new,
                .mt_drain_mutex = .{},
                .header_lock = .{},
                .apply_mutex = .{},
                .apply_cond = .{},
                .last_applied_seq = if (wal_writer) |w| w.sequence else 0,
                .snapshot_in_progress = std.atomic.Value(bool).init(false),
                .read_only = std.atomic.Value(bool).init(false),
                .replica_streaming = std.atomic.Value(bool).init(false),
                .commit_gate = std.atomic.Value(?*replication.CommitGate).init(null),
                .sync_gate = .{},
                .checkpoint_interval_s = 0,
                .last_checkpoint_time = 0,
            };

            inline for (s.indexes) |idx| {
                @field(self.trees, idx.name) = BPlusTree.init(
                    undefined,
                    undefined,
                    @intCast(@field(header, idx.name ++ "_root")),
                );
                @field(self.trees, idx.name).entry_count = @field(header, idx.name ++ "_count");
            }

            inline for (s.memtable_indexes) |name| {
                @field(self.memtables, name) = MemTable.init(allocator);
            }
            errdefer inline for (s.memtable_indexes) |name| {
                @field(self.memtables, name).deinit();
            };

            self.free_list.cache = &self.cache;
            inline for (s.indexes) |idx| {
                @field(self.trees, idx.name).cache = &self.cache;
                @field(self.trees, idx.name).free_list = &self.free_list;
            }
            if (self.wal_writer) |*w| {
                w.startFlusher() catch |err| {
                    log.warn("WAL flusher start failed: {}", .{err});
                };
            }

            return self;
        }

        /// Tear down the store the `init` heap-allocated: take a final `checkpoint`, close the
        /// WAL and data file, free every owned resource, and finally `destroy` the store
        /// allocation itself. Stop every worker first. Call once on the `*Store` returned by
        /// `init`; the pointer is invalid afterward.
        pub fn deinit(self: *Store) void {
            _ = self.checkpoint() catch |err| {
                log.err("deinit checkpoint failed: {}", .{err});
            };

            if (self.wal_writer) |*w| w.deinit();

            inline for (s.memtable_indexes) |name| {
                @field(self.memtables, name).deinit();
            }
            self.cache.deinit();
            self.journal.close();
            self.file.close();

            const allocator = self.allocator;
            allocator.destroy(self);
        }

        /// A pointer to the named index's B+Tree. The name is resolved and checked at comptime.
        pub fn tree(self: *Store, comptime name: [:0]const u8) *BPlusTree {
            comptime assertIndex(name);
            return &@field(self.trees, name);
        }

        /// A pointer to the named index's write memtable. The name must be a declared
        /// `memtable_index`; otherwise this is a comptime error.
        pub fn memtable(self: *Store, comptime name: [:0]const u8) *MemTable {
            comptime assertMemtable(name);
            return &@field(self.memtables, name);
        }

        /// A pointer to the named counter's persisted slot in the header. Single-writer:
        /// the caller serializes all counter mutations; this raw header field is written
        /// non-atomically and must not be touched concurrently with another counter
        /// mutation or with `checkpoint`/`flushPages` (which read the header under
        /// `header_lock`).
        pub fn counter(self: *Store, comptime name: [:0]const u8) *u64 {
            comptime assertCounter(name);
            return &@field(self.header, name);
        }

        /// Increment the named counter and return its new value. Allocates ids from `1`.
        /// Single-writer / caller-serialized: the read-modify-write is non-atomic, so the
        /// caller must not call it concurrently with another counter mutation or with
        /// `checkpoint`/`flushPages`.
        pub fn nextId(self: *Store, comptime name: [:0]const u8) u64 {
            const slot = self.counter(name);
            slot.* += 1;
            return slot.*;
        }

        /// Drain every write memtable into its backing B+Tree: tombstones delete, live entries
        /// insert, applied in shard order. Serialized against concurrent drains. When the page
        /// cache's dirty pressure is high mid-drain, the pages are flushed (`flushPages`) between
        /// two inserts so a large drain can never exhaust the no-steal cache. A drain that fails
        /// midway leaves the undrained rows in the memtable's back buffer; the next drain merges
        /// and retries them.
        pub fn drainMemtables(self: *Store) !void {
            self.mt_drain_mutex.lock();
            defer self.mt_drain_mutex.unlock();
            try self.drainMemtablesLocked(false);
        }

        /// Drain one memtable into its tree under `mt_drain_mutex`, pulling the pressure valve
        /// between rows exactly as `drainMemtables` does. For a consumer that needs a single
        /// index settled before a tree scan; the caller must not hold any engine lock.
        pub fn drainMemtableInto(self: *Store, mt: *MemTable, dst: *BPlusTree) !void {
            self.mt_drain_mutex.lock();
            defer self.mt_drain_mutex.unlock();
            try self.drainOne(mt, dst, false);
        }

        fn drainMemtablesLocked(self: *Store, apply_held: bool) !void {
            inline for (s.memtable_indexes) |name| {
                try self.drainOne(&@field(self.memtables, name), &@field(self.trees, name), apply_held);
            }
        }

        fn drainOne(self: *Store, mt: *MemTable, dst: *BPlusTree, apply_held: bool) !void {
            mt.lockAll();
            var backs: [MEMTABLE_SHARDS]*MemTable.Buffer = undefined;
            const swapped = swapAllShards(mt, &backs);
            mt.unlockAll();
            try swapped;

            for (0..MEMTABLE_SHARDS) |i| {
                var it = backs[i].map.iterator();
                while (it.next()) |entry| {
                    try self.relieveDrainPressure(apply_held);
                    const key = entry.key_ptr.*;
                    const val = entry.value_ptr.*;
                    if (val.tombstone) {
                        _ = try dst.delete(key);
                    } else {
                        try dst.insert(key, val.value);
                    }
                }
            }

            mt.lockAll();
            for (0..MEMTABLE_SHARDS) |i| mt.resetShardBackLocked(i);
            mt.unlockAll();
        }

        fn swapAllShards(mt: *MemTable, backs: *[MEMTABLE_SHARDS]*MemTable.Buffer) !void {
            for (0..MEMTABLE_SHARDS) |i| backs[i] = try mt.swapShardLocked(i);
        }

        fn relieveDrainPressure(self: *Store, apply_held: bool) !void {
            if (!self.cache.dirtyPressureHigh()) return;
            if (!apply_held) self.apply_mutex.lock();
            defer if (!apply_held) self.apply_mutex.unlock();
            try self.persistLocked(self.header.seq);
        }

        /// Write every dirty page and the header to `store.dat` atomically (journal, then in
        /// place) without draining memtables or claiming new WAL entries durable-in-tree:
        /// `header.seq` is unchanged. The pressure valve a `spawnCheckpointer` worker pulls when
        /// `cache.dirtyPressureHigh()`; takes `mt_drain_mutex` then `apply_mutex`.
        pub fn flushPages(self: *Store) !void {
            self.mt_drain_mutex.lock();
            defer self.mt_drain_mutex.unlock();
            self.apply_mutex.lock();
            defer self.apply_mutex.unlock();
            try self.persistLocked(self.header.seq);
        }

        /// `flushPages` only when `cache.dirtyPressureHigh()`, re-checked under the locks so a
        /// thundering herd of callers flushes once. Safe to call from any thread holding no
        /// engine lock; the commit path calls it before appending.
        pub fn relieveDirtyPressure(self: *Store) !void {
            if (!self.cache.dirtyPressureHigh()) return;
            self.mt_drain_mutex.lock();
            defer self.mt_drain_mutex.unlock();
            try self.relieveDrainPressure(false);
        }

        /// Make everything committed so far durable in the tree, atomically with respect to a
        /// crash, and return the new `header.seq`. Takes `mt_drain_mutex` then `apply_mutex` and
        /// runs `checkpointLocked`.
        pub fn checkpoint(self: *Store) !u64 {
            self.mt_drain_mutex.lock();
            defer self.mt_drain_mutex.unlock();
            self.apply_mutex.lock();
            defer self.apply_mutex.unlock();
            return self.checkpointLocked();
        }

        /// The checkpoint body; the caller holds `mt_drain_mutex` and `apply_mutex`. Pauses WAL
        /// appends and waits until every appended entry has applied, flushes pages first if the
        /// cache is under pressure, drains every memtable into its tree, writes every dirty page
        /// and the header (with `header.seq` = the WAL sequence) through the checkpoint journal,
        /// then truncates the WAL (unless a replica still retains it). Returns the durable
        /// sequence. The `SnapshotHost` seam calls this.
        pub fn checkpointLocked(self: *Store) !u64 {
            if (self.wal_writer) |*w| {
                w.pauseAppends();
                defer w.resumeAppends();
                return self.checkpointQuiesced(w);
            }
            return self.checkpointQuiesced(null);
        }

        fn checkpointQuiesced(self: *Store, w: ?*wal.WalWriter) !u64 {
            if (w) |writer| {
                while (self.last_applied_seq < writer.getSequence()) {
                    self.apply_cond.wait(&self.apply_mutex);
                }
            }

            if (self.cache.dirtyPressureHigh()) try self.persistLocked(self.header.seq);
            try self.drainMemtablesLocked(true);

            const durable: u64 = if (w) |writer| writer.getSequence() else self.header.seq;
            try self.persistLocked(durable);

            if (w) |writer| writer.truncateAfterCheckpoint() catch |err| switch (err) {
                error.WalRetainedByReplica => {},
                else => return err,
            };
            return durable;
        }

        fn persistLocked(self: *Store, durable_seq: u64) !void {
            const previous_seq = self.header.seq;
            errdefer self.header.seq = previous_seq;
            const header_bytes = self.serializeHeaderLocked(durable_seq);

            try self.cache.checkpoint(self.journal, &header_bytes);
            try self.journal.setEndPos(0);
        }

        fn serializeHeaderLocked(self: *Store, durable_seq: u64) [page.PAGE_SIZE]u8 {
            self.header_lock.lock();
            defer self.header_lock.unlock();

            inline for (s.indexes) |idx| {
                @field(self.header, idx.name ++ "_root") = @field(self.trees, idx.name).getRootPage();
                @field(self.header, idx.name ++ "_count") = @field(self.trees, idx.name).entryCount();
            }
            self.header.free_list_head = self.free_list.getHead();
            self.cache.alloc_lock.lock();
            self.header.page_count = self.cache.page_count;
            self.cache.alloc_lock.unlock();
            self.header.seq = durable_seq;

            return file_header.serialize(self.header);
        }

        fn journalPath(allocator: std.mem.Allocator, data_dir: []const u8) ![]u8 {
            return std.fs.path.join(allocator, &.{ data_dir, "checkpoint.journal" });
        }

        fn openJournal(allocator: std.mem.Allocator, data_dir: []const u8, file: std.fs.File) !std.fs.File {
            const journal_path = try journalPath(allocator, data_dir);
            defer allocator.free(journal_path);

            const journal = std.fs.openFileAbsolute(journal_path, .{ .mode = .read_write }) catch |err| switch (err) {
                error.FileNotFound => {
                    const created = try std.fs.createFileAbsolute(journal_path, .{ .read = true });
                    snapshot.fsyncDir(data_dir);
                    return created;
                },
                else => return err,
            };
            errdefer journal.close();

            if ((try journal.getEndPos()) > 0) {
                if (try page_cache.PageCache.redoJournal(journal, file)) {
                    log.info("open: redid the checkpoint journal left by an interrupted checkpoint", .{});
                } else {
                    log.warn("open: discarded an incomplete checkpoint journal", .{});
                }
                try journal.setEndPos(0);
                try journal.sync();
            }
            return journal;
        }

        /// Spawn the background checkpointer: a `Worker` ticking every second that pulls the
        /// pressure valve (`flushPages`) whenever `cache.dirtyPressureHigh()` and runs a full
        /// `checkpoint` every `interval_s` seconds. Tick errors are logged by the worker and the
        /// next tick retries. Stop it with `worker.stop()` before `deinit`.
        pub fn spawnCheckpointer(self: *Store, interval_s: u32) !*Worker {
            self.checkpoint_interval_s = interval_s;
            self.last_checkpoint_time = std.time.timestamp();
            return self.spawnWorker(self, .{ .interval_ns = std.time.ns_per_s, .tick = checkpointerTick });
        }

        fn checkpointerTick(ctx: *anyopaque) anyerror!void {
            const self: *Store = @ptrCast(@alignCast(ctx));
            try self.relieveDirtyPressure();

            const now = std.time.timestamp();
            if (now - self.last_checkpoint_time < @as(i64, self.checkpoint_interval_s)) return;
            _ = try self.checkpoint();
            self.last_checkpoint_time = now;
        }

        /// Replay the WAL, checkpoint, and bootstrap — the engine owns the ordering, the caller
        /// owns the per-entry semantics. For each decoded entry with a sequence above
        /// `header.seq` (entries at or below it are already in the on-disk tree),
        /// `apply_entry(ctx, entry)` runs, with the pressure valve pulled between entries; then,
        /// if anything was in the WAL, a full `checkpoint` lands it in the tree and truncates
        /// the WAL; then `on_replayed(ctx)` runs; and if the store opened empty, `bootstrap(ctx)`
        /// runs. A failed checkpoint fails recovery. The engine imports no application module —
        /// decoding and applying an entry lives entirely inside `apply_entry`.
        pub fn recover(self: *Store, ctx: *anyopaque, hooks: struct {
            apply_entry: *const fn (ctx: *anyopaque, entry: ReplayEntry) anyerror!void,
            on_replayed: *const fn (ctx: *anyopaque) anyerror!void,
            bootstrap: *const fn (ctx: *anyopaque) anyerror!void,
        }) !void {
            var applier = ReplayAdapter{ .store = self, .ctx = ctx, .apply_entry = hooks.apply_entry };
            const last_seq = wal_replay.replayWal(self.config.data_dir, self.header.seq, &applier) catch |err| {
                log.err("WAL replay failed: {}", .{err});
                return err;
            };

            if (last_seq > 0) _ = try self.checkpoint();

            try hooks.on_replayed(ctx);

            if (self.was_empty) try hooks.bootstrap(ctx);
        }

        const ReplayAdapter = struct {
            store: *Store,
            ctx: *anyopaque,
            apply_entry: *const fn (ctx: *anyopaque, entry: ReplayEntry) anyerror!void,

            pub fn apply(self: *ReplayAdapter, entry: ReplayEntry) !void {
                try self.apply_entry(self.ctx, entry);
                try self.store.relieveDirtyPressure();
            }
        };

        /// Spawn a background worker that fires `cfg.tick(ctx)` every `cfg.interval_ns`. Returns
        /// an owned `*Worker`; call `worker.stop()` to halt and join it.
        pub fn spawnWorker(self: *Store, ctx: *anyopaque, cfg: struct {
            interval_ns: u64,
            tick: *const fn (ctx: *anyopaque) anyerror!void,
        }) !*Worker {
            const worker = try self.allocator.create(Worker);
            errdefer self.allocator.destroy(worker);
            worker.* = .{
                .thread = undefined,
                .shutdown = std.atomic.Value(bool).init(false),
                .cond = .{},
                .mutex = .{},
                .interval_ns = cfg.interval_ns,
                .ctx = ctx,
                .tick = cfg.tick,
                .allocator = self.allocator,
            };
            worker.thread = try std.Thread.spawn(.{}, Worker.loop, .{worker});
            return worker;
        }

        /// The `snapshot.SnapshotHost` view of this store: the in-progress guard, the data
        /// directory, the drain and apply locks, and the two function pointers the generic
        /// snapshot routine drives (`checkpointLocked`, page count). Pass the result to
        /// `snapshot.forceSnapshot`.
        pub fn snapshotHost(self: *Store) snapshot.SnapshotHost {
            return .{
                .snapshot_in_progress = &self.snapshot_in_progress,
                .data_dir = self.config.data_dir,
                .apply_mutex = &self.apply_mutex,
                .mt_drain_mutex = &self.mt_drain_mutex,
                .checkpointLocked = snapshotCheckpointLocked,
                .pageCount = snapshotPageCount,
                .ctx = self,
            };
        }

        /// Whether the store is a read-only replica (`commit` fails with
        /// `error.ReadOnlyReplica` while set). Set by `Receiver.start` / `demote`, cleared
        /// by `promote`.
        pub fn isReadOnly(self: *Store) bool {
            return self.read_only.load(.acquire);
        }

        /// The engine-owned health facts for a consumer's health/ping op: the read-only
        /// flag, the last applied WAL sequence, and the last durable WAL sequence (0 when
        /// the store runs without a WAL). Cheap enough for a readiness probe path.
        pub fn healthStatus(self: *Store) HealthStatus {
            self.apply_mutex.lock();
            const applied = self.last_applied_seq;
            self.apply_mutex.unlock();

            return .{
                .read_only = self.read_only.load(.acquire),
                .last_applied_lsn = applied,
                .durable_lsn = if (self.wal_writer) |*w| w.durableBoundary().sequence else 0,
            };
        }

        /// Whether this store's WAL opened with `O_DIRECT` or fell back to buffered I/O; `false`
        /// when the store has no WAL. Lets a benchmark or operator report the real durability
        /// mode rather than assume one.
        pub fn walUsingDirectIo(self: *Store) bool {
            return if (self.wal_writer) |*w| w.usingDirectIo() else false;
        }

        /// The count of `fdatasync` calls this store's WAL has completed (`0` without a WAL).
        /// Divided into the commit count it gives the mean group-commit batch occupancy.
        pub fn walFsyncCount(self: *Store) u64 {
            return if (self.wal_writer) |*w| w.fsyncCount() else 0;
        }

        /// Clears the read-only flag so the store accepts commits. Fails with
        /// `error.ReplicaStillStreaming` while a `Receiver` is attached and live — stop the
        /// receiver first, so a local commit can never interleave with streamed appends in
        /// the same WAL. Only call after the old leader is fenced — the engine ships no
        /// election or fencing of its own.
        pub fn promote(self: *Store) !void {
            if (self.replica_streaming.load(.acquire)) return error.ReplicaStillStreaming;
            self.read_only.store(false, .release);
        }

        /// Marks the store read-only; in-flight commits finish, new ones fail with
        /// `error.ReadOnlyReplica`.
        pub fn demote(self: *Store) void {
            self.read_only.store(true, .release);
        }

        /// Installs (or clears with null) the quorum gate `commit` blocks on after
        /// durability. For synchronous replication pass `syncGate()` here and in
        /// `HubConfig.commit_gate`.
        pub fn setCommitGate(self: *Store, gate: ?*replication.CommitGate) void {
            self.commit_gate.store(gate, .release);
        }

        /// The store-owned quorum gate. Its lifetime is the store's, so a commit can never
        /// outlive it the way it could a hub-owned gate: wire it into both
        /// `HubConfig.commit_gate` (the hub advances and closes it) and `setCommitGate`
        /// (commits wait on it). After `Hub.stop`, waiting commits fail with
        /// `error.ReplicationStopped`; a new hub re-arms the gate.
        pub fn syncGate(self: *Store) *replication.CommitGate {
            return &self.sync_gate;
        }

        /// Registers the deferred-response durability listener on both the WAL and the
        /// store-owned quorum gate — the doorbell rung when durability or quorum advances (or a
        /// flush fails). The async server layer passes a notifier that wakes every reactor. The
        /// gate object persists across hub restarts, so registering once at boot is sufficient.
        pub fn setDeferredNotifier(self: *Store, n: wal.DurabilityNotifier) void {
            if (self.wal_writer) |*w| w.setNotifier(n);
            self.sync_gate.setNotifier(n);
        }

        /// Removes the deferred-response listener from the WAL and gate under their locks — the
        /// teardown barrier the server layer calls after its reactors stop and before they (the
        /// listener's target) are freed, so a late flusher/hub fire can never touch freed memory.
        pub fn clearDeferredNotifier(self: *Store) void {
            if (self.wal_writer) |*w| w.clearNotifier();
            self.sync_gate.clearNotifier();
        }

        /// The published (lock-free) durable WAL watermark for a parked commit's release check.
        pub fn durableSeqPublished(self: *Store) u64 {
            return if (self.wal_writer) |*w| w.durableSeqPublished() else 0;
        }

        /// Whether the WAL has permanently failed a flush — a parked commit past the failure
        /// boundary must be failed, never released as durable.
        pub fn flushFailedPublished(self: *Store) bool {
            return if (self.wal_writer) |*w| w.flushFailedPublished() else false;
        }

        /// The published quorum watermark of the store-owned gate (0 when no quorum has acked).
        pub fn quorumWatermarkPublished(self: *Store) u64 {
            return self.sync_gate.watermarkPublished();
        }

        /// Whether the store-owned quorum gate is closed (replication stopped) — a parked money
        /// op that owes quorum must be failed so its client retries once replication returns.
        pub fn quorumClosedPublished(self: *Store) bool {
            return self.sync_gate.closedPublished();
        }

        /// The `replication.PrimaryHost` view of this store for `Hub.start`: its WAL
        /// writer, data directory, and snapshot host (so the hub can serve base backups).
        /// Fails with `error.WalDisabled` when the store opened without a WAL. The interior
        /// WAL pointer is stable — the store is heap-allocated.
        pub fn primaryHost(self: *Store) !replication.PrimaryHost {
            if (self.wal_writer == null) return error.WalDisabled;
            return .{
                .wal = &self.wal_writer.?,
                .data_dir = self.config.data_dir,
                .snapshot_host = self.snapshotHost(),
            };
        }

        /// The `replication.ReplicaHost` view of this store for `Receiver.start`: the WAL
        /// writer, the apply-ordering state, the read-only flag, the page-cache pressure valve,
        /// and the consumer's `apply_entry` hook (same shape as `recover`'s). Fails with
        /// `error.WalDisabled` when the store opened without a WAL.
        pub fn replicaHost(
            self: *Store,
            ctx: *anyopaque,
            apply_entry: *const fn (ctx: *anyopaque, entry: ReplayEntry) anyerror!void,
        ) !replication.ReplicaHost {
            if (self.wal_writer == null) return error.WalDisabled;
            return .{
                .wal = &self.wal_writer.?,
                .apply_mutex = &self.apply_mutex,
                .apply_cond = &self.apply_cond,
                .last_applied_seq = &self.last_applied_seq,
                .read_only = &self.read_only,
                .streaming = &self.replica_streaming,
                .store_ctx = self,
                .relieve_pressure = replicaRelievePressure,
                .ctx = ctx,
                .apply_entry = apply_entry,
            };
        }

        fn replicaRelievePressure(store_ctx: *anyopaque) anyerror!void {
            const self: *Store = @ptrCast(@alignCast(store_ctx));
            try self.relieveDirtyPressure();
        }

        fn snapshotCheckpointLocked(ctx: *anyopaque) anyerror!u64 {
            const self: *Store = @ptrCast(@alignCast(ctx));
            return self.checkpointLocked();
        }

        fn snapshotPageCount(ctx: *anyopaque) u64 {
            const self: *Store = @ptrCast(@alignCast(ctx));
            return self.header.page_count;
        }

        fn assertIndex(comptime name: [:0]const u8) void {
            if (indexOfName(s.indexes, name) == null)
                @compileError("no index named '" ++ name ++ "' in this schema");
        }

        fn assertMemtable(comptime name: [:0]const u8) void {
            for (s.memtable_indexes) |m| {
                if (std.mem.eql(u8, m, name)) return;
            }
            @compileError("no memtable index named '" ++ name ++ "' in this schema");
        }

        fn assertCounter(comptime name: [:0]const u8) void {
            for (s.counters) |c| {
                if (std.mem.eql(u8, c, name)) return;
            }
            @compileError("no counter named '" ++ name ++ "' in this schema");
        }
    };
}

const test_schema = schema(.{
    .magic = 0x5A494753,
    .format_version = 1,
    .indexes = .{
        .{ .name = "by_id", .key = .u64 },
        .{ .name = "by_parent_child", .key = .{ .composite = &.{ "parent_id", "child_id" } } },
        .{ .name = "by_slug", .key = .bytes },
    },
    .memtable_indexes = &.{ "by_id", "by_parent_child" },
    .counters = &.{ "next_id", "next_seq" },
});

const TestStore = Engine(test_schema);

fn openTestStore(allocator: std.mem.Allocator, tmp: *std.testing.TmpDir) !*TestStore {
    const path = try tmp.dir.realpathAlloc(allocator, ".");
    errdefer allocator.free(path);
    return TestStore.init(allocator, .{ .data_dir = path, .cache_size_mb = 16 });
}

fn openTestStoreSized(allocator: std.mem.Allocator, tmp: *std.testing.TmpDir, cache_size_mb: u32) !*TestStore {
    const path = try tmp.dir.realpathAlloc(allocator, ".");
    errdefer allocator.free(path);
    return TestStore.init(allocator, .{ .data_dir = path, .cache_size_mb = cache_size_mb });
}

fn closeTestStore(store: *TestStore) void {
    const allocator = store.allocator;
    const data_dir = store.config.data_dir;
    store.deinit();
    allocator.free(data_dir);
}

fn crashClose(store: *TestStore) void {
    const allocator = store.allocator;
    const data_dir = store.config.data_dir;
    if (store.wal_writer) |*w| w.deinit();
    inline for (test_schema.memtable_indexes) |name| store.memtable(name).deinit();
    store.cache.deinit();
    store.journal.close();
    store.file.close();
    allocator.destroy(store);
    allocator.free(data_dir);
}

const TestRecord = struct { id: u64, value: []const u8 = "v" };

fn serializeTestRecord(allocator: std.mem.Allocator, rec: TestRecord) anyerror![]u8 {
    const buf = try allocator.alloc(u8, 8 + rec.value.len);
    std.mem.writeInt(u64, buf[0..8], rec.id, .big);
    @memcpy(buf[8..], rec.value);
    return buf;
}

fn applyTestRecord(ctx: *anyopaque, rec: TestRecord) anyerror!void {
    const store: *TestStore = @ptrCast(@alignCast(ctx));
    try store.tree("by_id").insert(&codec.encodeU64(rec.id), rec.value);
}

fn replayTestRecord(ctx: *anyopaque, entry: ReplayEntry) anyerror!void {
    const store: *TestStore = @ptrCast(@alignCast(ctx));
    try store.tree("by_id").insert(entry.data[0..8], entry.data[8..]);
}

fn commitKey(store: *TestStore, id: u64) !u64 {
    return commit.commitSeq(TestRecord, store, 1, .{ .id = id }, store, serializeTestRecord, applyTestRecord, .{});
}

fn commitKeyUnawaited(store: *TestStore, id: u64, value: []const u8) !void {
    var completion = commit.CommitCompletion{};
    _ = try commit.commitSeq(TestRecord, store, 1, .{ .id = id, .value = value }, store, serializeTestRecord, applyTestRecord, .{ .completion = &completion });
}

fn expectKeyPresent(store: *TestStore, id: u64, present: bool) !void {
    var buf: [1024]u8 = undefined;
    const found = try store.tree("by_id").search(&codec.encodeU64(id), &buf);
    try std.testing.expectEqual(present, found != null);
}

fn walIsEmpty(dir: []const u8) !bool {
    var applier = CountingApplier{};
    const last = try wal_replay.replayWal(dir, 0, &applier);
    return last == 0 and applier.count == 0;
}

fn noopHook(ctx: *anyopaque) anyerror!void {
    _ = ctx;
}

const CountingApplier = struct {
    count: usize = 0,

    pub fn apply(self: *CountingApplier, entry: ReplayEntry) !void {
        _ = entry;
        self.count += 1;
    }
};

test "Header carries magic, generic slots, a root/count per index, and is PAGE_SIZE" {
    try std.testing.expect(@hasField(TestStore.Header, "magic"));
    try std.testing.expect(@hasField(TestStore.Header, "seq"));
    try std.testing.expect(@hasField(TestStore.Header, "by_id_root"));
    try std.testing.expect(@hasField(TestStore.Header, "by_id_count"));
    try std.testing.expect(@hasField(TestStore.Header, "by_slug_root"));
    try std.testing.expect(@hasField(TestStore.Header, "next_seq"));
    try std.testing.expectEqual(@as(usize, page.PAGE_SIZE), @sizeOf(TestStore.Header));
}

test "paged store: insert, checkpoint, reopen, search round-trips through page 0" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const store = try openTestStore(allocator, &tmp);
        defer closeTestStore(store);

        const id = store.nextId("next_id");
        try std.testing.expectEqual(@as(u64, 1), id);

        try store.tree("by_id").insert(&codec.encodeU64(id), "hello");
        _ = try store.checkpoint();
    }

    {
        const store = try openTestStore(allocator, &tmp);
        defer closeTestStore(store);

        try std.testing.expectEqual(@as(u64, 1), store.header.next_id);
        try std.testing.expect(store.header.by_id_count >= 1);

        var buf: [64]u8 = undefined;
        const found = try store.tree("by_id").search(&codec.encodeU64(1), &buf);
        try std.testing.expect(found != null);
        try std.testing.expectEqualSlices(u8, "hello", found.?);
    }
}

test "paged store: a split (moved) root survives checkpoint, reopen, and search" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const total: u64 = 2000;
    var entry_count: u64 = undefined;

    {
        const store = try openTestStore(allocator, &tmp);
        defer closeTestStore(store);

        const initial_root = store.tree("by_id").getRootPage();
        var moved = false;
        var i: u64 = 1;
        while (i <= total) : (i += 1) {
            try store.tree("by_id").insert(&codec.encodeU64(i), "v");
            if (store.tree("by_id").getRootPage() != initial_root) moved = true;
        }
        try std.testing.expect(moved);

        entry_count = store.tree("by_id").entryCount();
        _ = try store.checkpoint();
    }

    {
        const store = try openTestStore(allocator, &tmp);
        defer closeTestStore(store);

        try std.testing.expectEqual(entry_count, store.tree("by_id").entryCount());

        var buf: [64]u8 = undefined;
        var i: u64 = 1;
        while (i <= total) : (i += 1) {
            const found = try store.tree("by_id").search(&codec.encodeU64(i), &buf);
            try std.testing.expect(found != null);
        }
    }
}

test "paged store: memtable drain lands rows in the backing tree" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const store = try openTestStore(allocator, &tmp);
    defer closeTestStore(store);

    try store.memtable("by_id").put(&codec.encodeU64(7), "seven");
    try store.drainMemtables();

    var buf: [64]u8 = undefined;
    const found = try store.tree("by_id").search(&codec.encodeU64(7), &buf);
    try std.testing.expect(found != null);
    try std.testing.expectEqualSlices(u8, "seven", found.?);
}

test "counters persist in the header and allocate from 1" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const store = try openTestStore(allocator, &tmp);
        defer closeTestStore(store);
        try std.testing.expectEqual(@as(u64, 1), store.nextId("next_id"));
        try std.testing.expectEqual(@as(u64, 2), store.nextId("next_id"));
        store.counter("next_seq").* = 99;
        _ = try store.checkpoint();
    }

    {
        const store = try openTestStore(allocator, &tmp);
        defer closeTestStore(store);
        try std.testing.expectEqual(@as(u64, 2), store.header.next_id);
        try std.testing.expectEqual(@as(u64, 99), store.header.next_seq);
    }
}

const RecoverProbe = struct {
    applied: usize = 0,
    on_replayed_calls: usize = 0,
    bootstrap_calls: usize = 0,

    fn applyEntry(ctx: *anyopaque, entry: ReplayEntry) anyerror!void {
        _ = entry;
        const self: *RecoverProbe = @ptrCast(@alignCast(ctx));
        self.applied += 1;
    }
    fn onReplayed(ctx: *anyopaque) anyerror!void {
        const self: *RecoverProbe = @ptrCast(@alignCast(ctx));
        self.on_replayed_calls += 1;
    }
    fn bootstrap(ctx: *anyopaque) anyerror!void {
        const self: *RecoverProbe = @ptrCast(@alignCast(ctx));
        self.bootstrap_calls += 1;
    }
};

test "recover: applies each WAL entry once, replays-hook once, bootstraps only when empty" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(dir);

    {
        const op_code: u8 = 1;
        var w = try wal.WalWriter.init(allocator, dir, 32, 0, true);
        defer w.deinit();
        _ = try w.append(op_code, "one");
        _ = try w.append(op_code, "two");
        _ = try w.append(op_code, "three");
        try w.sync();
    }

    const store = try openTestStore(allocator, &tmp);
    defer closeTestStore(store);

    var probe = RecoverProbe{};
    try store.recover(&probe, .{
        .apply_entry = RecoverProbe.applyEntry,
        .on_replayed = RecoverProbe.onReplayed,
        .bootstrap = RecoverProbe.bootstrap,
    });

    try std.testing.expectEqual(@as(usize, 3), probe.applied);
    try std.testing.expectEqual(@as(usize, 1), probe.on_replayed_calls);
    try std.testing.expectEqual(@as(usize, 1), probe.bootstrap_calls);
}

test "recover: cold start with no WAL applies nothing, replays once, bootstraps once" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const store = try openTestStore(allocator, &tmp);
    defer closeTestStore(store);

    var probe = RecoverProbe{};
    try store.recover(&probe, .{
        .apply_entry = RecoverProbe.applyEntry,
        .on_replayed = RecoverProbe.onReplayed,
        .bootstrap = RecoverProbe.bootstrap,
    });

    try std.testing.expectEqual(@as(usize, 0), probe.applied);
    try std.testing.expectEqual(@as(usize, 1), probe.on_replayed_calls);
    try std.testing.expectEqual(@as(usize, 1), probe.bootstrap_calls);
}

const TickProbe = struct {
    count: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    fn tick(ctx: *anyopaque) anyerror!void {
        const self: *TickProbe = @ptrCast(@alignCast(ctx));
        _ = self.count.fetchAdd(1, .monotonic);
    }
};

test "spawnWorker: ticks on the interval and stops cleanly without leaking" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const store = try openTestStore(allocator, &tmp);
    defer closeTestStore(store);

    var probe = TickProbe{};
    const worker = try store.spawnWorker(&probe, .{ .interval_ns = 1_000_000, .tick = TickProbe.tick });

    while (probe.count.load(.monotonic) < 1) {
        std.Thread.yield() catch {};
    }
    worker.stop();

    try std.testing.expect(probe.count.load(.monotonic) >= 1);
}

test "reopen resumes the WAL sequence from snapshot metadata after truncation" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const store = try openTestStore(allocator, &tmp);
        defer closeTestStore(store);

        _ = try commitKey(store, 1);
        _ = try commitKey(store, 2);
        _ = try commitKey(store, 3);

        const result = try snapshot.forceSnapshot(store.snapshotHost());
        try std.testing.expectEqual(@as(u64, 3), result.wal_sequence);
        try std.testing.expect(try walIsEmpty(store.config.data_dir));
    }

    {
        const store = try openTestStore(allocator, &tmp);
        defer closeTestStore(store);
        try std.testing.expectEqual(@as(u64, 3), store.wal_writer.?.getSequence());
    }
}

test "healthStatus reports the read-only flag and applied/durable LSNs" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const store = try openTestStore(allocator, &tmp);
    defer closeTestStore(store);

    const fresh = store.healthStatus();
    try std.testing.expect(!fresh.read_only);
    try std.testing.expectEqual(@as(u64, 0), fresh.last_applied_lsn);
    try std.testing.expectEqual(@as(u64, 0), fresh.durable_lsn);

    _ = try commitKey(store, 1);
    _ = try commitKey(store, 2);

    store.demote();
    const demoted = store.healthStatus();
    try std.testing.expect(demoted.read_only);
    try std.testing.expectEqual(@as(u64, 2), demoted.durable_lsn);
    try std.testing.expectEqual(@as(u64, 2), demoted.last_applied_lsn);
}

test "header layout is byte-stable" {
    const Header = TestStore.Header;
    try std.testing.expectEqual(@as(usize, page.PAGE_SIZE), @sizeOf(Header));
    try std.testing.expectEqual(@as(usize, 0), @offsetOf(Header, "magic"));
    try std.testing.expectEqual(@as(usize, 4), @offsetOf(Header, "format_version"));
    try std.testing.expectEqual(@as(usize, 8), @offsetOf(Header, "page_size"));
    try std.testing.expectEqual(@as(usize, 12), @offsetOf(Header, "_pad"));
    try std.testing.expectEqual(@as(usize, 16), @offsetOf(Header, "free_list_head"));
    try std.testing.expectEqual(@as(usize, 24), @offsetOf(Header, "page_count"));
    try std.testing.expectEqual(@as(usize, 32), @offsetOf(Header, "seq"));
    try std.testing.expectEqual(@as(usize, 40), @offsetOf(Header, "by_id_root"));
    try std.testing.expectEqual(comptime headerPrefixSize(test_schema), @offsetOf(Header, "_reserved"));
}

test "crash after a root split and before a checkpoint still finds every pre-split key" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const pre_split: u64 = 100;
    var last_inserted: u64 = pre_split;

    {
        const store = try openTestStore(allocator, &tmp);
        var i: u64 = 1;
        while (i <= pre_split) : (i += 1) try store.tree("by_id").insert(&codec.encodeU64(i), "v");
        _ = try store.checkpoint();

        const initial_root = store.tree("by_id").getRootPage();
        while (store.tree("by_id").getRootPage() == initial_root) {
            last_inserted += 1;
            try store.tree("by_id").insert(&codec.encodeU64(last_inserted), "v");
        }
        crashClose(store);
    }

    {
        const store = try openTestStore(allocator, &tmp);
        defer closeTestStore(store);

        var i: u64 = 1;
        while (i <= pre_split) : (i += 1) try expectKeyPresent(store, i, true);
        try expectKeyPresent(store, last_inserted, false);
        try std.testing.expectEqual(pre_split, store.tree("by_id").entryCount());
    }
}

test "a checkpoint interrupted after its journal is durable completes on reopen" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const first_batch: u64 = 50;
    const total: u64 = 100;

    {
        const store = try openTestStore(allocator, &tmp);
        var i: u64 = 1;
        while (i <= first_batch) : (i += 1) _ = try commitKey(store, i);
        try std.testing.expectEqual(first_batch, try store.checkpoint());

        var pre_checkpoint_header: [page.PAGE_SIZE]u8 = undefined;
        try std.testing.expectEqual(@as(usize, page.PAGE_SIZE), try store.file.preadAll(&pre_checkpoint_header, 0));

        while (i <= total) : (i += 1) _ = try commitKey(store, i);

        const header_bytes = store.serializeHeaderLocked(store.wal_writer.?.getSequence());
        try store.cache.checkpoint(store.journal, &header_bytes);

        try store.file.pwriteAll(&pre_checkpoint_header, 0);
        try store.file.sync();
        crashClose(store);
    }

    {
        const store = try openTestStore(allocator, &tmp);
        defer closeTestStore(store);

        try std.testing.expectEqual(total, store.header.seq);
        var i: u64 = 1;
        while (i <= total) : (i += 1) try expectKeyPresent(store, i, true);
        try std.testing.expectEqual(@as(u64, 0), try store.journal.getEndPos());
    }
}

const ConcurrentCommitter = struct {
    store: *TestStore,
    base: u64,
    count: u64,
    failed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    fn run(self: *ConcurrentCommitter) void {
        var i: u64 = 0;
        while (i < self.count) : (i += 1) {
            _ = commitKey(self.store, self.base + i) catch {
                self.failed.store(true, .release);
                return;
            };
        }
    }
};

const CheckpointLooper = struct {
    store: *TestStore,
    stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    checkpoints: u64 = 0,
    failed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    fn run(self: *CheckpointLooper) void {
        while (!self.stop.load(.acquire)) {
            _ = self.store.checkpoint() catch {
                self.failed.store(true, .release);
                return;
            };
            self.checkpoints += 1;
        }
    }
};

test "every committed key survives a crash during concurrent checkpoints" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const per_thread: u64 = 500;
    const bases = [_]u64{ 0, 10_000, 20_000, 30_000 };

    {
        const store = try openTestStore(allocator, &tmp);

        var committers: [bases.len]ConcurrentCommitter = undefined;
        for (&committers, bases) |*c, base| c.* = .{ .store = store, .base = base, .count = per_thread };
        var looper = CheckpointLooper{ .store = store };

        const looper_thread = try std.Thread.spawn(.{}, CheckpointLooper.run, .{&looper});
        var threads: [bases.len]std.Thread = undefined;
        for (&threads, &committers) |*t, *c| t.* = try std.Thread.spawn(.{}, ConcurrentCommitter.run, .{c});
        for (threads) |t| t.join();
        looper.stop.store(true, .release);
        looper_thread.join();

        for (committers) |c| try std.testing.expect(!c.failed.load(.acquire));
        try std.testing.expect(!looper.failed.load(.acquire));
        try std.testing.expect(looper.checkpoints >= 1);
        crashClose(store);
    }

    {
        const store = try openTestStore(allocator, &tmp);
        defer closeTestStore(store);

        try store.recover(store, .{
            .apply_entry = replayTestRecord,
            .on_replayed = noopHook,
            .bootstrap = noopHook,
        });

        for (bases) |base| {
            var i: u64 = 0;
            while (i < per_thread) : (i += 1) try expectKeyPresent(store, base + i, true);
        }
        try std.testing.expectEqual(bases.len * per_thread, store.tree("by_id").entryCount());
    }
}

test "recovery with a page cache smaller than the replayed data succeeds" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const total: u64 = 20_000;
    const value = [_]u8{'x'} ** 512;

    {
        const store = try openTestStoreSized(allocator, &tmp, 8);
        var i: u64 = 1;
        while (i <= total) : (i += 1) try commitKeyUnawaited(store, i, &value);
        try store.wal_writer.?.sync();
        crashClose(store);
    }

    {
        const store = try openTestStoreSized(allocator, &tmp, 8);
        defer closeTestStore(store);

        try store.recover(store, .{
            .apply_entry = replayTestRecord,
            .on_replayed = noopHook,
            .bootstrap = noopHook,
        });

        try std.testing.expectEqual(total, store.header.seq);
        try std.testing.expect(try walIsEmpty(store.config.data_dir));
        var i: u64 = 1;
        while (i <= total) : (i += 997) try expectKeyPresent(store, i, true);
        try expectKeyPresent(store, total, true);
        try expectKeyPresent(store, total + 1, false);
    }
}

test "the WAL sequence resumes from header.seq after a checkpoint even without snapshot.meta" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const store = try openTestStore(allocator, &tmp);
        defer closeTestStore(store);
        _ = try commitKey(store, 1);
        _ = try commitKey(store, 2);
        _ = try commitKey(store, 3);
        try std.testing.expectEqual(@as(u64, 3), try store.checkpoint());
        try std.testing.expect(try walIsEmpty(store.config.data_dir));
    }

    try std.testing.expectError(error.FileNotFound, tmp.dir.access("snapshot.meta", .{}));

    {
        const store = try openTestStore(allocator, &tmp);
        defer closeTestStore(store);
        try std.testing.expectEqual(@as(u64, 3), store.header.seq);
        try std.testing.expectEqual(@as(u64, 3), store.wal_writer.?.getSequence());
        try std.testing.expectEqual(@as(u64, 4), try commitKey(store, 4));
    }
}

test "a store whose WAL cannot open refuses to start" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.makeDir("wal.bin");

    const path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(path);

    try std.testing.expectError(error.IsDir, TestStore.init(allocator, .{ .data_dir = path, .cache_size_mb = 16 }));
}

test "recover truncates the WAL after its post-replay checkpoint" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(dir);

    {
        var w = try wal.WalWriter.init(allocator, dir, 32, 0, true);
        defer w.deinit();
        var i: u64 = 1;
        while (i <= 5) : (i += 1) {
            const encoded = try serializeTestRecord(allocator, .{ .id = i });
            defer allocator.free(encoded);
            _ = try w.append(1, encoded);
        }
        try w.sync();
    }
    try std.testing.expect(!try walIsEmpty(dir));

    const store = try openTestStore(allocator, &tmp);
    defer closeTestStore(store);

    try store.recover(store, .{
        .apply_entry = replayTestRecord,
        .on_replayed = noopHook,
        .bootstrap = noopHook,
    });

    try std.testing.expectEqual(@as(u64, 5), store.header.seq);
    try std.testing.expect(try walIsEmpty(dir));
    try expectKeyPresent(store, 5, true);
}

test "a drain that fails midway keeps the newest value visible and the retry drains it" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const store = try openTestStore(allocator, &tmp);
    defer closeTestStore(store);

    const oversized = try allocator.alloc(u8, page.BODY_SIZE + 1);
    defer allocator.free(oversized);
    @memset(oversized, 'o');

    try store.memtable("by_id").put(&codec.encodeU64(1), "one");
    try store.memtable("by_id").put(&codec.encodeU64(2), oversized);
    try std.testing.expectError(error.ValueTooLarge, store.drainMemtables());

    try std.testing.expectEqualSlices(u8, "one", store.memtable("by_id").get(&codec.encodeU64(1)).found);
    try store.memtable("by_id").put(&codec.encodeU64(2), "two");
    try std.testing.expectEqualSlices(u8, "two", store.memtable("by_id").get(&codec.encodeU64(2)).found);

    try store.drainMemtables();

    var buf: [64]u8 = undefined;
    try std.testing.expectEqualSlices(u8, "one", (try store.tree("by_id").search(&codec.encodeU64(1), &buf)).?);
    try std.testing.expectEqualSlices(u8, "two", (try store.tree("by_id").search(&codec.encodeU64(2), &buf)).?);
    try std.testing.expect(store.memtable("by_id").get(&codec.encodeU64(2)) == .not_found);
}
