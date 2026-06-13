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
/// persisted-counter access via `counter(name)` / `nextId(name)`, durable `flushHeader`,
/// memtable draining via `drainMemtables`, and the `recover`/`spawnWorker` runtime seams. Tree,
/// memtable, and counter names are checked at comptime against the schema.
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

        /// Open (or create) the store under `config.data_dir`, returning a heap-allocated
        /// `*Store` at a stable address. On a fresh directory the header is formatted with the
        /// schema's identity and every index starts at an empty (lazily-allocated) root; on an
        /// existing one the header is loaded and validated and each tree is wired to the shared
        /// cache/free list at its persisted root and count. The store owns its own allocation:
        /// every tree's `cache`/`free_list` and the free list's `cache` are bound to the settled
        /// in-place pointers and the WAL flusher is started before returning, so the value never
        /// needs an external rebind. Release it with `deinit`.
        pub fn init(allocator: std.mem.Allocator, config: Config) !*Store {
            std.fs.makeDirAbsolute(config.data_dir) catch |err| switch (err) {
                error.PathAlreadyExists => {},
                else => return err,
            };

            const path = try std.fmt.allocPrint(allocator, "{s}/store.dat", .{config.data_dir});
            defer allocator.free(path);

            const file = try std.fs.createFileAbsolute(path, .{ .read = true, .truncate = false });
            errdefer file.close();

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
            var wal_writer = wal.WalWriter.init(allocator, config.data_dir, config.wal_batch_size, snapshot_base, config.wal_direct_io) catch |err| blk: {
                log.warn("WAL open failed: {} — continuing without WAL", .{err});
                break :blk null;
            };
            errdefer if (wal_writer) |*w| w.deinit();

            const self = try allocator.create(Store);
            errdefer allocator.destroy(self);

            self.* = Store{
                .allocator = allocator,
                .config = config,
                .file = file,
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

        /// Tear down the store the `init` heap-allocated: drain memtables, flush dirty pages and
        /// the header, close the WAL and data file, free every owned resource, and finally
        /// `destroy` the store allocation itself. Call once on the `*Store` returned by `init`;
        /// the pointer is invalid afterward.
        pub fn deinit(self: *Store) void {
            self.drainMemtables() catch |err| {
                log.err("deinit drain failed: {}", .{err});
            };
            self.cache.flushAll() catch |err| {
                log.err("deinit cache flush failed: {}", .{err});
            };
            self.flushHeader() catch |err| {
                log.err("deinit header flush failed: {}", .{err});
            };

            if (self.wal_writer) |*w| w.deinit();

            inline for (s.memtable_indexes) |name| {
                @field(self.memtables, name).deinit();
            }
            self.cache.deinit();
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
        /// mutation or with `flushHeader` (which reads the header under `header_lock`).
        pub fn counter(self: *Store, comptime name: [:0]const u8) *u64 {
            comptime assertCounter(name);
            return &@field(self.header, name);
        }

        /// Increment the named counter and return its new value. Allocates ids from `1`.
        /// Single-writer / caller-serialized: the read-modify-write is non-atomic, so the
        /// caller must not call it concurrently with another counter mutation or with
        /// `flushHeader`.
        pub fn nextId(self: *Store, comptime name: [:0]const u8) u64 {
            const slot = self.counter(name);
            slot.* += 1;
            return slot.*;
        }

        /// Drain every write memtable into its backing B+Tree: tombstones delete, live entries
        /// insert, applied in shard order. Serialized against concurrent drains.
        pub fn drainMemtables(self: *Store) !void {
            self.mt_drain_mutex.lock();
            defer self.mt_drain_mutex.unlock();
            inline for (s.memtable_indexes) |name| {
                try drainOne(&@field(self.memtables, name), &@field(self.trees, name));
            }
        }

        fn drainOne(mt: *MemTable, dst: *BPlusTree) !void {
            mt.lockAll();
            var backs: [MEMTABLE_SHARDS]*MemTable.Buffer = undefined;
            for (0..MEMTABLE_SHARDS) |i| backs[i] = mt.swapShardLocked(i);
            mt.unlockAll();

            for (0..MEMTABLE_SHARDS) |i| {
                var it = backs[i].map.iterator();
                while (it.next()) |entry| {
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

        /// Sync every tree's live root/count and the persisted counters into the header, then
        /// write the superblock to page 0 and fsync. Serialized by the header lock.
        pub fn flushHeader(self: *Store) !void {
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

            const header_bytes = file_header.serialize(self.header);
            try self.file.seekTo(0);
            try self.file.writeAll(&header_bytes);
            try self.file.sync();
        }

        /// Replay the WAL, drain, flush, and bootstrap — the engine owns the ordering, the
        /// caller owns the per-entry semantics. For each decoded entry past the checkpoint,
        /// `apply_entry(ctx, entry)` runs; then memtables drain and the header is flushed;
        /// then `on_replayed(ctx)` runs; and if the store opened empty, `bootstrap(ctx)` runs.
        /// The engine imports no application module — decoding and applying an entry lives
        /// entirely inside `apply_entry`.
        pub fn recover(self: *Store, ctx: *anyopaque, hooks: struct {
            apply_entry: *const fn (ctx: *anyopaque, entry: ReplayEntry) anyerror!void,
            on_replayed: *const fn (ctx: *anyopaque) anyerror!void,
            bootstrap: *const fn (ctx: *anyopaque) anyerror!void,
        }) !void {
            var applier = ReplayAdapter{ .ctx = ctx, .apply_entry = hooks.apply_entry };
            const last_seq = wal_replay.replayWal(self.config.data_dir, 0, &applier) catch |err| {
                log.err("WAL replay failed: {}", .{err});
                return err;
            };

            if (last_seq > 0) {
                try self.drainMemtables();
                self.cache.flushAll() catch |err| {
                    log.err("recover cache flush failed: {}", .{err});
                };
                try self.flushHeader();
            }

            try hooks.on_replayed(ctx);

            if (self.was_empty) try hooks.bootstrap(ctx);
        }

        const ReplayAdapter = struct {
            ctx: *anyopaque,
            apply_entry: *const fn (ctx: *anyopaque, entry: ReplayEntry) anyerror!void,

            pub fn apply(self: *ReplayAdapter, entry: ReplayEntry) !void {
                try self.apply_entry(self.ctx, entry);
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
        /// directory, the apply and drain locks, and the four function pointers the generic
        /// snapshot routine drives (WAL sequence, cache flush, header flush, page count). Pass
        /// the result to `snapshot.forceSnapshot`.
        pub fn snapshotHost(self: *Store) snapshot.SnapshotHost {
            return .{
                .snapshot_in_progress = &self.snapshot_in_progress,
                .data_dir = self.config.data_dir,
                .apply_mutex = &self.apply_mutex,
                .mt_drain_mutex = &self.mt_drain_mutex,
                .walSequence = snapshotWalSequence,
                .flushCache = snapshotFlushCache,
                .flushHeader = snapshotFlushHeader,
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
        /// writer, the apply-ordering state, the read-only flag, and the consumer's
        /// `apply_entry` hook (same shape as `recover`'s). Fails with `error.WalDisabled`
        /// when the store opened without a WAL.
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
                .ctx = ctx,
                .apply_entry = apply_entry,
            };
        }

        fn snapshotWalSequence(ctx: *anyopaque) u64 {
            const self: *Store = @ptrCast(@alignCast(ctx));
            return if (self.wal_writer) |*w| w.getSequence() else 0;
        }

        fn snapshotFlushCache(ctx: *anyopaque) anyerror!void {
            const self: *Store = @ptrCast(@alignCast(ctx));
            try self.cache.flushAll();
        }

        fn snapshotFlushHeader(ctx: *anyopaque) anyerror!void {
            const self: *Store = @ptrCast(@alignCast(ctx));
            try self.flushHeader();
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

fn closeTestStore(store: *TestStore) void {
    const allocator = store.allocator;
    const data_dir = store.config.data_dir;
    store.deinit();
    allocator.free(data_dir);
}

test "Header carries magic, generic slots, a root/count per index, and is PAGE_SIZE" {
    try std.testing.expect(@hasField(TestStore.Header, "magic"));
    try std.testing.expect(@hasField(TestStore.Header, "seq"));
    try std.testing.expect(@hasField(TestStore.Header, "by_id_root"));
    try std.testing.expect(@hasField(TestStore.Header, "by_id_count"));
    try std.testing.expect(@hasField(TestStore.Header, "by_slug_root"));
    try std.testing.expect(@hasField(TestStore.Header, "next_seq"));
    try std.testing.expectEqual(@as(usize, page.PAGE_SIZE), @sizeOf(TestStore.Header));
}

test "paged store: insert, flushHeader, reopen, search round-trips through page 0" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const store = try openTestStore(allocator, &tmp);
        defer closeTestStore(store);

        const id = store.nextId("next_id");
        try std.testing.expectEqual(@as(u64, 1), id);

        try store.tree("by_id").insert(&codec.encodeU64(id), "hello");
        try store.flushHeader();
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

test "paged store: a split (moved) root survives flushHeader, reopen, and search" {
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
        try store.flushHeader();
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
        try store.flushHeader();
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

        const w = &store.wal_writer.?;
        _ = try w.append(1, "a");
        _ = try w.append(1, "b");
        _ = try w.append(1, "c");
        try w.sync();

        _ = try snapshot.forceSnapshot(store.snapshotHost());
        try w.truncateAfterCheckpoint();
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

    const w = &store.wal_writer.?;
    _ = try w.append(1, "a");
    _ = try w.append(1, "b");
    try w.sync();

    store.demote();
    const demoted = store.healthStatus();
    try std.testing.expect(demoted.read_only);
    try std.testing.expectEqual(@as(u64, 2), demoted.durable_lsn);
}
