//! A multi-sharded writer: partition an opaque byte keyspace across N independent `Store`
//! instances so writes to different shards proceed in parallel instead of serializing through a
//! single store's WAL-append lock, apply mutex, per-tree write lock, and free-list mutex. This
//! is the in-process realization of the sharding tier in `scale.md` — N shards are N independent
//! serialization domains, and aggregate write throughput rises with them.
//!
//! `ShardSet(Store)` composes over an already-generated `Engine(schema)` store type; it does not
//! regenerate the data plane. Each shard is a full `Store` rooted at `data_dir/shard-<i>/` with
//! its own `store.dat`, `wal.bin`, page cache, free list, and durable write pipeline. A write
//! routes by `Wyhash(key) % shard_count`; that hash is the only routing function, so the shard
//! that stores a key is the shard every read of that key must consult.
//!
//! Caller contract — the costs of horizontal write partitioning:
//!
//!   - **The routing key is the canonical key.** Route every read and write of a record through
//!     the same key bytes it is indexed under (`shardFor(key)`). Routing a write by one key and
//!     a later read by another sends them to different shards, and the write becomes invisible.
//!   - **`shard_count` is part of the on-disk contract.** It is recorded in `shardset.meta` under
//!     `data_dir`; reopening with a different count fails with `error.ShardCountMismatch` rather
//!     than silently re-routing every key.
//!   - **Counters are per-shard.** A `Store`'s `nextId` slots live in each shard's header, so ids
//!     minted by one shard are not globally unique. Never use a per-shard-minted id as a routing
//!     key; mint globally-unique ids outside the set (e.g. shard-stamped) if you need them.
//!   - **A commit is atomic only within its shard.** An operation spanning shards is not; colocate
//!     co-accessed records by routing them on a shared leading key component.
//!   - **Whole-keyspace range scans must merge the shards.** A range within one shard is
//!     unaffected; reach an individual shard through `shards` to scan or to wire its own
//!     replication/snapshots.

const std = @import("std");
const engine = @import("engine.zig");
const commitOne = @import("commit.zig").commit;

/// The routing-hash identifier recorded in `shardset.meta`. A reopen against a different
/// algorithm fails with `error.ShardHashMismatch`. Bump only if the routing hash changes — a
/// breaking on-disk event, since it re-partitions every existing key.
pub const ROUTING_HASH_WYHASH_V0: u32 = 0;

const META_MAGIC: u32 = 0x53484453;
const META_VERSION: u32 = 1;

const Meta = extern struct {
    magic: u32,
    version: u32,
    shard_count: u64,
    routing_hash: u32,
    _pad: u32 = 0,
};

fn routeHash(key: []const u8) u64 {
    return std.hash.Wyhash.hash(0, key);
}

/// Generate the multi-sharded writer over the comptime `Store` type produced by `Engine(schema)`.
///
/// The returned type exposes `Config`, `init`/`deinit`, deterministic routing (`shardIndexFor`,
/// `shardFor`), a convenience durable `commit`, and the fan-out maintenance/recovery seams
/// (`recoverAll`, `drainMemtablesAll`, `flushHeaderAll`, aggregate `healthStatus`). The `shards`
/// slice is public so a consumer can reach an individual `*Store` for per-shard work the set does
/// not fan out (replication, snapshots, single-shard range scans).
pub fn ShardSet(comptime Store: type) type {
    return struct {
        const Self = @This();

        /// One replayed WAL entry handed to a `recoverAll` hook — the engine's `ReplayEntry`.
        pub const ReplayEntry = engine.ReplayEntry;

        /// Open/create configuration. `shard_count` is durable: recorded in `shardset.meta` and
        /// validated on reopen. `cache_size_mb` is the **total** page-cache budget across all
        /// shards — each shard receives `cache_size_mb / shard_count`, so growing the shard count
        /// does not grow total memory. `wal_batch_size` is passed through to each shard's WAL.
        pub const Config = struct {
            data_dir: []const u8,
            shard_count: usize,
            cache_size_mb: u32 = 64,
            wal_batch_size: u32 = 32,
            wal_direct_io: bool = true,
        };

        /// The recovery hooks, identical in shape to `Store.recover`'s. Each hook receives the
        /// shard `*Store` being recovered as its `ctx`, so a sharded apply path applies an entry
        /// straight to the shard that owns it.
        pub const RecoverHooks = struct {
            apply_entry: *const fn (ctx: *anyopaque, entry: ReplayEntry) anyerror!void,
            on_replayed: *const fn (ctx: *anyopaque) anyerror!void,
            bootstrap: *const fn (ctx: *anyopaque) anyerror!void,
        };

        allocator: std.mem.Allocator,
        /// The shard stores, indexed `0..shard_count`. Reach an individual `*Store` here to run a
        /// single-shard range scan or to wire per-shard replication/snapshots.
        shards: []*Store,
        shard_paths: [][]u8,

        /// Open (or create) the shard set under `config.data_dir`. Creates the parent directory
        /// and a `shard-<i>` subdirectory per shard, opens a `Store` in each with an even split of
        /// the total cache budget, and records (or validates) `shard_count` in `shardset.meta`.
        /// Fails with `error.ShardCountMismatch`/`error.ShardHashMismatch` on a topology change,
        /// and tears down any shards already opened on a mid-fan-out failure. Returns a
        /// heap-allocated `*Self` at a stable address; release it with `deinit`.
        pub fn init(allocator: std.mem.Allocator, config: Config) !*Self {
            if (config.shard_count == 0) return error.InvalidShardCount;

            try std.fs.cwd().makePath(config.data_dir);
            try reconcileMeta(allocator, config.data_dir, config.shard_count);

            const self = try allocator.create(Self);
            errdefer allocator.destroy(self);

            const shards = try allocator.alloc(*Store, config.shard_count);
            errdefer allocator.free(shards);

            const shard_paths = try allocator.alloc([]u8, config.shard_count);
            errdefer allocator.free(shard_paths);

            const per_shard_cache = @max(config.cache_size_mb / @as(u32, @intCast(config.shard_count)), 1);

            var opened: usize = 0;
            errdefer {
                var j: usize = 0;
                while (j < opened) : (j += 1) {
                    shards[j].deinit();
                    allocator.free(shard_paths[j]);
                }
            }

            for (0..config.shard_count) |i| {
                const path = try std.fmt.allocPrint(allocator, "{s}/shard-{d}", .{ config.data_dir, i });
                shard_paths[i] = path;
                shards[i] = Store.init(allocator, .{
                    .data_dir = path,
                    .cache_size_mb = per_shard_cache,
                    .wal_batch_size = config.wal_batch_size,
                    .wal_direct_io = config.wal_direct_io,
                }) catch |err| {
                    allocator.free(path);
                    return err;
                };
                opened += 1;
            }

            self.* = .{
                .allocator = allocator,
                .shards = shards,
                .shard_paths = shard_paths,
            };
            return self;
        }

        /// Tear down every shard (draining, flushing, and closing each), then free the owned path
        /// strings and the shard slices and the set itself. Every shard is torn down even if an
        /// earlier one logs an error, so no flusher thread or file handle leaks. The path strings
        /// are freed only here, after the last `Store.deinit`, because each `Store` borrows its
        /// `data_dir` by reference for its whole lifetime.
        pub fn deinit(self: *Self) void {
            for (self.shards) |s| s.deinit();
            for (self.shard_paths) |p| self.allocator.free(p);
            self.allocator.free(self.shards);
            self.allocator.free(self.shard_paths);
            self.allocator.destroy(self);
        }

        /// The number of shards in the set.
        pub fn shardCount(self: *const Self) usize {
            return self.shards.len;
        }

        /// The shard index a key routes to: `Wyhash(key) % shard_count`. Deterministic, stateless,
        /// and safe to call concurrently — the set's shape never changes after `init`.
        pub fn shardIndexFor(self: *const Self, key: []const u8) usize {
            return @intCast(routeHash(key) % self.shards.len);
        }

        /// The shard `*Store` a key routes to. Route every read and write of a key through this so
        /// the key is always served by the shard that stores it.
        pub fn shardFor(self: *const Self, key: []const u8) *Store {
            return self.shards[self.shardIndexFor(key)];
        }

        /// Route `record` to the shard owning `key` and run the engine's durable write path there:
        /// serialize, WAL-append, apply in WAL-sequence order, and wait until durable. `key` MUST
        /// be the canonical key the record is stored and read under (see the module contract).
        /// `apply_fn` receives the destination shard `*Store` as its `ctx`. Different keys commit
        /// concurrently on different shards; same-key commits serialize within their shard.
        pub fn commit(
            self: *Self,
            comptime Record: type,
            key: []const u8,
            op_code: u8,
            record: Record,
            serialize_fn: *const fn (std.mem.Allocator, Record) anyerror![]u8,
            apply_fn: *const fn (ctx: *anyopaque, Record) anyerror!void,
        ) !void {
            const shard = self.shardFor(key);
            return commitOne(Record, shard, op_code, record, shard, serialize_fn, apply_fn);
        }

        /// Recover every shard from its own WAL, in shard order, serially. Each shard's `recover`
        /// is driven with that shard's `*Store` as the hook `ctx`, so the consumer's `apply_entry`
        /// applies an entry directly to the shard that owns it. Recovery is partition-correct: a
        /// key always routes to one shard, so a shard's WAL holds only its own keys and replays
        /// independently of the others — no global ordering is needed. Fail-closed: on the first
        /// shard that fails, the error propagates and recovery stops; the caller must then
        /// `deinit` the set and must not serve a partially-recovered set.
        pub fn recoverAll(self: *Self, hooks: RecoverHooks) !void {
            for (self.shards) |s| {
                try s.recover(s, .{
                    .apply_entry = hooks.apply_entry,
                    .on_replayed = hooks.on_replayed,
                    .bootstrap = hooks.bootstrap,
                });
            }
        }

        /// Drain every shard's write memtables into their backing trees, in shard order.
        pub fn drainMemtablesAll(self: *Self) !void {
            for (self.shards) |s| try s.drainMemtables();
        }

        /// Flush every shard's superblock to disk, in shard order.
        pub fn flushHeaderAll(self: *Self) !void {
            for (self.shards) |s| try s.flushHeader();
        }

        /// The set's health, reduced across shards for a worst-case readiness probe: `read_only`
        /// is true if **any** shard is read-only, and the LSNs are the **minimum** across shards
        /// (the laggard). Per-shard WAL sequence spaces are independent, so the aggregate LSNs are
        /// not globally monotonic — for true per-shard lag, inspect each `shards[i].healthStatus()`.
        pub fn healthStatus(self: *Self) engine.HealthStatus {
            var agg = engine.HealthStatus{
                .read_only = false,
                .last_applied_lsn = std.math.maxInt(u64),
                .durable_lsn = std.math.maxInt(u64),
            };
            for (self.shards) |s| {
                const h = s.healthStatus();
                if (h.read_only) agg.read_only = true;
                agg.last_applied_lsn = @min(agg.last_applied_lsn, h.last_applied_lsn);
                agg.durable_lsn = @min(agg.durable_lsn, h.durable_lsn);
            }
            return agg;
        }

        /// Whether the shards' WALs opened with `O_DIRECT` or fell back to buffered I/O. All shards
        /// share one `data_dir` and thus one filesystem, so shard 0 is representative.
        pub fn walUsingDirectIo(self: *Self) bool {
            return self.shards[0].walUsingDirectIo();
        }

        /// The total `fdatasync` count summed across every shard's WAL. Divided into the commit
        /// count it gives the set-wide mean group-commit batch occupancy.
        pub fn walFsyncCount(self: *Self) u64 {
            var total: u64 = 0;
            for (self.shards) |s| total += s.walFsyncCount();
            return total;
        }

        fn reconcileMeta(allocator: std.mem.Allocator, data_dir: []const u8, shard_count: usize) !void {
            const path = try std.fs.path.join(allocator, &.{ data_dir, "shardset.meta" });
            defer allocator.free(path);

            const file = std.fs.cwd().openFile(path, .{}) catch |err| switch (err) {
                error.FileNotFound => {
                    const meta = Meta{
                        .magic = META_MAGIC,
                        .version = META_VERSION,
                        .shard_count = shard_count,
                        .routing_hash = ROUTING_HASH_WYHASH_V0,
                    };
                    const created = try std.fs.cwd().createFile(path, .{});
                    defer created.close();
                    try created.writeAll(std.mem.asBytes(&meta));
                    try created.sync();
                    return;
                },
                else => return err,
            };
            defer file.close();

            var buf: [@sizeOf(Meta)]u8 = undefined;
            const n = try file.readAll(&buf);
            if (n < @sizeOf(Meta)) return error.ShardSetMetaCorrupt;

            const meta = std.mem.bytesToValue(Meta, &buf);
            if (meta.magic != META_MAGIC) return error.ShardSetMetaCorrupt;
            if (meta.routing_hash != ROUTING_HASH_WYHASH_V0) return error.ShardHashMismatch;
            if (meta.shard_count != shard_count) return error.ShardCountMismatch;
        }
    };
}

const codec = @import("codec.zig");

const test_schema = engine.schema(.{
    .magic = 0x53484454,
    .format_version = 1,
    .indexes = .{
        .{ .name = "by_key", .key = .bytes },
    },
    .memtable_indexes = &.{},
    .counters = &.{},
});

const TestStore = engine.Engine(test_schema);
const TestSet = ShardSet(TestStore);

const TestRecord = struct {
    key: u64,
    val: u64,
};

fn serializeTestRecord(allocator: std.mem.Allocator, rec: TestRecord) anyerror![]u8 {
    const buf = try allocator.alloc(u8, 16);
    std.mem.writeInt(u64, buf[0..8], rec.key, .big);
    std.mem.writeInt(u64, buf[8..16], rec.val, .big);
    return buf;
}

fn applyTestRecord(ctx: *anyopaque, rec: TestRecord) anyerror!void {
    const store: *TestStore = @ptrCast(@alignCast(ctx));
    try store.tree("by_key").insert(&codec.encodeU64(rec.key), &codec.encodeU64(rec.val));
}

fn openTestSet(allocator: std.mem.Allocator, dir: []const u8, shard_count: usize) !*TestSet {
    return TestSet.init(allocator, .{ .data_dir = dir, .shard_count = shard_count, .cache_size_mb = 16 });
}

fn commitTestKey(set: *TestSet, key: u64) !void {
    const kb = codec.encodeU64(key);
    try set.commit(TestRecord, &kb, 1, .{ .key = key, .val = key *% 7 +% 1 }, serializeTestRecord, applyTestRecord);
}

test "shardIndexFor is deterministic and within range" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(dir);

    const set = try openTestSet(allocator, dir, 4);
    defer set.deinit();

    var key: u64 = 0;
    while (key < 1000) : (key += 1) {
        const kb = codec.encodeU64(key);
        const idx = set.shardIndexFor(&kb);
        try std.testing.expect(idx < 4);
        try std.testing.expectEqual(idx, set.shardIndexFor(&kb));
        try std.testing.expectEqual(set.shards[idx], set.shardFor(&kb));
    }
}

test "a committed key is stored in exactly the shard it routes to" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(dir);

    const shard_count = 4;
    const set = try openTestSet(allocator, dir, shard_count);
    defer set.deinit();

    var key: u64 = 0;
    while (key < 400) : (key += 1) try commitTestKey(set, key);

    key = 0;
    while (key < 400) : (key += 1) {
        const kb = codec.encodeU64(key);
        const owner = set.shardIndexFor(&kb);
        for (set.shards, 0..) |s, i| {
            var buf: [16]u8 = undefined;
            const found = try s.tree("by_key").search(&kb, &buf);
            if (i == owner) {
                try std.testing.expect(found != null);
            } else {
                try std.testing.expect(found == null);
            }
        }
    }
}

test "reopen with the same shard_count preserves the partitioning and finds every key" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(dir);

    {
        const set = try openTestSet(allocator, dir, 4);
        defer set.deinit();
        var key: u64 = 0;
        while (key < 500) : (key += 1) try commitTestKey(set, key);
        try set.flushHeaderAll();
    }

    {
        const set = try openTestSet(allocator, dir, 4);
        defer set.deinit();
        var key: u64 = 0;
        while (key < 500) : (key += 1) {
            const kb = codec.encodeU64(key);
            var buf: [16]u8 = undefined;
            const found = try set.shardFor(&kb).tree("by_key").search(&kb, &buf);
            try std.testing.expect(found != null);
            try std.testing.expectEqual(key *% 7 +% 1, codec.decodeU64(found.?));
        }
    }
}

test "reopen with a different shard_count fails rather than re-routing keys" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(dir);

    {
        const set = try openTestSet(allocator, dir, 4);
        defer set.deinit();
        try commitTestKey(set, 1);
    }

    try std.testing.expectError(error.ShardCountMismatch, openTestSet(allocator, dir, 8));

    {
        const set = try openTestSet(allocator, dir, 4);
        defer set.deinit();
        const kb = codec.encodeU64(1);
        var buf: [16]u8 = undefined;
        try std.testing.expect((try set.shardFor(&kb).tree("by_key").search(&kb, &buf)) != null);
    }
}

test "init creates a not-yet-existing nested parent directory" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(root);

    const nested = try std.fmt.allocPrint(allocator, "{s}/a/b/c", .{root});
    defer allocator.free(nested);

    const set = try openTestSet(allocator, nested, 3);
    defer set.deinit();

    try commitTestKey(set, 42);
    const kb = codec.encodeU64(42);
    var buf: [16]u8 = undefined;
    try std.testing.expect((try set.shardFor(&kb).tree("by_key").search(&kb, &buf)) != null);
}

test "shard_count = 1 routes every key to the sole shard" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(dir);

    const set = try openTestSet(allocator, dir, 1);
    defer set.deinit();

    var key: u64 = 0;
    while (key < 100) : (key += 1) {
        const kb = codec.encodeU64(key);
        try std.testing.expectEqual(@as(usize, 0), set.shardIndexFor(&kb));
        try commitTestKey(set, key);
    }
    try std.testing.expectEqual(@as(u64, 100), set.shards[0].tree("by_key").entryCount());
}

test "init partial failure tears down opened shards without leaking" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(dir);

    var saw_failure = false;
    var fail_index: usize = 0;
    while (fail_index < 256) : (fail_index += 1) {
        var failing = std.testing.FailingAllocator.init(allocator, .{ .fail_index = fail_index });
        const set = TestSet.init(failing.allocator(), .{ .data_dir = dir, .shard_count = 4, .cache_size_mb = 16 }) catch {
            saw_failure = true;
            continue;
        };
        set.deinit();
        break;
    }
    try std.testing.expect(saw_failure);
}

test "recoverAll runs every shard and bootstraps each empty shard once" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(dir);

    const set = try openTestSet(allocator, dir, 3);
    defer set.deinit();

    const Probe = struct {
        var bootstraps: usize = 0;
        var replays: usize = 0;
        fn applyEntry(ctx: *anyopaque, entry: ReplayEntryArg) anyerror!void {
            _ = ctx;
            _ = entry;
        }
        fn onReplayed(ctx: *anyopaque) anyerror!void {
            _ = ctx;
            replays += 1;
        }
        fn bootstrap(ctx: *anyopaque) anyerror!void {
            _ = ctx;
            bootstraps += 1;
        }
        const ReplayEntryArg = TestSet.ReplayEntry;
    };

    Probe.bootstraps = 0;
    Probe.replays = 0;
    try set.recoverAll(.{
        .apply_entry = Probe.applyEntry,
        .on_replayed = Probe.onReplayed,
        .bootstrap = Probe.bootstrap,
    });

    try std.testing.expectEqual(@as(usize, 3), Probe.replays);
    try std.testing.expectEqual(@as(usize, 3), Probe.bootstraps);
}

test "healthStatus reduces read_only across shards with OR" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(dir);

    const set = try openTestSet(allocator, dir, 3);
    defer set.deinit();

    try std.testing.expect(!set.healthStatus().read_only);
    set.shards[1].demote();
    try std.testing.expect(set.healthStatus().read_only);
}

const ConcurrentCommitter = struct {
    set: *TestSet,
    base: u64,
    n: u64,

    fn run(self: *ConcurrentCommitter) void {
        var i: u64 = 0;
        while (i < self.n) : (i += 1) {
            commitTestKey(self.set, self.base + i) catch {};
        }
    }
};

test "concurrent commits across shards all land and stay findable" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(dir);

    const set = try openTestSet(allocator, dir, 4);
    defer set.deinit();

    const per_thread = 250;
    var a = ConcurrentCommitter{ .set = set, .base = 0, .n = per_thread };
    var b = ConcurrentCommitter{ .set = set, .base = 100_000, .n = per_thread };
    var c = ConcurrentCommitter{ .set = set, .base = 200_000, .n = per_thread };
    var d = ConcurrentCommitter{ .set = set, .base = 300_000, .n = per_thread };

    const ta = try std.Thread.spawn(.{}, ConcurrentCommitter.run, .{&a});
    const tb = try std.Thread.spawn(.{}, ConcurrentCommitter.run, .{&b});
    const tc = try std.Thread.spawn(.{}, ConcurrentCommitter.run, .{&c});
    const td = try std.Thread.spawn(.{}, ConcurrentCommitter.run, .{&d});
    ta.join();
    tb.join();
    tc.join();
    td.join();

    for ([_]u64{ 0, 100_000, 200_000, 300_000 }) |base| {
        var i: u64 = 0;
        while (i < per_thread) : (i += 1) {
            const key = base + i;
            const kb = codec.encodeU64(key);
            var buf: [16]u8 = undefined;
            try std.testing.expect((try set.shardFor(&kb).tree("by_key").search(&kb, &buf)) != null);
        }
    }

    var total: u64 = 0;
    for (set.shards) |s| total += s.tree("by_key").entryCount();
    try std.testing.expectEqual(@as(u64, 4 * per_thread), total);
}
