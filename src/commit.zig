//! The durable write path for a store: serialize a record, append it to the WAL, apply it to
//! the in-memory indexes in strict WAL-sequence order, and wait until the entry is durable
//! before returning.
//!
//! The engine names no application record type. The caller supplies the record type and the two
//! functions that encode it and apply it; the engine owns only the append, the monotonic apply
//! ordering, and the durability wait.

const std = @import("std");

/// Append `record` to the WAL under op code `op_code`, apply it in WAL-sequence order, and
/// return once it is durable.
///
/// `serialize_fn` encodes the record to a freshly-allocated buffer the engine frees. `apply_fn`
/// receives the caller's opaque `ctx` — the application context the engine never names — and
/// applies the record while the apply mutex is held, after every lower-sequence commit has
/// applied, so the in-memory state mutates in exact WAL order. Fails with `error.WalDisabled` if
/// the store has no WAL writer, and with `error.ReadOnlyReplica` on a demoted store (only the
/// replication receiver writes a replica's WAL). When a `CommitGate` is set on the store,
/// returns only after the configured quorum of followers has acked the entry durable, or fails
/// with `error.ReplicationStopped` once replication shuts down.
pub fn commit(
    comptime Record: type,
    store: anytype,
    op_code: u8,
    record: Record,
    ctx: *anyopaque,
    serialize_fn: *const fn (std.mem.Allocator, Record) anyerror![]u8,
    apply_fn: *const fn (ctx: *anyopaque, Record) anyerror!void,
) !void {
    if (store.read_only.load(.acquire)) return error.ReadOnlyReplica;

    const encoded = try serialize_fn(store.allocator, record);
    defer store.allocator.free(encoded);

    var seq: u64 = 0;
    if (store.wal_writer) |*w| {
        seq = try w.append(op_code, encoded);
    } else {
        return error.WalDisabled;
    }

    {
        store.apply_mutex.lock();
        defer store.apply_mutex.unlock();
        while (store.last_applied_seq + 1 < seq) {
            store.apply_cond.wait(&store.apply_mutex);
        }
        const apply_result = apply_fn(ctx, record);
        store.last_applied_seq = seq;
        store.apply_cond.broadcast();
        try apply_result;
    }

    if (store.wal_writer) |*w| {
        try w.awaitDurable(seq);
    }

    if (store.commit_gate.load(.acquire)) |gate| {
        try gate.awaitQuorum(seq);
    }
}

const engine = @import("engine.zig");

const test_schema = engine.schema(.{
    .magic = 0x5A434D54,
    .format_version = 1,
    .indexes = .{
        .{ .name = "by_id", .key = .u64 },
    },
    .memtable_indexes = &.{},
    .counters = &.{},
});

const TestStore = engine.Engine(test_schema);

const TestRecord = struct {
    id: u64,
    value: u8,
};

const Applied = struct {
    seqs: [256]u64 = undefined,
    count: usize = 0,
    mutex: std.Thread.Mutex = .{},

    fn record(self: *Applied, seq: u64) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.count < self.seqs.len) {
            self.seqs[self.count] = seq;
            self.count += 1;
        }
    }
};

var applied_log: Applied = .{};

fn serializeTestRecord(allocator: std.mem.Allocator, rec: TestRecord) anyerror![]u8 {
    const buf = try allocator.alloc(u8, 1);
    buf[0] = rec.value;
    return buf;
}

fn applyTestRecord(ctx: *anyopaque, rec: TestRecord) anyerror!void {
    const store: *TestStore = @ptrCast(@alignCast(ctx));
    applied_log.record(store.last_applied_seq + 1);
    _ = rec;
}

const Committer = struct {
    store: *TestStore,
    base: u64,
    n: usize,

    fn run(self: *Committer) void {
        var i: usize = 0;
        while (i < self.n) : (i += 1) {
            commit(TestRecord, self.store, 1, .{ .id = self.base + i, .value = 1 }, self.store, serializeTestRecord, applyTestRecord) catch {};
        }
    }
};

test "commit fails without a WAL writer" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(dir);

    const store = try TestStore.init(allocator, .{ .data_dir = dir, .cache_size_mb = 16 });
    defer store.deinit();

    if (store.wal_writer) |*w| w.deinit();
    store.wal_writer = null;

    try std.testing.expectError(error.WalDisabled, commit(TestRecord, store, 1, .{ .id = 1, .value = 1 }, store, serializeTestRecord, applyTestRecord));
}

test "concurrent commits apply in WAL-sequence order and advance last_applied_seq monotonically" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(dir);

    applied_log = .{};

    const store = try TestStore.init(allocator, .{ .data_dir = dir, .cache_size_mb = 16 });
    defer store.deinit();

    const per_thread = 50;
    var a = Committer{ .store = store, .base = 0, .n = per_thread };
    var b = Committer{ .store = store, .base = 1000, .n = per_thread };

    const ta = try std.Thread.spawn(.{}, Committer.run, .{&a});
    const tb = try std.Thread.spawn(.{}, Committer.run, .{&b});
    ta.join();
    tb.join();

    const total = 2 * per_thread;
    try std.testing.expectEqual(@as(u64, total), store.last_applied_seq);
    try std.testing.expectEqual(@as(usize, total), applied_log.count);

    for (applied_log.seqs[0..applied_log.count], 0..) |seq, i| {
        try std.testing.expectEqual(@as(u64, i + 1), seq);
    }
}
