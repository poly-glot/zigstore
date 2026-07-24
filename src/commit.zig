//! The durable write path for a store: serialize a record, append it to the WAL, apply it to
//! the in-memory indexes in strict WAL-sequence order, and wait until the entry is durable
//! before returning.
//!
//! The engine names no application record type. The caller supplies the record type and the two
//! functions that encode it and apply it; the engine owns only the append, the monotonic apply
//! ordering, and the durability wait.

const std = @import("std");

/// Per-commit durability policy for `commitSeq`.
pub const CommitOptions = struct {
    /// When a `CommitGate` is installed on the store and this is true, the commit returns only
    /// after the gate's quorum has acked the entry durable (synchronous replication). Set false
    /// to return as soon as the entry is locally durable, skipping the quorum wait even while a
    /// gate is installed — for writes whose loss on an un-acked-leader crash carries no cost and
    /// must not pay the cross-node ack latency.
    await_quorum: bool = true,
    /// Upper bound on the quorum wait, in nanoseconds. 0 (the default) means no deadline — the
    /// historical unbounded behaviour. When exceeded, `commitSeq` fails with `error.QuorumTimeout`;
    /// the entry is already leader-durable and applied and will replicate once a follower returns,
    /// so the caller treats the timeout as retryable/degraded, never as data loss.
    quorum_timeout_ns: u64 = 0,
    /// When set, `commitSeq` appends and applies synchronously (identical ordering) but does NOT
    /// wait for durability or quorum — it fills the completion with the assigned seq and whether a
    /// quorum wait is owed, then returns. The caller (the async server layer) parks the response
    /// and releases it when the WAL's published durable watermark — and, if `needs_quorum`, the
    /// gate's published watermark — reach `seq`. Null (the default) is the historical synchronous
    /// path. The record is durable exactly as in the synchronous path (append happened); only the
    /// WAIT moves off the calling thread.
    completion: ?*CommitCompletion = null,
};

/// Filled by `commitSeq` when `CommitOptions.completion` is set: the assigned WAL sequence and
/// whether the entry still owes a quorum wait (a gate was installed and `await_quorum` was true).
/// The async server layer keys a parked response on `seq` and releases it once durability — and,
/// when `needs_quorum`, quorum — reaches it. Local durability must be satisfied before the quorum
/// watermark is consulted at release time, preserving the synchronous path's ordering.
pub const CommitCompletion = struct {
    seq: u64 = 0,
    needs_quorum: bool = false,
};

/// Append `record` to the WAL under op code `op_code`, apply it in WAL-sequence order, wait
/// until durable, and return the entry's assigned WAL sequence.
///
/// `serialize_fn` encodes the record to a freshly-allocated buffer the engine frees. `apply_fn`
/// receives the caller's opaque `ctx` — the application context the engine never names — and
/// applies the record while the apply mutex is held, after every lower-sequence commit has
/// applied, so the in-memory state mutates in exact WAL order. Fails with `error.WalDisabled` if
/// the store has no WAL writer, and with `error.ReadOnlyReplica` on a demoted store (only the
/// replication receiver writes a replica's WAL). When a `CommitGate` is set on the store and
/// `options.await_quorum` is true, returns only after the configured quorum of followers has
/// acked the entry durable, or fails with `error.ReplicationStopped` once replication shuts down;
/// when `options.await_quorum` is false the quorum wait is skipped (the entry is still locally
/// durable and still streamed to followers asynchronously).
///
/// The returned sequence is the durable — and, under a quorum-awaited commit, replicated — LSN
/// of this write: hand it back to a reader to fence a read-your-writes path, or await it on a
/// `CommitGate` out-of-band.
pub fn commitSeq(
    comptime Record: type,
    store: anytype,
    op_code: u8,
    record: Record,
    ctx: *anyopaque,
    serialize_fn: *const fn (std.mem.Allocator, Record) anyerror![]u8,
    apply_fn: *const fn (ctx: *anyopaque, Record) anyerror!void,
    options: CommitOptions,
) !u64 {
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

    if (options.completion) |completion| {
        completion.* = .{
            .seq = seq,
            .needs_quorum = options.await_quorum and store.commit_gate.load(.acquire) != null,
        };
        return seq;
    }

    if (store.wal_writer) |*w| {
        try w.awaitDurable(seq);
    }

    if (options.await_quorum) {
        if (store.commit_gate.load(.acquire)) |gate| {
            try gate.awaitQuorumTimeout(seq, options.quorum_timeout_ns);
        }
    }

    return seq;
}

/// Append, apply in WAL-sequence order, and return once durable (and quorum-acked when a
/// `CommitGate` is set). The fixed-policy form of `commitSeq` with default `CommitOptions`,
/// discarding the assigned sequence; existing consumers call this and its behaviour is unchanged.
pub fn commit(
    comptime Record: type,
    store: anytype,
    op_code: u8,
    record: Record,
    ctx: *anyopaque,
    serialize_fn: *const fn (std.mem.Allocator, Record) anyerror![]u8,
    apply_fn: *const fn (ctx: *anyopaque, Record) anyerror!void,
) !void {
    _ = try commitSeq(Record, store, op_code, record, ctx, serialize_fn, apply_fn, .{});
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

test "commitSeq returns the assigned monotonic WAL sequence" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(dir);

    const store = try TestStore.init(allocator, .{ .data_dir = dir, .cache_size_mb = 16 });
    defer store.deinit();

    const s1 = try commitSeq(TestRecord, store, 1, .{ .id = 1, .value = 1 }, store, serializeTestRecord, applyTestRecord, .{});
    const s2 = try commitSeq(TestRecord, store, 1, .{ .id = 2, .value = 1 }, store, serializeTestRecord, applyTestRecord, .{});

    try std.testing.expectEqual(@as(u64, 1), s1);
    try std.testing.expectEqual(@as(u64, 2), s2);
    try std.testing.expectEqual(s2, store.last_applied_seq);
}

test "commitSeq await_quorum=false skips an installed gate that commit blocks on" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(dir);

    const store = try TestStore.init(allocator, .{ .data_dir = dir, .cache_size_mb = 16 });
    defer store.deinit();

    const gate = store.syncGate();
    store.setCommitGate(gate);
    gate.close();

    try std.testing.expectError(error.ReplicationStopped, commit(TestRecord, store, 1, .{ .id = 1, .value = 1 }, store, serializeTestRecord, applyTestRecord));

    const relaxed_seq = try commitSeq(TestRecord, store, 1, .{ .id = 2, .value = 1 }, store, serializeTestRecord, applyTestRecord, .{ .await_quorum = false });
    try std.testing.expectEqual(store.last_applied_seq, relaxed_seq);
}

test "completion path: applies synchronously, returns without waiting, records the quorum need" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(dir);

    const store = try TestStore.init(allocator, .{ .data_dir = dir, .cache_size_mb = 16 });
    defer store.deinit();

    var c1 = CommitCompletion{};
    const s1 = try commitSeq(TestRecord, store, 1, .{ .id = 1, .value = 1 }, store, serializeTestRecord, applyTestRecord, .{ .completion = &c1 });
    try std.testing.expectEqual(s1, c1.seq);
    try std.testing.expectEqual(false, c1.needs_quorum);
    try std.testing.expectEqual(s1, store.last_applied_seq);

    const gate = store.syncGate();
    store.setCommitGate(gate);
    gate.close();

    var c2 = CommitCompletion{};
    const s2 = try commitSeq(TestRecord, store, 1, .{ .id = 2, .value = 1 }, store, serializeTestRecord, applyTestRecord, .{ .completion = &c2 });
    try std.testing.expectEqual(s2, c2.seq);
    try std.testing.expectEqual(true, c2.needs_quorum);
    try std.testing.expectEqual(s2, store.last_applied_seq);

    var c3 = CommitCompletion{};
    const s3 = try commitSeq(TestRecord, store, 1, .{ .id = 3, .value = 1 }, store, serializeTestRecord, applyTestRecord, .{ .completion = &c3, .await_quorum = false });
    try std.testing.expectEqual(false, c3.needs_quorum);
    try std.testing.expectEqual(s3, store.last_applied_seq);
}
