//! Point-in-time snapshot of a store's durable state: under the drain and apply locks the
//! host runs its atomic checkpoint (drain, journaled page + header write, WAL truncation),
//! then a `snapshot.meta` superblock recording the checkpoint's WAL sequence and page count
//! is written atomically (temp file + rename + directory fsync).
//!
//! The engine names no application type. A consumer drives a snapshot through `SnapshotHost`,
//! the small interface a generated `Store` satisfies via `Store.snapshotHost()`.

const std = @import("std");
const Io = std.Io;
const sys = @import("sys.zig");

/// The `snapshot.meta` magic: ASCII "SNAP".
pub const SNAP_MAGIC: u32 = 0x534E4150;

/// What a completed `forceSnapshot` reports: the WAL sequence the snapshot is consistent up to
/// and the wall-clock time the flush+write took.
pub const SnapshotResult = struct {
    wal_sequence: u64,
    duration_ms: u64,
};

/// The seam between the generic snapshot routine and a concrete store. The host hands over the
/// `Io` its locks and files run on, the in-progress guard, the data directory, the two locks
/// taken (drain, then apply) around the checkpoint, and two function pointers closing over the
/// store as `ctx`: `checkpointLocked` (the store's checkpoint body, run with both locks held;
/// returns the durable WAL sequence) and `pageCount`.
pub const SnapshotHost = struct {
    io: Io,
    snapshot_in_progress: *std.atomic.Value(bool),
    data_dir: []const u8,
    apply_mutex: *Io.Mutex,
    mt_drain_mutex: *Io.Mutex,
    checkpointLocked: *const fn (ctx: *anyopaque) anyerror!u64,
    pageCount: *const fn (ctx: *anyopaque) u64,
    ctx: *anyopaque,
};

/// Take a snapshot now, returning its WAL sequence and duration. Fails with
/// `error.SnapshotInProgress` if another snapshot holds the in-progress guard.
pub fn forceSnapshot(host: SnapshotHost) !SnapshotResult {
    return takeSnapshot(host, false);
}

/// The on-disk name of the consistent `store.dat` copy `forceBaseBackup` produces.
pub const BASE_BACKUP_FILE = "store.dat.base";

/// Like `forceSnapshot`, but additionally copies `store.dat` to `store.dat.base` while the
/// drain and apply locks are still held, so the copy is page-consistent at the returned WAL
/// sequence (pages mutate in place — an unlocked copy could tear). Serves a replica base
/// backup; the caller owns deleting the copy once consumed.
pub fn forceBaseBackup(host: SnapshotHost) !SnapshotResult {
    return takeSnapshot(host, true);
}

fn takeSnapshot(host: SnapshotHost, copy_base: bool) !SnapshotResult {
    if (host.snapshot_in_progress.cmpxchgStrong(false, true, .acq_rel, .acquire) != null) {
        return error.SnapshotInProgress;
    }
    defer host.snapshot_in_progress.store(false, .release);

    const started = Io.Clock.awake.now(host.io);

    var mgr = SnapshotManager.init(host.io, host.data_dir, 0);
    const wal_seq = try checkpointUnderLocks(host, copy_base);
    try mgr.writeMeta(host, wal_seq);

    const duration_ms: u64 = @intCast(started.untilNow(host.io, .awake).toMilliseconds());
    return .{ .wal_sequence = wal_seq, .duration_ms = duration_ms };
}

fn checkpointUnderLocks(host: SnapshotHost, copy_base: bool) !u64 {
    host.mt_drain_mutex.lockUncancelable(host.io);
    defer host.mt_drain_mutex.unlock(host.io);
    host.apply_mutex.lockUncancelable(host.io);
    defer host.apply_mutex.unlock(host.io);

    const wal_seq = try host.checkpointLocked(host.ctx);
    if (copy_base) try copyStoreToBase(host.io, host.data_dir);
    return wal_seq;
}

fn copyStoreToBase(io: Io, data_dir: []const u8) !void {
    const src = try Io.Dir.path.join(std.heap.page_allocator, &.{ data_dir, "store.dat" });
    defer std.heap.page_allocator.free(src);

    const dst = try Io.Dir.path.join(std.heap.page_allocator, &.{ data_dir, BASE_BACKUP_FILE });
    defer std.heap.page_allocator.free(dst);

    try Io.Dir.cwd().copyFile(src, Io.Dir.cwd(), dst, io, .{});

    const copy = try Io.Dir.cwd().openFile(io, dst, .{ .mode = .read_write });
    defer copy.close(io);
    try copy.sync(io);
}

/// `fsync` a directory so a rename, create, or delete inside it is durable. Failures are logged
/// at `warn`, never returned: the data write itself has already been fsynced by the caller.
pub fn fsyncDir(path: []const u8) void {
    const log = std.log.scoped(.snapshot);
    const flags: sys.OpenFlags = .{ .ACCMODE = .RDONLY, .DIRECTORY = true, .CLOEXEC = true };
    const fd = sys.open(path, flags, 0) catch |err| {
        log.warn("dir open failed for fsync: {}", .{err});
        return;
    };
    defer sys.close(fd);
    sys.fsync(fd) catch |err| {
        log.warn("dir fsync failed: {}", .{err});
    };
}

/// The `snapshot.meta` superblock: magic, version, the consistent WAL sequence, the wall-clock
/// timestamp, the page count, and a pad to 64 bytes.
pub const SnapshotHeader = extern struct {
    magic: u32 = SNAP_MAGIC,
    version: u32 = 1,
    wal_sequence: u64,
    timestamp: i64,
    page_count: u32,
    _reserved: [36]u8 = @splat(0),
};

comptime {
    if (@sizeOf(SnapshotHeader) != 64) @compileError("SnapshotHeader size mismatch");
}

const SNAP_HEADER_SIZE: usize = @sizeOf(SnapshotHeader);

/// Drives snapshot creation against a data directory, tracking the last-taken time so a
/// scheduler can honor a minimum interval.
pub const SnapshotManager = struct {
    io: Io,
    data_dir: []const u8,
    interval_s: u32,
    last_snapshot_time: i64,

    pub fn init(io: Io, data_dir: []const u8, interval_s: u32) SnapshotManager {
        return SnapshotManager{
            .io = io,
            .data_dir = data_dir,
            .interval_s = interval_s,
            .last_snapshot_time = 0,
        };
    }

    /// Whether at least `interval_s` seconds have elapsed since the last snapshot.
    pub fn shouldSnapshot(self: *const SnapshotManager) bool {
        const now = Io.Clock.real.now(self.io).toSeconds();
        return (now - self.last_snapshot_time) >= @as(i64, self.interval_s);
    }

    /// Checkpoint the host under the drain and apply locks, then write `snapshot.meta`
    /// atomically (temp file, rename, directory fsync) recording the checkpoint's WAL
    /// sequence and the host's current page count.
    pub fn createSnapshot(self: *SnapshotManager, host: SnapshotHost) !void {
        const wal_sequence = try checkpointUnderLocks(host, false);
        try self.writeMeta(host, wal_sequence);
    }

    fn writeMeta(self: *SnapshotManager, host: SnapshotHost, wal_sequence: u64) !void {
        const now = Io.Clock.real.now(self.io).toSeconds();

        const snap_header = SnapshotHeader{
            .wal_sequence = wal_sequence,
            .timestamp = now,
            .page_count = @intCast(host.pageCount(host.ctx)),
        };

        const tmp_path = try Io.Dir.path.join(std.heap.page_allocator, &.{ self.data_dir, "snapshot.meta.tmp" });
        defer std.heap.page_allocator.free(tmp_path);

        const final_path = try Io.Dir.path.join(std.heap.page_allocator, &.{ self.data_dir, "snapshot.meta" });
        defer std.heap.page_allocator.free(final_path);

        {
            const tmp_file = try Io.Dir.cwd().createFile(self.io, tmp_path, .{ .truncate = true });
            defer tmp_file.close(self.io);

            const header_bytes: *const [SNAP_HEADER_SIZE]u8 = @ptrCast(&snap_header);
            try tmp_file.writePositionalAll(self.io, header_bytes, 0);
            try tmp_file.sync(self.io);
        }

        try Io.Dir.cwd().rename(tmp_path, Io.Dir.cwd(), final_path, self.io);

        fsyncDir(self.data_dir);

        self.last_snapshot_time = now;
    }

    /// Read back `snapshot.meta` from `data_dir`, or `null` if none exists. Errors on a bad magic.
    pub fn loadSnapshotMeta(io: Io, data_dir: []const u8) !?SnapshotHeader {
        const path = try Io.Dir.path.join(std.heap.page_allocator, &.{ data_dir, "snapshot.meta" });
        defer std.heap.page_allocator.free(path);

        const file = Io.Dir.cwd().openFile(io, path, .{ .mode = .read_only }) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer file.close(io);

        var buf: [SNAP_HEADER_SIZE]u8 = undefined;
        const n = try file.readPositionalAll(io, &buf, 0);
        if (n < SNAP_HEADER_SIZE) return null;

        const snap = std.mem.bytesToValue(SnapshotHeader, &buf);

        if (snap.magic != SNAP_MAGIC) return error.InvalidSnapshotMagic;

        return snap;
    }

    /// The WAL sequence the last snapshot is consistent up to, or 0 if none exists.
    pub fn getWalSequence(io: Io, data_dir: []const u8) !u64 {
        const snap = try loadSnapshotMeta(io, data_dir);
        if (snap) |s| {
            return s.wal_sequence;
        }
        return 0;
    }
};

test "snapshot header size" {
    try std.testing.expectEqual(@as(usize, 64), @sizeOf(SnapshotHeader));
}

test "loadSnapshotMeta returns null for missing file" {
    const result = try SnapshotManager.loadSnapshotMeta(std.testing.io, "/tmp/snapshot_test_nonexistent_12345");
    try std.testing.expect(result == null);
}

fn resetTestDir(tmp_dir: []const u8) void {
    Io.Dir.cwd().deleteTree(std.testing.io, tmp_dir) catch {};
    Io.Dir.createDirAbsolute(std.testing.io, tmp_dir, .default_dir) catch {};
}

fn removeTestDir(tmp_dir: []const u8) void {
    Io.Dir.cwd().deleteTree(std.testing.io, tmp_dir) catch {};
}

fn writeTestMeta(tmp_dir: []const u8, snap_header: SnapshotHeader) !void {
    const path = try Io.Dir.path.join(std.testing.allocator, &.{ tmp_dir, "snapshot.meta" });
    defer std.testing.allocator.free(path);

    const file = try Io.Dir.cwd().createFile(std.testing.io, path, .{ .truncate = true });
    defer file.close(std.testing.io);
    const header_bytes: *const [SNAP_HEADER_SIZE]u8 = @ptrCast(&snap_header);
    try file.writePositionalAll(std.testing.io, header_bytes, 0);
}

test "create and load snapshot meta roundtrip" {
    const tmp_dir = "/tmp/snapshot_test_roundtrip";
    resetTestDir(tmp_dir);
    defer removeTestDir(tmp_dir);

    try writeTestMeta(tmp_dir, .{
        .magic = 0x534E4150,
        .version = 1,
        .wal_sequence = 42,
        .timestamp = 1700000000,
        .page_count = 100,
    });

    const loaded = (try SnapshotManager.loadSnapshotMeta(std.testing.io, tmp_dir)).?;
    try std.testing.expectEqual(@as(u32, 0x534E4150), loaded.magic);
    try std.testing.expectEqual(@as(u32, 1), loaded.version);
    try std.testing.expectEqual(@as(u64, 42), loaded.wal_sequence);
    try std.testing.expectEqual(@as(i64, 1700000000), loaded.timestamp);
    try std.testing.expectEqual(@as(u32, 100), loaded.page_count);
}

test "getWalSequence returns 0 when no snapshot exists" {
    const seq = try SnapshotManager.getWalSequence(std.testing.io, "/tmp/snapshot_test_nonexistent_67890");
    try std.testing.expectEqual(@as(u64, 0), seq);
}

test "getWalSequence returns stored sequence" {
    const tmp_dir = "/tmp/snapshot_test_getseq";
    resetTestDir(tmp_dir);
    defer removeTestDir(tmp_dir);

    try writeTestMeta(tmp_dir, .{
        .wal_sequence = 99,
        .timestamp = 1700000000,
        .page_count = 50,
    });

    const seq = try SnapshotManager.getWalSequence(std.testing.io, tmp_dir);
    try std.testing.expectEqual(@as(u64, 99), seq);
}

test "shouldSnapshot respects interval" {
    var mgr = SnapshotManager.init(std.testing.io, "/tmp", 300);

    try std.testing.expect(mgr.shouldSnapshot());

    mgr.last_snapshot_time = Io.Clock.real.now(std.testing.io).toSeconds();
    try std.testing.expect(!mgr.shouldSnapshot());
}

const SnapProbe = struct {
    checkpoint_calls: usize = 0,
    seq: u64,
    pages: u64,

    fn checkpointLocked(ctx: *anyopaque) anyerror!u64 {
        const self: *SnapProbe = @ptrCast(@alignCast(ctx));
        self.checkpoint_calls += 1;
        return self.seq;
    }
    fn pageCount(ctx: *anyopaque) u64 {
        const self: *SnapProbe = @ptrCast(@alignCast(ctx));
        return self.pages;
    }
};

test "forceSnapshot over a host checkpoints once, writes snapshot.meta, and records the checkpoint's seq/page_count" {
    const tmp_dir = "/tmp/snapshot_test_host";
    resetTestDir(tmp_dir);
    defer removeTestDir(tmp_dir);

    var in_progress = std.atomic.Value(bool).init(false);
    var apply_mutex: Io.Mutex = .init;
    var drain_mutex: Io.Mutex = .init;
    var probe = SnapProbe{ .seq = 7, .pages = 13 };

    const host = SnapshotHost{
        .io = std.testing.io,
        .snapshot_in_progress = &in_progress,
        .data_dir = tmp_dir,
        .apply_mutex = &apply_mutex,
        .mt_drain_mutex = &drain_mutex,
        .checkpointLocked = SnapProbe.checkpointLocked,
        .pageCount = SnapProbe.pageCount,
        .ctx = &probe,
    };

    const result = try forceSnapshot(host);
    try std.testing.expectEqual(@as(u64, 7), result.wal_sequence);
    try std.testing.expectEqual(@as(usize, 1), probe.checkpoint_calls);
    try std.testing.expect(!in_progress.load(.acquire));

    const loaded = (try SnapshotManager.loadSnapshotMeta(std.testing.io, tmp_dir)).?;
    try std.testing.expectEqual(@as(u64, 7), loaded.wal_sequence);
    try std.testing.expectEqual(@as(u32, 13), loaded.page_count);
}

test "forceSnapshot rejects a concurrent snapshot via the in-progress guard" {
    const tmp_dir = "/tmp/snapshot_test_host_busy";
    resetTestDir(tmp_dir);
    defer removeTestDir(tmp_dir);

    var in_progress = std.atomic.Value(bool).init(true);
    var apply_mutex: Io.Mutex = .init;
    var drain_mutex: Io.Mutex = .init;
    var probe = SnapProbe{ .seq = 1, .pages = 1 };

    const host = SnapshotHost{
        .io = std.testing.io,
        .snapshot_in_progress = &in_progress,
        .data_dir = tmp_dir,
        .apply_mutex = &apply_mutex,
        .mt_drain_mutex = &drain_mutex,
        .checkpointLocked = SnapProbe.checkpointLocked,
        .pageCount = SnapProbe.pageCount,
        .ctx = &probe,
    };

    try std.testing.expectError(error.SnapshotInProgress, forceSnapshot(host));
    try std.testing.expect(in_progress.load(.acquire));
}
