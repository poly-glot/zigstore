//! Point-in-time snapshot of a store's durable state: under the apply and drain locks the
//! caller's cache and header are flushed, then a `snapshot.meta` superblock recording the WAL
//! sequence and page count is written atomically (temp file + rename + directory fsync).
//!
//! The engine names no application type. A consumer drives a snapshot through `SnapshotHost`,
//! the small interface a generated `Store` satisfies via `Store.snapshotHost()`.

const std = @import("std");

/// The `snapshot.meta` magic: ASCII "SNAP".
pub const SNAP_MAGIC: u32 = 0x534E4150;

/// What a completed `forceSnapshot` reports: the WAL sequence the snapshot is consistent up to
/// and the wall-clock time the flush+write took.
pub const SnapshotResult = struct {
    wal_sequence: u64,
    duration_ms: u64,
};

/// The seam between the generic snapshot routine and a concrete store. The host hands over the
/// in-progress guard, the data directory, the two locks held while flushing, and four function
/// pointers (each closing over the store as `ctx`) for the durability work and the page count.
pub const SnapshotHost = struct {
    snapshot_in_progress: *std.atomic.Value(bool),
    data_dir: []const u8,
    apply_mutex: *std.Thread.Mutex,
    mt_drain_mutex: *std.Thread.Mutex,
    walSequence: *const fn (ctx: *anyopaque) u64,
    flushCache: *const fn (ctx: *anyopaque) anyerror!void,
    flushHeader: *const fn (ctx: *anyopaque) anyerror!void,
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
/// apply and drain locks are still held, so the copy is page-consistent at the returned WAL
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

    const start_ns = std.time.nanoTimestamp();

    const wal_seq: u64 = host.walSequence(host.ctx);

    var mgr = SnapshotManager.init(host.data_dir, 0);
    try flushUnderLocks(host, copy_base);
    try mgr.writeMeta(host, wal_seq);

    const end_ns = std.time.nanoTimestamp();
    const duration_ms: u64 = @intCast(@divTrunc(end_ns - start_ns, std.time.ns_per_ms));
    return .{ .wal_sequence = wal_seq, .duration_ms = duration_ms };
}

fn flushUnderLocks(host: SnapshotHost, copy_base: bool) !void {
    host.apply_mutex.lock();
    defer host.apply_mutex.unlock();
    host.mt_drain_mutex.lock();
    defer host.mt_drain_mutex.unlock();

    try host.flushCache(host.ctx);
    try host.flushHeader(host.ctx);
    if (copy_base) try copyStoreToBase(host.data_dir);
}

fn copyStoreToBase(data_dir: []const u8) !void {
    const src = try std.fs.path.join(std.heap.page_allocator, &.{ data_dir, "store.dat" });
    defer std.heap.page_allocator.free(src);

    const dst = try std.fs.path.join(std.heap.page_allocator, &.{ data_dir, BASE_BACKUP_FILE });
    defer std.heap.page_allocator.free(dst);

    try std.fs.cwd().copyFile(src, std.fs.cwd(), dst, .{});

    const copy = try std.fs.cwd().openFile(dst, .{ .mode = .read_write });
    defer copy.close();
    try copy.sync();
}

/// The `snapshot.meta` superblock: magic, version, the consistent WAL sequence, the wall-clock
/// timestamp, the page count, and a pad to 64 bytes.
pub const SnapshotHeader = extern struct {
    magic: u32 = SNAP_MAGIC,
    version: u32 = 1,
    wal_sequence: u64,
    timestamp: i64,
    page_count: u32,
    _reserved: [36]u8 = [_]u8{0} ** 36,
};

comptime {
    if (@sizeOf(SnapshotHeader) != 64) @compileError("SnapshotHeader size mismatch");
}

const SNAP_HEADER_SIZE: usize = @sizeOf(SnapshotHeader);

/// Drives snapshot creation against a data directory, tracking the last-taken time so a
/// scheduler can honor a minimum interval.
pub const SnapshotManager = struct {
    data_dir: []const u8,
    interval_s: u32,
    last_snapshot_time: i64,

    pub fn init(data_dir: []const u8, interval_s: u32) SnapshotManager {
        return SnapshotManager{
            .data_dir = data_dir,
            .interval_s = interval_s,
            .last_snapshot_time = 0,
        };
    }

    /// Whether at least `interval_s` seconds have elapsed since the last snapshot.
    pub fn shouldSnapshot(self: *const SnapshotManager) bool {
        const now = std.time.timestamp();
        return (now - self.last_snapshot_time) >= @as(i64, self.interval_s);
    }

    /// Flush the host's cache and header under the apply and drain locks, then write
    /// `snapshot.meta` atomically (temp file, rename, directory fsync) recording
    /// `wal_sequence` and the host's current page count.
    pub fn createSnapshot(
        self: *SnapshotManager,
        host: SnapshotHost,
        wal_sequence: u64,
    ) !void {
        try flushUnderLocks(host, false);
        try self.writeMeta(host, wal_sequence);
    }

    fn writeMeta(self: *SnapshotManager, host: SnapshotHost, wal_sequence: u64) !void {
        const now = std.time.timestamp();

        const snap_header = SnapshotHeader{
            .wal_sequence = wal_sequence,
            .timestamp = now,
            .page_count = @intCast(host.pageCount(host.ctx)),
        };

        const tmp_path = try std.fs.path.join(std.heap.page_allocator, &.{ self.data_dir, "snapshot.meta.tmp" });
        defer std.heap.page_allocator.free(tmp_path);

        const final_path = try std.fs.path.join(std.heap.page_allocator, &.{ self.data_dir, "snapshot.meta" });
        defer std.heap.page_allocator.free(final_path);

        {
            const tmp_file = try std.fs.cwd().createFile(tmp_path, .{ .truncate = true });
            defer tmp_file.close();

            const header_bytes: *const [SNAP_HEADER_SIZE]u8 = @ptrCast(&snap_header);
            try tmp_file.writeAll(header_bytes);
            try tmp_file.sync();
        }

        try std.fs.cwd().rename(tmp_path, final_path);

        fsyncDir(self.data_dir);

        self.last_snapshot_time = now;
    }

    fn fsyncDir(path: []const u8) void {
        const log = std.log.scoped(.snapshot);
        const flags: std.posix.O = .{ .ACCMODE = .RDONLY, .DIRECTORY = true, .CLOEXEC = true };
        const fd = std.posix.open(path, flags, 0) catch |err| {
            log.warn("snapshot: dir open failed for fsync: {}", .{err});
            return;
        };
        defer std.posix.close(fd);
        std.posix.fsync(fd) catch |err| {
            log.warn("snapshot: dir fsync failed: {}", .{err});
        };
    }

    /// Read back `snapshot.meta` from `data_dir`, or `null` if none exists. Errors on a bad magic.
    pub fn loadSnapshotMeta(data_dir: []const u8) !?SnapshotHeader {
        const path = try std.fs.path.join(std.heap.page_allocator, &.{ data_dir, "snapshot.meta" });
        defer std.heap.page_allocator.free(path);

        const file = std.fs.cwd().openFile(path, .{ .mode = .read_only }) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer file.close();

        var buf: [SNAP_HEADER_SIZE]u8 = undefined;
        const n = try file.readAll(&buf);
        if (n < SNAP_HEADER_SIZE) return null;

        const snap = std.mem.bytesToValue(SnapshotHeader, &buf);

        if (snap.magic != SNAP_MAGIC) return error.InvalidSnapshotMagic;

        return snap;
    }

    /// The WAL sequence the last snapshot is consistent up to, or 0 if none exists.
    pub fn getWalSequence(data_dir: []const u8) !u64 {
        const snap = try loadSnapshotMeta(data_dir);
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
    const result = try SnapshotManager.loadSnapshotMeta("/tmp/snapshot_test_nonexistent_12345");
    try std.testing.expect(result == null);
}

test "create and load snapshot meta roundtrip" {
    const tmp_dir = "/tmp/snapshot_test_roundtrip";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    std.fs.makeDirAbsolute(tmp_dir) catch {};
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    const snap_header = SnapshotHeader{
        .magic = 0x534E4150,
        .version = 1,
        .wal_sequence = 42,
        .timestamp = 1700000000,
        .page_count = 100,
    };

    const path = try std.fs.path.join(std.testing.allocator, &.{ tmp_dir, "snapshot.meta" });
    defer std.testing.allocator.free(path);

    {
        const file = try std.fs.cwd().createFile(path, .{ .truncate = true });
        defer file.close();
        const header_bytes: *const [SNAP_HEADER_SIZE]u8 = @ptrCast(&snap_header);
        try file.writeAll(header_bytes);
    }

    const loaded = (try SnapshotManager.loadSnapshotMeta(tmp_dir)).?;
    try std.testing.expectEqual(@as(u32, 0x534E4150), loaded.magic);
    try std.testing.expectEqual(@as(u32, 1), loaded.version);
    try std.testing.expectEqual(@as(u64, 42), loaded.wal_sequence);
    try std.testing.expectEqual(@as(i64, 1700000000), loaded.timestamp);
    try std.testing.expectEqual(@as(u32, 100), loaded.page_count);
}

test "getWalSequence returns 0 when no snapshot exists" {
    const seq = try SnapshotManager.getWalSequence("/tmp/snapshot_test_nonexistent_67890");
    try std.testing.expectEqual(@as(u64, 0), seq);
}

test "getWalSequence returns stored sequence" {
    const tmp_dir = "/tmp/snapshot_test_getseq";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    std.fs.makeDirAbsolute(tmp_dir) catch {};
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    const snap_header = SnapshotHeader{
        .wal_sequence = 99,
        .timestamp = 1700000000,
        .page_count = 50,
    };

    const path = try std.fs.path.join(std.testing.allocator, &.{ tmp_dir, "snapshot.meta" });
    defer std.testing.allocator.free(path);

    {
        const file = try std.fs.cwd().createFile(path, .{ .truncate = true });
        defer file.close();
        const header_bytes: *const [SNAP_HEADER_SIZE]u8 = @ptrCast(&snap_header);
        try file.writeAll(header_bytes);
    }

    const seq = try SnapshotManager.getWalSequence(tmp_dir);
    try std.testing.expectEqual(@as(u64, 99), seq);
}

test "shouldSnapshot respects interval" {
    var mgr = SnapshotManager.init("/tmp", 300);

    try std.testing.expect(mgr.shouldSnapshot());

    mgr.last_snapshot_time = std.time.timestamp();
    try std.testing.expect(!mgr.shouldSnapshot());
}

const SnapProbe = struct {
    flush_cache_calls: usize = 0,
    flush_header_calls: usize = 0,
    seq: u64,
    pages: u64,

    fn walSequence(ctx: *anyopaque) u64 {
        const self: *SnapProbe = @ptrCast(@alignCast(ctx));
        return self.seq;
    }
    fn flushCache(ctx: *anyopaque) anyerror!void {
        const self: *SnapProbe = @ptrCast(@alignCast(ctx));
        self.flush_cache_calls += 1;
    }
    fn flushHeader(ctx: *anyopaque) anyerror!void {
        const self: *SnapProbe = @ptrCast(@alignCast(ctx));
        self.flush_header_calls += 1;
    }
    fn pageCount(ctx: *anyopaque) u64 {
        const self: *SnapProbe = @ptrCast(@alignCast(ctx));
        return self.pages;
    }
};

test "forceSnapshot over a host flushes, writes snapshot.meta, and records seq/page_count" {
    const tmp_dir = "/tmp/snapshot_test_host";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    std.fs.makeDirAbsolute(tmp_dir) catch {};
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    var in_progress = std.atomic.Value(bool).init(false);
    var apply_mutex = std.Thread.Mutex{};
    var drain_mutex = std.Thread.Mutex{};
    var probe = SnapProbe{ .seq = 7, .pages = 13 };

    const host = SnapshotHost{
        .snapshot_in_progress = &in_progress,
        .data_dir = tmp_dir,
        .apply_mutex = &apply_mutex,
        .mt_drain_mutex = &drain_mutex,
        .walSequence = SnapProbe.walSequence,
        .flushCache = SnapProbe.flushCache,
        .flushHeader = SnapProbe.flushHeader,
        .pageCount = SnapProbe.pageCount,
        .ctx = &probe,
    };

    const result = try forceSnapshot(host);
    try std.testing.expectEqual(@as(u64, 7), result.wal_sequence);
    try std.testing.expectEqual(@as(usize, 1), probe.flush_cache_calls);
    try std.testing.expectEqual(@as(usize, 1), probe.flush_header_calls);
    try std.testing.expect(!in_progress.load(.acquire));

    const loaded = (try SnapshotManager.loadSnapshotMeta(tmp_dir)).?;
    try std.testing.expectEqual(@as(u64, 7), loaded.wal_sequence);
    try std.testing.expectEqual(@as(u32, 13), loaded.page_count);
}

test "forceSnapshot rejects a concurrent snapshot via the in-progress guard" {
    const tmp_dir = "/tmp/snapshot_test_host_busy";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    std.fs.makeDirAbsolute(tmp_dir) catch {};
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    var in_progress = std.atomic.Value(bool).init(true);
    var apply_mutex = std.Thread.Mutex{};
    var drain_mutex = std.Thread.Mutex{};
    var probe = SnapProbe{ .seq = 1, .pages = 1 };

    const host = SnapshotHost{
        .snapshot_in_progress = &in_progress,
        .data_dir = tmp_dir,
        .apply_mutex = &apply_mutex,
        .mt_drain_mutex = &drain_mutex,
        .walSequence = SnapProbe.walSequence,
        .flushCache = SnapProbe.flushCache,
        .flushHeader = SnapProbe.flushHeader,
        .pageCount = SnapProbe.pageCount,
        .ctx = &probe,
    };

    try std.testing.expectError(error.SnapshotInProgress, forceSnapshot(host));
    try std.testing.expect(in_progress.load(.acquire));
}
