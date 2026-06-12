//! Incremental WAL follower for replication: re-reads a live `wal.bin` that a
//! `WalWriter` is still appending to, yielding each durable entry exactly once
//! in sequence order. Unlike `wal_replay.WalReader` (which reads a finished
//! file and stops at the first invalid byte), the follower only trusts bytes
//! below a caller-supplied durable boundary and retries transiently unreadable
//! tails until the writer settles them.

const std = @import("std");
const posix = std.posix;
const wal = @import("wal.zig");

const BLOCK_SIZE: u64 = 4096;

const MAX_FOLLOW_DATA_LEN: u32 = 16 * 1024 * 1024;

const MAX_STALLS_AT_SAME_OFFSET: u32 = 64;

/// One WAL entry yielded by `FollowReader.next`. `data` is borrowed from the
/// reader's internal buffer and is only valid until the next call to `next`.
pub const FollowedEntry = struct {
    sequence: u64,
    op_code: u8,
    checksum: u32,
    data: []const u8,
};

/// Tails a live WAL file for streaming replication. The caller polls `next`
/// with a fresh `wal.DurableBoundary` snapshot from the owning `WalWriter`;
/// the reader never reads past that boundary, so it only ever observes bytes
/// the writer has already fsynced.
pub const FollowReader = struct {
    allocator: std.mem.Allocator,
    path: []const u8,
    file: ?std.fs.File,
    offset: u64,
    truncation_epoch: u64,
    after_lsn: u64,
    last_sequence: u64,
    data_buf: std.ArrayList(u8),
    stall_offset: u64,
    stall_count: u32,

    /// Creates a follower that emits entries with `sequence > after_lsn`.
    /// The WAL file may not exist yet; `next` returns null until it does.
    pub fn init(allocator: std.mem.Allocator, dir: []const u8, after_lsn: u64) !FollowReader {
        const path = try std.fs.path.join(allocator, &.{ dir, "wal.bin" });
        return FollowReader{
            .allocator = allocator,
            .path = path,
            .file = null,
            .offset = 0,
            .truncation_epoch = 0,
            .after_lsn = after_lsn,
            .last_sequence = after_lsn,
            .data_buf = .{},
            .stall_offset = 0,
            .stall_count = 0,
        };
    }

    /// Releases the file handle and buffers.
    pub fn deinit(self: *FollowReader) void {
        if (self.file) |f| f.close();
        self.data_buf.deinit(self.allocator);
        self.allocator.free(self.path);
    }

    /// Returns the next entry at or below `boundary`, or null when caught up
    /// (re-poll with a fresh boundary). A boundary truncation epoch ahead of
    /// the reader's means the writer truncated after a checkpoint; the reader
    /// rescans from the start, where sequences continue monotonically.
    /// Errors: `WalSequenceGap` when the file no longer covers the next
    /// expected sequence, `InconsistentWal` when the same offset stays
    /// unreadable across `MAX_STALLS_AT_SAME_OFFSET` polls.
    pub fn next(self: *FollowReader, boundary: wal.DurableBoundary) !?FollowedEntry {
        if (boundary.truncation_epoch != self.truncation_epoch) {
            self.truncation_epoch = boundary.truncation_epoch;
            self.offset = 0;
            self.stall_offset = 0;
            self.stall_count = 0;
        }

        const file = (try self.ensureFile()) orelse return null;

        var probe: [4096]u8 = undefined;

        while (self.offset + wal.HEADER_SIZE <= boundary.offset) {
            const probe_cap: usize = @intCast(@min(@as(u64, probe.len), boundary.offset - self.offset));
            const pn = try file.preadAll(probe[0..probe_cap], self.offset);
            if (pn < wal.HEADER_SIZE) return self.stall(file, boundary);

            const header = std.mem.bytesToValue(wal.WalEntryHeader, probe[0..wal.HEADER_SIZE]);
            if (header.sequence == 0) return self.stall(file, boundary);

            if (header.data_len > MAX_FOLLOW_DATA_LEN) {
                self.offset = std.mem.alignForward(u64, self.offset + 1, BLOCK_SIZE);
                continue;
            }

            const entry_end = self.offset + wal.HEADER_SIZE + header.data_len;
            if (entry_end > boundary.offset) return self.stall(file, boundary);

            try self.data_buf.resize(self.allocator, header.data_len);
            const inline_end = wal.HEADER_SIZE + @as(usize, header.data_len);
            if (inline_end <= pn) {
                @memcpy(self.data_buf.items, probe[wal.HEADER_SIZE..inline_end]);
            } else {
                const dn = try file.preadAll(self.data_buf.items, self.offset + wal.HEADER_SIZE);
                if (dn < header.data_len) return self.stall(file, boundary);
            }

            const computed = std.hash.crc.Crc32.hash(self.data_buf.items);
            if (computed != header.checksum) return self.stall(file, boundary);

            self.offset = entry_end;
            self.stall_count = 0;

            if (header.sequence <= self.after_lsn) continue;

            if (header.sequence != self.last_sequence + 1) return error.WalSequenceGap;
            self.last_sequence = header.sequence;

            return FollowedEntry{
                .sequence = header.sequence,
                .op_code = header.op_code,
                .checksum = header.checksum,
                .data = self.data_buf.items,
            };
        }

        return null;
    }

    /// Probes the sequence of the first entry currently in the file (skipping
    /// padding blocks), or null when no entry lies below `boundary`. Used by
    /// the leader handshake to detect a follower that fell behind a
    /// checkpoint truncation.
    pub fn firstSequence(self: *FollowReader, boundary: wal.DurableBoundary) !?u64 {
        const file = (try self.ensureFile()) orelse return null;

        var pos: u64 = 0;
        while (pos + wal.HEADER_SIZE <= boundary.offset) {
            var header_buf: [wal.HEADER_SIZE]u8 = undefined;
            const hn = try file.preadAll(&header_buf, pos);
            if (hn < wal.HEADER_SIZE) return null;

            const header = std.mem.bytesToValue(wal.WalEntryHeader, &header_buf);
            if (header.sequence == 0) return null;

            if (header.data_len > MAX_FOLLOW_DATA_LEN) {
                pos = std.mem.alignForward(u64, pos + 1, BLOCK_SIZE);
                continue;
            }

            return header.sequence;
        }

        return null;
    }

    fn ensureFile(self: *FollowReader) !?std.fs.File {
        if (self.file) |f| return f;
        const opened = std.fs.cwd().openFile(self.path, .{ .mode = .read_only }) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        self.file = opened;
        return opened;
    }

    fn stall(self: *FollowReader, file: std.fs.File, boundary: wal.DurableBoundary) !?FollowedEntry {
        if (self.stall_offset == self.offset) {
            self.stall_count += 1;
            if (self.stall_count >= MAX_STALLS_AT_SAME_OFFSET) return error.InconsistentWal;
        } else {
            self.stall_offset = self.offset;
            self.stall_count = 1;
        }

        dropPageCache(file, self.offset, boundary.offset);
        return null;
    }

    fn dropPageCache(file: std.fs.File, from: u64, to: u64) void {
        if (to <= from) return;
        _ = std.os.linux.fadvise(
            file.handle,
            @intCast(from),
            @intCast(to - from),
            posix.POSIX_FADV.DONTNEED,
        );
    }
};

const test_op: u8 = 100;

fn initWriter(dir: []const u8) !*wal.WalWriter {
    const w = try std.testing.allocator.create(wal.WalWriter);
    w.* = try wal.WalWriter.init(std.testing.allocator, dir, 32, 0);
    try w.startFlusher();
    return w;
}

fn deinitWriter(w: *wal.WalWriter) void {
    w.deinit();
    std.testing.allocator.destroy(w);
}

test "follow reader yields entries appended and synced after open" {
    const tmp_dir = "/tmp/wal_follow_test_basic";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    try std.fs.makeDirAbsolute(tmp_dir);
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    const writer = try initWriter(tmp_dir);
    defer deinitWriter(writer);

    var reader = try FollowReader.init(std.testing.allocator, tmp_dir, 0);
    defer reader.deinit();

    try std.testing.expectEqual(@as(?FollowedEntry, null), try reader.next(writer.durableBoundary()));

    _ = try writer.append(test_op, "alpha");
    _ = try writer.append(test_op, "beta");
    try writer.sync();

    const boundary = writer.durableBoundary();

    const e1 = (try reader.next(boundary)).?;
    try std.testing.expectEqual(@as(u64, 1), e1.sequence);
    try std.testing.expectEqualStrings("alpha", e1.data);

    const e2 = (try reader.next(boundary)).?;
    try std.testing.expectEqual(@as(u64, 2), e2.sequence);
    try std.testing.expectEqualStrings("beta", e2.data);

    try std.testing.expectEqual(@as(?FollowedEntry, null), try reader.next(boundary));

    _ = try writer.append(test_op, "gamma");
    try writer.sync();

    const e3 = (try reader.next(writer.durableBoundary())).?;
    try std.testing.expectEqual(@as(u64, 3), e3.sequence);
    try std.testing.expectEqualStrings("gamma", e3.data);
}

test "follow reader respects a stale durable boundary" {
    const tmp_dir = "/tmp/wal_follow_test_stale_boundary";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    try std.fs.makeDirAbsolute(tmp_dir);
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    const writer = try initWriter(tmp_dir);
    defer deinitWriter(writer);

    _ = try writer.append(test_op, "one");
    _ = try writer.append(test_op, "two");
    try writer.sync();
    const stale = writer.durableBoundary();

    _ = try writer.append(test_op, "three");
    try writer.sync();

    var reader = try FollowReader.init(std.testing.allocator, tmp_dir, 0);
    defer reader.deinit();

    try std.testing.expectEqual(@as(u64, 1), (try reader.next(stale)).?.sequence);
    try std.testing.expectEqual(@as(u64, 2), (try reader.next(stale)).?.sequence);
    try std.testing.expectEqual(@as(?FollowedEntry, null), try reader.next(stale));

    const fresh = writer.durableBoundary();
    try std.testing.expectEqual(@as(u64, 3), (try reader.next(fresh)).?.sequence);
}

test "follow reader continues across a checkpoint truncation via the epoch rewind" {
    const tmp_dir = "/tmp/wal_follow_test_truncate";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    try std.fs.makeDirAbsolute(tmp_dir);
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    const writer = try initWriter(tmp_dir);
    defer deinitWriter(writer);

    var reader = try FollowReader.init(std.testing.allocator, tmp_dir, 0);
    defer reader.deinit();

    _ = try writer.append(test_op, "pre-1");
    _ = try writer.append(test_op, "pre-2");
    try writer.sync();

    try std.testing.expectEqual(@as(u64, 1), (try reader.next(writer.durableBoundary())).?.sequence);
    try std.testing.expectEqual(@as(u64, 2), (try reader.next(writer.durableBoundary())).?.sequence);

    try writer.truncateAfterCheckpoint();

    _ = try writer.append(test_op, "post-3");
    try writer.sync();

    const e = (try reader.next(writer.durableBoundary())).?;
    try std.testing.expectEqual(@as(u64, 3), e.sequence);
    try std.testing.expectEqualStrings("post-3", e.data);
}

test "follow reader skips entries at or below after_lsn" {
    const tmp_dir = "/tmp/wal_follow_test_after_lsn";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    try std.fs.makeDirAbsolute(tmp_dir);
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    const writer = try initWriter(tmp_dir);
    defer deinitWriter(writer);

    _ = try writer.append(test_op, "one");
    _ = try writer.append(test_op, "two");
    _ = try writer.append(test_op, "three");
    try writer.sync();

    var reader = try FollowReader.init(std.testing.allocator, tmp_dir, 2);
    defer reader.deinit();

    const boundary = writer.durableBoundary();
    const e = (try reader.next(boundary)).?;
    try std.testing.expectEqual(@as(u64, 3), e.sequence);
    try std.testing.expectEqual(@as(?FollowedEntry, null), try reader.next(boundary));
}

test "follow reader surfaces a sequence gap when the file starts past the requested LSN" {
    const tmp_dir = "/tmp/wal_follow_test_gap";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    try std.fs.makeDirAbsolute(tmp_dir);
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    const writer = try initWriter(tmp_dir);
    defer deinitWriter(writer);

    _ = try writer.append(test_op, "one");
    _ = try writer.append(test_op, "two");
    try writer.sync();
    try writer.truncateAfterCheckpoint();

    _ = try writer.append(test_op, "three");
    try writer.sync();

    var reader = try FollowReader.init(std.testing.allocator, tmp_dir, 0);
    defer reader.deinit();

    try std.testing.expectError(error.WalSequenceGap, reader.next(writer.durableBoundary()));
}

test "firstSequence probes the first entry in the file" {
    const tmp_dir = "/tmp/wal_follow_test_first_seq";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    try std.fs.makeDirAbsolute(tmp_dir);
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    const writer = try initWriter(tmp_dir);
    defer deinitWriter(writer);

    var reader = try FollowReader.init(std.testing.allocator, tmp_dir, 0);
    defer reader.deinit();

    try std.testing.expectEqual(@as(?u64, null), try reader.firstSequence(writer.durableBoundary()));

    _ = try writer.append(test_op, "one");
    _ = try writer.append(test_op, "two");
    try writer.sync();
    try writer.truncateAfterCheckpoint();

    _ = try writer.append(test_op, "three");
    try writer.sync();

    try std.testing.expectEqual(@as(?u64, 3), try reader.firstSequence(writer.durableBoundary()));
}
