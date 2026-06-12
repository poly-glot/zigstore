const std = @import("std");
const posix = std.posix;

const log = std.log.scoped(.wal);

const BLOCK_SIZE: usize = 4096;

const PREALLOC_SIZE: u64 = 64 * 1024 * 1024;

const PAD_BYTE: u8 = 0xFF;

const test_op: u8 = 100;

pub const WalEntryHeader = extern struct {
    sequence: u64,
    op_code: u8,
    _pad: [3]u8 = .{0} ** 3,
    data_len: u32,
    checksum: u32,
    _pad2: [4]u8 = .{0} ** 4,
};

comptime {
    if (@sizeOf(WalEntryHeader) != 24) @compileError("WalEntryHeader size mismatch");
}

pub const HEADER_SIZE: usize = @sizeOf(WalEntryHeader);

pub const DurableBoundary = struct {
    sequence: u64,
    offset: u64,
    truncation_epoch: u64,
};

pub const WalWriter = struct {
    file: std.fs.File,
    sequence: u64,
    allocator: std.mem.Allocator,

    front: std.ArrayList(u8),
    back: std.ArrayList(u8),
    entry_count: u32,
    batch_size: u32,

    lock: std.Thread.Mutex,
    flush_cond: std.Thread.Condition,
    back_in_flight: bool,
    flush_done_cond: std.Thread.Condition,
    pending_max_seq: u64,
    last_durable_seq: u64,
    fsync_failed: bool,
    shutdown: std.atomic.Value(bool),
    flusher_thread: ?std.Thread,

    direct_io: bool,
    direct_buf: []u8,
    write_offset: u64,
    durable_offset: u64,
    truncation_epoch: u64,
    retain_floor: std.atomic.Value(u64),

    pub fn init(allocator: std.mem.Allocator, dir: []const u8, batch_size: u32, base_sequence: u64) !WalWriter {
        const path = try std.fs.path.join(allocator, &.{ dir, "wal.bin" });
        defer allocator.free(path);

        const scan_file = try openOrCreateFile(path);
        const scan = scanWal(scan_file, allocator) catch ScanResult{ .last_sequence = 0, .valid_end = 0 };

        const direct_result = openWithDirect(path);
        const file, const direct_io = direct_result;
        errdefer file.close();

        const initial_offset = settleTail(scan_file, scan.valid_end, direct_io) catch |err| {
            scan_file.close();
            return err;
        };
        scan_file.close();

        if (direct_io) {
            fallocatePosix(file.handle, PREALLOC_SIZE) catch |err| {
                log.warn("WAL fallocate({d} MiB) failed: {} — proceeding without preallocation", .{ PREALLOC_SIZE / (1024 * 1024), err });
            };
        }

        try file.seekTo(initial_offset);

        const direct_buf = if (direct_io)
            try allocator.alignedAlloc(u8, .fromByteUnits(BLOCK_SIZE), 256 * 1024)
        else
            try allocator.alignedAlloc(u8, .fromByteUnits(BLOCK_SIZE), 0);
        errdefer allocator.free(direct_buf);

        if (!direct_io) {
            log.warn("WAL: O_DIRECT not supported by filesystem at {s} — using buffered I/O", .{path});
        }

        return WalWriter{
            .file = file,
            .sequence = @max(scan.last_sequence, base_sequence),
            .front = .{},
            .back = .{},
            .entry_count = 0,
            .batch_size = if (batch_size == 0) 32 else batch_size,
            .lock = .{},
            .flush_cond = .{},
            .back_in_flight = false,
            .flush_done_cond = .{},
            .pending_max_seq = 0,
            .last_durable_seq = @max(scan.last_sequence, base_sequence),
            .fsync_failed = false,
            .shutdown = std.atomic.Value(bool).init(false),
            .flusher_thread = null,
            .allocator = allocator,
            .direct_io = direct_io,
            .direct_buf = direct_buf,
            .write_offset = initial_offset,
            .durable_offset = initial_offset,
            .truncation_epoch = 0,
            .retain_floor = std.atomic.Value(u64).init(std.math.maxInt(u64)),
        };
    }

    pub fn startFlusher(self: *WalWriter) !void {
        self.flusher_thread = std.Thread.spawn(.{}, flusherLoop, .{self}) catch |err| {
            log.err("Failed to spawn WAL flusher thread: {}", .{err});
            return err;
        };
    }

    pub fn deinit(self: *WalWriter) void {
        self.shutdown.store(true, .release);
        self.flush_cond.signal();

        if (self.flusher_thread) |t| t.join();

        self.lock.lock();
        self.swapAndFlush() catch |err| {
            log.err("WAL final flush failed: {}", .{err});
        };
        self.lock.unlock();

        self.front.deinit(self.allocator);
        self.back.deinit(self.allocator);
        self.allocator.free(self.direct_buf);
        self.file.close();
    }

    pub fn truncateAfterCheckpoint(self: *WalWriter) !void {
        self.lock.lock();
        defer self.lock.unlock();

        if (self.retain_floor.load(.acquire) < self.sequence) return error.WalRetainedByReplica;

        try self.swapAndFlush();

        try self.file.setEndPos(0);
        try self.file.seekTo(0);
        posix.fdatasync(self.file.handle) catch |err| {
            log.warn("WAL truncate fdatasync failed: {}", .{err});
        };

        if (self.direct_io) {
            fallocatePosix(self.file.handle, PREALLOC_SIZE) catch |err| {
                log.warn("WAL post-truncate fallocate failed: {}", .{err});
            };
        }

        self.last_durable_seq = self.sequence;
        self.pending_max_seq = 0;
        self.write_offset = 0;
        self.durable_offset = 0;
        self.truncation_epoch += 1;
        self.flush_done_cond.broadcast();
    }

    pub fn append(self: *WalWriter, op_code: u8, data: []const u8) !u64 {
        if (data.len > std.math.maxInt(u32)) return error.Overflow;

        const checksum = std.hash.crc.Crc32.hash(data);

        self.lock.lock();
        defer self.lock.unlock();

        self.sequence += 1;
        const seq = self.sequence;

        const header = WalEntryHeader{
            .sequence = seq,
            .op_code = op_code,
            ._pad = .{0} ** 3,
            .data_len = @intCast(data.len),
            .checksum = checksum,
            ._pad2 = .{0} ** 4,
        };

        const header_bytes: *const [HEADER_SIZE]u8 = @ptrCast(&header);
        try self.front.appendSlice(self.allocator, header_bytes);
        try self.front.appendSlice(self.allocator, data);

        self.entry_count += 1;
        self.pending_max_seq = seq;

        if (self.entry_count >= self.batch_size) {
            self.flush_cond.signal();
        }

        return seq;
    }

    pub fn awaitDurable(self: *WalWriter, seq: u64) !void {
        if (seq == 0) return;
        self.lock.lock();
        defer self.lock.unlock();
        while (self.last_durable_seq < seq) {
            if (self.fsync_failed) return error.WalFlushFailed;
            self.flush_cond.signal();
            self.flush_done_cond.wait(&self.lock);
        }
    }

    pub fn sync(self: *WalWriter) !void {
        self.lock.lock();
        defer self.lock.unlock();
        try self.swapAndFlush();
    }

    pub fn getSequence(self: *WalWriter) u64 {
        self.lock.lock();
        defer self.lock.unlock();
        return self.sequence;
    }

    pub fn durableBoundary(self: *WalWriter) DurableBoundary {
        self.lock.lock();
        defer self.lock.unlock();
        return .{
            .sequence = self.last_durable_seq,
            .offset = self.durable_offset,
            .truncation_epoch = self.truncation_epoch,
        };
    }

    pub fn waitDurableBeyond(self: *WalWriter, sequence: u64, timeout_ns: u64) bool {
        self.lock.lock();
        defer self.lock.unlock();
        if (self.last_durable_seq > sequence) return true;
        self.flush_done_cond.timedWait(&self.lock, timeout_ns) catch {};
        return self.last_durable_seq > sequence;
    }

    pub fn setRetainFloor(self: *WalWriter, lsn: u64) void {
        self.retain_floor.store(lsn, .release);
    }

    fn swapAndFlush(self: *WalWriter) !void {
        while (self.back_in_flight) {
            self.flush_done_cond.wait(&self.lock);
        }

        if (self.front.items.len == 0) return;

        const flush_seq = self.pending_max_seq;

        const tmp = self.front;
        self.front = self.back;
        self.back = tmp;
        self.entry_count = 0;
        self.pending_max_seq = 0;

        self.flushBack() catch |err| {
            self.fsync_failed = true;
            self.flush_done_cond.broadcast();
            return err;
        };

        self.markDurable(flush_seq);
        self.flush_done_cond.broadcast();
    }

    fn markDurable(self: *WalWriter, flush_seq: u64) void {
        if (flush_seq > self.last_durable_seq) self.last_durable_seq = flush_seq;
        self.durable_offset = self.write_offset;
    }

    fn flushBack(self: *WalWriter) !void {
        if (self.back.items.len == 0) return;
        defer self.back.clearRetainingCapacity();

        if (self.direct_io) {
            const n = self.back.items.len;
            const padded = paddedLength(n);

            if (padded > self.direct_buf.len) {
                var new_cap = if (self.direct_buf.len == 0) BLOCK_SIZE else self.direct_buf.len;
                while (new_cap < padded) new_cap *= 2;
                const new_buf = try self.allocator.alignedAlloc(u8, .fromByteUnits(BLOCK_SIZE), new_cap);
                self.allocator.free(self.direct_buf);
                self.direct_buf = new_buf;
            }

            @memcpy(self.direct_buf[0..n], self.back.items);
            @memset(self.direct_buf[n..padded], PAD_BYTE);

            try self.file.pwriteAll(self.direct_buf[0..padded], self.write_offset);
            self.write_offset += padded;
        } else {
            try self.file.writeAll(self.back.items);
            self.write_offset += self.back.items.len;
        }

        try posix.fdatasync(self.file.handle);
    }

    fn paddedLength(n: usize) usize {
        const aligned = std.mem.alignForward(usize, n, BLOCK_SIZE);
        if (aligned > n and aligned - n < HEADER_SIZE) return aligned + BLOCK_SIZE;
        return aligned;
    }

    fn settleTail(scan_file: std.fs.File, valid_end: u64, direct_io: bool) !u64 {
        try scan_file.setEndPos(valid_end);
        if (!direct_io) return valid_end;

        const settled = paddedLength(@intCast(valid_end));
        if (settled > valid_end) {
            var pad: [2 * BLOCK_SIZE]u8 = undefined;
            @memset(&pad, PAD_BYTE);
            try scan_file.pwriteAll(pad[0..@intCast(settled - valid_end)], valid_end);
            try scan_file.sync();
        }
        return settled;
    }

    fn flusherLoop(self: *WalWriter) void {
        while (!self.shutdown.load(.acquire)) {
            self.lock.lock();

            if (self.entry_count < self.batch_size and !self.shutdown.load(.acquire)) {
                self.flush_cond.timedWait(
                    &self.lock,
                    2 * std.time.ns_per_ms,
                ) catch {};
            }

            if (self.front.items.len == 0) {
                self.lock.unlock();
                continue;
            }

            const flush_seq = self.pending_max_seq;

            const tmp = self.front;
            self.front = self.back;
            self.back = tmp;
            self.entry_count = 0;
            self.pending_max_seq = 0;
            self.back_in_flight = true;
            self.lock.unlock();

            const flush_result = self.flushBack();

            self.lock.lock();
            if (flush_result) |_| {
                self.markDurable(flush_seq);
            } else |err| {
                log.err("WAL flusher: flush failed: {}", .{err});
                self.fsync_failed = true;
            }
            self.back_in_flight = false;
            self.flush_done_cond.broadcast();
            self.lock.unlock();
        }

        self.lock.lock();
        if (self.front.items.len > 0) {
            const flush_seq = self.pending_max_seq;
            const tmp = self.front;
            self.front = self.back;
            self.back = tmp;
            self.entry_count = 0;
            self.pending_max_seq = 0;
            self.back_in_flight = true;
            self.lock.unlock();

            const drain_result = self.flushBack();

            self.lock.lock();
            if (drain_result) |_| {
                self.markDurable(flush_seq);
            } else |_| {
                self.fsync_failed = true;
            }
            self.back_in_flight = false;
            self.flush_done_cond.broadcast();
        }
        self.lock.unlock();
    }

    fn fallocatePosix(fd: i32, length: u64) !void {
        const rc = std.os.linux.fallocate(fd, 0, 0, @intCast(length));
        const err = posix.errno(rc);
        switch (err) {
            .SUCCESS => return,
            .OPNOTSUPP, .NOSYS => return error.OperationNotSupported,
            else => return posix.unexpectedErrno(err),
        }
    }

    fn openWithDirect(path: []const u8) struct { std.fs.File, bool } {
        const O = posix.O;
        const flags: O = .{
            .ACCMODE = .RDWR,
            .CREAT = true,
            .CLOEXEC = true,
            .DIRECT = true,
        };
        const direct_fd = posix.open(path, flags, 0o644) catch |err| {
            log.warn("WAL: O_DIRECT open failed ({}); using buffered I/O", .{err});
            const fallback = openOrCreateFile(path) catch unreachable;
            return .{ fallback, false };
        };
        return .{ std.fs.File{ .handle = direct_fd }, true };
    }

    fn openOrCreateFile(path: []const u8) !std.fs.File {
        return std.fs.cwd().openFile(path, .{ .mode = .read_write }) catch |err| switch (err) {
            error.FileNotFound => {
                return try std.fs.cwd().createFile(path, .{ .read = true, .truncate = false });
            },
            else => return err,
        };
    }

    const MAX_RECOVERY_DATA_LEN: u32 = 16 * 1024 * 1024;

    const ScanResult = struct { last_sequence: u64, valid_end: u64 };

    fn scanWal(file: std.fs.File, allocator: std.mem.Allocator) !ScanResult {
        const file_size = try file.getEndPos();
        if (file_size == 0) return .{ .last_sequence = 0, .valid_end = 0 };

        try file.seekTo(0);

        var data_buf: std.ArrayList(u8) = .{};
        defer data_buf.deinit(allocator);

        var last_seq: u64 = 0;
        var pos: u64 = 0;
        var saw_padding: bool = false;

        while (pos + HEADER_SIZE <= file_size) {
            var header_buf: [HEADER_SIZE]u8 = undefined;
            const hn = try file.readAll(&header_buf);
            if (hn < HEADER_SIZE) break;

            const header = std.mem.bytesToValue(WalEntryHeader, &header_buf);
            if (header.sequence == 0) {
                saw_padding = true;
                break;
            }
            const data_len: u32 = header.data_len;

            if (data_len > MAX_RECOVERY_DATA_LEN) {
                const next_block = std.mem.alignForward(u64, pos + 1, BLOCK_SIZE);
                if (next_block >= file_size) {
                    saw_padding = true;
                    break;
                }
                pos = next_block;
                try file.seekTo(pos);
                continue;
            }
            if (pos + HEADER_SIZE + data_len > file_size) break;

            try data_buf.resize(allocator, data_len);
            const dn = try file.readAll(data_buf.items);
            if (dn < data_len) break;

            const computed = std.hash.crc.Crc32.hash(data_buf.items);
            if (computed != header.checksum) break;

            last_seq = header.sequence;
            pos += HEADER_SIZE + data_len;
            try file.seekTo(pos);
        }

        if (pos < file_size) {
            if (saw_padding) {
                log.debug("WAL: stripped {d} bytes of preallocated/padded tail at offset {d}", .{ file_size - pos, pos });
            } else {
                log.warn("WAL: truncating {d} bytes of torn/corrupt tail at offset {d}", .{ file_size - pos, pos });
            }
        }

        return .{ .last_sequence = last_seq, .valid_end = pos };
    }
};

fn initHeap(allocator: std.mem.Allocator, dir: []const u8, batch_size: u32, base_sequence: u64) !*WalWriter {
    const w = try allocator.create(WalWriter);
    w.* = try WalWriter.init(allocator, dir, batch_size, base_sequence);
    try w.startFlusher();
    return w;
}

fn deinitHeap(w: *WalWriter) void {
    const allocator = w.allocator;
    w.deinit();
    allocator.destroy(w);
}

test "append entries and verify sequence" {
    const tmp_dir = "/tmp/wal_test_append";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    std.fs.makeDirAbsolute(tmp_dir) catch {};
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    const writer = try initHeap(std.testing.allocator, tmp_dir, 32, 0);
    defer deinitHeap(writer);

    const seq1 = try writer.append(test_op, "cat1");
    const seq2 = try writer.append(test_op, "link1");
    const seq3 = try writer.append(test_op, "cat1-updated");

    try std.testing.expectEqual(@as(u64, 1), seq1);
    try std.testing.expectEqual(@as(u64, 2), seq2);
    try std.testing.expectEqual(@as(u64, 3), seq3);
    try std.testing.expectEqual(@as(u64, 3), writer.getSequence());
}

test "sync flushes to disk" {
    const tmp_dir = "/tmp/wal_test_sync";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    try std.fs.makeDirAbsolute(tmp_dir);
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    {
        const writer = try initHeap(std.testing.allocator, tmp_dir, 32, 0);
        defer deinitHeap(writer);

        _ = try writer.append(test_op, "data1");
        _ = try writer.append(test_op, "data2");
        try writer.sync();
    }

    {
        const writer2 = try initHeap(std.testing.allocator, tmp_dir, 32, 0);
        defer deinitHeap(writer2);

        try std.testing.expectEqual(@as(u64, 2), writer2.getSequence());

        const seq3 = try writer2.append(test_op, "data3");
        try std.testing.expectEqual(@as(u64, 3), seq3);
    }
}

test "batch auto-flush on reaching batch_size" {
    const tmp_dir = "/tmp/wal_test_batch";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    std.fs.makeDirAbsolute(tmp_dir) catch {};
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    const writer = try initHeap(std.testing.allocator, tmp_dir, 3, 0);
    defer deinitHeap(writer);

    _ = try writer.append(test_op, "a");
    _ = try writer.append(test_op, "b");
    {
        writer.lock.lock();
        const has_data = writer.front.items.len > 0;
        writer.lock.unlock();
        try std.testing.expect(has_data);
    }

    _ = try writer.append(test_op, "c");

    std.Thread.sleep(20 * std.time.ns_per_ms);

    {
        writer.lock.lock();
        const front_empty = writer.front.items.len == 0;
        const count_zero = writer.entry_count == 0;
        writer.lock.unlock();
        try std.testing.expect(front_empty);
        try std.testing.expect(count_zero);
    }
}

test "async WAL: concurrent writers" {
    const tmp_dir = "/tmp/wal_test_concurrent";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    std.fs.makeDirAbsolute(tmp_dir) catch {};
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    const writer = try initHeap(std.testing.allocator, tmp_dir, 16, 0);
    defer deinitHeap(writer);

    const Writer = struct {
        fn run(w: *WalWriter) void {
            var i: usize = 0;
            while (i < 500) : (i += 1) {
                _ = w.append(test_op, "concurrent-data") catch {};
            }
        }
    };

    const N = 4;
    var threads: [N]std.Thread = undefined;
    for (&threads) |*t| {
        t.* = try std.Thread.spawn(.{}, Writer.run, .{writer});
    }
    for (&threads) |t| t.join();

    try writer.sync();

    try std.testing.expectEqual(@as(u64, 2000), writer.getSequence());
}

test "async WAL: deinit flushes all pending entries" {
    const tmp_dir = "/tmp/wal_test_deinit";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    std.fs.makeDirAbsolute(tmp_dir) catch {};
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    {
        const writer = try initHeap(std.testing.allocator, tmp_dir, 1000, 0);
        var i: u32 = 0;
        while (i < 50) : (i += 1) {
            _ = try writer.append(test_op, "pending-data");
        }
        deinitHeap(writer);
    }

    {
        const writer2 = try initHeap(std.testing.allocator, tmp_dir, 32, 0);
        defer deinitHeap(writer2);
        try std.testing.expectEqual(@as(u64, 50), writer2.getSequence());
    }
}

test "truncateAfterCheckpoint zeroes the WAL and preserves the sequence" {
    const tmp_dir = "/tmp/wal_test_truncate_checkpoint";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    std.fs.makeDirAbsolute(tmp_dir) catch {};
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    {
        const writer = try initHeap(std.testing.allocator, tmp_dir, 32, 0);
        defer deinitHeap(writer);

        _ = try writer.append(test_op, "cat-a");
        _ = try writer.append(test_op, "link-a");
        _ = try writer.append(test_op, "cat-a-v2");
        try std.testing.expectEqual(@as(u64, 3), writer.getSequence());

        try writer.truncateAfterCheckpoint();

        try std.testing.expectEqual(@as(u64, 3), writer.getSequence());

        const path = try std.fs.path.join(std.testing.allocator, &.{ tmp_dir, "wal.bin" });
        defer std.testing.allocator.free(path);
        const scan_file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
        defer scan_file.close();
        const recovered = try WalWriter.scanWal(scan_file, std.testing.allocator);
        try std.testing.expectEqual(@as(u64, 0), recovered.last_sequence);

        const seq = try writer.append(test_op, "post-truncate");
        try std.testing.expectEqual(@as(u64, 4), seq);
    }

    {
        const writer2 = try initHeap(std.testing.allocator, tmp_dir, 32, 0);
        defer deinitHeap(writer2);
        try std.testing.expectEqual(@as(u64, 4), writer2.getSequence());
    }
}

test "init resumes the sequence from base_sequence over an empty WAL" {
    const tmp_dir = "/tmp/wal_test_base_sequence";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    std.fs.makeDirAbsolute(tmp_dir) catch {};
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    const writer = try initHeap(std.testing.allocator, tmp_dir, 32, 17);
    defer deinitHeap(writer);

    try std.testing.expectEqual(@as(u64, 17), writer.getSequence());
    const seq = try writer.append(test_op, "resumed");
    try std.testing.expectEqual(@as(u64, 18), seq);
}

test "retain floor blocks truncation until replicas ack the full WAL" {
    const tmp_dir = "/tmp/wal_test_retain_floor";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    std.fs.makeDirAbsolute(tmp_dir) catch {};
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    const writer = try initHeap(std.testing.allocator, tmp_dir, 32, 0);
    defer deinitHeap(writer);

    _ = try writer.append(test_op, "a");
    _ = try writer.append(test_op, "b");
    _ = try writer.append(test_op, "c");

    writer.setRetainFloor(2);
    try std.testing.expectError(error.WalRetainedByReplica, writer.truncateAfterCheckpoint());

    writer.setRetainFloor(3);
    try writer.truncateAfterCheckpoint();
    try std.testing.expectEqual(@as(u64, 3), writer.getSequence());
}

test "post-truncate appends carry monotonic sequences and survive reopen" {
    const tmp_dir = "/tmp/wal_test_post_truncate_monotonic";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    std.fs.makeDirAbsolute(tmp_dir) catch {};
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    var final_seq: u64 = 0;
    {
        const writer = try initHeap(std.testing.allocator, tmp_dir, 32, 0);
        defer deinitHeap(writer);

        _ = try writer.append(test_op, "pre-1");
        _ = try writer.append(test_op, "pre-2");
        try writer.truncateAfterCheckpoint();

        try std.testing.expectEqual(@as(u64, 3), try writer.append(test_op, "post-1"));
        try std.testing.expectEqual(@as(u64, 4), try writer.append(test_op, "post-2"));
        try writer.sync();
        final_seq = writer.getSequence();
    }

    {
        const writer2 = try initHeap(std.testing.allocator, tmp_dir, 32, final_seq);
        defer deinitHeap(writer2);
        try std.testing.expectEqual(final_seq, writer2.getSequence());
        try std.testing.expectEqual(final_seq + 1, try writer2.append(test_op, "post-reopen"));
    }
}

test "direct-io: restart after appends does not orphan later entries" {
    const wal_replay = @import("wal_replay.zig");

    const tmp_dir = "/tmp/wal_test_restart_no_orphan";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    std.fs.makeDirAbsolute(tmp_dir) catch {};
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    {
        const writer = try initHeap(std.testing.allocator, tmp_dir, 32, 0);
        _ = try writer.append(test_op, "session1-a");
        _ = try writer.append(test_op, "session1-b");
        deinitHeap(writer);
    }

    {
        const writer = try initHeap(std.testing.allocator, tmp_dir, 32, 0);
        _ = try writer.append(test_op, "session2-c");
        deinitHeap(writer);
    }

    {
        const writer = try initHeap(std.testing.allocator, tmp_dir, 32, 0);
        defer deinitHeap(writer);
        try std.testing.expectEqual(@as(u64, 3), writer.getSequence());
    }

    const reader_opt = try wal_replay.WalReader.init(tmp_dir);
    var reader = reader_opt.?;
    defer reader.close();
    var count: u64 = 0;
    while (try reader.next()) |entry| {
        count += 1;
        try std.testing.expectEqual(count, entry.sequence);
    }
    try std.testing.expectEqual(@as(u64, 3), count);
}

const StressStats = struct {
    total_appends: std.atomic.Value(u64) = .{ .raw = 0 },
    total_truncates: std.atomic.Value(u64) = .{ .raw = 0 },
    total_syncs: std.atomic.Value(u64) = .{ .raw = 0 },
};

const StressCtx = struct {
    writer: *WalWriter,
    stats: *StressStats,
    should_stop: *std.atomic.Value(bool),
};

fn stressAppenderRun(ctx: StressCtx) void {
    var payload: [8]u8 = undefined;
    while (!ctx.should_stop.load(.acquire)) {
        const seq = ctx.writer.append(test_op, &payload) catch {
            return;
        };
        _ = seq;
        _ = ctx.stats.total_appends.fetchAdd(1, .monotonic);
    }
}

fn stressTruncateRun(ctx: StressCtx) void {
    var prng = std.Random.DefaultPrng.init(0xABCDEF01);
    while (!ctx.should_stop.load(.acquire)) {
        const jitter_ms = prng.random().intRangeAtMost(u64, 30, 80);
        std.Thread.sleep(jitter_ms * std.time.ns_per_ms);

        if (ctx.should_stop.load(.acquire)) break;
        ctx.writer.truncateAfterCheckpoint() catch return;
        _ = ctx.stats.total_truncates.fetchAdd(1, .monotonic);
    }
}

fn stressSyncRun(ctx: StressCtx) void {
    var prng = std.Random.DefaultPrng.init(0x13579BDF);
    while (!ctx.should_stop.load(.acquire)) {
        const jitter_ms = prng.random().intRangeAtMost(u64, 5, 25);
        std.Thread.sleep(jitter_ms * std.time.ns_per_ms);

        if (ctx.should_stop.load(.acquire)) break;
        ctx.writer.sync() catch return;
        _ = ctx.stats.total_syncs.fetchAdd(1, .monotonic);
    }
}

test "stress: concurrent append + sync + truncate, then recovery contract" {
    const wal_replay = @import("wal_replay.zig");

    const tmp_dir = "/tmp/wal_stress_v2";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    try std.fs.makeDirAbsolute(tmp_dir);
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    const writer = try initHeap(std.testing.allocator, tmp_dir, 64, 0);
    defer deinitHeap(writer);

    var stats = StressStats{};
    var should_stop = std.atomic.Value(bool).init(false);

    const ctx = StressCtx{
        .writer = writer,
        .stats = &stats,
        .should_stop = &should_stop,
    };

    const N_APPENDERS = 4;
    var appender_threads: [N_APPENDERS]std.Thread = undefined;
    for (&appender_threads) |*t| {
        t.* = try std.Thread.spawn(.{}, stressAppenderRun, .{ctx});
    }
    const truncate_thread = try std.Thread.spawn(.{}, stressTruncateRun, .{ctx});
    const sync_thread = try std.Thread.spawn(.{}, stressSyncRun, .{ctx});

    std.Thread.sleep(1500 * std.time.ns_per_ms);
    should_stop.store(true, .release);

    for (&appender_threads) |*t| t.join();
    truncate_thread.join();
    sync_thread.join();

    try writer.sync();
    const final_seq = writer.getSequence();

    {
        var reader_opt = try wal_replay.WalReader.init(tmp_dir);
        if (reader_opt) |*reader| {
            defer reader.close();
            var last_seen: u64 = 0;
            while (try reader.next()) |entry| {
                if (last_seen != 0) {
                    try std.testing.expectEqual(last_seen + 1, entry.sequence);
                }
                try std.testing.expectEqual(@as(usize, 8), entry.data.len);
                last_seen = entry.sequence;
            }
            if (last_seen != 0) {
                try std.testing.expectEqual(final_seq, last_seen);
            }
        } else {
            try std.testing.expectEqual(@as(u64, 0), final_seq);
        }
    }

    {
        const writer2 = try initHeap(std.testing.allocator, tmp_dir, 64, final_seq);
        defer deinitHeap(writer2);
        try std.testing.expectEqual(final_seq, writer2.getSequence());
        const next_payload = [_]u8{0} ** 8;
        const new_seq = try writer2.append(test_op, &next_payload);
        try std.testing.expectEqual(final_seq + 1, new_seq);
        try writer2.sync();
    }

    try std.testing.expect(stats.total_appends.load(.acquire) > 100);
    try std.testing.expect(stats.total_truncates.load(.acquire) >= 3);
    try std.testing.expect(stats.total_syncs.load(.acquire) >= 3);
}
