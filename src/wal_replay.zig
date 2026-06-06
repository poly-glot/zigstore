const std = @import("std");
const wal = @import("wal.zig");

const MAX_REPLAY_DATA_LEN: usize = 16 * 1024 * 1024;

const BLOCK_SIZE: usize = 4096;

pub const ReplayEntry = struct {
    sequence: u64,
    op_code: wal.OpCode,
    data: []const u8,
};

pub const WalReader = struct {
    file: std.fs.File,
    buf: []u8,
    buf_len: usize = 0,
    pos: usize = 0,
    file_pos: u64 = 0,

    const INITIAL_BUF_LEN: usize = 65536;
    const MAX_BUF_LEN: usize = MAX_REPLAY_DATA_LEN + wal.HEADER_SIZE;

    pub fn init(dir: []const u8) !?WalReader {
        const path = try std.fs.path.join(std.heap.page_allocator, &.{ dir, "wal.bin" });
        defer std.heap.page_allocator.free(path);

        const file = std.fs.cwd().openFile(path, .{ .mode = .read_only }) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        errdefer file.close();

        const buf = try std.heap.page_allocator.alloc(u8, INITIAL_BUF_LEN);
        return WalReader{
            .file = file,
            .buf = buf,
        };
    }

    pub fn next(self: *WalReader) !?ReplayEntry {
        const header_size = wal.HEADER_SIZE;

        while (true) {
            try self.ensureBuffered(header_size);
            if (self.available() < header_size) return null;

            var header_aligned: wal.WalEntryHeader align(@alignOf(wal.WalEntryHeader)) = undefined;
            const dst: *[header_size]u8 = @ptrCast(&header_aligned);
            @memcpy(dst, self.buf[self.pos..][0..header_size]);

            if (header_aligned.sequence == 0) return null;

            if (header_aligned.data_len > MAX_REPLAY_DATA_LEN) {
                if (!try self.skipToNextBlock()) return null;
                continue;
            }

            const data_len: usize = header_aligned.data_len;
            const total_entry_size = header_size + data_len;

            try self.ensureBuffered(total_entry_size);
            if (self.available() < total_entry_size) return null;

            const data = self.buf[self.pos + header_size ..][0..data_len];

            const computed_crc = std.hash.crc.Crc32.hash(data);
            if (computed_crc != header_aligned.checksum) {
                return null;
            }

            const op_code: wal.OpCode = std.meta.intToEnum(wal.OpCode, header_aligned.op_code) catch {
                return null;
            };

            const entry = ReplayEntry{
                .sequence = header_aligned.sequence,
                .op_code = op_code,
                .data = data,
            };

            self.pos += total_entry_size;
            self.file_pos += total_entry_size;

            return entry;
        }
    }

    fn skipToNextBlock(self: *WalReader) !bool {
        const next_block: u64 = std.mem.alignForward(u64, self.file_pos + 1, BLOCK_SIZE);

        const end_pos: u64 = self.file.getEndPos() catch return false;
        if (next_block >= end_pos) return false;

        try self.file.seekTo(next_block);
        self.buf_len = 0;
        self.pos = 0;
        self.file_pos = next_block;
        return true;
    }

    pub fn close(self: *WalReader) void {
        std.heap.page_allocator.free(self.buf);
        self.file.close();
    }

    fn available(self: *const WalReader) usize {
        return self.buf_len - self.pos;
    }

    fn ensureBuffered(self: *WalReader, needed: usize) !void {
        if (self.available() >= needed) return;

        if (needed > self.buf.len) {
            const want = @min(@max(needed, self.buf.len *| 2), MAX_BUF_LEN);
            if (want >= needed) {
                self.buf = try std.heap.page_allocator.realloc(self.buf, want);
            }
        }

        const remaining = self.available();
        if (remaining > 0) {
            std.mem.copyForwards(u8, self.buf[0..remaining], self.buf[self.pos..self.buf_len]);
        }
        self.buf_len = remaining;
        self.pos = 0;

        while (self.buf_len < self.buf.len) {
            const n = try self.file.read(self.buf[self.buf_len..]);
            if (n == 0) break;
            self.buf_len += n;
            if (self.buf_len >= needed) break;
        }
    }
};

pub fn replayWal(dir: []const u8, min_sequence: u64, callback: anytype) !u64 {
    comptime {
        if (!@hasDecl(@TypeOf(callback.*), "apply")) {
            @compileError("replayWal callback must have a pub fn apply(self, ReplayEntry) !void method");
        }
    }
    var reader = (try WalReader.init(dir)) orelse return 0;
    defer reader.close();

    var last_seq: u64 = 0;

    while (try reader.next()) |entry| {
        if (entry.sequence > min_sequence) {
            try callback.apply(entry);
        }
        last_seq = entry.sequence;
    }

    return last_seq;
}

test "write and read back WAL entries" {
    const tmp_dir = "/tmp/wal_replay_test_rw";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    std.fs.makeDirAbsolute(tmp_dir) catch {};
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    {
        var writer = try wal.WalWriter.init(std.testing.allocator, tmp_dir, 32);
        defer writer.deinit();

        _ = try writer.append(.changeset, "category-one");
        _ = try writer.append(.changeset, "link-data-here");
        _ = try writer.append(.changeset, "updated-category");
        try writer.sync();
    }

    {
        var reader = (try WalReader.init(tmp_dir)).?;
        defer reader.close();

        const e1 = (try reader.next()).?;
        try std.testing.expectEqual(@as(u64, 1), e1.sequence);
        try std.testing.expectEqual(wal.OpCode.changeset, e1.op_code);
        try std.testing.expectEqualSlices(u8, "category-one", e1.data);

        const e2 = (try reader.next()).?;
        try std.testing.expectEqual(@as(u64, 2), e2.sequence);
        try std.testing.expectEqual(wal.OpCode.changeset, e2.op_code);
        try std.testing.expectEqualSlices(u8, "link-data-here", e2.data);

        const e3 = (try reader.next()).?;
        try std.testing.expectEqual(@as(u64, 3), e3.sequence);
        try std.testing.expectEqual(wal.OpCode.changeset, e3.op_code);
        try std.testing.expectEqualSlices(u8, "updated-category", e3.data);

        const e4_opt = try reader.next();
        try std.testing.expect(e4_opt == null);
    }
}

test "reader returns null for nonexistent WAL" {
    const reader = try WalReader.init("/tmp/wal_replay_nonexistent_dir_12345");
    try std.testing.expect(reader == null);
}

test "replay with min_sequence filter" {
    const tmp_dir = "/tmp/wal_replay_test_filter";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    std.fs.makeDirAbsolute(tmp_dir) catch {};
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    {
        var writer = try wal.WalWriter.init(std.testing.allocator, tmp_dir, 32);
        defer writer.deinit();

        _ = try writer.append(.changeset, "first");
        _ = try writer.append(.changeset, "second");
        _ = try writer.append(.changeset, "third");
        _ = try writer.append(.changeset, "fourth");
        try writer.sync();
    }

    const Collector = struct {
        count: usize = 0,

        pub fn apply(self: *@This(), entry: ReplayEntry) !void {
            _ = entry;
            self.count += 1;
        }
    };

    var collector = Collector{};
    const last_seq = try replayWal(tmp_dir, 2, &collector);
    try std.testing.expectEqual(@as(u64, 4), last_seq);
    try std.testing.expectEqual(@as(usize, 2), collector.count);
}

test "corrupted entry stops replay" {
    const tmp_dir = "/tmp/wal_replay_test_corrupt";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    std.fs.makeDirAbsolute(tmp_dir) catch {};
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    {
        var writer = try wal.WalWriter.init(std.testing.allocator, tmp_dir, 32);
        defer writer.deinit();

        _ = try writer.append(.changeset, "valid-entry");
        try writer.sync();
    }

    {
        const path = try std.fs.path.join(std.testing.allocator, &.{ tmp_dir, "wal.bin" });
        defer std.testing.allocator.free(path);

        const file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
        defer file.close();

        const end = try file.getEndPos();
        try file.seekTo(end);

        const bad_header = wal.WalEntryHeader{
            .sequence = 99,
            .op_code = @intFromEnum(wal.OpCode.changeset),
            .data_len = 5,
            .checksum = 0xDEADBEEF,
        };
        const header_bytes: *const [wal.HEADER_SIZE]u8 = @ptrCast(&bad_header);
        try file.writeAll(header_bytes);
        try file.writeAll("hello");
    }

    {
        var reader = (try WalReader.init(tmp_dir)).?;
        defer reader.close();

        const e1 = (try reader.next()).?;
        try std.testing.expectEqual(@as(u64, 1), e1.sequence);
        try std.testing.expectEqualSlices(u8, "valid-entry", e1.data);

        const e2 = try reader.next();
        try std.testing.expect(e2 == null);
    }
}

const TestCollector = struct {
    seqs: [256]u64 = undefined,
    count: usize = 0,

    pub fn apply(self: *@This(), entry: ReplayEntry) !void {
        if (self.count >= self.seqs.len) return;
        self.seqs[self.count] = entry.sequence;
        self.count += 1;
    }
};

test "wal_replay: empty WAL replays cleanly" {
    const tmp_dir = "/tmp/wal_replay_test_empty";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    std.fs.makeDirAbsolute(tmp_dir) catch {};
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    const path = try std.fs.path.join(std.testing.allocator, &.{ tmp_dir, "wal.bin" });
    defer std.testing.allocator.free(path);
    const f = try std.fs.cwd().createFile(path, .{});
    f.close();

    var collector = TestCollector{};
    const last = try replayWal(tmp_dir, 0, &collector);
    try std.testing.expectEqual(@as(u64, 0), last);
    try std.testing.expectEqual(@as(usize, 0), collector.count);
}

test "wal_replay: single entry replays once with correct payload" {
    const tmp_dir = "/tmp/wal_replay_test_single";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    std.fs.makeDirAbsolute(tmp_dir) catch {};
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    {
        var writer = try wal.WalWriter.init(std.testing.allocator, tmp_dir, 32);
        defer writer.deinit();
        _ = try writer.append(.changeset, "only-entry");
        try writer.sync();
    }

    const Capture = struct {
        seen_seq: u64 = 0,
        seen_data: [64]u8 = undefined,
        seen_data_len: usize = 0,

        pub fn apply(self: *@This(), entry: ReplayEntry) !void {
            self.seen_seq = entry.sequence;
            @memcpy(self.seen_data[0..entry.data.len], entry.data);
            self.seen_data_len = entry.data.len;
        }
    };
    var cap = Capture{};
    const last = try replayWal(tmp_dir, 0, &cap);
    try std.testing.expectEqual(@as(u64, 1), last);
    try std.testing.expectEqual(@as(u64, 1), cap.seen_seq);
    try std.testing.expectEqualSlices(u8, "only-entry", cap.seen_data[0..cap.seen_data_len]);
}

test "wal_replay: multi-entry replay preserves sequence order" {
    const tmp_dir = "/tmp/wal_replay_test_multi";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    std.fs.makeDirAbsolute(tmp_dir) catch {};
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    {
        var writer = try wal.WalWriter.init(std.testing.allocator, tmp_dir, 32);
        defer writer.deinit();
        var buf: [16]u8 = undefined;
        for (0..50) |i| {
            const data = try std.fmt.bufPrint(&buf, "ent-{d}", .{i});
            _ = try writer.append(.changeset, data);
        }
        try writer.sync();
    }

    var collector = TestCollector{};
    const last = try replayWal(tmp_dir, 0, &collector);
    try std.testing.expectEqual(@as(u64, 50), last);
    try std.testing.expectEqual(@as(usize, 50), collector.count);
    for (collector.seqs[0..collector.count], 0..) |seq, i| {
        try std.testing.expectEqual(@as(u64, i + 1), seq);
    }
}

test "wal_replay: torn tail discards partial entry without error" {
    const tmp_dir = "/tmp/wal_replay_test_torntail";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    std.fs.makeDirAbsolute(tmp_dir) catch {};
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    {
        var writer = try wal.WalWriter.init(std.testing.allocator, tmp_dir, 32);
        defer writer.deinit();
        for (0..10) |i| {
            var buf: [16]u8 = undefined;
            const data = try std.fmt.bufPrint(&buf, "row-{d}", .{i});
            _ = try writer.append(.changeset, data);
        }
        try writer.sync();
    }

    {
        const path = try std.fs.path.join(std.testing.allocator, &.{ tmp_dir, "wal.bin" });
        defer std.testing.allocator.free(path);
        const f = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
        defer f.close();
        try f.setEndPos(9 * 29 + 5);
    }

    var collector = TestCollector{};
    const last = try replayWal(tmp_dir, 0, &collector);
    try std.testing.expectEqual(@as(usize, 9), collector.count);
    try std.testing.expectEqual(@as(u64, 9), last);
}

test "wal_replay: idempotent on second pass" {
    const tmp_dir = "/tmp/wal_replay_test_idem";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    std.fs.makeDirAbsolute(tmp_dir) catch {};
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    {
        var writer = try wal.WalWriter.init(std.testing.allocator, tmp_dir, 32);
        defer writer.deinit();
        _ = try writer.append(.changeset, "first");
        _ = try writer.append(.changeset, "second");
        _ = try writer.append(.changeset, "third");
        try writer.sync();
    }

    var c1 = TestCollector{};
    _ = try replayWal(tmp_dir, 0, &c1);
    var c2 = TestCollector{};
    _ = try replayWal(tmp_dir, 0, &c2);

    try std.testing.expectEqual(c1.count, c2.count);
    for (c1.seqs[0..c1.count], c2.seqs[0..c2.count]) |a, b| {
        try std.testing.expectEqual(a, b);
    }
}

test "wal_replay: oversized entry terminates replay cleanly" {
    const tmp_dir = "/tmp/wal_replay_test_toolarge";
    std.fs.deleteTreeAbsolute(tmp_dir) catch {};
    std.fs.makeDirAbsolute(tmp_dir) catch {};
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    const path = try std.fs.path.join(std.testing.allocator, &.{ tmp_dir, "wal.bin" });
    defer std.testing.allocator.free(path);
    const f = try std.fs.cwd().createFile(path, .{ .read = true, .truncate = true });
    defer f.close();

    const bogus_header = wal.WalEntryHeader{
        .sequence = 1,
        .op_code = @intFromEnum(wal.OpCode.changeset),
        .data_len = 100 * 1024 * 1024,
        .checksum = 0,
    };
    const header_bytes: *const [wal.HEADER_SIZE]u8 = @ptrCast(&bogus_header);
    try f.writeAll(header_bytes);

    var reader = (try WalReader.init(tmp_dir)).?;
    defer reader.close();
    try std.testing.expectEqual(@as(?ReplayEntry, null), try reader.next());
}

const FuzzNoopApplier = struct {
    pub fn apply(self: *FuzzNoopApplier, entry: ReplayEntry) !void {
        _ = self;
        _ = entry;
    }
};

fn fuzzWalReaderOne(_: void, input: []const u8) anyerror!void {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    {
        const f = try tmp.dir.createFile("wal.bin", .{});
        defer f.close();
        try f.writeAll(input);
    }
    const dir_path = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(dir_path);

    var applier = FuzzNoopApplier{};
    _ = replayWal(dir_path, 0, &applier) catch {};
}

test "fuzz: WAL reader tolerates arbitrary file bytes" {
    try std.testing.fuzz({}, fuzzWalReaderOne, .{
        .corpus = &.{
            &.{},
            &[_]u8{ 1, 0, 0, 0, 0, 0, 0, 0 },
            &[_]u8{ 0xFF, 0xFF, 0xFF, 0xFF },
        },
    });
}
