const std = @import("std");
const posix = std.posix;

pub const Phase = enum(u8) {
    empty,
    reading_request,
    writing_response,
};

pub const REQUEST_BUF_SIZE = 65536;
pub const RESPONSE_BUF_SIZE = 262144;

pub const BufferPair = struct {
    request_buf: [REQUEST_BUF_SIZE]u8 = undefined,
    response_buf: [RESPONSE_BUF_SIZE]u8 = undefined,
};

pub const Connection = struct {
    fd: posix.fd_t = -1,
    phase: Phase = .empty,
    buf: ?*BufferPair = null,
    bytes_read: usize = 0,
    bytes_written: usize = 0,
    response_len: usize = 0,
    last_activity: i64 = 0,
    armed_for_write: bool = false,

    pub fn reset(self: *Connection) void {
        self.fd = -1;
        self.phase = .empty;
        self.buf = null;
        self.bytes_read = 0;
        self.bytes_written = 0;
        self.response_len = 0;
        self.last_activity = 0;
        self.armed_for_write = false;
    }

    pub fn isActive(self: *const Connection) bool {
        return self.phase != .empty;
    }

    pub fn writeChunk(self: *Connection) !bool {
        if (self.bytes_written >= self.response_len) return true;

        const bp = self.buf orelse return error.NotOpenForWriting;
        const remaining = bp.response_buf[self.bytes_written..self.response_len];
        const n = posix.write(self.fd, remaining) catch |err| switch (err) {
            error.WouldBlock => return false,
            else => return err,
        };

        if (n == 0) return error.ConnectionReset;

        self.bytes_written += n;
        return self.bytes_written >= self.response_len;
    }
};

test "Connection default state" {
    const conn = Connection{};
    try std.testing.expect(!conn.isActive());
    try std.testing.expectEqual(Phase.empty, conn.phase);
    try std.testing.expectEqual(@as(posix.fd_t, -1), conn.fd);
    try std.testing.expectEqual(@as(?*BufferPair, null), conn.buf);
}

test "Connection reset" {
    var bp = BufferPair{};
    var conn = Connection{};
    conn.fd = 5;
    conn.phase = .reading_request;
    conn.buf = &bp;
    conn.bytes_read = 100;
    conn.reset();
    try std.testing.expect(!conn.isActive());
    try std.testing.expectEqual(@as(posix.fd_t, -1), conn.fd);
    try std.testing.expectEqual(@as(usize, 0), conn.bytes_read);
    try std.testing.expectEqual(@as(?*BufferPair, null), conn.buf);
}

test "Connection last_activity resets to zero" {
    var conn = Connection{};
    conn.fd = 3;
    conn.phase = .reading_request;
    conn.last_activity = 1000;
    conn.reset();
    try std.testing.expectEqual(@as(i64, 0), conn.last_activity);
}
