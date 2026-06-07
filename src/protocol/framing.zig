const std = @import("std");
const connection = @import("../connection.zig");
const histogram = @import("../histogram.zig");

/// Request frame header: `[u32 total_len][u8 op][u8 reserved][u16 count]`.
pub const REQUEST_HEADER_SIZE: usize = 8;

/// Response frame header: `[u32 total_len][u8 op][u8 status][u8 sub_status][u8 reserved][u16 count]`.
pub const RESPONSE_HEADER_SIZE: usize = 10;

/// The number of leading bytes a reader must have buffered before a request
/// frame's `total_len` can be read. Aliases `REQUEST_HEADER_SIZE`.
pub const HEADER_SIZE: usize = REQUEST_HEADER_SIZE;

/// Base response status codes. Applications extend the wire space with raw
/// `u8` values at or above `5`; this enum reserves only the low codes.
pub const Status = enum(u8) {
    ok = 0,
    not_found = 1,
    duplicate = 2,
    invalid = 3,
    err = 4,
};

fn writeResponseHeader(
    buf: []u8,
    total_len: u32,
    op: u8,
    status: u8,
    sub_status: u8,
    count: u16,
) void {
    std.mem.writeInt(u32, buf[0..4], total_len, .little);
    buf[4] = op;
    buf[5] = status;
    buf[6] = sub_status;
    buf[7] = 0;
    std.mem.writeInt(u16, buf[8..10], count, .little);
}

/// Stamp the 10-byte response header in place with raw `status_byte`,
/// `sub_byte`, `total_len`, and `count`. The single public header primitive
/// applications use to finalize a manually built frame whose status falls
/// outside the base `Status` enum's low range.
pub fn writeRawHeader(buf: []u8, op: u8, total_len: u32, status_byte: u8, sub_byte: u8, count: u16) void {
    writeResponseHeader(buf, total_len, op, status_byte, sub_byte, count);
}

/// Finalize a manually built response frame: write the 10-byte header in place
/// with `status = ok`, `sub_status = 0`, the given `total_len`, and `count`.
/// Used by handlers that assemble a custom body before stamping the header.
pub fn writeOkHeader(buf: []u8, op: u8, total_len: u32, count: u16) void {
    writeRawHeader(buf, op, total_len, @intFromEnum(Status.ok), 0, count);
}

/// Write an ok/data response: header followed by `payload`, tagged with `count`.
/// Returns the total bytes written, or `0` if `buf` is too small.
pub fn writeResp(buf: []u8, op: u8, status: Status, count: u16, payload: []const u8) usize {
    const total: usize = RESPONSE_HEADER_SIZE + payload.len;
    if (buf.len < total) {
        return 0;
    }
    writeResponseHeader(buf, @intCast(total), op, @intFromEnum(status), 0, count);
    if (payload.len > 0) @memcpy(buf[RESPONSE_HEADER_SIZE..][0..payload.len], payload);
    return total;
}

/// Write a header-only error response with `sub_status = 0`.
pub fn writeErrorResp(buf: []u8, op: u8, status: Status) usize {
    return writeErrorRespSub(buf, op, status, 0);
}

/// Write a header-only error response carrying an application `sub` code.
pub fn writeErrorRespSub(buf: []u8, op: u8, status: Status, sub: u8) usize {
    return writeRawErrorResp(buf, op, @intFromEnum(status), sub);
}

/// Write a header-only error response carrying a raw `status_byte` (for an
/// application status code outside the base `Status` enum's low range) and an
/// application `sub` code.
pub fn writeRawErrorResp(buf: []u8, op: u8, status_byte: u8, sub: u8) usize {
    const total: u32 = @intCast(RESPONSE_HEADER_SIZE);
    writeRawHeader(buf, op, total, status_byte, sub, 0);
    return total;
}

/// Read a length-prefixed optional string when `mask & bit` is set, advancing
/// `off`. Returns `null` on a truncated frame, `?[]const u8{null}` when the bit
/// is clear, or the slice when present.
pub fn readOptionalString(payload: []const u8, off: *usize, mask: u8, bit: u8) ?(?[]const u8) {
    if (mask & bit == 0) return @as(?[]const u8, null);
    if (off.* + 2 > payload.len) return null;
    const len = std.mem.readInt(u16, payload[off.*..][0..2], .little);
    off.* += 2;
    if (off.* + len > payload.len) return null;
    const s = payload[off.*..][0..len];
    off.* += len;
    return s;
}

/// Pack a contiguous list of fixed-layout records into a response frame, one
/// `@sizeOf(T)` block per item, stopping when `resp` is full. Returns the total
/// bytes written; the header `count` reflects how many records fit.
pub fn writeRowList(comptime T: type, resp: []u8, op_byte: u8, items: []const T) usize {
    var off: usize = RESPONSE_HEADER_SIZE;
    var written_count: u16 = 0;
    for (items) |item| {
        const bytes = std.mem.asBytes(&item);
        if (off + bytes.len > resp.len) break;
        @memcpy(resp[off..][0..bytes.len], bytes);
        off += bytes.len;
        written_count += 1;
    }
    writeResponseHeader(resp, @intCast(off), op_byte, @intFromEnum(Status.ok), 0, written_count);
    return off;
}

fn ReadResult(comptime fields: []const struct { []const u8, type }) type {
    var sf: [fields.len]std.builtin.Type.StructField = undefined;
    for (fields, 0..) |f, i| {
        sf[i] = .{
            .name = @ptrCast(f[0]),
            .type = f[1],
            .default_value_ptr = null,
            .is_comptime = false,
            .alignment = @alignOf(f[1]),
        };
    }
    return @Type(.{ .@"struct" = .{
        .layout = .auto,
        .fields = &sf,
        .decls = &.{},
        .is_tuple = false,
    } });
}

/// Result of `parsePayload`: the decoded struct plus the unconsumed tail.
pub fn ParsedPayload(comptime fields: []const struct { []const u8, type }) type {
    return struct {
        result: ReadResult(fields),
        rest: []const u8,
    };
}

/// Decode a TLV payload into a struct shaped by `fields`. Supports `u64`, `u32`,
/// and length-prefixed `[]const u8`. Returns `null` on a truncated frame.
pub fn parsePayload(
    comptime fields: []const struct { []const u8, type },
    data: []const u8,
) ?ParsedPayload(fields) {
    var off: usize = 0;
    var result: ReadResult(fields) = undefined;

    inline for (fields) |f| {
        const name: [:0]const u8 = @ptrCast(f[0]);
        const T = f[1];

        if (T == u64) {
            if (off + 8 > data.len) return null;
            @field(result, name) = std.mem.readInt(u64, data[off..][0..8], .little);
            off += 8;
        } else if (T == u32) {
            if (off + 4 > data.len) return null;
            @field(result, name) = std.mem.readInt(u32, data[off..][0..4], .little);
            off += 4;
        } else if (T == []const u8) {
            if (off + 2 > data.len) return null;
            const len = std.mem.readInt(u16, data[off..][0..2], .little);
            off += 2;
            if (off + len > data.len) return null;
            @field(result, name) = data[off..][0..len];
            off += len;
        } else {
            @compileError("parsePayload: unsupported type for field '" ++ f[0] ++ "'");
        }
    }

    return .{ .result = result, .rest = data[off..] };
}

/// Skip one TLV record shaped by `fields` without decoding it. Returns the
/// unconsumed tail, or `null` on a truncated frame.
pub fn advancePayload(
    comptime fields: []const struct { []const u8, type },
    data: []const u8,
) ?[]const u8 {
    var off: usize = 0;
    inline for (fields) |f| {
        const T = f[1];
        if (T == u64) {
            if (off + 8 > data.len) return null;
            off += 8;
        } else if (T == u32) {
            if (off + 4 > data.len) return null;
            off += 4;
        } else if (T == []const u8) {
            if (off + 2 > data.len) return null;
            const len = std.mem.readInt(u16, data[off..][0..2], .little);
            off += 2;
            if (off + len > data.len) return null;
            off += len;
        }
    }
    return data[off..];
}

/// One frame's worth of response-buffer headroom the pipeline reserves before
/// starting another frame. Sized to the largest single response the engine
/// guarantees room for; an empty buffer always admits one frame.
pub const RESPONSE_RESERVE: usize = 64 * 1024;

fn pipelineHasRoom(resp_off: usize, buf_len: usize) bool {
    return resp_off == 0 or buf_len - resp_off >= RESPONSE_RESERVE;
}

/// Drive the pipelined request/response loop over one connection's buffers.
///
/// Each complete request frame in `conn`'s read buffer is decoded down to its
/// raw `op_byte`, `count`, and `payload`; its dispatch latency is recorded into
/// `op_latency[op_byte]`; and `dispatch_fn` is invoked with the opaque `ctx` to
/// produce the response into the connection's write buffer. The raw `op_byte`
/// is passed through verbatim — this loop never maps it to an application op
/// enum. Consumed request bytes are compacted to the front; `conn.response_len`
/// is set to the total framed response size.
pub fn processFrames(
    ctx: *anyopaque,
    conn: *connection.Connection,
    dispatch_fn: *const fn (ctx: *anyopaque, op_byte: u8, payload: []const u8, count: u16, resp: []u8) usize,
    op_latency: *[256]histogram.AtomicHistogram,
) void {
    const bp = conn.buf orelse return;
    const data = bp.request_buf[0..conn.bytes_read];
    var consumed: usize = 0;
    var resp_off: usize = 0;

    while (consumed + REQUEST_HEADER_SIZE <= data.len) {
        if (!pipelineHasRoom(resp_off, bp.response_buf.len)) break;
        if (resp_off + RESPONSE_HEADER_SIZE > bp.response_buf.len) break;

        const frame = data[consumed..];
        const total_len = std.mem.readInt(u32, frame[0..4], .little);

        if (total_len > data.len - consumed) break;

        const op_byte = frame[4];

        if (total_len < REQUEST_HEADER_SIZE) {
            resp_off += writeErrorResp(bp.response_buf[resp_off..], op_byte, .invalid);
            consumed += REQUEST_HEADER_SIZE;
            continue;
        }

        const count = std.mem.readInt(u16, frame[6..8], .little);
        const payload = frame[REQUEST_HEADER_SIZE..total_len];

        const t0 = std.time.nanoTimestamp();
        const written = dispatch_fn(ctx, op_byte, payload, count, bp.response_buf[resp_off..]);
        const t1 = std.time.nanoTimestamp();
        const dt: u64 = if (t1 > t0) @intCast(t1 - t0) else 0;
        op_latency[op_byte].recordValue(dt);

        if (written == 0) break;
        resp_off += written;
        consumed += total_len;
    }

    if (consumed > 0) {
        const remaining = conn.bytes_read - consumed;
        if (remaining > 0) {
            std.mem.copyForwards(u8, bp.request_buf[0..remaining], bp.request_buf[consumed..conn.bytes_read]);
        }
        conn.bytes_read = remaining;
    }

    conn.response_len = resp_off;
}

const echo_fields = &[_]struct { []const u8, type }{
    .{ "a", u64 },
    .{ "s", []const u8 },
};

test "parsePayload decodes mixed fixed and length-prefixed fields" {
    var buf: [100]u8 = undefined;
    std.mem.writeInt(u64, buf[0..8], 42, .little);
    const s = "https://test.com";
    std.mem.writeInt(u16, buf[8..10], s.len, .little);
    @memcpy(buf[10..][0..s.len], s);
    const total = 10 + s.len;

    const parsed = parsePayload(echo_fields, buf[0..total]).?;
    try std.testing.expectEqual(@as(u64, 42), parsed.result.a);
    try std.testing.expectEqualSlices(u8, s, parsed.result.s);
    try std.testing.expectEqual(@as(usize, 0), parsed.rest.len);
}

test "advancePayload matches parsePayload tail" {
    var buf: [100]u8 = undefined;
    std.mem.writeInt(u64, buf[0..8], 1, .little);
    std.mem.writeInt(u16, buf[8..10], 3, .little);
    @memcpy(buf[10..13], "abc");
    const extra = "tail";
    @memcpy(buf[13..][0..extra.len], extra);
    const total = 13 + extra.len;

    const parsed = parsePayload(echo_fields, buf[0..total]).?;
    const advanced = advancePayload(echo_fields, buf[0..total]).?;
    try std.testing.expectEqual(parsed.rest.len, advanced.len);
    try std.testing.expectEqualSlices(u8, extra, advanced);
}

test "readOptionalString" {
    var buf: [20]u8 = undefined;
    std.mem.writeInt(u16, buf[0..2], 5, .little);
    @memcpy(buf[2..7], "hello");

    var off: usize = 0;
    const result = readOptionalString(&buf, &off, 0x01, 0x01);
    try std.testing.expect(result != null);
    try std.testing.expectEqualSlices(u8, "hello", result.?.?);
    try std.testing.expectEqual(@as(usize, 7), off);

    var off2: usize = 0;
    const result2 = readOptionalString(&buf, &off2, 0x00, 0x01);
    try std.testing.expect(result2 != null);
    try std.testing.expect(result2.? == null);
}

test "writeResp and writeErrorRespSub emit a 10-byte header with status fields" {
    var buf: [64]u8 = undefined;

    const ok_len = writeResp(&buf, 7, .ok, 3, "xy");
    try std.testing.expectEqual(@as(usize, RESPONSE_HEADER_SIZE + 2), ok_len);
    try std.testing.expectEqual(@as(u8, 7), buf[4]);
    try std.testing.expectEqual(@intFromEnum(Status.ok), buf[5]);
    try std.testing.expectEqual(@as(u8, 0), buf[6]);
    try std.testing.expectEqual(@as(u16, 3), std.mem.readInt(u16, buf[8..10], .little));
    try std.testing.expectEqualSlices(u8, "xy", buf[RESPONSE_HEADER_SIZE..ok_len]);

    const err_len = writeErrorRespSub(&buf, 9, .invalid, 4);
    try std.testing.expectEqual(@as(usize, RESPONSE_HEADER_SIZE), err_len);
    try std.testing.expectEqual(@intFromEnum(Status.invalid), buf[5]);
    try std.testing.expectEqual(@as(u8, 4), buf[6]);
}

const EchoCtx = struct {
    dispatch_calls: u32 = 0,
    last_op: u8 = 0,
};

fn echoDispatch(ctx: *anyopaque, op_byte: u8, payload: []const u8, count: u16, resp: []u8) usize {
    const self: *EchoCtx = @ptrCast(@alignCast(ctx));
    self.dispatch_calls += 1;
    self.last_op = op_byte;
    return writeResp(resp, op_byte, .ok, count, payload);
}

test "processFrames passes the raw op byte to dispatch, frames the response, and records latency" {
    var bp = connection.BufferPair{};
    var conn = connection.Connection{};
    conn.buf = &bp;

    const op: u8 = 200;
    const payload = "ping-body";
    var off: usize = 0;
    const frames: usize = 4;
    var i: usize = 0;
    while (i < frames) : (i += 1) {
        const total: u32 = @intCast(REQUEST_HEADER_SIZE + payload.len);
        std.mem.writeInt(u32, bp.request_buf[off..][0..4], total, .little);
        bp.request_buf[off + 4] = op;
        bp.request_buf[off + 5] = 0;
        std.mem.writeInt(u16, bp.request_buf[off + 6 ..][0..2], 0, .little);
        @memcpy(bp.request_buf[off + REQUEST_HEADER_SIZE ..][0..payload.len], payload);
        off += total;
    }
    conn.bytes_read = off;

    var op_latency: [256]histogram.AtomicHistogram = undefined;
    for (&op_latency) |*h| h.* = .{};

    var ctx = EchoCtx{};
    processFrames(&ctx, &conn, echoDispatch, &op_latency);

    try std.testing.expectEqual(@as(u32, frames), ctx.dispatch_calls);
    try std.testing.expectEqual(op, ctx.last_op);
    try std.testing.expectEqual(@as(usize, 0), conn.bytes_read);

    const per_frame = RESPONSE_HEADER_SIZE + payload.len;
    try std.testing.expectEqual(per_frame * frames, conn.response_len);

    try std.testing.expectEqual(@as(u8, op), bp.response_buf[4]);
    try std.testing.expectEqual(@intFromEnum(Status.ok), bp.response_buf[5]);
    try std.testing.expectEqualSlices(u8, payload, bp.response_buf[RESPONSE_HEADER_SIZE..per_frame]);

    try std.testing.expectEqual(@as(u64, frames), op_latency[op].samples());
    try std.testing.expectEqual(@as(u64, 0), op_latency[op + 1].samples());
}

test "processFrames emits an invalid response for a sub-header total_len" {
    var bp = connection.BufferPair{};
    var conn = connection.Connection{};
    conn.buf = &bp;

    std.mem.writeInt(u32, bp.request_buf[0..4], 4, .little);
    bp.request_buf[4] = 5;
    bp.request_buf[5] = 0;
    std.mem.writeInt(u16, bp.request_buf[6..8], 0, .little);
    conn.bytes_read = REQUEST_HEADER_SIZE;

    var op_latency: [256]histogram.AtomicHistogram = undefined;
    for (&op_latency) |*h| h.* = .{};

    var ctx = EchoCtx{};
    processFrames(&ctx, &conn, echoDispatch, &op_latency);

    try std.testing.expectEqual(@as(u32, 0), ctx.dispatch_calls);
    try std.testing.expectEqual(@as(usize, RESPONSE_HEADER_SIZE), conn.response_len);
    try std.testing.expectEqual(@intFromEnum(Status.invalid), bp.response_buf[5]);
}
