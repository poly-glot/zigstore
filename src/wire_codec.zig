//! Generic `@typeInfo`-driven wire marshaller: walks an arbitrary struct and serializes
//! each field to a flat, big-endian byte image, then reconstructs it.
//!
//! Everything here is application-neutral. It knows nothing about any record schema, op
//! tag, or version frame — an app wraps these with its own framing and owns its tags.
//!
//! Field encoding:
//!   - integers: big-endian, `bits / 8` bytes.
//!   - bools: one byte, `0` or `1`.
//!   - enums: one byte, the tag's integer value.
//!   - extern structs: their raw in-memory byte image (`@sizeOf` bytes).
//!   - non-extern structs: each field encoded in declaration order.
//!   - slices: a `u32` big-endian length, then the elements (raw bytes for `[]u8`,
//!     otherwise each element encoded recursively).

const std = @import("std");

/// Errors raised while encoding a struct or field.
pub const EncodeError = error{
    OutOfMemory,
    StringTooLong,
};

/// Errors raised while decoding a struct or field.
pub const DecodeError = error{
    BufferTooShort,
    InvalidEnumValue,
    OutOfMemory,
};

/// Encode every field of `value` (a struct) into `buf` in declaration order.
pub fn encodeStruct(a: std.mem.Allocator, buf: *std.ArrayList(u8), value: anytype) EncodeError!void {
    inline for (@typeInfo(@TypeOf(value)).@"struct".fields) |f| {
        try encodeField(a, buf, @field(value, f.name));
    }
}

/// Encode a single field according to its type, appending its bytes to `buf`.
pub fn encodeField(a: std.mem.Allocator, buf: *std.ArrayList(u8), value: anytype) EncodeError!void {
    const T = @TypeOf(value);
    switch (@typeInfo(T)) {
        .int => |info| {
            var b: [@divExact(info.bits, 8)]u8 = undefined;
            std.mem.writeInt(T, &b, value, .big);
            try buf.appendSlice(a, &b);
        },
        .bool => try buf.append(a, if (value) 1 else 0),
        .@"enum" => try buf.append(a, @intFromEnum(value)),
        .@"struct" => |s| {
            if (s.layout == .@"extern") {
                try buf.appendSlice(a, std.mem.asBytes(&value));
            } else try encodeStruct(a, buf, value);
        },
        .pointer => |p| {
            comptime std.debug.assert(p.size == .slice);
            if (value.len > std.math.maxInt(u32)) return EncodeError.StringTooLong;
            try encodeField(a, buf, @as(u32, @intCast(value.len)));
            if (p.child == u8) {
                try buf.appendSlice(a, value);
            } else for (value) |item| try encodeField(a, buf, item);
        },
        else => @compileError("wire_codec: unsupported field type " ++ @typeName(T)),
    }
}

/// Decode a value of struct type `T` from `bytes`, advancing `cur` past the consumed bytes.
pub fn decodeStruct(arena: std.mem.Allocator, comptime T: type, bytes: []const u8, cur: *usize) DecodeError!T {
    var out: T = undefined;
    inline for (@typeInfo(T).@"struct".fields) |f| {
        @field(out, f.name) = try decodeField(arena, f.type, bytes, cur);
    }
    return out;
}

/// Decode a single field of type `T` from `bytes`, advancing `cur`. Slice fields are
/// duped into `arena`, so the result borrows from it.
pub fn decodeField(arena: std.mem.Allocator, comptime T: type, bytes: []const u8, cur: *usize) DecodeError!T {
    switch (@typeInfo(T)) {
        .int => |info| {
            const sz = @divExact(info.bits, 8);
            if (cur.* + sz > bytes.len) return DecodeError.BufferTooShort;
            const v = std.mem.readInt(T, bytes[cur.*..][0..sz], .big);
            cur.* += sz;
            return v;
        },
        .bool => {
            if (cur.* >= bytes.len) return DecodeError.BufferTooShort;
            const v = bytes[cur.*] != 0;
            cur.* += 1;
            return v;
        },
        .@"enum" => {
            if (cur.* >= bytes.len) return DecodeError.BufferTooShort;
            const v = std.meta.intToEnum(T, bytes[cur.*]) catch return DecodeError.InvalidEnumValue;
            cur.* += 1;
            return v;
        },
        .@"struct" => |s| {
            if (s.layout == .@"extern") {
                if (cur.* + @sizeOf(T) > bytes.len) return DecodeError.BufferTooShort;
                const v = std.mem.bytesToValue(T, bytes[cur.*..][0..@sizeOf(T)]);
                cur.* += @sizeOf(T);
                return v;
            } else return try decodeStruct(arena, T, bytes, cur);
        },
        .pointer => |p| {
            comptime std.debug.assert(p.size == .slice);
            const n = try decodeField(arena, u32, bytes, cur);
            if (p.child == u8) {
                if (cur.* + n > bytes.len) return DecodeError.BufferTooShort;
                const out = try arena.dupe(u8, bytes[cur.* .. cur.* + n]);
                cur.* += n;
                return out;
            }
            if (n > bytes.len - cur.*) return DecodeError.BufferTooShort;
            const out = try arena.alloc(p.child, n);
            for (out) |*item| item.* = try decodeField(arena, p.child, bytes, cur);
            return out;
        },
        else => @compileError("wire_codec: unsupported field type " ++ @typeName(T)),
    }
}

test "encodeStruct/decodeStruct roundtrip over a generic extern struct" {
    const Rec = extern struct { id: u64, n: u32 };

    const allocator = std.testing.allocator;
    var buf: std.ArrayList(u8) = .{};
    defer buf.deinit(allocator);

    const original = Rec{ .id = 0xDEADBEEFCAFEBABE, .n = 1234 };
    try encodeStruct(allocator, &buf, original);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var cur: usize = 0;
    const decoded = try decodeStruct(arena.allocator(), Rec, buf.items, &cur);

    try std.testing.expectEqual(original, decoded);
    try std.testing.expectEqual(buf.items.len, cur);
}

test "non-extern struct with int/bool/enum/slice fields roundtrips" {
    const Field = enum(u8) { a = 0, b = 1, c = 2 };
    const Item = struct { id: u64, field: Field };
    const Rec = struct {
        count: u32,
        flag: bool,
        label: []const u8,
        items: []const Item,
    };

    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    const items = try aa.dupe(Item, &.{
        .{ .id = 7, .field = .b },
        .{ .id = 9, .field = .c },
    });
    const original = Rec{
        .count = 2,
        .flag = true,
        .label = try aa.dupe(u8, "hello"),
        .items = items,
    };

    var buf: std.ArrayList(u8) = .{};
    defer buf.deinit(allocator);
    try encodeStruct(allocator, &buf, original);

    var cur: usize = 0;
    const decoded = try decodeStruct(aa, Rec, buf.items, &cur);

    try std.testing.expectEqual(@as(u32, 2), decoded.count);
    try std.testing.expect(decoded.flag);
    try std.testing.expectEqualStrings("hello", decoded.label);
    try std.testing.expectEqual(@as(usize, 2), decoded.items.len);
    try std.testing.expectEqual(@as(u64, 9), decoded.items[1].id);
    try std.testing.expectEqual(Field.c, decoded.items[1].field);
    try std.testing.expectEqual(buf.items.len, cur);
}

test "decodeField reports BufferTooShort on a truncated int" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var cur: usize = 0;
    const truncated = [_]u8{ 0x00, 0x01, 0x02 };
    try std.testing.expectError(DecodeError.BufferTooShort, decodeField(arena.allocator(), u64, &truncated, &cur));
}
