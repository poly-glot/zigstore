//! Generic byte-codec toolkit: fixed-layout record (de)serialization, fixed-capacity
//! inline strings, big-endian composite keys, and order-preserving `u64` key encoding.
//!
//! Everything here is application-neutral. It knows nothing about any record schema.
//! An app composes these primitives in its own `schema.zig` to define concrete records
//! and key wrappers.

const std = @import("std");

/// Mixin that adds byte (de)serialization to an `extern struct`, so a record can be
/// written to and read from the store's value slots as a flat byte image.
///
/// `T` must be an `extern struct`. A fixed, predictable in-memory layout keeps the byte
/// image stable across writes and reads.
pub fn Serializable(comptime T: type) type {
    comptime {
        const info = @typeInfo(T);
        if (info != .@"struct" or info.@"struct".layout != .@"extern")
            @compileError("Serializable requires an extern struct, got " ++ @typeName(T));
    }

    return struct {
        /// The record's in-memory bytes as a read-only slice (borrowed from `ptr`).
        pub fn asBytes(ptr: *const T) []const u8 {
            return std.mem.asBytes(ptr);
        }

        /// The record's in-memory bytes as a mutable slice (borrowed from `ptr`).
        pub fn asMutableBytes(ptr: *T) []u8 {
            return std.mem.asBytes(ptr);
        }

        /// A caller-owned copy of the record's bytes as a fixed-size array.
        pub fn toBytes(ptr: *const T) [@sizeOf(T)]u8 {
            return std.mem.toBytes(ptr.*);
        }

        /// Reconstruct a record from the first `@sizeOf(T)` bytes of `bytes`.
        pub fn fromBytes(bytes: []const u8) T {
            return std.mem.bytesToValue(T, bytes[0..@sizeOf(T)]);
        }
    };
}

/// A fixed-capacity inline string of up to `N` bytes plus a `u16` length, suitable as a
/// field in an `extern struct` record. Over-long inputs are truncated to `N` bytes.
///
/// `N` must be even so the trailing `len: u16` stays naturally aligned.
pub fn FixedString(comptime N: usize) type {
    comptime {
        if (N % 2 != 0) @compileError("FixedString capacity N must be even for alignment");
    }

    return extern struct {
        data: [N]u8 = [_]u8{0} ** N,
        len: u16 = 0,

        const Self = @This();

        /// Build a `FixedString` from a slice, truncating to the `N`-byte capacity.
        pub fn fromSlice(s: []const u8) Self {
            var fs = Self{};
            const copy_len = @min(s.len, N);
            @memcpy(fs.data[0..copy_len], s[0..copy_len]);
            fs.len = @intCast(copy_len);
            return fs;
        }

        /// The stored bytes as a slice of length `len`.
        pub fn slice(self: *const Self) []const u8 {
            return self.data[0..@min(self.len, N)];
        }

        /// Whether the stored bytes equal `other`.
        pub fn eql(self: *const Self, other: []const u8) bool {
            return std.mem.eql(u8, self.slice(), other);
        }

        /// `std.Io.Writer` formatter that writes the stored bytes verbatim.
        pub fn format(self: *const Self, writer: *std.Io.Writer) std.Io.Writer.Error!void {
            try writer.writeAll(self.slice());
        }

        comptime {
            if (@sizeOf(Self) != N + 2) @compileError("FixedString size mismatch");
        }
    };
}

/// A multi-`u64` key encoded big-endian so that lexical byte order over the encoded key
/// matches tuple order over the fields. A range scan relies on that property.
///
/// `fields` names the components; the generated `decode` returns a struct with one
/// `u64` field per name.
pub fn CompositeKey(comptime fields: []const [:0]const u8) type {
    comptime {
        if (fields.len == 0) @compileError("CompositeKey requires at least one field");
    }

    const num_fields = fields.len;
    const key_size = num_fields * 8;

    const struct_fields = comptime blk: {
        var sf: [num_fields]std.builtin.Type.StructField = undefined;
        for (fields, 0..) |name, i| {
            sf[i] = .{
                .name = name,
                .type = u64,
                .default_value_ptr = null,
                .is_comptime = false,
                .alignment = @alignOf(u64),
            };
        }
        break :blk sf;
    };

    const GeneratedStruct = @Type(.{ .@"struct" = .{
        .layout = .@"extern",
        .fields = &struct_fields,
        .decls = &.{},
        .is_tuple = false,
    } });

    return struct {
        /// The decoded key as a struct with one `u64` field per named component.
        pub const KeyStruct = GeneratedStruct;

        /// The encoded key width in bytes (`8 × component count`).
        pub const encoded_size = key_size;

        /// Encode the component values into a big-endian, order-preserving key.
        pub fn encode(values: [num_fields]u64) [key_size]u8 {
            var buf: [key_size]u8 = undefined;
            inline for (0..num_fields) |i| {
                buf[i * 8 ..][0..8].* = std.mem.toBytes(
                    std.mem.nativeTo(u64, values[i], .big),
                );
            }
            return buf;
        }

        /// Decode a key back into its named `u64` components.
        pub fn decode(bytes: []const u8) GeneratedStruct {
            var result: GeneratedStruct = undefined;
            inline for (fields, 0..) |name, i| {
                @field(result, name) = std.mem.toNative(
                    u64,
                    std.mem.bytesToValue(u64, bytes[i * 8 ..][0..8]),
                    .big,
                );
            }
            return result;
        }
    };
}

/// Encode a `u64` as 8 big-endian bytes so byte order matches numeric order.
pub fn encodeU64(val: u64) [8]u8 {
    return std.mem.toBytes(std.mem.nativeTo(u64, val, .big));
}

/// Decode the first 8 bytes of `bytes` as a big-endian `u64`.
pub fn decodeU64(bytes: []const u8) u64 {
    return std.mem.toNative(u64, std.mem.bytesToValue(u64, bytes[0..8]), .big);
}

/// A stable, non-cryptographic hash over arbitrary bytes. Use it for hashed-key indexes
/// (e.g. URL de-duplication) and bloom filters.
pub fn hash(bytes: []const u8) u64 {
    return std.hash.Wyhash.hash(0, bytes);
}

test "FixedString fromSlice / slice roundtrip" {
    const fs = FixedString(256).fromSlice("hello world");
    try std.testing.expectEqualSlices(u8, "hello world", fs.slice());
    try std.testing.expectEqual(@as(u16, 11), fs.len);
}

test "FixedString truncation" {
    const fs = FixedString(4).fromSlice("abcdefgh");
    try std.testing.expectEqualSlices(u8, "abcd", fs.slice());
    try std.testing.expectEqual(@as(u16, 4), fs.len);
}

test "FixedString eql" {
    const fs = FixedString(64).fromSlice("test");
    try std.testing.expect(fs.eql("test"));
    try std.testing.expect(!fs.eql("other"));
}

test "FixedString default is empty" {
    const fs = FixedString(256){};
    try std.testing.expectEqual(@as(u16, 0), fs.len);
    try std.testing.expectEqualSlices(u8, "", fs.slice());
}

test "encodeU64 / decodeU64 roundtrip" {
    const values = [_]u64{ 0, 1, 42, 0xDEADBEEF, std.math.maxInt(u64) };
    for (values) |v| {
        const encoded = encodeU64(v);
        const decoded = decodeU64(&encoded);
        try std.testing.expectEqual(v, decoded);
    }
}

test "encodeU64 big-endian ordering" {
    const a = encodeU64(100);
    const b = encodeU64(200);
    try std.testing.expect(std.mem.order(u8, &a, &b) == .lt);
}

test "CompositeKey generic encode/decode" {
    const TripleKey = CompositeKey(&.{ "a", "b", "c" });

    const encoded = TripleKey.encode(.{ 1, 2, 3 });
    try std.testing.expectEqual(@as(usize, 24), encoded.len);

    const decoded = TripleKey.decode(&encoded);
    try std.testing.expectEqual(@as(u64, 1), decoded.a);
    try std.testing.expectEqual(@as(u64, 2), decoded.b);
    try std.testing.expectEqual(@as(u64, 3), decoded.c);
}

test "CompositeKey preserves sort order" {
    const Pair = CompositeKey(&.{ "x", "y" });

    const a = Pair.encode(.{ 1, 999 });
    const b = Pair.encode(.{ 2, 0 });
    try std.testing.expect(std.mem.order(u8, &a, &b) == .lt);

    const c = Pair.encode(.{ 5, 10 });
    const d = Pair.encode(.{ 5, 20 });
    try std.testing.expect(std.mem.order(u8, &c, &d) == .lt);
}

test "hash deterministic and input-sensitive" {
    try std.testing.expectEqual(hash("https://example.com"), hash("https://example.com"));
    try std.testing.expect(hash("https://a.com") != hash("https://b.com"));
}

test "Serializable extern-struct byte roundtrip" {
    const Rec = extern struct {
        id: u64 = 0,
        name: FixedString(64) = .{},

        const Ser = Serializable(@This());
        pub const asBytes = Ser.asBytes;
        pub const fromBytes = Ser.fromBytes;
    };

    var rec = Rec{ .id = 42, .name = FixedString(64).fromSlice("zigstore") };
    const restored = Rec.fromBytes(rec.asBytes());
    try std.testing.expectEqual(@as(u64, 42), restored.id);
    try std.testing.expect(restored.name.eql("zigstore"));
}
