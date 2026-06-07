//! Generic superblock codec: serialize/deserialize/validate over a schema-generated
//! `Header` type. The engine generates the named root/count/counter slots into `Header`;
//! this module only round-trips that struct through a `PAGE_SIZE` page-0 block and checks
//! the app-supplied `magic`/`format_version` and the fixed `page_size`.

const std = @import("std");
const page = @import("page.zig");

/// Serialize a generated header into a `PAGE_SIZE` block. The header must be an `extern`
/// struct sized exactly `PAGE_SIZE` (the engine pads it with `_reserved`).
pub fn serialize(header: anytype) [page.PAGE_SIZE]u8 {
    const Header = @TypeOf(header);
    comptime assertPageSized(Header);
    return std.mem.toBytes(header);
}

/// Deserialize a `PAGE_SIZE` block back into the generated header type `Header`.
pub fn deserialize(comptime Header: type, bytes: *const [page.PAGE_SIZE]u8) Header {
    comptime assertPageSized(Header);
    return std.mem.bytesToValue(Header, bytes);
}

/// Validate a deserialized header against the app's expected identity. Returns
/// `error.InvalidMagic`, `error.UnsupportedVersion`, or `error.InvalidPageSize` on mismatch.
pub fn validate(header: anytype, expected_magic: u32, expected_version: u32) !void {
    if (header.magic != expected_magic) return error.InvalidMagic;
    if (header.format_version != expected_version) return error.UnsupportedVersion;
    if (header.page_size != page.PAGE_SIZE) return error.InvalidPageSize;
}

fn assertPageSized(comptime Header: type) void {
    if (@sizeOf(Header) != page.PAGE_SIZE)
        @compileError("header must be PAGE_SIZE bytes (pad it with _reserved)");
}

const TestHeader = extern struct {
    magic: u32 = 0,
    format_version: u32 = 0,
    page_size: u32 = page.PAGE_SIZE,
    _reserved: [page.PAGE_SIZE - 3 * @sizeOf(u32)]u8 = [_]u8{0} ** (page.PAGE_SIZE - 3 * @sizeOf(u32)),
};

const TEST_MAGIC: u32 = 0x5A494753;
const TEST_VERSION: u32 = 7;

fn freshTestHeader() TestHeader {
    return .{ .magic = TEST_MAGIC, .format_version = TEST_VERSION };
}

test "serialize/deserialize round-trips a page-sized header" {
    const h = freshTestHeader();
    const bytes = serialize(h);
    const h2 = deserialize(TestHeader, &bytes);
    try std.testing.expectEqual(TEST_MAGIC, h2.magic);
    try std.testing.expectEqual(TEST_VERSION, h2.format_version);
    try std.testing.expectEqual(page.PAGE_SIZE, h2.page_size);
}

test "validate accepts a matching header" {
    const h = freshTestHeader();
    try validate(h, TEST_MAGIC, TEST_VERSION);
}

test "validate catches bad magic" {
    var h = freshTestHeader();
    h.magic = 0xDEADBEEF;
    try std.testing.expectError(error.InvalidMagic, validate(h, TEST_MAGIC, TEST_VERSION));
}

test "validate catches bad version" {
    var h = freshTestHeader();
    h.format_version = 99;
    try std.testing.expectError(error.UnsupportedVersion, validate(h, TEST_MAGIC, TEST_VERSION));
}

test "validate catches bad page size" {
    var h = freshTestHeader();
    h.page_size = 8192;
    try std.testing.expectError(error.InvalidPageSize, validate(h, TEST_MAGIC, TEST_VERSION));
}
