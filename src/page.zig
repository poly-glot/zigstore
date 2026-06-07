const std = @import("std");

pub const PAGE_SIZE: u32 = 16384;
pub const PageId = u32;
pub const INVALID_PAGE: PageId = std.math.maxInt(PageId);

pub const PageType = enum(u8) {
    leaf = 0,
    internal = 1,
    overflow = 2,
    free = 3,
};

pub const PageHeader = extern struct {
    page_id: u32,
    right_sibling: u32,
    checksum: u32,
    key_count: u32,
    free_space_offset: u32,
    page_type: u8,
    _pad: [3]u8 = .{ 0, 0, 0 },
};

comptime {
    if (@sizeOf(PageHeader) != 24) @compileError("PageHeader must be 24 bytes");
}

pub const BODY_SIZE: u32 = PAGE_SIZE - @sizeOf(PageHeader);

pub const Page = extern struct {
    header: PageHeader,
    body: [BODY_SIZE]u8,
};

comptime {
    if (@sizeOf(Page) != PAGE_SIZE) @compileError("Page must be PAGE_SIZE bytes");
}

pub const SlotEntry = extern struct {
    key_offset: u32,
    key_len: u32,
    value_offset: u32,
    value_len: u32,
};

comptime {
    if (@sizeOf(SlotEntry) != 16) @compileError("SlotEntry must be 16 bytes");
}

pub fn getSlots(p: *const Page) []const SlotEntry {
    const count = p.header.key_count;
    if (count == 0) return &[_]SlotEntry{};
    const ptr: [*]const SlotEntry = @ptrCast(@alignCast(&p.body));
    return ptr[0..count];
}

pub fn getSlotsMut(p: *Page) []SlotEntry {
    const count = p.header.key_count;
    if (count == 0) return &[_]SlotEntry{};
    const ptr: [*]SlotEntry = @ptrCast(@alignCast(&p.body));
    return ptr[0..count];
}

pub fn getKeyAt(p: *const Page, slot: SlotEntry) []const u8 {
    return p.body[slot.key_offset..][0..slot.key_len];
}

pub fn getValueAt(p: *const Page, slot: SlotEntry) []const u8 {
    return p.body[slot.value_offset..][0..slot.value_len];
}

pub fn initLeaf(p: *Page, pid: PageId) void {
    @memset(std.mem.asBytes(p), 0);
    p.header.page_id = pid;
    p.header.page_type = @intFromEnum(PageType.leaf);
    p.header.key_count = 0;
    p.header.right_sibling = INVALID_PAGE;
    p.header.free_space_offset = BODY_SIZE;
    p.header.checksum = 0;
}

pub fn initInternal(p: *Page, pid: PageId) void {
    @memset(std.mem.asBytes(p), 0);
    p.header.page_id = pid;
    p.header.page_type = @intFromEnum(PageType.internal);
    p.header.key_count = 0;
    p.header.right_sibling = INVALID_PAGE;
    p.header.free_space_offset = BODY_SIZE;
    p.header.checksum = 0;
}

pub fn computeChecksum(p: *const Page) u32 {
    const bytes = std.mem.asBytes(p);
    const checksum_offset = @offsetOf(PageHeader, "checksum");
    const checksum_end = checksum_offset + @sizeOf(u32);
    var crc = std.hash.crc.Crc32.init();
    crc.update(bytes[0..checksum_offset]);
    crc.update(bytes[checksum_end..]);
    const v = crc.final();
    return if (v == 0) 0xFFFF_FFFF else v;
}

pub fn verifyChecksum(p: *const Page) bool {
    return p.header.checksum == computeChecksum(p);
}

pub fn freeSpace(p: *const Page) u32 {
    const slots_end: u32 = p.header.key_count * @sizeOf(SlotEntry);
    return p.header.free_space_offset - slots_end;
}

pub const PageError = error{
    NotEnoughSpace,
    InvalidPosition,
    KeyTooLarge,
    ValueTooLarge,
};

pub fn insertEntry(p: *Page, key: []const u8, value: []const u8, pos: u32) !void {
    if (key.len > BODY_SIZE) return PageError.KeyTooLarge;
    if (value.len > BODY_SIZE) return PageError.ValueTooLarge;
    const needed: u32 = @sizeOf(SlotEntry) + @as(u32, @intCast(key.len)) + @as(u32, @intCast(value.len));
    if (needed > freeSpace(p)) return PageError.NotEnoughSpace;
    if (pos > p.header.key_count) return PageError.InvalidPosition;

    const value_offset: u32 = p.header.free_space_offset - @as(u32, @intCast(value.len));
    @memcpy(p.body[value_offset..][0..value.len], value);

    const key_offset: u32 = value_offset - @as(u32, @intCast(key.len));
    @memcpy(p.body[key_offset..][0..key.len], key);

    p.header.free_space_offset = key_offset;

    const slots_ptr: [*]SlotEntry = @ptrCast(@alignCast(&p.body));
    const count = p.header.key_count;

    var i: u32 = count;
    while (i > pos) {
        i -= 1;
        slots_ptr[i + 1] = slots_ptr[i];
    }

    slots_ptr[pos] = SlotEntry{
        .key_offset = key_offset,
        .key_len = @intCast(key.len),
        .value_offset = value_offset,
        .value_len = @intCast(value.len),
    };

    p.header.key_count += 1;
}

pub fn removeEntry(p: *Page, pos: u32) void {
    if (pos >= p.header.key_count) return;

    const slots_ptr: [*]SlotEntry = @ptrCast(@alignCast(&p.body));
    const count = p.header.key_count;

    var i: u32 = pos;
    while (i + 1 < count) : (i += 1) {
        slots_ptr[i] = slots_ptr[i + 1];
    }

    p.header.key_count -= 1;
}

test "page init leaf" {
    var pg: Page = undefined;
    initLeaf(&pg, 42);
    try std.testing.expectEqual(@as(u32, 42), pg.header.page_id);
    try std.testing.expectEqual(@as(u8, @intFromEnum(PageType.leaf)), pg.header.page_type);
    try std.testing.expectEqual(@as(u32, 0), pg.header.key_count);
    try std.testing.expectEqual(INVALID_PAGE, pg.header.right_sibling);
    try std.testing.expectEqual(BODY_SIZE, pg.header.free_space_offset);
}

test "page init internal" {
    var pg: Page = undefined;
    initInternal(&pg, 7);
    try std.testing.expectEqual(@as(u8, @intFromEnum(PageType.internal)), pg.header.page_type);
}

test "insert entries" {
    var pg: Page = undefined;
    initLeaf(&pg, 0);

    try insertEntry(&pg, "apple", "red", 0);
    try std.testing.expectEqual(@as(u32, 1), pg.header.key_count);

    const slots = getSlots(&pg);
    try std.testing.expectEqualSlices(u8, "apple", getKeyAt(&pg, slots[0]));
    try std.testing.expectEqualSlices(u8, "red", getValueAt(&pg, slots[0]));

    try insertEntry(&pg, "banana", "yellow", 1);
    try std.testing.expectEqual(@as(u32, 2), pg.header.key_count);

    try insertEntry(&pg, "aardvark", "brown", 0);
    try std.testing.expectEqual(@as(u32, 3), pg.header.key_count);

    const all = getSlots(&pg);
    try std.testing.expectEqualSlices(u8, "aardvark", getKeyAt(&pg, all[0]));
    try std.testing.expectEqualSlices(u8, "apple", getKeyAt(&pg, all[1]));
    try std.testing.expectEqualSlices(u8, "banana", getKeyAt(&pg, all[2]));
}

test "remove entry" {
    var pg: Page = undefined;
    initLeaf(&pg, 0);

    try insertEntry(&pg, "a", "1", 0);
    try insertEntry(&pg, "b", "2", 1);
    try insertEntry(&pg, "c", "3", 2);
    try std.testing.expectEqual(@as(u32, 3), pg.header.key_count);

    removeEntry(&pg, 1);
    try std.testing.expectEqual(@as(u32, 2), pg.header.key_count);

    const slots = getSlots(&pg);
    try std.testing.expectEqualSlices(u8, "a", getKeyAt(&pg, slots[0]));
    try std.testing.expectEqualSlices(u8, "c", getKeyAt(&pg, slots[1]));
}

test "checksum round-trip" {
    var pg: Page = undefined;
    initLeaf(&pg, 10);
    try insertEntry(&pg, "key", "value", 0);

    pg.header.checksum = computeChecksum(&pg);
    try std.testing.expect(verifyChecksum(&pg));

    pg.body[0] ^= 0xFF;
    try std.testing.expect(!verifyChecksum(&pg));
}
