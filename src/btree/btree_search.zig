const std = @import("std");
const page = @import("../page.zig");
const btree = @import("btree.zig");

const PageId = page.PageId;
const Page = page.Page;
const INVALID_PAGE = page.INVALID_PAGE;

const BPlusTree = btree.BPlusTree;
const PathStack = btree.PathStack;
const LeafWithPath = btree.LeafWithPath;
const BTreeError = btree.BTreeError;
const MAX_ENTRIES_PER_PAGE = btree.MAX_ENTRIES_PER_PAGE;
const compareKeys = btree.compareKeys;
const slotsFromConstPage = btree.slotsFromConstPage;
const isLeaf = btree.isLeaf;

pub fn search(self: *BPlusTree, key: []const u8, out_buf: []u8) !?[]const u8 {
    self.lock.lockShared();
    defer self.lock.unlockShared();

    if (self.root_page == INVALID_PAGE) return null;

    const leaf_id = try findLeaf(self, key);
    const pg = try self.cache.getPage(leaf_id);
    defer self.cache.unpinPage(leaf_id);

    const count = pg.header.key_count;
    const slots = slotsFromConstPage(pg);

    for (0..count) |i| {
        const slot = slots[i];
        const k = page.getKeyAt(pg, slot);
        const ord = compareKeys(k, key);
        if (ord == .eq) {
            const val = page.getValueAt(pg, slot);
            if (out_buf.len < val.len) return error.BufferTooSmall;
            @memcpy(out_buf[0..val.len], val);
            return out_buf[0..val.len];
        }
        if (ord == .gt) break;
    }

    return null;
}

pub fn findLeaf(self: *BPlusTree, key: []const u8) !PageId {
    var current = self.root_page;
    if (current == 0) return BTreeError.Corrupted;

    while (true) {
        const pg = try self.cache.getPage(current);

        if (isLeaf(pg)) {
            self.cache.unpinPage(current);
            return current;
        }

        const count = pg.header.key_count;
        const slots = slotsFromConstPage(pg);

        var child: PageId = INVALID_PAGE;
        var found = false;

        for (0..count) |i| {
            const slot = slots[i];
            const k = page.getKeyAt(pg, slot);
            if (compareKeys(key, k) == .lt) {
                const val = page.getValueAt(pg, slot);
                child = btree.readPageId(val);
                found = true;
                break;
            }
        }

        if (!found) {
            child = pg.header.right_sibling;
        }

        self.cache.unpinPage(current);
        if (child == INVALID_PAGE or child == 0) return BTreeError.Corrupted;
        current = child;
    }
}

pub fn findLeafWithPath(self: *BPlusTree, key: []const u8) !LeafWithPath {
    var current = self.root_page;
    var path = PathStack{};

    while (true) {
        const pg = try self.cache.getPage(current);

        if (isLeaf(pg)) {
            self.cache.unpinPage(current);
            return LeafWithPath{ .leaf = current, .path = path };
        }

        path.push(current);

        const count = pg.header.key_count;
        const slots = slotsFromConstPage(pg);

        if (count > MAX_ENTRIES_PER_PAGE) {
            std.debug.print(
                "btree: corrupt internal page {d}: key_count={d} > MAX={d}\n",
                .{ current, count, MAX_ENTRIES_PER_PAGE },
            );
            self.cache.unpinPage(current);
            return BTreeError.Corrupted;
        }
        for (0..count) |i| {
            const slot = slots[i];
            if (slot.key_offset >= page.BODY_SIZE or
                slot.key_len == 0 or
                @as(u32, slot.key_offset) + @as(u32, slot.key_len) > page.BODY_SIZE)
            {
                std.debug.print(
                    "btree: corrupt slot in internal page {d}: i={d} key_offset={d} key_len={d} BODY_SIZE={d}\n",
                    .{ current, i, slot.key_offset, slot.key_len, page.BODY_SIZE },
                );
                self.cache.unpinPage(current);
                return BTreeError.Corrupted;
            }
        }

        var child: PageId = INVALID_PAGE;
        var found = false;

        for (0..count) |i| {
            const slot = slots[i];
            const k = page.getKeyAt(pg, slot);
            if (compareKeys(key, k) == .lt) {
                const val = page.getValueAt(pg, slot);
                child = btree.readPageId(val);
                found = true;
                break;
            }
        }

        if (!found) {
            child = pg.header.right_sibling;
        }

        self.cache.unpinPage(current);
        if (child == INVALID_PAGE) return BTreeError.Corrupted;
        current = child;
    }
}

pub fn findInsertPos(_: *BPlusTree, pg: *const Page, key: []const u8) u32 {
    const count = pg.header.key_count;
    const slots = slotsFromConstPage(pg);

    for (0..count) |i| {
        const slot = slots[i];
        const k = page.getKeyAt(pg, slot);
        if (compareKeys(key, k) == .lt) return @intCast(i);
    }
    return count;
}

const page_cache = @import("../page_cache.zig");
const freelist = @import("../freelist.zig");

fn createTempFile(name: []const u8) !std.fs.File {
    return std.fs.cwd().createFile(name, .{
        .read = true,
        .truncate = true,
    });
}

test "insert and search" {
    const path = "/tmp/test_btree_basic.db";
    const file = try createTempFile(path);
    defer file.close();
    defer std.fs.cwd().deleteFile(path) catch {};

    var cache = try page_cache.PageCache.init(std.testing.allocator, file, 64);
    defer cache.deinit();
    var fl = freelist.FreeList.init(&cache, INVALID_PAGE);

    var tree = BPlusTree.init(&cache, &fl, INVALID_PAGE);

    try tree.insert("hello", "world");
    try tree.insert("foo", "bar");
    try tree.insert("zig", "lang");

    var sb: [page.PAGE_SIZE]u8 = undefined;

    const v1 = try tree.search("hello", &sb);
    try std.testing.expect(v1 != null);
    try std.testing.expectEqualSlices(u8, "world", v1.?);

    const v2 = try tree.search("foo", &sb);
    try std.testing.expect(v2 != null);
    try std.testing.expectEqualSlices(u8, "bar", v2.?);

    const v3 = try tree.search("zig", &sb);
    try std.testing.expect(v3 != null);
    try std.testing.expectEqualSlices(u8, "lang", v3.?);

    const v4 = try tree.search("missing", &sb);
    try std.testing.expect(v4 == null);
}

test "range scan" {
    const path = "/tmp/test_btree_range.db";
    const file = try createTempFile(path);
    defer file.close();
    defer std.fs.cwd().deleteFile(path) catch {};

    var cache = try page_cache.PageCache.init(std.testing.allocator, file, 64);
    defer cache.deinit();
    var fl = freelist.FreeList.init(&cache, INVALID_PAGE);

    var tree = BPlusTree.init(&cache, &fl, INVALID_PAGE);

    try tree.insert("a", "1");
    try tree.insert("b", "2");
    try tree.insert("c", "3");
    try tree.insert("d", "4");
    try tree.insert("e", "5");

    var iter = try tree.rangeScan("b", "d");
    var n: usize = 0;
    while (try iter.next()) |_| {
        n += 1;
    }

    try std.testing.expectEqual(@as(usize, 2), n);
}
