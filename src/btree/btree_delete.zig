const std = @import("std");
const page = @import("../page.zig");
const btree = @import("btree.zig");

const PageId = page.PageId;
const INVALID_PAGE = page.INVALID_PAGE;

const BPlusTree = btree.BPlusTree;
const PathStack = btree.PathStack;
const compareKeys = btree.compareKeys;
const slotsFromConstPage = btree.slotsFromConstPage;
const slotsFromPage = btree.slotsFromPage;

pub fn delete(self: *BPlusTree, key: []const u8) !bool {
    self.lock.lock();
    defer self.lock.unlock();

    if (self.root_page == INVALID_PAGE) return false;

    var result = try self.findLeafWithPath(key);
    const leaf_id = result.leaf;
    const pg = try self.getMutablePage(leaf_id);

    const count = pg.header.key_count;
    const slots = slotsFromConstPage(pg);

    for (0..count) |i| {
        const slot = slots[i];
        const k = page.getKeyAt(pg, slot);
        const ord = compareKeys(k, key);
        if (ord == .eq) {
            page.removeEntry(pg, @intCast(i));

            if (pg.header.key_count == 0 and leaf_id != self.root_page) {
                self.cache.unpinPage(leaf_id);
                try removeEmptyLeaf(self, leaf_id, &result.path);
            } else {
                self.cache.unpinPage(leaf_id);
            }
            self.entry_count -|= 1;
            return true;
        }
        if (ord == .gt) break;
    }

    self.cache.unpinPage(leaf_id);
    return false;
}

fn removeEmptyLeaf(self: *BPlusTree, leaf_id: PageId, path: *PathStack) !void {
    const parent_id = path.pop() orelse return;

    const parent = try self.getMutablePage(parent_id);
    defer self.cache.unpinPage(parent_id);

    const count = parent.header.key_count;
    const slots = slotsFromPage(parent);

    const leaf_pg = try self.cache.getPage(leaf_id);
    const leaf_right_sibling = leaf_pg.header.right_sibling;
    self.cache.unpinPage(leaf_id);

    var child_pos: ?usize = null;
    for (0..count) |i| {
        const v = page.getValueAt(parent, slots[i]);
        if (btree.readPageId(v) == leaf_id) {
            child_pos = i;
            break;
        }
    }

    if (child_pos) |pos| {
        if (pos == 0) return;

        if (count == 1 and parent.header.right_sibling == INVALID_PAGE) return;

        const left_sib_id = btree.readPageId(page.getValueAt(parent, slots[pos - 1]));
        const left_sib = try self.getMutablePage(left_sib_id);
        left_sib.header.right_sibling = leaf_right_sibling;
        self.cache.unpinPage(left_sib_id);

        page.removeEntry(parent, @intCast(pos));
    } else if (parent.header.right_sibling == leaf_id) {
        if (count > 0) {
            const left_sib_id = btree.readPageId(page.getValueAt(parent, slots[count - 1]));
            const left_sib = try self.getMutablePage(left_sib_id);
            left_sib.header.right_sibling = leaf_right_sibling;
            self.cache.unpinPage(left_sib_id);

            parent.header.right_sibling = left_sib_id;
            page.removeEntry(parent, @intCast(count - 1));
        } else {
            return;
        }
    } else {
        return;
    }

    if (self.cached_rightmost_leaf == leaf_id) {
        self.cached_rightmost_leaf = INVALID_PAGE;
        self.cached_rightmost_path = .{};
    }

    try self.free_list.freePage(leaf_id);
}

const page_cache = @import("../page_cache.zig");
const freelist = @import("../freelist.zig");

fn createTempFile(name: []const u8) !std.fs.File {
    return std.fs.cwd().createFile(name, .{
        .read = true,
        .truncate = true,
    });
}

test "delete" {
    const path = "/tmp/test_btree_delete.db";
    const file = try createTempFile(path);
    defer file.close();
    defer std.fs.cwd().deleteFile(path) catch {};

    var cache = try page_cache.PageCache.init(std.testing.allocator, file, 64);
    defer cache.deinit();
    var fl = freelist.FreeList.init(&cache, INVALID_PAGE);

    var tree = BPlusTree.init(&cache, &fl, INVALID_PAGE);

    try tree.insert("alpha", "1");
    try tree.insert("beta", "2");
    try tree.insert("gamma", "3");

    const deleted = try tree.delete("beta");
    try std.testing.expect(deleted);

    var sb: [page.PAGE_SIZE]u8 = undefined;
    const v = try tree.search("beta", &sb);
    try std.testing.expect(v == null);

    try std.testing.expect((try tree.search("alpha", &sb)) != null);
    try std.testing.expect((try tree.search("gamma", &sb)) != null);

    const d2 = try tree.delete("nonexistent");
    try std.testing.expect(!d2);
}

test "entry_count tracks insert/delete/duplicate" {
    const path = "/tmp/test_btree_entry_count.db";
    const file = try createTempFile(path);
    defer file.close();
    defer std.fs.cwd().deleteFile(path) catch {};

    var cache = try page_cache.PageCache.init(std.testing.allocator, file, 64);
    defer cache.deinit();
    var fl = freelist.FreeList.init(&cache, INVALID_PAGE);

    var tree = BPlusTree.init(&cache, &fl, INVALID_PAGE);

    try std.testing.expectEqual(@as(u64, 0), tree.entry_count);

    var k1: [8]u8 = .{ 0, 0, 0, 0, 0, 0, 0, 1 };
    var v: [8]u8 = .{ 0, 0, 0, 0, 0, 0, 0, 42 };
    try tree.insert(&k1, &v);
    try std.testing.expectEqual(@as(u64, 1), tree.entry_count);

    try tree.insert(&k1, &v);
    try std.testing.expectEqual(@as(u64, 1), tree.entry_count);

    var k2: [8]u8 = .{ 0, 0, 0, 0, 0, 0, 0, 2 };
    try tree.insert(&k2, &v);
    try std.testing.expectEqual(@as(u64, 2), tree.entry_count);

    _ = try tree.delete(&k1);
    try std.testing.expectEqual(@as(u64, 1), tree.entry_count);

    _ = try tree.delete(&k1);
    try std.testing.expectEqual(@as(u64, 1), tree.entry_count);
}

test "H3: deleting the cached rightmost leaf invalidates the cache" {
    const path = "/tmp/test_btree_h3_rightmost.db";
    const file = try createTempFile(path);
    defer file.close();
    defer std.fs.cwd().deleteFile(path) catch {};

    var cache = try page_cache.PageCache.init(std.testing.allocator, file, 256);
    defer cache.deinit();
    var fl = freelist.FreeList.init(&cache, INVALID_PAGE);
    var tree = BPlusTree.init(&cache, &fl, INVALID_PAGE);

    var i: u64 = 0;
    while (i < 2000) : (i += 1) {
        var k: [8]u8 = undefined;
        std.mem.writeInt(u64, &k, i, .big);
        try tree.insert(&k, &k);
    }
    try std.testing.expect(tree.cached_rightmost_leaf != INVALID_PAGE);

    i = 0;
    while (i < 2000) : (i += 1) {
        var k: [8]u8 = undefined;
        std.mem.writeInt(u64, &k, i, .big);
        _ = try tree.delete(&k);
    }
    try std.testing.expectEqual(INVALID_PAGE, tree.cached_rightmost_leaf);
}

test "H4: leaf chain stays scannable across delete + reuse churn" {
    const path = "/tmp/test_btree_h4_chain.db";
    const file = try createTempFile(path);
    defer file.close();
    defer std.fs.cwd().deleteFile(path) catch {};

    var cache = try page_cache.PageCache.init(std.testing.allocator, file, 512);
    defer cache.deinit();
    var fl = freelist.FreeList.init(&cache, INVALID_PAGE);
    var tree = BPlusTree.init(&cache, &fl, INVALID_PAGE);

    var i: u64 = 0;
    while (i < 3000) : (i += 1) {
        var k: [8]u8 = undefined;
        std.mem.writeInt(u64, &k, i, .big);
        try tree.insert(&k, &k);
    }
    i = 0;
    while (i < 1500) : (i += 1) {
        var k: [8]u8 = undefined;
        std.mem.writeInt(u64, &k, i, .big);
        _ = try tree.delete(&k);
    }
    i = 3000;
    while (i < 3500) : (i += 1) {
        var k: [8]u8 = undefined;
        std.mem.writeInt(u64, &k, i, .big);
        try tree.insert(&k, &k);
    }

    const min_key: [8]u8 = .{0} ** 8;
    var iter = try tree.rangeScan(&min_key, null);
    var expect: u64 = 1500;
    var count: u64 = 0;
    while (try iter.next()) |kv| {
        try std.testing.expectEqual(@as(usize, 8), kv.key.len);
        try std.testing.expectEqual(expect, std.mem.readInt(u64, kv.key[0..8], .big));
        expect += 1;
        count += 1;
    }
    try std.testing.expectEqual(@as(u64, 2000), count);
    try std.testing.expectEqual(@as(u64, 3500), expect);
}
