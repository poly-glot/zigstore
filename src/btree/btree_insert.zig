const std = @import("std");
const page = @import("../page.zig");
const btree = @import("btree.zig");

const PageId = page.PageId;
const Page = page.Page;
const SlotEntry = page.SlotEntry;
const INVALID_PAGE = page.INVALID_PAGE;

const BPlusTree = btree.BPlusTree;
const PathStack = btree.PathStack;
const BTreeError = btree.BTreeError;
const MAX_ENTRIES_PER_PAGE = btree.MAX_ENTRIES_PER_PAGE;
const SPLIT_BUF_SIZE = btree.SPLIT_BUF_SIZE;
const Range = btree.Range;
const compareKeys = btree.compareKeys;
const slotsFromConstPage = btree.slotsFromConstPage;
const slotsFromPage = btree.slotsFromPage;
const isLeaf = btree.isLeaf;

pub fn insert(self: *BPlusTree, key: []const u8, value: []const u8) !void {
    self.lock.lock();
    defer self.lock.unlock();

    if (try tryRightmostInsert(self, key, value)) {
        self.entry_count += 1;
        return;
    }

    if (self.root_page == INVALID_PAGE) {
        const new_id = try self.free_list.allocPage();
        const pg = try self.getMutablePage(new_id);
        page.initLeaf(pg, new_id);
        try page.insertEntry(pg, key, value, 0);
        self.cache.unpinPage(new_id);
        self.root_page = new_id;
        self.cached_rightmost_leaf = new_id;
        self.cached_rightmost_path = .{};
        self.entry_count += 1;
        return;
    }

    var result = try self.findLeafWithPath(key);

    {
        const leaf_id = result.leaf;
        const pg = try self.getMutablePage(leaf_id);
        const count = pg.header.key_count;
        const slots = slotsFromPage(pg);

        for (0..count) |i| {
            const slot = slots[i];
            const k = page.getKeyAt(pg, slot);
            if (compareKeys(k, key) == .eq) {
                page.removeEntry(pg, @intCast(i));
                const pos = self.findInsertPos(pg, key);
                page.insertEntry(pg, key, value, pos) catch {
                    self.cache.unpinPage(leaf_id);
                    try compactAndInsert(self, leaf_id, key, value, &result.path);
                    return;
                };
                self.cache.unpinPage(leaf_id);
                return;
            }
        }
        self.cache.unpinPage(leaf_id);
    }

    try insertIntoLeaf(self, result.leaf, key, value, &result.path);
    self.entry_count += 1;
}

fn tryRightmostInsert(self: *BPlusTree, key: []const u8, value: []const u8) !bool {
    if (self.cached_rightmost_leaf == INVALID_PAGE) return false;

    const leaf_id = self.cached_rightmost_leaf;
    const pg = try self.getMutablePage(leaf_id);

    if (!isLeaf(pg)) {
        self.cache.unpinPage(leaf_id);
        invalidateRightmostCache(self);
        return false;
    }

    const count = pg.header.key_count;
    if (count > 0) {
        const slots = slotsFromConstPage(pg);
        const max_key = page.getKeyAt(pg, slots[count - 1]);
        if (compareKeys(key, max_key) != .gt) {
            self.cache.unpinPage(leaf_id);
            return false;
        }
    }

    page.insertEntry(pg, key, value, count) catch {
        self.cache.unpinPage(leaf_id);
        if (self.cached_rightmost_path.len > 0) {
            var path_copy = self.cached_rightmost_path;
            try splitLeaf(self, leaf_id, key, value, &path_copy);
            return true;
        }
        return false;
    };

    self.cache.unpinPage(leaf_id);
    return true;
}

fn invalidateRightmostCache(self: *BPlusTree) void {
    self.cached_rightmost_leaf = INVALID_PAGE;
    self.cached_rightmost_path = .{};
}

fn updateRightmostCache(self: *BPlusTree, new_right_leaf: PageId, path: *const PathStack) void {
    self.cached_rightmost_leaf = new_right_leaf;
    self.cached_rightmost_path = path.*;
}

fn insertIntoLeaf(self: *BPlusTree, leaf_id: PageId, key: []const u8, value: []const u8, path: *PathStack) !void {
    const pg = try self.getMutablePage(leaf_id);

    const pos = self.findInsertPos(pg, key);

    page.insertEntry(pg, key, value, pos) catch {
        self.cache.unpinPage(leaf_id);
        try splitLeaf(self, leaf_id, key, value, path);
        return;
    };

    self.cache.unpinPage(leaf_id);
}

fn splitLeaf(self: *BPlusTree, leaf_id: PageId, key: []const u8, value: []const u8, path: *PathStack) !void {
    const pg = try self.getMutablePage(leaf_id);
    var pages_pinned = true;
    errdefer if (pages_pinned) self.cache.unpinPage(leaf_id);

    const count = pg.header.key_count;
    const slots = slotsFromConstPage(pg);

    const total: usize = @as(usize, count) + 1;
    const allocator = self.cache.allocator;
    const data_buf = try allocator.alloc(u8, SPLIT_BUF_SIZE);
    defer allocator.free(data_buf);
    const key_ranges = try allocator.alloc(Range, MAX_ENTRIES_PER_PAGE);
    defer allocator.free(key_ranges);
    const val_ranges = try allocator.alloc(Range, MAX_ENTRIES_PER_PAGE);
    defer allocator.free(val_ranges);
    var data_off: usize = 0;

    var insert_pos: usize = count;
    for (0..count) |i| {
        const slot = slots[i];
        const k = page.getKeyAt(pg, slot);
        if (compareKeys(key, k) == .lt) {
            insert_pos = i;
            break;
        }
    }

    for (0..total) |i| {
        if (i == insert_pos) {
            if (data_off + key.len + value.len > SPLIT_BUF_SIZE) return BTreeError.Corrupted;
            @memcpy(data_buf[data_off..][0..key.len], key);
            key_ranges[i] = .{ .off = data_off, .len = @intCast(key.len) };
            data_off += key.len;
            @memcpy(data_buf[data_off..][0..value.len], value);
            val_ranges[i] = .{ .off = data_off, .len = @intCast(value.len) };
            data_off += value.len;
        } else {
            const src: usize = if (i < insert_pos) i else i - 1;
            const slot = slots[src];
            const k = page.getKeyAt(pg, slot);
            const v = page.getValueAt(pg, slot);
            if (data_off + k.len + v.len > SPLIT_BUF_SIZE) return BTreeError.Corrupted;
            @memcpy(data_buf[data_off..][0..k.len], k);
            key_ranges[i] = .{ .off = data_off, .len = @intCast(k.len) };
            data_off += k.len;
            @memcpy(data_buf[data_off..][0..v.len], v);
            val_ranges[i] = .{ .off = data_off, .len = @intCast(v.len) };
            data_off += v.len;
        }
    }

    const SLOT_COST: usize = @sizeOf(SlotEntry);
    var total_bytes: usize = 0;
    for (0..total) |i| {
        total_bytes += SLOT_COST + key_ranges[i].len + val_ranges[i].len;
    }
    var mid: usize = total / 2;
    var left_bytes: usize = 0;
    for (0..total) |i| {
        const cost = SLOT_COST + key_ranges[i].len + val_ranges[i].len;
        if (left_bytes + cost > total_bytes / 2 and i > 0) {
            mid = i;
            break;
        }
        left_bytes += cost;
    }
    var left_check: usize = 0;
    for (0..mid) |i| left_check += SLOT_COST + key_ranges[i].len + val_ranges[i].len;
    var right_check: usize = 0;
    for (mid..total) |i| right_check += SLOT_COST + key_ranges[i].len + val_ranges[i].len;
    if (left_check > page.BODY_SIZE or right_check > page.BODY_SIZE) {
        std.debug.print(
            "btree: splitLeaf cannot find a valid split: total={d} mid={d} left_bytes={d} right_bytes={d} BODY_SIZE={d}\n",
            .{ total, mid, left_check, right_check, page.BODY_SIZE },
        );
        return BTreeError.Corrupted;
    }

    const old_right_sibling = pg.header.right_sibling;

    const right_id = try self.free_list.allocPage();
    if (right_id == leaf_id) {
        std.debug.print("btree.splitLeaf ALIAS: free_list.allocPage returned the same page being split! leaf_id={d} right_id={d}\n", .{ leaf_id, right_id });
        return BTreeError.Corrupted;
    }
    const right_pg = try self.getMutablePage(right_id);
    errdefer if (pages_pinned) self.cache.unpinPage(right_id);
    if (@intFromPtr(right_pg) == @intFromPtr(pg)) {
        std.debug.print("btree.splitLeaf POINTER ALIAS: getMutablePage returned same pointer for different ids leaf_id={d} right_id={d}\n", .{ leaf_id, right_id });
        return BTreeError.Corrupted;
    }
    page.initLeaf(right_pg, right_id);

    page.initLeaf(pg, leaf_id);
    pg.header.right_sibling = right_id;

    for (0..mid) |i| {
        const k = data_buf[key_ranges[i].off..][0..key_ranges[i].len];
        const v = data_buf[val_ranges[i].off..][0..val_ranges[i].len];
        page.insertEntry(pg, k, v, @intCast(i)) catch |err| {
            std.debug.print("btree.splitLeaf LEFT fail: i={d} mid={d} total={d} key.len={d} val.len={d} freeSpace={d} left_check={d} right_check={d}\n", .{
                i, mid, total, k.len, v.len, page.freeSpace(pg), left_check, right_check,
            });
            return err;
        };
    }

    for (mid..total) |i| {
        const k = data_buf[key_ranges[i].off..][0..key_ranges[i].len];
        const v = data_buf[val_ranges[i].off..][0..val_ranges[i].len];
        page.insertEntry(right_pg, k, v, @intCast(i - mid)) catch |err| {
            std.debug.print("btree.splitLeaf RIGHT fail: i={d} mid={d} total={d} key.len={d} val.len={d} freeSpace={d} key_count={d} body_size={d}\n", .{
                i, mid, total, k.len, v.len, page.freeSpace(right_pg), right_pg.header.key_count, page.BODY_SIZE,
            });
            std.debug.print("  expected right total={d}, but used={d}\n", .{ right_check, page.BODY_SIZE - page.freeSpace(right_pg) });
            const sample_end = @min(mid + 5, total);
            for (mid..sample_end) |j| {
                std.debug.print("  right[{d}]: key.len={d} val.len={d} (range_off={d})\n", .{ j - mid, key_ranges[j].len, val_ranges[j].len, key_ranges[j].off });
            }
            std.debug.print("  ...\n  right[{d}] (last): key.len={d} val.len={d}\n", .{ total - 1 - mid, key_ranges[total - 1].len, val_ranges[total - 1].len });
            return err;
        };
    }

    right_pg.header.right_sibling = old_right_sibling;

    const median_right_slots = slotsFromConstPage(right_pg);
    const median_key = page.getKeyAt(right_pg, median_right_slots[0]);

    var median_buf: [256]u8 = undefined;
    if (median_key.len > median_buf.len) return BTreeError.Corrupted;
    const median_len = median_key.len;
    @memcpy(median_buf[0..median_len], median_key);

    self.cache.unpinPage(leaf_id);
    self.cache.unpinPage(right_id);
    pages_pinned = false;

    if (old_right_sibling == INVALID_PAGE) {
        updateRightmostCache(self, right_id, path);
    }

    try insertIntoParent(self, median_buf[0..median_len], leaf_id, right_id, path);
}

fn insertIntoParent(self: *BPlusTree, key: []const u8, left_pid: PageId, right_pid: PageId, path: *PathStack) BTreeError!void {
    const parent_id = path.pop() orelse {
        const new_root_id = try self.free_list.allocPage();
        const new_root = try self.getMutablePage(new_root_id);
        errdefer self.cache.unpinPage(new_root_id);
        page.initInternal(new_root, new_root_id);

        var pid_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &pid_buf, left_pid, .little);
        try page.insertEntry(new_root, key, &pid_buf, 0);
        new_root.header.right_sibling = right_pid;

        self.cache.unpinPage(new_root_id);
        self.root_page = new_root_id;
        if (self.cached_rightmost_leaf != INVALID_PAGE) {
            var new_path = PathStack{};
            new_path.push(new_root_id);
            for (0..self.cached_rightmost_path.len) |i| {
                new_path.push(self.cached_rightmost_path.items[i]);
            }
            self.cached_rightmost_path = new_path;
        }
        return;
    };

    const parent = try self.getMutablePage(parent_id);
    const pos = self.findInsertPos(parent, key);

    var pid_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &pid_buf, left_pid, .little);

    page.insertEntry(parent, key, &pid_buf, pos) catch {
        self.cache.unpinPage(parent_id);
        try splitInternal(self, parent_id, key, left_pid, right_pid, path);
        return;
    };

    const parent_slots = slotsFromPage(parent);
    if (pos + 1 < parent.header.key_count) {
        const next_slot = parent_slots[pos + 1];
        std.mem.writeInt(u32, parent.body[next_slot.value_offset..][0..4], right_pid, .little);
    } else {
        parent.header.right_sibling = right_pid;
    }

    self.cache.unpinPage(parent_id);
}

fn splitInternal(self: *BPlusTree, node_id: PageId, new_key: []const u8, new_left: PageId, new_right: PageId, path: *PathStack) BTreeError!void {
    const pg = try self.getMutablePage(node_id);
    var pages_pinned = true;
    errdefer if (pages_pinned) self.cache.unpinPage(node_id);

    const count = pg.header.key_count;
    const slots = slotsFromConstPage(pg);

    const allocator = self.cache.allocator;
    const data_buf = try allocator.alloc(u8, SPLIT_BUF_SIZE);
    defer allocator.free(data_buf);
    const key_ranges = try allocator.alloc(Range, MAX_ENTRIES_PER_PAGE);
    defer allocator.free(key_ranges);
    const child_ptrs = try allocator.alloc(PageId, MAX_ENTRIES_PER_PAGE);
    defer allocator.free(child_ptrs);
    var data_off: usize = 0;
    var rightmost_child: PageId = pg.header.right_sibling;

    var insert_pos: usize = count;
    for (0..count) |i| {
        const slot = slots[i];
        const k = page.getKeyAt(pg, slot);
        if (compareKeys(new_key, k) == .lt) {
            insert_pos = i;
            break;
        }
    }

    const total: usize = @as(usize, count) + 1;
    for (0..total) |i| {
        if (i == insert_pos) {
            @memcpy(data_buf[data_off..][0..new_key.len], new_key);
            key_ranges[i] = .{ .off = data_off, .len = @intCast(new_key.len) };
            data_off += new_key.len;
            child_ptrs[i] = new_left;
        } else {
            const src: usize = if (i < insert_pos) i else i - 1;
            const slot = slots[src];
            const k = page.getKeyAt(pg, slot);
            const v = page.getValueAt(pg, slot);
            @memcpy(data_buf[data_off..][0..k.len], k);
            key_ranges[i] = .{ .off = data_off, .len = @intCast(k.len) };
            data_off += k.len;
            child_ptrs[i] = btree.readPageId(v);
        }
    }

    if (insert_pos + 1 < total) {
        child_ptrs[insert_pos + 1] = new_right;
    } else {
        rightmost_child = new_right;
    }

    const SLOT_COST_INT: usize = @sizeOf(SlotEntry);
    const VAL_COST_INT: usize = 4;
    var total_bytes: usize = 0;
    for (0..total) |i| {
        total_bytes += SLOT_COST_INT + key_ranges[i].len + VAL_COST_INT;
    }
    var mid: usize = total / 2;
    var left_bytes: usize = 0;
    for (0..total) |i| {
        const cost = SLOT_COST_INT + key_ranges[i].len + VAL_COST_INT;
        if (left_bytes + cost > total_bytes / 2 and i > 0) {
            mid = i;
            break;
        }
        left_bytes += cost;
    }
    if (mid == 0) mid = 1;
    if (mid >= total) mid = total - 1;

    var median_buf: [256]u8 = undefined;
    const median_len = key_ranges[mid].len;
    if (median_len > median_buf.len) return BTreeError.Corrupted;
    @memcpy(median_buf[0..median_len], data_buf[key_ranges[mid].off..][0..median_len]);

    const right_id = try self.free_list.allocPage();
    const right_pg = try self.getMutablePage(right_id);
    errdefer if (pages_pinned) self.cache.unpinPage(right_id);
    page.initInternal(right_pg, right_id);

    page.initInternal(pg, node_id);

    for (0..mid) |i| {
        const k = data_buf[key_ranges[i].off..][0..key_ranges[i].len];
        var buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &buf, child_ptrs[i], .little);
        try page.insertEntry(pg, k, &buf, @intCast(i));
    }
    pg.header.right_sibling = child_ptrs[mid];

    for (mid + 1..total) |i| {
        const k = data_buf[key_ranges[i].off..][0..key_ranges[i].len];
        var buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &buf, child_ptrs[i], .little);
        try page.insertEntry(right_pg, k, &buf, @intCast(i - mid - 1));
    }
    right_pg.header.right_sibling = rightmost_child;

    self.cache.unpinPage(node_id);
    self.cache.unpinPage(right_id);
    pages_pinned = false;

    invalidateRightmostCache(self);

    try insertIntoParent(self, median_buf[0..median_len], node_id, right_id, path);
}

fn compactAndInsert(self: *BPlusTree, leaf_id: PageId, key: []const u8, value: []const u8, path: *PathStack) !void {
    const pg = try self.getMutablePage(leaf_id);
    var pinned = true;
    errdefer if (pinned) self.cache.unpinPage(leaf_id);

    const count = pg.header.key_count;
    const slots = slotsFromConstPage(pg);

    const allocator = self.cache.allocator;
    const data_buf = try allocator.alloc(u8, SPLIT_BUF_SIZE);
    defer allocator.free(data_buf);
    const key_ranges = try allocator.alloc(Range, MAX_ENTRIES_PER_PAGE);
    defer allocator.free(key_ranges);
    const val_ranges = try allocator.alloc(Range, MAX_ENTRIES_PER_PAGE);
    defer allocator.free(val_ranges);
    var data_off: usize = 0;

    for (0..count) |i| {
        const slot = slots[i];
        const k = page.getKeyAt(pg, slot);
        const v = page.getValueAt(pg, slot);
        @memcpy(data_buf[data_off..][0..k.len], k);
        key_ranges[i] = .{ .off = data_off, .len = slot.key_len };
        data_off += k.len;
        @memcpy(data_buf[data_off..][0..v.len], v);
        val_ranges[i] = .{ .off = data_off, .len = slot.value_len };
        data_off += v.len;
    }

    const old_sibling = pg.header.right_sibling;
    page.initLeaf(pg, leaf_id);
    pg.header.right_sibling = old_sibling;

    for (0..count) |i| {
        const k = data_buf[key_ranges[i].off..][0..key_ranges[i].len];
        const v = data_buf[val_ranges[i].off..][0..val_ranges[i].len];
        try page.insertEntry(pg, k, v, @intCast(i));
    }

    const pos = self.findInsertPos(pg, key);
    page.insertEntry(pg, key, value, pos) catch {
        pinned = false;
        self.cache.unpinPage(leaf_id);
        try splitLeaf(self, leaf_id, key, value, path);
        return;
    };

    pinned = false;
    self.cache.unpinPage(leaf_id);
}

const page_cache = @import("../page_cache.zig");
const freelist = @import("../freelist.zig");

fn createTempFile(name: []const u8) !std.fs.File {
    return std.fs.cwd().createFile(name, .{
        .read = true,
        .truncate = true,
    });
}

test "sequential inserts cause splits" {
    const path = "/tmp/test_btree_splits.db";
    const file = try createTempFile(path);
    defer file.close();
    defer std.fs.cwd().deleteFile(path) catch {};

    var cache = try page_cache.PageCache.init(std.testing.allocator, file, 64);
    defer cache.deinit();
    var fl = freelist.FreeList.init(&cache, INVALID_PAGE);

    var tree = BPlusTree.init(&cache, &fl, INVALID_PAGE);

    var buf: [32]u8 = undefined;
    const count: usize = 200;
    for (0..count) |i| {
        const key_slice = std.fmt.bufPrint(&buf, "key_{d:0>6}", .{i}) catch unreachable;
        try tree.insert(key_slice, "value");
    }

    var sb: [page.PAGE_SIZE]u8 = undefined;
    for (0..count) |i| {
        const key_slice = std.fmt.bufPrint(&buf, "key_{d:0>6}", .{i}) catch unreachable;
        const v = try tree.search(key_slice, &sb);
        try std.testing.expect(v != null);
        try std.testing.expectEqualSlices(u8, "value", v.?);
    }
}

test "stress: phase-9 shape — iter one tree while inserting into another (shared cache)" {
    const path = "/tmp/test_btree_stress_phase9.db";
    const file = try createTempFile(path);
    defer file.close();
    defer std.fs.cwd().deleteFile(path) catch {};

    var cache = try page_cache.PageCache.init(std.testing.allocator, file, 1024);
    defer cache.deinit();
    var fl = freelist.FreeList.init(&cache, INVALID_PAGE);

    var existing_tree = BPlusTree.init(&cache, &fl, INVALID_PAGE);
    var slug_tree = BPlusTree.init(&cache, &fl, INVALID_PAGE);

    const N: u32 = 150_000;

    {
        var key_buf: [8]u8 = undefined;
        var val_buf: [16]u8 = undefined;
        for (&val_buf) |*b| b.* = 'x';
        var i: u32 = 0;
        while (i < N) : (i += 1) {
            std.mem.writeInt(u64, &key_buf, i + 1, .big);
            try existing_tree.insert(&key_buf, &val_buf);
        }
    }

    var prng = std.Random.DefaultPrng.init(0xC0FFEE);
    const r = prng.random();
    var key_buf: [256]u8 = undefined;
    var val_buf: [8]u8 = undefined;

    const min_key = std.mem.toBytes(@as(u64, 0));
    var iter = try existing_tree.rangeScan(&min_key, null);
    var i: u32 = 0;
    while (try iter.next()) |entry| {
        _ = entry;
        const depth = 1 + r.intRangeLessThan(u32, 0, 4);
        var pos: usize = 0;
        @memcpy(key_buf[pos..][0..3], "top");
        pos += 3;
        var d: u32 = 0;
        while (d < depth) : (d += 1) {
            key_buf[pos] = '/';
            pos += 1;
            const seg_len = 3 + r.intRangeLessThan(u32, 0, 10);
            var s: u32 = 0;
            while (s < seg_len) : (s += 1) {
                key_buf[pos] = 'a' + @as(u8, @intCast(r.intRangeLessThan(u32, 0, 26)));
                pos += 1;
            }
        }
        const suffix = std.fmt.bufPrint(key_buf[pos..], "_{d}", .{i}) catch unreachable;
        pos += suffix.len;

        std.mem.writeInt(u64, &val_buf, i, .big);
        try slug_tree.insert(key_buf[0..pos], &val_buf);
        i += 1;
    }

    try std.testing.expect(slug_tree.entry_count == N);
}

test "stress: 150k inserts of variable-length slug-path-shaped keys" {
    const path = "/tmp/test_btree_stress_slug.db";
    const file = try createTempFile(path);
    defer file.close();
    defer std.fs.cwd().deleteFile(path) catch {};

    var cache = try page_cache.PageCache.init(std.testing.allocator, file, 1024);
    defer cache.deinit();
    var fl = freelist.FreeList.init(&cache, INVALID_PAGE);
    var tree = BPlusTree.init(&cache, &fl, INVALID_PAGE);

    var prng = std.Random.DefaultPrng.init(0xC0FFEE);
    const r = prng.random();
    const N: u32 = 150_000;

    var key_buf: [256]u8 = undefined;
    var val_buf: [8]u8 = undefined;

    var i: u32 = 0;
    while (i < N) : (i += 1) {
        const depth = 1 + r.intRangeLessThan(u32, 0, 4);
        var pos: usize = 0;
        @memcpy(key_buf[pos..][0..3], "top");
        pos += 3;
        var d: u32 = 0;
        while (d < depth) : (d += 1) {
            key_buf[pos] = '/';
            pos += 1;
            const seg_len = 3 + r.intRangeLessThan(u32, 0, 10);
            var s: u32 = 0;
            while (s < seg_len) : (s += 1) {
                key_buf[pos] = 'a' + @as(u8, @intCast(r.intRangeLessThan(u32, 0, 26)));
                pos += 1;
            }
        }
        const suffix = std.fmt.bufPrint(key_buf[pos..], "_{d}", .{i}) catch unreachable;
        pos += suffix.len;

        std.mem.writeInt(u64, &val_buf, i, .big);
        try tree.insert(key_buf[0..pos], &val_buf);
    }

    try std.testing.expect(tree.entry_count == N);
}
