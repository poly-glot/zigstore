const std = @import("std");
const page = @import("../page.zig");
const btree = @import("btree.zig");

const PageId = page.PageId;
const SlotEntry = page.SlotEntry;
const INVALID_PAGE = page.INVALID_PAGE;

const BPlusTree = btree.BPlusTree;
const PathStack = btree.PathStack;
const RepairStats = btree.RepairStats;
const slotsFromConstPage = btree.slotsFromConstPage;
const isLeaf = btree.isLeaf;

pub fn repairFromLeafChain(self: *BPlusTree, allocator: std.mem.Allocator) !RepairStats {
    self.lock.lock();
    defer self.lock.unlock();

    var stats = RepairStats{
        .repaired = false,
        .leaves_walked = 0,
        .new_internals_allocated = 0,
        .leaf_siblings_fixed = 0,
        .new_root = self.root_page,
    };

    if (self.root_page == INVALID_PAGE) return stats;

    const root_was_leaf = blk: {
        const pg = try self.cache.getPage(self.root_page);
        const is_leaf = isLeaf(pg);
        const has_sibling = pg.header.right_sibling != INVALID_PAGE;
        self.cache.unpinPage(self.root_page);
        if (!is_leaf) return stats;
        if (!has_sibling) return stats;
        break :blk is_leaf;
    };

    const leftmost_leaf = try self.findLeaf("");

    const Entry = struct { key_off: u32, key_len: u16, pid: PageId };
    var keys: std.ArrayListUnmanaged(u8) = .{};
    defer keys.deinit(allocator);
    var entries: std.ArrayListUnmanaged(Entry) = .{};

    const file_page_count = self.cache.page_count;

    var cur = leftmost_leaf;
    while (cur != INVALID_PAGE) {
        if (cur >= file_page_count) {
            std.log.scoped(.btree).warn(
                "repair: stopping leaf walk — sibling pid {d} >= page_count {d}",
                .{ cur, file_page_count },
            );
            break;
        }
        const pg_or = self.cache.getPage(cur);
        const pg = pg_or catch |err| {
            std.log.scoped(.btree).warn(
                "repair: stopping leaf walk — getPage({d}) failed: {}",
                .{ cur, err },
            );
            break;
        };
        if (!isLeaf(pg)) {
            std.log.scoped(.btree).warn(
                "repair: stopping leaf walk — page {d} is not a leaf (type={d})",
                .{ cur, pg.header.page_type },
            );
            self.cache.unpinPage(cur);
            break;
        }
        stats.leaves_walked += 1;
        if (pg.header.key_count > 0) {
            const slots = slotsFromConstPage(pg);
            const first_key = page.getKeyAt(pg, slots[0]);
            const off: u32 = @intCast(keys.items.len);
            keys.appendSlice(allocator, first_key) catch |e| {
                self.cache.unpinPage(cur);
                entries.deinit(allocator);
                return e;
            };
            entries.append(allocator, .{
                .key_off = off,
                .key_len = @intCast(first_key.len),
                .pid = cur,
            }) catch |e| {
                self.cache.unpinPage(cur);
                entries.deinit(allocator);
                return e;
            };
        }
        const sib = pg.header.right_sibling;
        self.cache.unpinPage(cur);
        cur = sib;
    }

    if (entries.items.len == 0) {
        entries.deinit(allocator);
        return stats;
    }

    var fixed_siblings: u64 = 0;
    for (entries.items, 0..) |ent, idx| {
        const next_pid: PageId = if (idx + 1 < entries.items.len)
            entries.items[idx + 1].pid
        else
            INVALID_PAGE;
        const lpg = self.cache.getPageMut(ent.pid) catch |err| {
            std.log.scoped(.btree).warn("repair: relink getPageMut({d}) failed: {}", .{ ent.pid, err });
            continue;
        };
        if (lpg.header.right_sibling != next_pid) {
            lpg.header.right_sibling = next_pid;
            fixed_siblings += 1;
        }
        self.cache.unpinPage(ent.pid);
    }
    if (fixed_siblings > 0) {
        std.log.scoped(.btree).info("repair: relinked {d} leaf siblings", .{fixed_siblings});
        stats.leaf_siblings_fixed = fixed_siblings;
        stats.repaired = true;
    }

    if (entries.items.len == 1) {
        entries.deinit(allocator);
        return stats;
    }

    if (!root_was_leaf) {
        entries.deinit(allocator);
        return stats;
    }

    var current: std.ArrayListUnmanaged(Entry) = entries;

    while (current.items.len > 1) {
        var next: std.ArrayListUnmanaged(Entry) = .{};
        errdefer next.deinit(allocator);

        var i: usize = 0;
        while (i < current.items.len) {
            const new_id = try self.cache.allocatePage();
            stats.new_internals_allocated += 1;

            const pg = try self.cache.getPageMut(new_id);
            page.initInternal(pg, new_id);

            const layer_first_key_off = current.items[i].key_off;
            const layer_first_key_len = current.items[i].key_len;

            var packed_count: usize = 1;
            var last_child_pid = current.items[i].pid;
            var pid_buf: [4]u8 = undefined;

            while (i + packed_count < current.items.len) {
                const ch = current.items[i + packed_count];
                const sep_key = keys.items[ch.key_off..][0..ch.key_len];
                std.mem.writeInt(u32, &pid_buf, last_child_pid, .little);

                const slot_overhead: u32 = @sizeOf(SlotEntry);
                const needed: u32 = slot_overhead + @as(u32, @intCast(sep_key.len)) + 4;
                if (needed > page.freeSpace(pg)) break;

                page.insertEntry(pg, sep_key, &pid_buf, pg.header.key_count) catch break;
                last_child_pid = ch.pid;
                packed_count += 1;
            }

            pg.header.right_sibling = last_child_pid;
            self.cache.unpinPage(new_id);

            try next.append(allocator, .{
                .key_off = layer_first_key_off,
                .key_len = layer_first_key_len,
                .pid = new_id,
            });

            i += packed_count;
        }

        current.deinit(allocator);
        current = next;
    }

    const new_root = current.items[0].pid;
    current.deinit(allocator);

    self.root_page = new_root;
    self.cached_rightmost_leaf = INVALID_PAGE;
    self.cached_rightmost_path = .{};

    stats.repaired = true;
    stats.new_root = new_root;
    return stats;
}

const page_cache = @import("../page_cache.zig");
const freelist = @import("../freelist.zig");

fn createTempFile(name: []const u8) !std.fs.File {
    return std.fs.cwd().createFile(name, .{
        .read = true,
        .truncate = true,
    });
}

test "repairFromLeafChain rebuilds navigation over an artificially broken tree" {
    const path = "/tmp/test_btree_repair.db";
    const file = try createTempFile(path);
    defer file.close();
    defer std.fs.cwd().deleteFile(path) catch {};

    var cache = try page_cache.PageCache.init(std.testing.allocator, file, 256);
    defer cache.deinit();
    var fl = freelist.FreeList.init(&cache, INVALID_PAGE);

    var tree = BPlusTree.init(&cache, &fl, INVALID_PAGE);

    const N: u32 = 600;
    var key_buf: [4]u8 = undefined;
    var val_buf: [400]u8 = undefined;
    for (&val_buf) |*b| b.* = 'x';
    var k: u32 = 0;
    while (k < N) : (k += 1) {
        std.mem.writeInt(u32, &key_buf, k, .big);
        try tree.insert(&key_buf, &val_buf);
    }

    {
        var sb: [page.PAGE_SIZE]u8 = undefined;
        std.mem.writeInt(u32, &key_buf, N / 2, .big);
        const found = try tree.search(&key_buf, &sb);
        try std.testing.expect(found != null);
    }

    const leftmost_leaf = try tree.findLeaf("");

    tree.root_page = leftmost_leaf;
    tree.cached_rightmost_leaf = INVALID_PAGE;
    tree.cached_rightmost_path = .{};

    {
        var sb: [page.PAGE_SIZE]u8 = undefined;
        std.mem.writeInt(u32, &key_buf, N - 1, .big);
        const before_repair = try tree.search(&key_buf, &sb);
        try std.testing.expect(before_repair == null);
    }

    const stats = try tree.repairFromLeafChain(std.testing.allocator);
    try std.testing.expect(stats.repaired);
    try std.testing.expect(stats.leaves_walked > 1);

    var k2: u32 = 0;
    while (k2 < N) : (k2 += 1) {
        var sb: [page.PAGE_SIZE]u8 = undefined;
        std.mem.writeInt(u32, &key_buf, k2, .big);
        const v = try tree.search(&key_buf, &sb);
        try std.testing.expect(v != null);
    }

    const stats2 = try tree.repairFromLeafChain(std.testing.allocator);
    try std.testing.expect(!stats2.repaired);
}
