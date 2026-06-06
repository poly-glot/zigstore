const std = @import("std");
const page = @import("../page.zig");
const page_cache = @import("../page_cache.zig");
const freelist = @import("../freelist.zig");

const PageId = page.PageId;
const INVALID_PAGE = page.INVALID_PAGE;
const SlotEntry = page.SlotEntry;
const Page = page.Page;

const MAX_DEPTH = 32;

pub const MAX_ENTRIES_PER_PAGE = page.BODY_SIZE / @sizeOf(page.SlotEntry);

pub const SPLIT_BUF_SIZE = page.PAGE_SIZE * 2;

pub const Range = struct { off: usize, len: u32 };

pub fn compareKeys(a: []const u8, b: []const u8) std.math.Order {
    return std.mem.order(u8, a, b);
}

pub inline fn slotsFromConstPage(pg: *const Page) [*]const SlotEntry {
    return @ptrCast(@alignCast(&pg.body));
}

pub inline fn slotsFromPage(pg: *Page) [*]SlotEntry {
    return @ptrCast(@alignCast(&pg.body));
}

pub inline fn isLeaf(pg: *const Page) bool {
    return pg.header.page_type == @intFromEnum(page.PageType.leaf);
}

pub inline fn isInternal(pg: *const Page) bool {
    return pg.header.page_type == @intFromEnum(page.PageType.internal);
}

pub fn readPageId(data: []const u8) PageId {
    if (data.len < 4) return INVALID_PAGE;
    return std.mem.readInt(u32, data[0..4], .little);
}

pub const PathStack = struct {
    items: [MAX_DEPTH]PageId = undefined,
    len: u8 = 0,

    pub fn push(self: *PathStack, pid: PageId) void {
        if (self.len < MAX_DEPTH) {
            self.items[self.len] = pid;
            self.len += 1;
        }
    }

    pub fn pop(self: *PathStack) ?PageId {
        if (self.len == 0) return null;
        self.len -= 1;
        return self.items[self.len];
    }
};

pub const LeafWithPath = struct {
    leaf: PageId,
    path: PathStack,
};

pub const BTreeError = error{
    NotEnoughSpace,
    InvalidPosition,
    CacheFull,
    PageNotFound,
    PageLimitExhausted,
    DiskError,
    KeyNotFound,
    KeyTooLarge,
    ValueTooLarge,
    Corrupted,
    OutOfMemory,
};

pub const KV = struct {
    key: []const u8,
    value: []const u8,
};

pub const RangeScanIterator = struct {
    cache: *page_cache.PageCache,
    current_page: PageId,
    current_slot: u32,
    end_key: ?[]const u8,

    key_buf: [256]u8 = undefined,
    val_buf: [page.PAGE_SIZE]u8 = undefined,
    key_len: u32 = 0,
    val_len: u32 = 0,

    pub fn next(self: *RangeScanIterator) !?KV {
        while (self.current_page != INVALID_PAGE and self.current_page != 0) {
            const pg = try self.cache.getPage(self.current_page);

            if (!isLeaf(pg)) {
                self.cache.unpinPage(self.current_page);
                self.current_page = INVALID_PAGE;
                return null;
            }

            if (self.current_slot >= pg.header.key_count) {
                const sibling = pg.header.right_sibling;
                self.cache.unpinPage(self.current_page);
                self.current_page = sibling;
                self.current_slot = 0;
                continue;
            }

            const slots = slotsFromConstPage(pg);
            const slot = slots[self.current_slot];
            const key = page.getKeyAt(pg, slot);
            const value = page.getValueAt(pg, slot);

            if (self.end_key) |ek| {
                if (compareKeys(key, ek) != .lt) {
                    self.cache.unpinPage(self.current_page);
                    self.current_page = INVALID_PAGE;
                    return null;
                }
            }

            self.key_len = slot.key_len;
            self.val_len = slot.value_len;
            @memcpy(self.key_buf[0..self.key_len], key);
            @memcpy(self.val_buf[0..self.val_len], value);

            self.current_slot += 1;
            self.cache.unpinPage(self.current_page);

            return KV{
                .key = self.key_buf[0..self.key_len],
                .value = self.val_buf[0..self.val_len],
            };
        }
        return null;
    }
};

pub const RepairStats = struct {
    repaired: bool,
    leaves_walked: u64,
    new_internals_allocated: u64,
    leaf_siblings_fixed: u64,
    new_root: PageId,
};

pub const BPlusTree = struct {
    cache: *page_cache.PageCache,
    free_list: *freelist.FreeList,
    root_page: PageId,
    lock: std.Thread.RwLock,

    cached_rightmost_leaf: PageId = INVALID_PAGE,
    cached_rightmost_path: PathStack = .{},

    entry_count: u64 = 0,

    pub const init = @import("btree_helpers.zig").init;
    pub const getRootPage = @import("btree_helpers.zig").getRootPage;
    pub const entryCount = @import("btree_helpers.zig").entryCount;
    pub const truncate = @import("btree_helpers.zig").truncate;
    pub const rangeScan = @import("btree_helpers.zig").rangeScan;
    pub const getMutablePage = @import("btree_helpers.zig").getMutablePage;

    pub const search = @import("btree_search.zig").search;
    pub const findLeaf = @import("btree_search.zig").findLeaf;
    pub const findLeafWithPath = @import("btree_search.zig").findLeafWithPath;
    pub const findInsertPos = @import("btree_search.zig").findInsertPos;

    pub const insert = @import("btree_insert.zig").insert;

    pub const delete = @import("btree_delete.zig").delete;

    pub const repairFromLeafChain = @import("btree_repair.zig").repairFromLeafChain;
};
