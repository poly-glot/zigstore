const std = @import("std");
const page = @import("page.zig");
const page_cache = @import("page_cache.zig");

pub const FreeListError = error{
    DoubleFree,
};

pub const FreeList = struct {
    head: page.PageId,
    cache: *page_cache.PageCache,
    mutex: std.Thread.Mutex,

    pub fn init(cache: *page_cache.PageCache, head: page.PageId) FreeList {
        return FreeList{
            .head = head,
            .cache = cache,
            .mutex = .{},
        };
    }

    pub fn allocPage(self: *FreeList) !page.PageId {
        self.mutex.lock();
        defer self.mutex.unlock();

        const log = std.log.scoped(.freelist);
        var skipped: u32 = 0;

        while (self.head != page.INVALID_PAGE and self.head != 0) {
            const pid = self.head;
            const pg_const = try self.cache.getPage(pid);
            const pg: *const page.Page = @ptrCast(@alignCast(pg_const));

            const next_bytes: *const [4]u8 = pg.body[0..4];
            const next = std.mem.readInt(u32, next_bytes, .little);
            const page_type = pg.header.page_type;
            self.cache.unpinPage(pid);
            self.head = next;

            if (page_type == @intFromEnum(page.PageType.free)) {
                if (skipped > 0) {
                    log.warn("allocPage: skipped {d} corrupted freelist entries before finding a valid free page (returned pid={d})", .{ skipped, pid });
                }
                return pid;
            }

            skipped += 1;
            if (skipped == 1) {
                log.warn("allocPage: freelist corruption — pid={d} marked head but page_type={d} (expected .free); skipping", .{ pid, page_type });
            }
            if (skipped > 1024) {
                log.warn("allocPage: freelist scan exceeded 1024 corrupted entries; truncating chain and extending file", .{});
                self.head = page.INVALID_PAGE;
                break;
            }
        }

        return try self.cache.allocatePage();
    }

    pub fn freePage(self: *FreeList, pid: page.PageId) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const pg_raw = try self.cache.getPageMut(pid);
        const pg: *page.Page = @ptrCast(@alignCast(pg_raw));

        if (pg.header.page_type == @intFromEnum(page.PageType.free)) {
            self.cache.unpinPage(pid);
            return FreeListError.DoubleFree;
        }

        pg.header.page_type = @intFromEnum(page.PageType.free);
        pg.header.key_count = 0;
        pg.header.page_id = pid;

        const next_bytes: *[4]u8 = pg.body[0..4];
        std.mem.writeInt(u32, next_bytes, self.head, .little);

        self.cache.unpinPage(pid);
        self.head = pid;
    }

    pub fn getHead(self: *FreeList) page.PageId {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.head;
    }
};

fn createTempFile() !std.fs.File {
    return std.fs.cwd().createFile("/tmp/test_freelist.db", .{
        .read = true,
        .truncate = true,
    });
}

test "alloc free roundtrip" {
    const file = try createTempFile();
    defer file.close();
    defer std.fs.cwd().deleteFile("/tmp/test_freelist.db") catch {};

    try file.setEndPos(page.PAGE_SIZE);

    var cache = try page_cache.PageCache.init(std.testing.allocator, file, 16);
    defer cache.deinit();

    var fl = FreeList.init(&cache, page.INVALID_PAGE);

    const p1 = try fl.allocPage();
    const p2 = try fl.allocPage();
    const p3 = try fl.allocPage();

    {
        const pg1 = try cache.getPageMut(p1);
        page.initLeaf(@ptrCast(@alignCast(pg1)), p1);
        cache.unpinPage(p1);
    }
    {
        const pg2 = try cache.getPageMut(p2);
        page.initLeaf(@ptrCast(@alignCast(pg2)), p2);
        cache.unpinPage(p2);
    }
    {
        const pg3 = try cache.getPageMut(p3);
        page.initLeaf(@ptrCast(@alignCast(pg3)), p3);
        cache.unpinPage(p3);
    }

    try fl.freePage(p3);
    try fl.freePage(p2);
    try fl.freePage(p1);

    try std.testing.expectEqual(p1, fl.getHead());

    const r1 = try fl.allocPage();
    const r2 = try fl.allocPage();
    const r3 = try fl.allocPage();

    try std.testing.expectEqual(p1, r1);
    try std.testing.expectEqual(p2, r2);
    try std.testing.expectEqual(p3, r3);

    try std.testing.expectEqual(page.INVALID_PAGE, fl.getHead());

    const p4 = try fl.allocPage();
    try std.testing.expect(p4 >= 3);
}

test "double free detection" {
    const file = try createTempFile();
    defer file.close();
    defer std.fs.cwd().deleteFile("/tmp/test_freelist.db") catch {};

    var cache = try page_cache.PageCache.init(std.testing.allocator, file, page_cache.NUM_SHARDS * 4);
    defer cache.deinit();

    var fl = FreeList.init(&cache, page.INVALID_PAGE);

    const p1 = try fl.allocPage();
    {
        const pg = try cache.getPageMut(p1);
        page.initLeaf(@ptrCast(@alignCast(pg)), p1);
        cache.unpinPage(p1);
    }

    try fl.freePage(p1);
    try std.testing.expectError(FreeListError.DoubleFree, fl.freePage(p1));
}

test "freelist getHead with mutex" {
    const file = try createTempFile();
    defer file.close();
    defer std.fs.cwd().deleteFile("/tmp/test_freelist.db") catch {};

    var cache = try page_cache.PageCache.init(std.testing.allocator, file, page_cache.NUM_SHARDS * 4);
    defer cache.deinit();

    var fl = FreeList.init(&cache, page.INVALID_PAGE);
    try std.testing.expectEqual(page.INVALID_PAGE, fl.getHead());

    const p1 = try fl.allocPage();
    {
        const pg = try cache.getPageMut(p1);
        page.initLeaf(@ptrCast(@alignCast(pg)), p1);
        cache.unpinPage(p1);
    }
    try fl.freePage(p1);
    try std.testing.expectEqual(p1, fl.getHead());
}
