const std = @import("std");
const posix = std.posix;
const page = @import("page.zig");

const log = std.log.scoped(.page_cache);

pub const CacheError = error{
    CacheFull,
    PageNotFound,
    DiskError,
    PageLimitExhausted,
};

pub const CacheEntry = struct {
    page_id: page.PageId = page.INVALID_PAGE,
    data: [page.PAGE_SIZE]u8 align(4096) = undefined,
    dirty: bool = false,
    pin_count: u32 = 0,
    prev: ?*CacheEntry = null,
    next: ?*CacheEntry = null,
};

pub const NUM_SHARDS = 64;

const CacheShard = struct {
    map: std.AutoHashMap(page.PageId, *CacheEntry),
    entries: []CacheEntry,
    free_list: std.ArrayListUnmanaged(*CacheEntry),
    head: ?*CacheEntry,
    tail: ?*CacheEntry,
    lock: std.Thread.Mutex,
};

pub const PageCache = struct {
    shards: [NUM_SHARDS]CacheShard,
    file: std.fs.File,
    allocator: std.mem.Allocator,
    hit_count: std.atomic.Value(u64),
    miss_count: std.atomic.Value(u64),
    page_count: u32,
    alloc_lock: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator, file: std.fs.File, capacity: usize) !PageCache {
        const file_size = file.getEndPos() catch 0;
        const raw_count = file_size / page.PAGE_SIZE;
        const reserved = @max(raw_count, 1);
        const pc: u32 = if (reserved > std.math.maxInt(u32)) std.math.maxInt(u32) else @intCast(reserved);

        const per_shard = @max(capacity / NUM_SHARDS, 1);

        var self = PageCache{
            .shards = undefined,
            .file = file,
            .allocator = allocator,
            .hit_count = std.atomic.Value(u64).init(0),
            .miss_count = std.atomic.Value(u64).init(0),
            .page_count = pc,
            .alloc_lock = .{},
        };

        for (&self.shards) |*shard| {
            const entries = try allocator.alloc(CacheEntry, per_shard);
            var fl: std.ArrayListUnmanaged(*CacheEntry) = .{};
            try fl.ensureTotalCapacity(allocator, per_shard);

            for (0..per_shard) |i| {
                entries[i] = CacheEntry{};
                fl.appendAssumeCapacity(&entries[i]);
            }

            shard.* = CacheShard{
                .map = std.AutoHashMap(page.PageId, *CacheEntry).init(allocator),
                .entries = entries,
                .free_list = fl,
                .head = null,
                .tail = null,
                .lock = .{},
            };
        }

        return self;
    }

    pub fn deinit(self: *PageCache) void {
        self.flushAll() catch |err| {
            std.log.err("PageCache.deinit: failed to flush dirty pages: {}", .{err});
        };
        for (&self.shards) |*shard| {
            shard.map.deinit();
            shard.free_list.deinit(self.allocator);
            self.allocator.free(shard.entries);
        }
    }

    inline fn shardIndex(page_id: page.PageId) usize {
        return @as(usize, page_id) % NUM_SHARDS;
    }

    pub fn getPage(self: *PageCache, page_id: page.PageId) !*const page.Page {
        return self.fetchPage(page_id, false);
    }

    pub fn getPageMut(self: *PageCache, page_id: page.PageId) !*page.Page {
        return self.fetchPage(page_id, true);
    }

    fn fetchPage(self: *PageCache, page_id: page.PageId, mark_dirty: bool) !*page.Page {
        const shard = &self.shards[shardIndex(page_id)];
        shard.lock.lock();
        defer shard.lock.unlock();

        if (shard.map.get(page_id)) |entry| {
            _ = self.hit_count.fetchAdd(1, .monotonic);
            entry.pin_count += 1;
            if (mark_dirty) entry.dirty = true;
            moveToFront(shard, entry);
            return @ptrCast(@alignCast(&entry.data));
        }

        _ = self.miss_count.fetchAdd(1, .monotonic);

        const entry = try getFreeShard(self, shard);
        try readFromDisk(page_id, &entry.data, self.file.handle);
        entry.page_id = page_id;
        entry.dirty = mark_dirty;
        entry.pin_count = 1;

        shard.map.put(page_id, entry) catch {
            entry.page_id = page.INVALID_PAGE;
            entry.pin_count = 0;
            shard.free_list.appendAssumeCapacity(entry);
            return error.OutOfMemory;
        };
        moveToFront(shard, entry);

        return @ptrCast(@alignCast(&entry.data));
    }

    pub fn unpinPage(self: *PageCache, page_id: page.PageId) void {
        const shard = &self.shards[shardIndex(page_id)];
        shard.lock.lock();
        defer shard.lock.unlock();

        if (shard.map.get(page_id)) |entry| {
            if (entry.pin_count > 0) {
                entry.pin_count -= 1;
            }
        }
    }

    pub fn flushPage(self: *PageCache, page_id: page.PageId) !void {
        const shard = &self.shards[shardIndex(page_id)];
        shard.lock.lock();
        defer shard.lock.unlock();

        const entry = shard.map.get(page_id) orelse return CacheError.PageNotFound;
        if (entry.dirty) {
            try writeToDisk(page_id, &entry.data, self.file.handle);
            entry.dirty = false;
        }
    }

    pub fn flushAll(self: *PageCache) !void {
        for (&self.shards) |*shard| {
            shard.lock.lock();
            defer shard.lock.unlock();

            var it = shard.map.iterator();
            while (it.next()) |kv| {
                const entry = kv.value_ptr.*;
                if (entry.dirty) {
                    try writeToDisk(kv.key_ptr.*, &entry.data, self.file.handle);
                    entry.dirty = false;
                }
            }
        }

        posix.fdatasync(self.file.handle) catch |err| {
            log.err("flushAll: fdatasync failed: {}", .{err});
            return CacheError.DiskError;
        };
    }

    pub fn allocatePage(self: *PageCache) !page.PageId {
        self.alloc_lock.lock();
        defer self.alloc_lock.unlock();

        if (self.page_count >= page.INVALID_PAGE) {
            return CacheError.PageLimitExhausted;
        }
        const new_id = self.page_count;
        self.page_count += 1;

        const offset: u64 = @as(u64, new_id) * page.PAGE_SIZE;
        const zeros = [_]u8{0} ** page.PAGE_SIZE;
        var total_written: usize = 0;
        while (total_written < page.PAGE_SIZE) {
            const n = std.posix.pwrite(self.file.handle, zeros[total_written..], offset + total_written) catch return CacheError.DiskError;
            if (n == 0) return CacheError.DiskError;
            total_written += n;
        }

        return new_id;
    }

    fn moveToFront(shard: *CacheShard, entry: *CacheEntry) void {
        if (shard.head == entry) return;

        if (entry.prev) |prev| prev.next = entry.next;
        if (entry.next) |nxt| nxt.prev = entry.prev;
        if (shard.tail == entry) shard.tail = entry.prev;

        entry.prev = null;
        entry.next = shard.head;
        if (shard.head) |old_head| old_head.prev = entry;
        shard.head = entry;
        if (shard.tail == null) shard.tail = entry;
    }

    fn getFreeShard(self: *PageCache, shard: *CacheShard) !*CacheEntry {
        if (shard.free_list.items.len > 0) {
            return shard.free_list.pop().?;
        }
        return try evictOneShard(self, shard);
    }

    fn evictOneShard(self: *PageCache, shard: *CacheShard) !*CacheEntry {
        var cursor = shard.tail;
        while (cursor) |entry| {
            if (entry.pin_count == 0) {
                if (entry.dirty) {
                    try writeToDisk(entry.page_id, &entry.data, self.file.handle);
                    entry.dirty = false;
                }

                if (entry.prev) |prev| prev.next = entry.next;
                if (entry.next) |nxt| nxt.prev = entry.prev;
                if (shard.head == entry) shard.head = entry.next;
                if (shard.tail == entry) shard.tail = entry.prev;
                entry.prev = null;
                entry.next = null;

                _ = shard.map.remove(entry.page_id);
                entry.page_id = page.INVALID_PAGE;

                return entry;
            }
            cursor = entry.prev;
        }
        return CacheError.CacheFull;
    }

    fn readFromDisk(page_id: page.PageId, buf: *[page.PAGE_SIZE]u8, file_handle: std.posix.fd_t) !void {
        const offset: u64 = @as(u64, page_id) * page.PAGE_SIZE;
        var total_read: usize = 0;
        while (total_read < page.PAGE_SIZE) {
            const n = std.posix.pread(file_handle, buf[total_read..], offset + total_read) catch return CacheError.DiskError;
            if (n == 0) break;
            total_read += n;
        }

        if (total_read == 0) {
            @memset(buf, 0);
            return;
        }
        if (total_read < page.PAGE_SIZE) {
            log.err("Short read for page {d}: {d} of {d} bytes — file truncated", .{ page_id, total_read, page.PAGE_SIZE });
            return CacheError.DiskError;
        }

        const pg: *const page.Page = @ptrCast(@alignCast(buf));
        if (pg.header.checksum != 0 and !page.verifyChecksum(pg)) {
            log.err("Checksum mismatch for page {d} — data corruption detected", .{page_id});
            return CacheError.DiskError;
        }
    }

    fn writeToDisk(page_id: page.PageId, buf: *const [page.PAGE_SIZE]u8, file_handle: std.posix.fd_t) !void {
        const mutable_buf: *[page.PAGE_SIZE]u8 = @constCast(buf);
        const pg: *page.Page = @ptrCast(@alignCast(mutable_buf));
        pg.header.checksum = page.computeChecksum(pg);

        const offset: u64 = @as(u64, page_id) * page.PAGE_SIZE;
        var total_written: usize = 0;
        while (total_written < page.PAGE_SIZE) {
            const n = std.posix.pwrite(file_handle, buf[total_written..], offset + total_written) catch return CacheError.DiskError;
            if (n == 0) return CacheError.DiskError;
            total_written += n;
        }
    }
};

fn createTempFile() !std.fs.File {
    return std.fs.cwd().createFile("/tmp/test_page_cache.db", .{
        .read = true,
        .truncate = true,
    });
}

test "basic get and flush" {
    const file = try createTempFile();
    defer file.close();
    defer std.fs.cwd().deleteFile("/tmp/test_page_cache.db") catch {};

    var cache = try PageCache.init(std.testing.allocator, file, NUM_SHARDS * 4);
    defer cache.deinit();

    const pid = try cache.allocatePage();

    const pg = try cache.getPageMut(pid);
    page.initLeaf(@ptrCast(@alignCast(pg)), pid);
    try page.insertEntry(@ptrCast(@alignCast(pg)), "hello", "world", 0);
    cache.unpinPage(pid);

    try cache.flushPage(pid);

    const pg2 = try cache.getPage(pid);
    const p2: *const page.Page = @ptrCast(@alignCast(pg2));
    const slots = page.getSlots(p2);
    try std.testing.expectEqual(@as(u16, 1), p2.header.key_count);
    try std.testing.expectEqualSlices(u8, "hello", page.getKeyAt(p2, slots[0]));
    cache.unpinPage(pid);
}

test "eviction of LRU" {
    const file = try createTempFile();
    defer file.close();
    defer std.fs.cwd().deleteFile("/tmp/test_page_cache.db") catch {};

    var cache = try PageCache.init(std.testing.allocator, file, NUM_SHARDS);
    defer cache.deinit();

    var pids: [2]page.PageId = undefined;
    for (0..NUM_SHARDS) |_| {
        _ = try cache.allocatePage();
    }
    pids[0] = 0;
    const extra = try cache.allocatePage();
    pids[1] = extra;

    const pg0 = try cache.getPageMut(pids[0]);
    _ = pg0;
    cache.unpinPage(pids[0]);

    const pg1 = try cache.getPageMut(pids[1]);
    _ = pg1;
    cache.unpinPage(pids[1]);

    try std.testing.expect(cache.miss_count.load(.monotonic) >= 2);
}

test "pin prevents eviction" {
    const file = try createTempFile();
    defer file.close();
    defer std.fs.cwd().deleteFile("/tmp/test_page_cache.db") catch {};

    var cache = try PageCache.init(std.testing.allocator, file, NUM_SHARDS);
    defer cache.deinit();

    for (0..NUM_SHARDS + 1) |_| {
        _ = try cache.allocatePage();
    }

    const pid1: page.PageId = 0;
    const pid2: page.PageId = @intCast(NUM_SHARDS);

    _ = try cache.getPageMut(pid1);

    const result = cache.getPageMut(pid2);
    try std.testing.expectError(CacheError.CacheFull, result);

    cache.unpinPage(pid1);
    const pg2 = try cache.getPageMut(pid2);
    _ = pg2;
    cache.unpinPage(pid2);
}

fn createTempFileAt(path: []const u8) !std.fs.File {
    return std.fs.cwd().createFile(path, .{ .read = true, .truncate = true });
}

test "LRU evicts oldest unpinned" {
    const path = "/tmp/test_page_cache_lru.db";
    const file = try createTempFileAt(path);
    defer file.close();
    defer std.fs.cwd().deleteFile(path) catch {};

    var cache = try PageCache.init(std.testing.allocator, file, NUM_SHARDS * 2);
    defer cache.deinit();

    for (0..2 * NUM_SHARDS + 1) |_| _ = try cache.allocatePage();

    const a: page.PageId = 0;
    const b: page.PageId = @intCast(NUM_SHARDS);
    const c: page.PageId = @intCast(2 * NUM_SHARDS);

    _ = try cache.getPage(a);
    cache.unpinPage(a);
    _ = try cache.getPage(b);
    cache.unpinPage(b);

    _ = try cache.getPage(c);
    cache.unpinPage(c);

    const shard = &cache.shards[0];
    shard.lock.lock();
    defer shard.lock.unlock();
    try std.testing.expect(shard.map.get(a) == null);
    try std.testing.expect(shard.map.get(b) != null);
    try std.testing.expect(shard.map.get(c) != null);
}

test "dirty page persists across re-fetch via flushAll" {
    const path = "/tmp/test_page_cache_dirty.db";
    const file = try createTempFileAt(path);
    defer file.close();
    defer std.fs.cwd().deleteFile(path) catch {};

    var cache = try PageCache.init(std.testing.allocator, file, NUM_SHARDS * 4);
    defer cache.deinit();

    const pid = try cache.allocatePage();
    const pg = try cache.getPageMut(pid);
    page.initLeaf(@ptrCast(@alignCast(pg)), pid);
    try page.insertEntry(@ptrCast(@alignCast(pg)), "k1", "v1", 0);
    cache.unpinPage(pid);

    try cache.flushAll();

    const shard = &cache.shards[shardIndexExternal(pid)];
    shard.lock.lock();
    const entry = shard.map.get(pid).?;
    try std.testing.expectEqual(false, entry.dirty);
    shard.lock.unlock();

    const pg2 = try cache.getPage(pid);
    const p2: *const page.Page = @ptrCast(@alignCast(pg2));
    try std.testing.expectEqual(@as(u32, 1), p2.header.key_count);
    cache.unpinPage(pid);
}

fn shardIndexExternal(pid: page.PageId) usize {
    return @as(usize, pid) % NUM_SHARDS;
}

test "cross-shard pages access without contention" {
    const path = "/tmp/test_page_cache_xshard.db";
    const file = try createTempFileAt(path);
    defer file.close();
    defer std.fs.cwd().deleteFile(path) catch {};

    var cache = try PageCache.init(std.testing.allocator, file, NUM_SHARDS * 2);
    defer cache.deinit();

    _ = try cache.allocatePage();
    _ = try cache.allocatePage();
    const pid_a: page.PageId = 0;
    const pid_b: page.PageId = 1;
    try std.testing.expect(shardIndexExternal(pid_a) != shardIndexExternal(pid_b));

    const a = try cache.getPageMut(pid_a);
    page.initLeaf(@ptrCast(@alignCast(a)), pid_a);
    const b = try cache.getPageMut(pid_b);
    page.initLeaf(@ptrCast(@alignCast(b)), pid_b);
    cache.unpinPage(pid_a);
    cache.unpinPage(pid_b);
}

test "re-pin after unpin returns same bytes" {
    const path = "/tmp/test_page_cache_repin.db";
    const file = try createTempFileAt(path);
    defer file.close();
    defer std.fs.cwd().deleteFile(path) catch {};

    var cache = try PageCache.init(std.testing.allocator, file, NUM_SHARDS * 4);
    defer cache.deinit();

    const pid = try cache.allocatePage();
    {
        const pg = try cache.getPageMut(pid);
        page.initLeaf(@ptrCast(@alignCast(pg)), pid);
        try page.insertEntry(@ptrCast(@alignCast(pg)), "alpha", "1", 0);
        cache.unpinPage(pid);
    }

    const again = try cache.getPage(pid);
    const p: *const page.Page = @ptrCast(@alignCast(again));
    const slots = page.getSlots(p);
    try std.testing.expectEqual(@as(u32, 1), p.header.key_count);
    try std.testing.expectEqualSlices(u8, "alpha", page.getKeyAt(p, slots[0]));
    cache.unpinPage(pid);
}

test "evicting dirty page flushes to disk first" {
    const path = "/tmp/test_page_cache_evictdirty.db";
    const file = try createTempFileAt(path);
    defer file.close();
    defer std.fs.cwd().deleteFile(path) catch {};

    var cache = try PageCache.init(std.testing.allocator, file, NUM_SHARDS);
    defer cache.deinit();

    for (0..NUM_SHARDS + 1) |_| _ = try cache.allocatePage();

    const a: page.PageId = 0;
    const b: page.PageId = @intCast(NUM_SHARDS);

    {
        const pg = try cache.getPageMut(a);
        page.initLeaf(@ptrCast(@alignCast(pg)), a);
        try page.insertEntry(@ptrCast(@alignCast(pg)), "evk", "evv", 0);
        cache.unpinPage(a);
    }

    {
        const pg = try cache.getPageMut(b);
        page.initLeaf(@ptrCast(@alignCast(pg)), b);
        cache.unpinPage(b);
    }

    const before_misses = cache.miss_count.load(.monotonic);
    const re = try cache.getPage(a);
    const p: *const page.Page = @ptrCast(@alignCast(re));
    try std.testing.expectEqual(@as(u32, 1), p.header.key_count);
    cache.unpinPage(a);
    try std.testing.expect(cache.miss_count.load(.monotonic) > before_misses);
}

test "concurrent allocatePage across threads" {
    const path = "/tmp/test_page_cache_concalloc.db";
    const file = try createTempFileAt(path);
    defer file.close();
    defer std.fs.cwd().deleteFile(path) catch {};

    var cache = try PageCache.init(std.testing.allocator, file, NUM_SHARDS * 4);
    defer cache.deinit();

    const Worker = struct {
        fn run(c: *PageCache, out: *[100]page.PageId) void {
            for (out) |*slot| {
                slot.* = c.allocatePage() catch page.INVALID_PAGE;
            }
        }
    };

    var ids_a: [100]page.PageId = undefined;
    var ids_b: [100]page.PageId = undefined;
    const t1 = try std.Thread.spawn(.{}, Worker.run, .{ &cache, &ids_a });
    const t2 = try std.Thread.spawn(.{}, Worker.run, .{ &cache, &ids_b });
    t1.join();
    t2.join();

    var seen = std.AutoHashMap(page.PageId, void).init(std.testing.allocator);
    defer seen.deinit();
    for (ids_a) |id| {
        try std.testing.expect(id != page.INVALID_PAGE);
        try seen.put(id, {});
    }
    for (ids_b) |id| {
        try std.testing.expect(id != page.INVALID_PAGE);
        try seen.put(id, {});
    }
    try std.testing.expectEqual(@as(u32, 200), seen.count());
}

test "allocatePage assigns sequential IDs and grows file" {
    const path = "/tmp/test_page_cache_grow.db";
    const file = try createTempFileAt(path);
    defer file.close();
    defer std.fs.cwd().deleteFile(path) catch {};

    var cache = try PageCache.init(std.testing.allocator, file, NUM_SHARDS * 4);
    defer cache.deinit();

    const start = cache.page_count;
    var ids: [8]page.PageId = undefined;
    for (&ids) |*slot| slot.* = try cache.allocatePage();
    for (ids, 0..) |id, i| {
        try std.testing.expectEqual(start + @as(page.PageId, @intCast(i)), id);
    }
    try std.testing.expectEqual(start + 8, cache.page_count);

    const size = try file.getEndPos();
    try std.testing.expect(size >= @as(u64, cache.page_count) * page.PAGE_SIZE);
}
