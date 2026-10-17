//! The sharded LRU page cache over `store.dat`. The cache is **no-steal**: eviction only ever
//! reclaims clean frames, and the one path that writes tree pages to disk is `checkpoint`,
//! which journals every dirty page (plus the superblock) to `checkpoint.journal` before
//! writing any of them in place. `redoJournal` replays a complete journal on open, so a crash
//! at any point leaves `store.dat` either entirely pre-checkpoint or entirely post-checkpoint.

const std = @import("std");
const Io = std.Io;
const sys = @import("sys.zig");
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
    free_list: std.ArrayList(*CacheEntry),
    head: ?*CacheEntry,
    tail: ?*CacheEntry,
    lock: Io.Mutex,
};

/// The `checkpoint.journal` magic: ASCII "CKPJ".
pub const JOURNAL_MAGIC: u32 = 0x434B504A;

/// The `checkpoint.journal` format version.
pub const JOURNAL_VERSION: u32 = 1;

/// The 32-byte `checkpoint.journal` header, written at offset 0 **after** every record so an
/// interrupted journal reads as `record_count == 0` or a CRC mismatch. `crc32` covers the
/// concatenated records (`page_id u32 LE` + `PAGE_SIZE` bytes each).
pub const JournalHeader = extern struct {
    magic: u32 = JOURNAL_MAGIC,
    version: u32 = JOURNAL_VERSION,
    record_count: u32,
    _pad: u32 = 0,
    crc32: u32,
    _reserved: [12]u8 = @splat(0),
};

comptime {
    if (@sizeOf(JournalHeader) != 32) @compileError("JournalHeader must be 32 bytes");
}

const JOURNAL_HEADER_SIZE: usize = @sizeOf(JournalHeader);
const JOURNAL_RECORD_SIZE: usize = @sizeOf(u32) + page.PAGE_SIZE;

pub const PageCache = struct {
    io: Io,
    shards: [NUM_SHARDS]CacheShard,
    file: Io.File,
    allocator: std.mem.Allocator,
    hit_count: std.atomic.Value(u64),
    miss_count: std.atomic.Value(u64),
    page_count: u32,
    alloc_lock: Io.Mutex,
    capacity: u32,
    dirty_count: std.atomic.Value(u32),

    pub fn init(io: Io, allocator: std.mem.Allocator, file: Io.File, capacity: usize) !PageCache {
        const file_size = file.length(io) catch 0;
        const raw_count = file_size / page.PAGE_SIZE;
        const reserved = @max(raw_count, 1);
        const pc: u32 = if (reserved > std.math.maxInt(u32)) std.math.maxInt(u32) else @intCast(reserved);

        const per_shard = @max(capacity / NUM_SHARDS, 1);

        var self = PageCache{
            .io = io,
            .shards = undefined,
            .file = file,
            .allocator = allocator,
            .hit_count = std.atomic.Value(u64).init(0),
            .miss_count = std.atomic.Value(u64).init(0),
            .page_count = pc,
            .alloc_lock = .init,
            .capacity = @intCast(per_shard * NUM_SHARDS),
            .dirty_count = std.atomic.Value(u32).init(0),
        };

        var built: usize = 0;
        errdefer {
            var j: usize = 0;
            while (j < built) : (j += 1) {
                self.shards[j].map.deinit();
                self.shards[j].free_list.deinit(allocator);
                allocator.free(self.shards[j].entries);
            }
        }

        for (&self.shards) |*shard| {
            const entries = try allocator.alloc(CacheEntry, per_shard);
            errdefer allocator.free(entries);

            var fl: std.ArrayList(*CacheEntry) = .empty;
            errdefer fl.deinit(allocator);
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
                .lock = .init,
            };
            built += 1;
        }

        return self;
    }

    /// Free every frame. Writes nothing: dirty pages that were never checkpointed are dropped
    /// (and reported at `warn`, since a deliberate crash-close reaches this path too), because
    /// the store checkpoints before tearing its cache down.
    pub fn deinit(self: *PageCache) void {
        const dropped = self.dirty_count.load(.acquire);
        if (dropped > 0) {
            log.warn("PageCache.deinit: dropping {d} dirty pages that were never checkpointed", .{dropped});
        }
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
        shard.lock.lockUncancelable(self.io);
        defer shard.lock.unlock(self.io);

        if (shard.map.get(page_id)) |entry| {
            _ = self.hit_count.fetchAdd(1, .monotonic);
            entry.pin_count += 1;
            if (mark_dirty) self.markDirty(entry);
            moveToFront(shard, entry);
            return @ptrCast(@alignCast(&entry.data));
        }

        _ = self.miss_count.fetchAdd(1, .monotonic);

        const entry = try getFreeShard(shard);
        errdefer {
            entry.page_id = page.INVALID_PAGE;
            entry.pin_count = 0;
            shard.free_list.appendAssumeCapacity(entry);
        }
        try readFromDisk(page_id, &entry.data, self.file.handle);
        try shard.map.put(page_id, entry);

        entry.page_id = page_id;
        entry.pin_count = 1;
        if (mark_dirty) self.markDirty(entry);
        moveToFront(shard, entry);

        return @ptrCast(@alignCast(&entry.data));
    }

    fn markDirty(self: *PageCache, entry: *CacheEntry) void {
        if (entry.dirty) return;
        entry.dirty = true;
        _ = self.dirty_count.fetchAdd(1, .acq_rel);
    }

    pub fn unpinPage(self: *PageCache, page_id: page.PageId) void {
        const shard = &self.shards[shardIndex(page_id)];
        shard.lock.lockUncancelable(self.io);
        defer shard.lock.unlock(self.io);

        if (shard.map.get(page_id)) |entry| {
            if (entry.pin_count > 0) {
                entry.pin_count -= 1;
            }
        }
    }

    /// Whether at least half of the frames hold dirty pages. The store uses this as the
    /// pressure valve that triggers a page flush before eviction can run out of clean frames.
    pub fn dirtyPressureHigh(self: *const PageCache) bool {
        return self.dirty_count.load(.acquire) >= self.capacity / 2;
    }

    /// Write every dirty page and the supplied superblock image to disk, atomically with
    /// respect to a crash. Journal phase: each dirty page (checksum stamped) is appended to
    /// `journal` as `page_id` + bytes, the header page last as page 0, then the journal header
    /// is written and the journal fsynced. In-place phase: the same images are written to
    /// `store.dat` (header page last), `fdatasync`ed, and the pages marked clean. The caller
    /// holds the store's drain and apply locks so nothing dirties a page between the two phases,
    /// and empties the journal afterward. The only path that writes tree pages.
    pub fn checkpoint(self: *PageCache, journal: Io.File, header_page: *const [page.PAGE_SIZE]u8) !void {
        _ = try self.writeJournal(journal, header_page);
        try self.writeDirtyPagesInPlace();
        try sys.pwriteAll(self.file.handle, header_page, 0);
        sys.fdatasync(self.file.handle) catch |err| {
            log.err("checkpoint: fdatasync failed: {}", .{err});
            return CacheError.DiskError;
        };
        self.markAllClean();
    }

    fn writeJournal(self: *PageCache, journal: Io.File, header_page: *const [page.PAGE_SIZE]u8) !u32 {
        try sys.ftruncate(journal.handle, 0);

        var crc = std.hash.crc.Crc32.init();
        var offset: u64 = JOURNAL_HEADER_SIZE;
        var count: u32 = 0;

        for (&self.shards) |*shard| {
            shard.lock.lockUncancelable(self.io);
            defer shard.lock.unlock(self.io);

            var it = shard.map.iterator();
            while (it.next()) |kv| {
                const entry = kv.value_ptr.*;
                if (!entry.dirty) continue;
                stampChecksum(&entry.data);
                try appendJournalRecord(journal.handle, &crc, &offset, entry.page_id, &entry.data);
                count += 1;
            }
        }

        try appendJournalRecord(journal.handle, &crc, &offset, 0, header_page);
        count += 1;

        const header = JournalHeader{ .record_count = count, .crc32 = crc.final() };
        try sys.pwriteAll(journal.handle, std.mem.asBytes(&header), 0);
        try sys.fsync(journal.handle);
        return count;
    }

    fn appendJournalRecord(journal: sys.fd_t, crc: *std.hash.crc.Crc32, offset: *u64, page_id: page.PageId, data: *const [page.PAGE_SIZE]u8) !void {
        var id_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &id_bytes, page_id, .little);
        crc.update(&id_bytes);
        crc.update(data);
        try sys.pwriteAll(journal, &id_bytes, offset.*);
        try sys.pwriteAll(journal, data, offset.* + id_bytes.len);
        offset.* += JOURNAL_RECORD_SIZE;
    }

    fn writeDirtyPagesInPlace(self: *PageCache) !void {
        for (&self.shards) |*shard| {
            shard.lock.lockUncancelable(self.io);
            defer shard.lock.unlock(self.io);

            var it = shard.map.iterator();
            while (it.next()) |kv| {
                const entry = kv.value_ptr.*;
                if (!entry.dirty) continue;
                try sys.pwriteAll(self.file.handle, &entry.data, @as(u64, entry.page_id) * page.PAGE_SIZE);
            }
        }
    }

    fn markAllClean(self: *PageCache) void {
        for (&self.shards) |*shard| {
            shard.lock.lockUncancelable(self.io);
            defer shard.lock.unlock(self.io);

            var it = shard.map.iterator();
            while (it.next()) |kv| {
                const entry = kv.value_ptr.*;
                if (!entry.dirty) continue;
                entry.dirty = false;
                _ = self.dirty_count.fetchSub(1, .acq_rel);
            }
        }
    }

    /// Replay a `checkpoint.journal` left behind by a crash into `file`. Returns `true` when
    /// the journal was complete (magic, version, count, and CRC all valid) and every image has
    /// been written and `fdatasync`ed; `false` when it was incomplete or invalid and `file` was
    /// left untouched. The caller empties the journal either way.
    pub fn redoJournal(io: Io, journal: Io.File, file: Io.File) !bool {
        const record_count = (try completeJournalRecordCount(io, journal)) orelse return false;
        try applyJournalRecords(journal.handle, file.handle, record_count);
        sys.fdatasync(file.handle) catch |err| {
            log.err("redoJournal: fdatasync failed: {}", .{err});
            return CacheError.DiskError;
        };
        return true;
    }

    fn completeJournalRecordCount(io: Io, journal: Io.File) !?u32 {
        var header_buf: [JOURNAL_HEADER_SIZE]u8 = undefined;
        const n = try sys.preadAll(journal.handle, &header_buf, 0);
        if (n < JOURNAL_HEADER_SIZE) return null;

        const header = std.mem.bytesToValue(JournalHeader, &header_buf);
        if (header.magic != JOURNAL_MAGIC or header.version != JOURNAL_VERSION or header.record_count == 0) return null;

        const needed = JOURNAL_HEADER_SIZE + @as(u64, header.record_count) * JOURNAL_RECORD_SIZE;
        if ((try journal.length(io)) < needed) return null;

        var crc = std.hash.crc.Crc32.init();
        var record: [JOURNAL_RECORD_SIZE]u8 = undefined;
        for (0..header.record_count) |i| {
            const got = try sys.preadAll(journal.handle, &record, JOURNAL_HEADER_SIZE + i * JOURNAL_RECORD_SIZE);
            if (got < JOURNAL_RECORD_SIZE) return null;
            crc.update(&record);
        }
        if (crc.final() != header.crc32) return null;
        return header.record_count;
    }

    fn applyJournalRecords(journal: sys.fd_t, file: sys.fd_t, record_count: u32) !void {
        var record: [JOURNAL_RECORD_SIZE]u8 = undefined;
        for (0..record_count) |i| {
            const got = try sys.preadAll(journal, &record, JOURNAL_HEADER_SIZE + i * JOURNAL_RECORD_SIZE);
            if (got < JOURNAL_RECORD_SIZE) return CacheError.DiskError;
            const page_id = std.mem.readInt(u32, record[0..4], .little);
            try sys.pwriteAll(file, record[4..], @as(u64, page_id) * page.PAGE_SIZE);
        }
    }

    pub fn allocatePage(self: *PageCache) !page.PageId {
        self.alloc_lock.lockUncancelable(self.io);
        defer self.alloc_lock.unlock(self.io);

        if (self.page_count >= page.INVALID_PAGE) {
            return CacheError.PageLimitExhausted;
        }
        const new_id = self.page_count;
        self.page_count += 1;

        const offset: u64 = @as(u64, new_id) * page.PAGE_SIZE;
        const zeros: [page.PAGE_SIZE]u8 = @splat(0);
        sys.pwriteAll(self.file.handle, &zeros, offset) catch return CacheError.DiskError;

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

    fn getFreeShard(shard: *CacheShard) !*CacheEntry {
        if (shard.free_list.items.len > 0) {
            return shard.free_list.pop().?;
        }
        return try evictOneShard(shard);
    }

    fn evictOneShard(shard: *CacheShard) !*CacheEntry {
        var cursor = shard.tail;
        while (cursor) |entry| {
            if (entry.pin_count == 0 and !entry.dirty) {
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

    fn readFromDisk(page_id: page.PageId, buf: *[page.PAGE_SIZE]u8, file_handle: sys.fd_t) !void {
        const offset: u64 = @as(u64, page_id) * page.PAGE_SIZE;
        const total_read = sys.preadAll(file_handle, buf, offset) catch return CacheError.DiskError;

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

    fn stampChecksum(buf: *[page.PAGE_SIZE]u8) void {
        const pg: *page.Page = @ptrCast(@alignCast(buf));
        pg.header.checksum = page.computeChecksum(pg);
    }
};

fn createTempFile() !Io.File {
    return Io.Dir.cwd().createFile(std.testing.io, "/tmp/test_page_cache.db", .{
        .read = true,
        .truncate = true,
    });
}

fn deleteTempFile(path: []const u8) void {
    Io.Dir.cwd().deleteFile(std.testing.io, path) catch {};
}

test "basic get, unpin, and re-fetch" {
    const file = try createTempFile();
    defer file.close(std.testing.io);
    defer deleteTempFile("/tmp/test_page_cache.db");

    var cache = try PageCache.init(std.testing.io, std.testing.allocator, file, NUM_SHARDS * 4);
    defer cache.deinit();

    const pid = try cache.allocatePage();

    const pg = try cache.getPageMut(pid);
    page.initLeaf(@ptrCast(@alignCast(pg)), pid);
    try page.insertEntry(@ptrCast(@alignCast(pg)), "hello", "world", 0);
    cache.unpinPage(pid);

    const pg2 = try cache.getPage(pid);
    const p2: *const page.Page = @ptrCast(@alignCast(pg2));
    const slots = page.getSlots(p2);
    try std.testing.expectEqual(@as(u16, 1), p2.header.key_count);
    try std.testing.expectEqualSlices(u8, "hello", page.getKeyAt(p2, slots[0]));
    cache.unpinPage(pid);

    try std.testing.expectEqual(@as(u32, 1), cache.dirty_count.load(.acquire));
    cache.markAllClean();
}

test "eviction of LRU" {
    const file = try createTempFile();
    defer file.close(std.testing.io);
    defer deleteTempFile("/tmp/test_page_cache.db");

    var cache = try PageCache.init(std.testing.io, std.testing.allocator, file, NUM_SHARDS);
    defer cache.deinit();

    var pids: [2]page.PageId = undefined;
    for (0..NUM_SHARDS) |_| {
        _ = try cache.allocatePage();
    }
    pids[0] = 0;
    const extra = try cache.allocatePage();
    pids[1] = extra;

    const pg0 = try cache.getPage(pids[0]);
    _ = pg0;
    cache.unpinPage(pids[0]);

    const pg1 = try cache.getPage(pids[1]);
    _ = pg1;
    cache.unpinPage(pids[1]);

    try std.testing.expect(cache.miss_count.load(.monotonic) >= 2);
}

test "pin prevents eviction" {
    const file = try createTempFile();
    defer file.close(std.testing.io);
    defer deleteTempFile("/tmp/test_page_cache.db");

    var cache = try PageCache.init(std.testing.io, std.testing.allocator, file, NUM_SHARDS);
    defer cache.deinit();

    for (0..NUM_SHARDS + 1) |_| {
        _ = try cache.allocatePage();
    }

    const pid1: page.PageId = 0;
    const pid2: page.PageId = @intCast(NUM_SHARDS);

    _ = try cache.getPage(pid1);

    const result = cache.getPage(pid2);
    try std.testing.expectError(CacheError.CacheFull, result);

    cache.unpinPage(pid1);
    const pg2 = try cache.getPage(pid2);
    _ = pg2;
    cache.unpinPage(pid2);
}

fn createTempFileAt(path: []const u8) !Io.File {
    return Io.Dir.cwd().createFile(std.testing.io, path, .{ .read = true, .truncate = true });
}

test "LRU evicts oldest unpinned" {
    const path = "/tmp/test_page_cache_lru.db";
    const file = try createTempFileAt(path);
    defer file.close(std.testing.io);
    defer deleteTempFile(path);

    var cache = try PageCache.init(std.testing.io, std.testing.allocator, file, NUM_SHARDS * 2);
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
    shard.lock.lockUncancelable(std.testing.io);
    defer shard.lock.unlock(std.testing.io);
    try std.testing.expect(shard.map.get(a) == null);
    try std.testing.expect(shard.map.get(b) != null);
    try std.testing.expect(shard.map.get(c) != null);
}

fn shardIndexExternal(pid: page.PageId) usize {
    return @as(usize, pid) % NUM_SHARDS;
}

test "cross-shard pages access without contention" {
    const path = "/tmp/test_page_cache_xshard.db";
    const file = try createTempFileAt(path);
    defer file.close(std.testing.io);
    defer deleteTempFile(path);

    var cache = try PageCache.init(std.testing.io, std.testing.allocator, file, NUM_SHARDS * 2);
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
    cache.markAllClean();
}

test "re-pin after unpin returns same bytes" {
    const path = "/tmp/test_page_cache_repin.db";
    const file = try createTempFileAt(path);
    defer file.close(std.testing.io);
    defer deleteTempFile(path);

    var cache = try PageCache.init(std.testing.io, std.testing.allocator, file, NUM_SHARDS * 4);
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
    cache.markAllClean();
}

fn fillLeaf(cache: *PageCache, pid: page.PageId, key: []const u8, value: []const u8) !void {
    const pg = try cache.getPageMut(pid);
    page.initLeaf(pg, pid);
    try page.insertEntry(pg, key, value, 0);
    cache.unpinPage(pid);
}

fn readPageBytes(file: Io.File, pid: page.PageId, out: *[page.PAGE_SIZE]u8) !void {
    const got = try file.readPositionalAll(std.testing.io, out, @as(u64, pid) * page.PAGE_SIZE);
    try std.testing.expectEqual(@as(usize, page.PAGE_SIZE), got);
}

test "a dirty page is never evicted: when every unpinned frame is dirty the cache reports CacheFull" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const file = try tmp.dir.createFile(std.testing.io, "store.dat", .{ .read = true });
    defer file.close(std.testing.io);

    var cache = try PageCache.init(std.testing.io, std.testing.allocator, file, NUM_SHARDS);
    defer cache.deinit();

    for (0..NUM_SHARDS + 1) |_| _ = try cache.allocatePage();

    const a: page.PageId = 0;
    const b: page.PageId = @intCast(NUM_SHARDS);
    try fillLeaf(&cache, a, "k", "v");
    try std.testing.expectEqual(@as(u32, 1), cache.dirty_count.load(.acquire));

    try std.testing.expectError(CacheError.CacheFull, cache.getPage(b));

    const journal = try tmp.dir.createFile(std.testing.io, "checkpoint.journal", .{ .read = true });
    defer journal.close(std.testing.io);
    const header_page: [page.PAGE_SIZE]u8 = @splat(0);
    try cache.checkpoint(journal, &header_page);
    try std.testing.expectEqual(@as(u32, 0), cache.dirty_count.load(.acquire));

    _ = try cache.getPage(b);
    cache.unpinPage(b);
}

test "dirty pages reach disk only through checkpoint: a crash before it leaves store.dat untouched" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const file = try tmp.dir.createFile(std.testing.io, "store.dat", .{ .read = true });
    defer file.close(std.testing.io);

    var pid: page.PageId = undefined;
    {
        var cache = try PageCache.init(std.testing.io, std.testing.allocator, file, NUM_SHARDS * 2);
        defer cache.deinit();
        pid = try cache.allocatePage();
        try fillLeaf(&cache, pid, "lost", "on-crash");
        try std.testing.expectEqual(@as(u32, 1), cache.dirty_count.load(.acquire));
    }

    var on_disk: [page.PAGE_SIZE]u8 = undefined;
    try readPageBytes(file, pid, &on_disk);
    try std.testing.expect(std.mem.allEqual(u8, &on_disk, 0));
}

test "a complete journal is redone on open and an incomplete one is discarded" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const source = try tmp.dir.createFile(std.testing.io, "source.dat", .{ .read = true });
    defer source.close(std.testing.io);

    var header_page: [page.PAGE_SIZE]u8 = @splat(0);
    header_page[0] = 0xAB;

    const journal = try tmp.dir.createFile(std.testing.io, "checkpoint.journal", .{ .read = true });
    defer journal.close(std.testing.io);

    var pid: page.PageId = undefined;
    {
        var cache = try PageCache.init(std.testing.io, std.testing.allocator, source, NUM_SHARDS * 2);
        defer cache.deinit();
        pid = try cache.allocatePage();
        try fillLeaf(&cache, pid, "journaled", "page");
        try std.testing.expectEqual(@as(u32, 2), try cache.writeJournal(journal, &header_page));
        cache.markAllClean();
    }

    const target = try tmp.dir.createFile(std.testing.io, "target.dat", .{ .read = true });
    defer target.close(std.testing.io);
    try target.setLength(std.testing.io, @as(u64, pid + 1) * page.PAGE_SIZE);

    try std.testing.expect(try PageCache.redoJournal(std.testing.io, journal, target));

    var redone: [page.PAGE_SIZE]u8 align(4096) = undefined;
    try readPageBytes(target, pid, &redone);
    const pg: *const page.Page = @ptrCast(@alignCast(&redone));
    try std.testing.expect(page.verifyChecksum(pg));
    try std.testing.expectEqualSlices(u8, "journaled", page.getKeyAt(pg, page.getSlots(pg)[0]));
    try readPageBytes(target, 0, &redone);
    try std.testing.expectEqual(@as(u8, 0xAB), redone[0]);

    const journal_size = try journal.length(std.testing.io);
    try journal.setLength(std.testing.io, journal_size - 1);
    const untouched = try tmp.dir.createFile(std.testing.io, "untouched.dat", .{ .read = true });
    defer untouched.close(std.testing.io);
    try untouched.setLength(std.testing.io, @as(u64, pid + 1) * page.PAGE_SIZE);

    try std.testing.expect(!try PageCache.redoJournal(std.testing.io, journal, untouched));
    try readPageBytes(untouched, pid, &redone);
    try std.testing.expect(std.mem.allEqual(u8, &redone, 0));
}

test "concurrent allocatePage across threads" {
    const path = "/tmp/test_page_cache_concalloc.db";
    const file = try createTempFileAt(path);
    defer file.close(std.testing.io);
    defer deleteTempFile(path);

    var cache = try PageCache.init(std.testing.io, std.testing.allocator, file, NUM_SHARDS * 4);
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
    defer file.close(std.testing.io);
    defer deleteTempFile(path);

    var cache = try PageCache.init(std.testing.io, std.testing.allocator, file, NUM_SHARDS * 4);
    defer cache.deinit();

    const start = cache.page_count;
    var ids: [8]page.PageId = undefined;
    for (&ids) |*slot| slot.* = try cache.allocatePage();
    for (ids, 0..) |id, i| {
        try std.testing.expectEqual(start + @as(page.PageId, @intCast(i)), id);
    }
    try std.testing.expectEqual(start + 8, cache.page_count);

    const size = try file.length(std.testing.io);
    try std.testing.expect(size >= @as(u64, cache.page_count) * page.PAGE_SIZE);
}

test "init frees every partial allocation when an allocation fails midway" {
    const path = "/tmp/test_page_cache_init_leak.db";
    const file = try createTempFileAt(path);
    defer file.close(std.testing.io);
    defer deleteTempFile(path);

    var saw_failure = false;
    var fail_index: usize = 0;
    while (fail_index < 2 * NUM_SHARDS) : (fail_index += 1) {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = fail_index });
        var cache = PageCache.init(std.testing.io, failing.allocator(), file, NUM_SHARDS * 4) catch {
            saw_failure = true;
            continue;
        };
        cache.deinit();
        break;
    }
    try std.testing.expect(saw_failure);
}
