const std = @import("std");

const log = std.log.scoped(.memtable);

pub const NUM_SHARDS = 32;

pub const MemTable = struct {
    shards: [NUM_SHARDS]Shard,
    allocator: std.mem.Allocator,

    pub const Entry = struct {
        value: []const u8,
        tombstone: bool,
    };

    pub const GetResult = union(enum) {
        found: []const u8,
        deleted,
        not_found,
    };

    pub const Buffer = struct {
        map: std.StringHashMap(Entry),
        arena: std.heap.ArenaAllocator,

        fn init(allocator: std.mem.Allocator) Buffer {
            return .{
                .map = std.StringHashMap(Entry).init(allocator),
                .arena = std.heap.ArenaAllocator.init(allocator),
            };
        }

        fn deinit(self: *Buffer) void {
            self.map.deinit();
            self.arena.deinit();
        }

        fn reset(self: *Buffer) void {
            self.map.clearRetainingCapacity();
            _ = self.arena.reset(.retain_capacity);
        }
    };

    const Shard = struct {
        front: Buffer,
        back: Buffer,
        lock: std.Thread.Mutex,
    };

    pub fn init(allocator: std.mem.Allocator) MemTable {
        var shards: [NUM_SHARDS]Shard = undefined;
        for (&shards) |*s| {
            s.* = .{
                .front = Buffer.init(allocator),
                .back = Buffer.init(allocator),
                .lock = .{},
            };
        }
        return .{ .shards = shards, .allocator = allocator };
    }

    pub fn deinit(self: *MemTable) void {
        for (&self.shards) |*s| {
            s.front.deinit();
            s.back.deinit();
        }
    }

    pub fn put(self: *MemTable, key: []const u8, value: []const u8) !void {
        const shard = self.getShard(key);
        shard.lock.lock();
        defer shard.lock.unlock();
        try putInto(&shard.front, key, value, false);
    }

    pub fn delete(self: *MemTable, key: []const u8) !void {
        const shard = self.getShard(key);
        shard.lock.lock();
        defer shard.lock.unlock();
        try putInto(&shard.front, key, &.{}, true);
    }

    pub fn get(self: *MemTable, key: []const u8) GetResult {
        const shard = self.getShard(key);
        shard.lock.lock();
        defer shard.lock.unlock();
        return getFrom(&shard.front, key);
    }

    pub fn putIfAbsent(self: *MemTable, key: []const u8, value: []const u8) !bool {
        const shard = self.getShard(key);
        shard.lock.lock();
        defer shard.lock.unlock();
        const result = getFrom(&shard.front, key);
        switch (result) {
            .found => return false,
            .deleted => {},
            .not_found => {},
        }
        try putInto(&shard.front, key, value, false);
        return true;
    }

    pub fn count(self: *MemTable) u32 {
        var total: u32 = 0;
        for (&self.shards) |*s| {
            s.lock.lock();
            total += s.front.map.count();
            s.lock.unlock();
        }
        return total;
    }

    pub fn lockAll(self: *MemTable) void {
        for (&self.shards) |*s| s.lock.lock();
    }

    pub fn unlockAll(self: *MemTable) void {
        for (&self.shards) |*s| s.lock.unlock();
    }

    pub fn swapShardLocked(self: *MemTable, i: usize) *Buffer {
        const s = &self.shards[i];
        const tmp = s.front;
        s.front = s.back;
        s.back = tmp;
        return &s.back;
    }

    pub fn resetShardBackLocked(self: *MemTable, i: usize) void {
        self.shards[i].back.reset();
    }

    fn getShard(self: *MemTable, key: []const u8) *Shard {
        const h = std.hash.Wyhash.hash(0, key);
        return &self.shards[h % NUM_SHARDS];
    }

    fn putInto(buf: *Buffer, key: []const u8, value: []const u8, tombstone: bool) !void {
        const alloc = buf.arena.allocator();
        const owned_key = try alloc.dupe(u8, key);
        const owned_val = if (value.len > 0) try alloc.dupe(u8, value) else value;
        buf.map.put(owned_key, .{ .value = owned_val, .tombstone = tombstone }) catch |err| {
            log.err("memtable put failed: {}", .{err});
            return err;
        };
    }

    fn getFrom(buf: *const Buffer, key: []const u8) GetResult {
        if (buf.map.get(key)) |entry| {
            if (entry.tombstone) return .deleted;
            return .{ .found = entry.value };
        }
        return .not_found;
    }
};

test "MemTable put and get" {
    var mt = MemTable.init(std.testing.allocator);
    defer mt.deinit();

    try mt.put("key1", "value1");
    try mt.put("key2", "value2");

    const r1 = mt.get("key1");
    try std.testing.expectEqualSlices(u8, "value1", r1.found);

    const r2 = mt.get("key2");
    try std.testing.expectEqualSlices(u8, "value2", r2.found);

    try std.testing.expect(mt.get("key3") == .not_found);
}

test "MemTable overwrite" {
    var mt = MemTable.init(std.testing.allocator);
    defer mt.deinit();

    try mt.put("key", "v1");
    try mt.put("key", "v2");
    try std.testing.expectEqualSlices(u8, "v2", mt.get("key").found);
}

test "MemTable delete (tombstone)" {
    var mt = MemTable.init(std.testing.allocator);
    defer mt.deinit();

    try mt.put("key", "value");
    try mt.delete("key");
    try std.testing.expect(mt.get("key") == .deleted);
}

test "MemTable putIfAbsent" {
    var mt = MemTable.init(std.testing.allocator);
    defer mt.deinit();

    const inserted = try mt.putIfAbsent("key", "value1");
    try std.testing.expect(inserted);

    const not_inserted = try mt.putIfAbsent("key", "value2");
    try std.testing.expect(!not_inserted);

    try std.testing.expectEqualSlices(u8, "value1", mt.get("key").found);
}

test "MemTable swap and drain" {
    var mt = MemTable.init(std.testing.allocator);
    defer mt.deinit();

    try mt.put("a", "1");
    try mt.put("b", "2");
    try std.testing.expectEqual(@as(u32, 2), mt.count());

    mt.lockAll();
    var total_back: u32 = 0;
    for (0..NUM_SHARDS) |i| {
        const back = mt.swapShardLocked(i);
        total_back += back.map.count();
    }
    mt.unlockAll();

    try std.testing.expectEqual(@as(u32, 0), mt.count());
    try std.testing.expectEqual(@as(u32, 2), total_back);

    mt.lockAll();
    for (0..NUM_SHARDS) |i| mt.resetShardBackLocked(i);
    mt.unlockAll();
}

test "MemTable concurrent writers" {
    var mt = MemTable.init(std.testing.allocator);
    defer mt.deinit();

    const Writer = struct {
        fn run(m: *MemTable, prefix: u8) void {
            var buf: [16]u8 = undefined;
            var i: usize = 0;
            while (i < 1000) : (i += 1) {
                buf[0] = prefix;
                std.mem.writeInt(u32, buf[1..5], @intCast(i), .little);
                m.put(buf[0..5], "val") catch {};
            }
        }
    };

    var threads: [4]std.Thread = undefined;
    for (&threads, 0..) |*t, idx| {
        t.* = try std.Thread.spawn(.{}, Writer.run, .{ &mt, @as(u8, @intCast(idx)) });
    }
    for (&threads) |t| t.join();

    try std.testing.expectEqual(@as(u32, 4000), mt.count());
}
