//! The comptime data plane: an app declares its store as a `Schema`, and `Engine(schema)`
//! generates the typed storage layer: a superblock `Header` with a root/count slot per
//! index and a slot per persisted counter, plus the named ordered byte-key/byte-value
//! trees themselves.
//!
//! This generalizes the hand-rolled index table that a concrete consumer would otherwise
//! walk with `inline for`: declare the indexes once, and the header layout, the trees, and
//! the counters are all generated from that single source.
//!
//! The tree backing in this cut is an in-memory ordered map. The on-disk paged B+Tree,
//! WAL, and snapshot machinery attach behind this same surface without changing it.

const std = @import("std");
const codec = @import("codec.zig");

/// How an index orders its keys. The kind is metadata for validation and client codegen;
/// the store compares the encoded key bytes either way, so big-endian encodings
/// (`codec.encodeU64`, `codec.CompositeKey`) sort numerically.
pub const KeyKind = union(enum) {
    /// A single big-endian `u64` key (see `codec.encodeU64`).
    u64,
    /// An opaque byte key compared lexically (e.g. a slug path).
    bytes,
    /// A multi-`u64` key whose components are listed for codegen (see `codec.CompositeKey`).
    composite: []const [:0]const u8,
};

/// One declared index: a name (used for the generated header slots and tree field) and
/// its key kind.
pub const IndexSpec = struct {
    name: [:0]const u8,
    key: KeyKind,
};

/// The normalized, comptime description of a store: its on-disk identity, its indexes, the
/// subset of indexes fronted by a write memtable, and its persisted monotonic counters.
pub const Schema = struct {
    magic: u32,
    format_version: u32,
    indexes: []const IndexSpec,
    memtable_indexes: []const [:0]const u8,
    counters: []const [:0]const u8,
};

/// Validate and normalize an anonymous schema literal into a `Schema`. Call at comptime and
/// feed the result to `Engine`.
///
/// Expects a struct with `magic`, `format_version`, `indexes` (a tuple of
/// `.{ .name, .key }`), `memtable_indexes`, and `counters`.
pub fn schema(comptime spec: anytype) Schema {
    comptime {
        var indexes: [spec.indexes.len]IndexSpec = undefined;
        for (spec.indexes, 0..) |entry, i| {
            indexes[i] = .{ .name = entry.name, .key = normalizeKey(entry.key) };
        }

        var memtables: [spec.memtable_indexes.len][:0]const u8 = undefined;
        for (spec.memtable_indexes, 0..) |name, i| memtables[i] = name;

        var counters: [spec.counters.len][:0]const u8 = undefined;
        for (spec.counters, 0..) |name, i| counters[i] = name;

        for (memtables) |name| {
            if (indexOfName(indexes[0..], name) == null)
                @compileError("memtable_index '" ++ name ++ "' is not a declared index");
        }

        const frozen_indexes = indexes;
        const frozen_memtables = memtables;
        const frozen_counters = counters;
        return .{
            .magic = spec.magic,
            .format_version = spec.format_version,
            .indexes = &frozen_indexes,
            .memtable_indexes = &frozen_memtables,
            .counters = &frozen_counters,
        };
    }
}

fn normalizeKey(comptime k: anytype) KeyKind {
    if (@typeInfo(@TypeOf(k)) == .@"struct") return .{ .composite = k.composite };
    if (k == .u64) return .u64;
    if (k == .bytes) return .bytes;
    @compileError("unsupported key kind in schema (expected .u64, .bytes, or .{ .composite = ... })");
}

fn indexOfName(comptime indexes: []const IndexSpec, comptime name: []const u8) ?usize {
    for (indexes, 0..) |idx, i| {
        if (std.mem.eql(u8, idx.name, name)) return i;
    }
    return null;
}

fn structField(comptime name: [:0]const u8, comptime T: type) std.builtin.Type.StructField {
    return .{
        .name = name,
        .type = T,
        .default_value_ptr = null,
        .is_comptime = false,
        .alignment = @alignOf(T),
    };
}

fn HeaderType(comptime s: Schema) type {
    comptime {
        const generic = [_]std.builtin.Type.StructField{
            structField("magic", u32),
            structField("format_version", u32),
            structField("page_size", u32),
            structField("_reserved", u32),
            structField("free_list_head", u64),
            structField("page_count", u64),
            structField("seq", u64),
        };

        var fields: [generic.len + s.indexes.len * 2 + s.counters.len]std.builtin.Type.StructField = undefined;
        var n: usize = 0;
        for (generic) |f| {
            fields[n] = f;
            n += 1;
        }
        for (s.indexes) |idx| {
            fields[n] = structField(std.fmt.comptimePrint("{s}_root", .{idx.name}), u64);
            n += 1;
            fields[n] = structField(std.fmt.comptimePrint("{s}_count", .{idx.name}), u64);
            n += 1;
        }
        for (s.counters) |c| {
            fields[n] = structField(c, u64);
            n += 1;
        }

        return @Type(.{ .@"struct" = .{
            .layout = .@"extern",
            .fields = fields[0..n],
            .decls = &.{},
            .is_tuple = false,
        } });
    }
}

fn TreesType(comptime s: Schema) type {
    comptime {
        var fields: [s.indexes.len]std.builtin.Type.StructField = undefined;
        for (s.indexes, 0..) |idx, i| {
            fields[i] = structField(idx.name, OrderedTree);
        }
        return @Type(.{ .@"struct" = .{
            .layout = .auto,
            .fields = &fields,
            .decls = &.{},
            .is_tuple = false,
        } });
    }
}

/// Generate the typed `Store` for a comptime `Schema`.
///
/// The returned type exposes `Header` (the generated superblock), `open`/`deinit`,
/// per-index tree access via `tree(name)`, and persisted-counter access via `counter(name)`
/// / `nextId(name)`. Tree and counter names are checked at comptime against the schema.
pub fn Engine(comptime s: Schema) type {
    return struct {
        const Store = @This();

        /// The generated superblock: the generic `magic`/`format_version`/`page_size`/
        /// `free_list_head`/`page_count`/`seq` fields, a `<name>_root`/`<name>_count` pair
        /// per index, and a slot per declared counter. App-supplied `magic`; no schema
        /// field names are hardcoded by the engine.
        pub const Header = HeaderType(s);

        /// The compile-time schema this store was generated from.
        pub const schema_def = s;

        const Trees = TreesType(s);

        allocator: std.mem.Allocator,
        header: Header,
        trees: Trees,

        /// Open a fresh in-memory store. The header is zeroed and stamped with the schema's
        /// `magic`, `format_version`, and a 16 KB `page_size`; every declared tree is
        /// initialized empty.
        pub fn open(allocator: std.mem.Allocator) Store {
            var store: Store = .{
                .allocator = allocator,
                .header = std.mem.zeroes(Header),
                .trees = undefined,
            };
            store.header.magic = s.magic;
            store.header.format_version = s.format_version;
            store.header.page_size = 16 * 1024;
            inline for (s.indexes) |idx| {
                @field(store.trees, idx.name) = OrderedTree.init(allocator);
            }
            return store;
        }

        /// Free every named tree and the keys/values it owns.
        pub fn deinit(self: *Store) void {
            inline for (s.indexes) |idx| {
                @field(self.trees, idx.name).deinit();
            }
        }

        /// A pointer to the named index's tree. The name is resolved and checked at comptime.
        pub fn tree(self: *Store, comptime name: [:0]const u8) *OrderedTree {
            comptime assertIndex(name);
            return &@field(self.trees, name);
        }

        /// A pointer to the named counter's persisted slot in the header.
        pub fn counter(self: *Store, comptime name: [:0]const u8) *u64 {
            comptime assertCounter(name);
            return &@field(self.header, name);
        }

        /// Increment the named counter and return its new value. Allocates ids from `1`.
        pub fn nextId(self: *Store, comptime name: [:0]const u8) u64 {
            const slot = self.counter(name);
            slot.* += 1;
            return slot.*;
        }

        fn assertIndex(comptime name: [:0]const u8) void {
            if (indexOfName(s.indexes, name) == null)
                @compileError("no index named '" ++ name ++ "' in this schema");
        }

        fn assertCounter(comptime name: [:0]const u8) void {
            for (s.counters) |c| {
                if (std.mem.eql(u8, c, name)) return;
            }
            @compileError("no counter named '" ++ name ++ "' in this schema");
        }
    };
}

/// An ordered byte-key/byte-value map: point read/write/delete and ascending range scans,
/// with the store owning copies of every key and value. The v0 backing for an `Engine`
/// index; range order matches lexical key order, so big-endian encodings scan numerically.
pub const OrderedTree = struct {
    allocator: std.mem.Allocator,
    entries: std.ArrayListUnmanaged(Entry),

    const Entry = struct { key: []u8, value: []u8 };

    /// An empty tree that allocates its owned keys and values from `allocator`.
    pub fn init(allocator: std.mem.Allocator) OrderedTree {
        return .{ .allocator = allocator, .entries = .{} };
    }

    /// Free every stored key and value, then the tree's own storage.
    pub fn deinit(self: *OrderedTree) void {
        for (self.entries.items) |e| {
            self.allocator.free(e.key);
            self.allocator.free(e.value);
        }
        self.entries.deinit(self.allocator);
    }

    fn lowerBound(self: *const OrderedTree, key: []const u8) usize {
        var lo: usize = 0;
        var hi: usize = self.entries.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (std.mem.order(u8, self.entries.items[mid].key, key) == .lt) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    /// Insert or replace the value at `key`. The tree copies both `key` and `value`.
    pub fn put(self: *OrderedTree, key: []const u8, value: []const u8) !void {
        const at = self.lowerBound(key);
        if (at < self.entries.items.len and std.mem.eql(u8, self.entries.items[at].key, key)) {
            const new_value = try self.allocator.dupe(u8, value);
            self.allocator.free(self.entries.items[at].value);
            self.entries.items[at].value = new_value;
            return;
        }
        const key_copy = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(key_copy);
        const value_copy = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(value_copy);
        try self.entries.insert(self.allocator, at, .{ .key = key_copy, .value = value_copy });
    }

    /// The value stored at `key`, or null. Borrowed from the tree; valid until the next
    /// mutation of this tree.
    pub fn get(self: *const OrderedTree, key: []const u8) ?[]const u8 {
        const at = self.lowerBound(key);
        if (at < self.entries.items.len and std.mem.eql(u8, self.entries.items[at].key, key))
            return self.entries.items[at].value;
        return null;
    }

    /// Remove `key`. Returns whether a row was removed.
    pub fn delete(self: *OrderedTree, key: []const u8) bool {
        const at = self.lowerBound(key);
        if (at < self.entries.items.len and std.mem.eql(u8, self.entries.items[at].key, key)) {
            const removed = self.entries.orderedRemove(at);
            self.allocator.free(removed.key);
            self.allocator.free(removed.value);
            return true;
        }
        return false;
    }

    /// The number of rows in the tree.
    pub fn count(self: *const OrderedTree) usize {
        return self.entries.items.len;
    }

    /// One row yielded by an `Iterator`: borrowed key and value, valid until the next mutation.
    pub const Row = struct { key: []const u8, value: []const u8 };

    /// An ascending cursor over a key range; obtained from `iterator` or `range`.
    pub const Iterator = struct {
        tree: *const OrderedTree,
        i: usize,
        end: usize,
        upper: ?[]const u8,

        /// The next row in ascending key order, or null at the end of the range.
        pub fn next(self: *Iterator) ?Row {
            if (self.i >= self.end) return null;
            const e = self.tree.entries.items[self.i];
            if (self.upper) |hi| {
                if (std.mem.order(u8, e.key, hi) != .lt) {
                    self.i = self.end;
                    return null;
                }
            }
            self.i += 1;
            return .{ .key = e.key, .value = e.value };
        }
    };

    /// Ascending iterator over every row.
    pub fn iterator(self: *const OrderedTree) Iterator {
        return .{ .tree = self, .i = 0, .end = self.entries.items.len, .upper = null };
    }

    /// Ascending iterator over the half-open key range `[lo, hi)`. A null `hi` scans to the
    /// end. With big-endian keys this is the primitive a subtree/prefix scan is built from.
    pub fn range(self: *const OrderedTree, lo: []const u8, hi: ?[]const u8) Iterator {
        return .{ .tree = self, .i = self.lowerBound(lo), .end = self.entries.items.len, .upper = hi };
    }
};

const test_schema = schema(.{
    .magic = 0x5A494753,
    .format_version = 1,
    .indexes = .{
        .{ .name = "by_id", .key = .u64 },
        .{ .name = "by_parent_child", .key = .{ .composite = &.{ "parent_id", "child_id" } } },
        .{ .name = "by_slug", .key = .bytes },
    },
    .memtable_indexes = &.{ "by_id", "by_parent_child" },
    .counters = &.{ "next_id", "next_seq" },
});

const TestStore = Engine(test_schema);

test "Header carries magic, generic slots, and a root/count per index" {
    try std.testing.expect(@hasField(TestStore.Header, "magic"));
    try std.testing.expect(@hasField(TestStore.Header, "seq"));
    try std.testing.expect(@hasField(TestStore.Header, "by_id_root"));
    try std.testing.expect(@hasField(TestStore.Header, "by_id_count"));
    try std.testing.expect(@hasField(TestStore.Header, "by_slug_root"));
    try std.testing.expect(@hasField(TestStore.Header, "next_seq"));
}

test "open stamps the app magic and 16 KB page size" {
    var store = TestStore.open(std.testing.allocator);
    defer store.deinit();
    try std.testing.expectEqual(@as(u32, 0x5A494753), store.header.magic);
    try std.testing.expectEqual(@as(u32, 16 * 1024), store.header.page_size);
}

test "point write / read / delete on a named tree" {
    var store = TestStore.open(std.testing.allocator);
    defer store.deinit();

    const by_id = store.tree("by_id");
    try by_id.put(&codec.encodeU64(2), "two");
    try by_id.put(&codec.encodeU64(1), "one");
    try by_id.put(&codec.encodeU64(2), "TWO");

    try std.testing.expectEqualSlices(u8, "one", by_id.get(&codec.encodeU64(1)).?);
    try std.testing.expectEqualSlices(u8, "TWO", by_id.get(&codec.encodeU64(2)).?);
    try std.testing.expectEqual(@as(usize, 2), by_id.count());
    try std.testing.expect(by_id.delete(&codec.encodeU64(1)));
    try std.testing.expect(by_id.get(&codec.encodeU64(1)) == null);
}

test "range scan returns big-endian keys in ascending numeric order" {
    var store = TestStore.open(std.testing.allocator);
    defer store.deinit();

    const by_id = store.tree("by_id");
    for ([_]u64{ 30, 10, 20, 40 }) |id| {
        try by_id.put(&codec.encodeU64(id), "x");
    }

    var seen: [4]u64 = undefined;
    var n: usize = 0;
    var it = by_id.iterator();
    while (it.next()) |row| : (n += 1) seen[n] = codec.decodeU64(row.key);
    try std.testing.expectEqualSlices(u64, &[_]u64{ 10, 20, 30, 40 }, seen[0..n]);
}

test "composite range scans the children of one parent" {
    var store = TestStore.open(std.testing.allocator);
    defer store.deinit();

    const Key = codec.CompositeKey(&.{ "parent_id", "child_id" });
    const tree = store.tree("by_parent_child");
    try tree.put(&Key.encode(.{ 1, 100 }), "a");
    try tree.put(&Key.encode(.{ 1, 200 }), "b");
    try tree.put(&Key.encode(.{ 2, 50 }), "c");

    var children: usize = 0;
    var it = tree.range(&Key.encode(.{ 1, 0 }), &Key.encode(.{ 2, 0 }));
    while (it.next()) |_| children += 1;
    try std.testing.expectEqual(@as(usize, 2), children);
}

test "counters persist in the header and allocate from 1" {
    var store = TestStore.open(std.testing.allocator);
    defer store.deinit();
    try std.testing.expectEqual(@as(u64, 1), store.nextId("next_id"));
    try std.testing.expectEqual(@as(u64, 2), store.nextId("next_id"));
    try std.testing.expectEqual(@as(u64, 0), store.header.next_seq);
    try std.testing.expectEqual(@as(u64, 2), store.header.next_id);
    store.counter("next_seq").* = 99;
    try std.testing.expectEqual(@as(u64, 99), store.header.next_seq);
}
