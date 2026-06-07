const std = @import("std");
const zigstore = @import("zigstore");
const codec = zigstore.codec;

const directory_schema = zigstore.schema(.{
    .magic = 0x444D4F5A,
    .format_version = 6,
    .indexes = .{
        .{ .name = "categories_by_id", .key = .u64 },
        .{ .name = "cat_by_parent", .key = .{ .composite = &.{ "parent_id", "child_id" } } },
        .{ .name = "links_by_id", .key = .u64 },
        .{ .name = "link_by_category", .key = .{ .composite = &.{ "category_id", "link_id" } } },
        .{ .name = "link_by_url_hash", .key = .u64 },
        .{ .name = "link_by_submitter", .key = .{ .composite = &.{ "submitter_id", "link_id" } } },
        .{ .name = "categories_by_slug_path", .key = .bytes },
        .{ .name = "categories_by_slug_only", .key = .bytes },
        .{ .name = "categories_index_tree", .key = .bytes },
        .{ .name = "links_index_tree", .key = .bytes },
        .{ .name = "slug_path_repair_queue", .key = .u64 },
    },
    .memtable_indexes = &.{
        "categories_by_id", "cat_by_parent",    "links_by_id",
        "link_by_category", "link_by_url_hash", "link_by_submitter",
    },
    .counters = &.{ "next_category_id", "next_link_id", "next_repair_seq" },
});

const Store = zigstore.Engine(directory_schema);

const Category = extern struct {
    id: u64 = 0,
    parent_id: u64 = 0,
    name: codec.FixedString(64) = .{},

    const Ser = codec.Serializable(@This());
    pub const asBytes = Ser.asBytes;
    pub const fromBytes = Ser.fromBytes;
};

const Link = extern struct {
    id: u64 = 0,
    category_id: u64 = 0,
    title: codec.FixedString(128) = .{},

    const Ser = codec.Serializable(@This());
    pub const asBytes = Ser.asBytes;
    pub const fromBytes = Ser.fromBytes;
};

const ParentChild = codec.CompositeKey(&.{ "parent_id", "child_id" });
const CategoryLink = codec.CompositeKey(&.{ "category_id", "link_id" });

fn addCategory(store: *Store, parent_id: u64, name: []const u8) !u64 {
    const id = store.nextId("next_category_id");
    var cat = Category{ .id = id, .parent_id = parent_id, .name = codec.FixedString(64).fromSlice(name) };
    try store.tree("categories_by_id").insert(&codec.encodeU64(id), cat.asBytes());
    try store.tree("cat_by_parent").insert(&ParentChild.encode(.{ parent_id, id }), &codec.encodeU64(id));
    return id;
}

fn addLink(store: *Store, category_id: u64, title: []const u8) !u64 {
    const id = store.nextId("next_link_id");
    var link = Link{ .id = id, .category_id = category_id, .title = codec.FixedString(128).fromSlice(title) };
    try store.tree("links_by_id").insert(&codec.encodeU64(id), link.asBytes());
    try store.tree("link_by_category").insert(&CategoryLink.encode(.{ category_id, id }), &codec.encodeU64(id));
    return id;
}

fn openStore(allocator: std.mem.Allocator, data_dir: []const u8) !*Store {
    return Store.init(allocator, .{ .data_dir = data_dir });
}

fn closeStore(store: *Store) void {
    store.deinit();
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const data_dir = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(data_dir);

    const store = try openStore(allocator, data_dir);
    defer closeStore(store);

    const arts = try addCategory(store, 0, "Arts");
    const music = try addCategory(store, arts, "Music");
    const film = try addCategory(store, arts, "Film");

    _ = try addLink(store, music, "AllMusic");
    _ = try addLink(store, music, "Discogs");
    _ = try addLink(store, film, "IMDb");

    try store.drainMemtables();

    std.debug.print("zigstore example: paged directory store, magic=0x{X}\n\n", .{store.header.magic});

    var buf: [256]u8 = undefined;
    const arts_bytes = (try store.tree("categories_by_id").search(&codec.encodeU64(arts), &buf)).?;
    const arts_cat = Category.fromBytes(arts_bytes);
    std.debug.print("category #{d}: {s}\n", .{ arts_cat.id, arts_cat.name.slice() });

    std.debug.print("\nchildren of '{s}' (cat_by_parent range scan):\n", .{arts_cat.name.slice()});
    var children = try store.tree("cat_by_parent").rangeScan(
        &ParentChild.encode(.{ arts, 0 }),
        &ParentChild.encode(.{ arts + 1, 0 }),
    );
    while (try children.next()) |row| {
        var cbuf: [256]u8 = undefined;
        const child = Category.fromBytes((try store.tree("categories_by_id").search(row.value, &cbuf)).?);
        std.debug.print("  - #{d} {s}\n", .{ child.id, child.name.slice() });
    }

    std.debug.print("\nlinks under 'Music' (link_by_category range scan):\n", .{});
    var links = try store.tree("link_by_category").rangeScan(
        &CategoryLink.encode(.{ music, 0 }),
        &CategoryLink.encode(.{ music + 1, 0 }),
    );
    while (try links.next()) |row| {
        var lbuf: [256]u8 = undefined;
        const link = Link.fromBytes((try store.tree("links_by_id").search(row.value, &lbuf)).?);
        std.debug.print("  - #{d} {s}\n", .{ link.id, link.title.slice() });
    }

    std.debug.print(
        "\ncounters: next_category_id={d}, next_link_id={d}\n",
        .{ store.header.next_category_id, store.header.next_link_id },
    );
    std.debug.print(
        "tree sizes: categories={d}, links={d}\n",
        .{ store.tree("categories_by_id").entryCount(), store.tree("links_by_id").entryCount() },
    );
}

test "example builds and drives the paged store without leaks" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const data_dir = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(data_dir);

    const store = try openStore(allocator, data_dir);
    defer closeStore(store);

    const arts = try addCategory(store, 0, "Arts");
    const music = try addCategory(store, arts, "Music");
    _ = try addLink(store, music, "AllMusic");
    _ = try addLink(store, music, "Discogs");
    try store.drainMemtables();

    var links: usize = 0;
    var it = try store.tree("link_by_category").rangeScan(
        &CategoryLink.encode(.{ music, 0 }),
        &CategoryLink.encode(.{ music + 1, 0 }),
    );
    while (try it.next()) |_| links += 1;

    try std.testing.expectEqual(@as(usize, 2), links);
    try std.testing.expectEqual(@as(u64, 2), store.header.next_category_id);
    try std.testing.expectEqual(@as(u64, 2), store.header.next_link_id);
}
