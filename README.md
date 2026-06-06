# zigstore

A fast, embeddable and networked ordered byte-key/byte-value record store for Zig.

`zigstore` is the generic database/server engine extracted from
[`dmozdb`](https://github.com/poly-glot/zig-directory): WAL-durable paged storage, a B+Tree
over opaque `[]const u8` keys, an epoll reactor, a binary-protocol framing/TLV codec, and
reflection-driven client codegen. You declare your store's shape as a comptime schema and the
engine generates the typed storage layer; the dynamic seams (request dispatch, recovery hooks,
periodic workers, the snapshot host) stay runtime callbacks you supply. It knows nothing about
your domain. Hierarchy, slugs, statuses, and product semantics live in your code, on top.

```zig
const zigstore = @import("zigstore");

const schema = zigstore.schema(.{
    .magic = 0x444D4F5A,
    .format_version = 6,
    .indexes = .{
        .{ .name = "categories_by_id", .key = .u64 },
        .{ .name = "cat_by_parent",     .key = .{ .composite = &.{ "parent_id", "child_id" } } },
        .{ .name = "links_by_id",        .key = .u64 },
        .{ .name = "link_by_category",   .key = .{ .composite = &.{ "category_id", "link_id" } } },
        .{ .name = "categories_by_slug", .key = .bytes },
    },
    .memtable_indexes = &.{ "categories_by_id", "links_by_id" },
    .counters = &.{ "next_category_id", "next_link_id" },
});

pub const Store = zigstore.Engine(schema);
```

From that one declaration the engine generates, at comptime:

- **`Store.Header`**: the superblock, carrying a `<name>_root`/`<name>_count` pair per index, a
  slot per declared counter, and the generic `magic`/`format_version`/`page_size`/
  `free_list_head`/`page_count`/`seq` fields. You supply `magic`; no schema field names are
  hardcoded by the engine.
- **the named trees**: one ordered byte-key/byte-value tree per index, reached by name.
- **the persisted counters**: monotonic id allocators, stored in the header, reloaded on open.

## Status

This repository is the bootstrapped engine, validated end to end on **Zig 0.15.2**:

| Surface | State |
|---|---|
| `codec`: `FixedString`, `CompositeKey`, `encodeU64`/`decodeU64`, `Serializable`, `hash` | implemented |
| `schema` / `Engine(schema)`: typed `Header`, named trees, persisted counters | implemented |
| `OrderedTree`: point read/write/delete + ascending range scans | implemented (in-memory backing) |
| Paged storage + WAL + snapshot + crash recovery | lands via the migration |
| epoll reactor + binary-protocol framing/TLV + `run`/`ServerConfig` | lands via the migration |
| reflection-driven TypeScript-client codegen (`tsgen`) | lands via the migration |

The public API surface is the contract the on-disk and networked layers attach behind without
changing it: the comptime schema, the generated `Header`, the named-tree and counter accessors,
and the `codec` toolkit. The full boundary, the hybrid API, and the ordered migration plan live
in [`docs/design/zigstore-library-extraction-design.md`](docs/design/zigstore-library-extraction-design.md).

## Use it

In another project's `build.zig.zon`, while iterating locally:

```zig
.dependencies = .{
    .zigstore = .{ .path = "../zigstore" },
},
```

…and in `build.zig`:

```zig
const zigstore = b.dependency("zigstore", .{
    .target = target,
    .optimize = optimize,
}).module("zigstore");
exe_mod.addImport("zigstore", zigstore);
```

Consume by tag once published. Never hand-edit the hash:

```bash
zig fetch --save "https://github.com/poly-glot/zigstore/archive/refs/tags/v1.0.0.tar.gz"
```

## API at a glance

```zig
var store = Store.open(allocator);
defer store.deinit();

const tree = store.tree("links_by_id");           // *OrderedTree, name checked at comptime
try tree.put(&zigstore.codec.encodeU64(id), bytes);
const value = tree.get(&zigstore.codec.encodeU64(id)); // ?[]const u8

var it = tree.range(lo_key, hi_key);              // ascending [lo, hi); big-endian keys scan numerically
while (it.next()) |row| { _ = row.key; _ = row.value; }

const next = store.nextId("next_link_id");        // persisted counter, allocates from 1
store.counter("next_link_id").* = 0;              // *u64 into the header
```

See [`examples/basic.zig`](examples/basic.zig) for a full directory-shaped store driven end to
end.

## Develop

The repo ships a devcontainer that pins Zig 0.15.2 and ZLS:

```bash
zig build test            # engine + example tests
zig build run-example     # build and run examples/basic.zig
zig build -Dcpu=baseline  # the Ampere/OKE baseline build
zig fmt --check src/ examples/ build.zig
```

The in-memory surface builds and tests on macOS and Linux alike. The on-disk WAL/file path
(once it lands) needs Linux `O_DIRECT`/`fallocate`, so the canonical gate runs in the Linux
devcontainer; CI runs it on Zig 0.15.2 plus a `-Dcpu=baseline` build.

## Versioning

semver. MAJOR for any wire-format or public-API break; MINOR is additive only. Extend ops
with optional trailing bytes; never fork an op number. Published tags are never moved (the
dependency hash is content-addressed). See [`CLAUDE.md`](CLAUDE.md) and
[`.claude/rules/`](.claude/rules/) for the engineering contract.

## License

[MIT](LICENSE) © 2026 Junaid Ahmed
