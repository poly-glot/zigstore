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

A complete embeddable **and** networked record store, validated on **Zig 0.15.2** (169 tests
plus a `-Dcpu=baseline` build). Every layer is implemented:

| Surface | State |
|---|---|
| `codec`: `FixedString`, `CompositeKey`, `encodeU64`/`decodeU64`, `Serializable`, `hash` | implemented |
| `schema` / `Engine(schema)`: typed `Header`, named trees, persisted counters | implemented |
| Paged storage (16 KB slotted pages, sharded LRU cache, on-page freelist) + a `[]const u8`-keyed B+Tree | implemented |
| WAL durability + replay, memtables, bloom filter, snapshot/checkpoint, crash recovery | implemented |
| `recover` / `spawnWorker` / seq-gated `commit` / `SnapshotHost` runtime seams | implemented |
| epoll reactor + binary-protocol framing/TLV codec + `run` / `ServerConfig` | implemented |
| `replication`: streaming-standby `Hub`/`Receiver`, base-backup bootstrap (`fetchBaseBackup`), retain floor, `CommitGate` quorum, `promote`/`demote` | implemented |
| reflection-driven TypeScript-client codegen (`tsgen`), incl. read/write split (`writeOpKindMap` + `writeReadWriteRouter`) | implemented |

`zigstore` was extracted from
[`dmozdb`](https://github.com/poly-glot/zig-directory), which now consumes it as a
`build.zig.zon` dependency and is its first production consumer. The boundary, the hybrid API,
and the extraction design live in
[`docs/design/zigstore-library-extraction-design.md`](docs/design/zigstore-library-extraction-design.md).

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
const Store = zigstore.Engine(schema);
const store = try Store.init(allocator, .{ .data_dir = dir });   // heap-stable *Store
defer store.deinit();

const tree = store.tree("links_by_id");           // *BPlusTree, name checked at comptime
try tree.insert(&codec.encodeU64(id), bytes);     // upsert

var buf: [4096]u8 = undefined;
const value = try tree.search(&codec.encodeU64(id), &buf); // !?[]const u8 (copied into buf)

var it = try tree.rangeScan(lo_key, hi_key);      // ascending [lo, hi); big-endian keys scan numerically
while (try it.next()) |row| { _ = row.key; _ = row.value; }

const next = store.nextId("next_link_id");        // persisted counter, allocates from 1
store.counter("next_link_id").* = 0;              // *u64 into the header (single-writer)

try store.drainMemtables();                       // flush the write memtables into the trees
```

The dynamic seams stay runtime callbacks the consumer supplies: `store.recover(ctx, .{ .apply_entry, .on_replayed, .bootstrap })`, `store.spawnWorker(ctx, .{ .interval_ns, .tick })`, `zigstore.commit(Record, store, op_code, record, ctx, serialize_fn, apply_fn)`, snapshot over a `SnapshotHost`, health/readiness facts via `store.healthStatus()`, replication over `store.primaryHost()` / `store.replicaHost(ctx, apply_entry)` (`zigstore.replication.Hub.start` on the leader, `Receiver.start` on a standby — see [`docs/design/replication-streaming-standby.md`](docs/design/replication-streaming-standby.md)), `zigstore.protocol.processFrames(ctx, conn, dispatch_fn, op_latency, response_reserve)`, and `zigstore.run(Store, ctx, handler, config)`.

See [`examples/basic.zig`](examples/basic.zig) for a full directory-shaped store driven end to end.

## Develop

The repo ships a devcontainer that pins Zig 0.15.2 and ZLS:

```bash
zig build test            # engine + example tests
zig build run-example     # build and run examples/basic.zig
zig build -Dcpu=baseline  # the Ampere/OKE baseline build
zig fmt --check src/ examples/ build.zig
```

The paged storage uses Linux `O_DIRECT`/`fallocate`, so `zig build test` runs in the Linux
devcontainer (or any Linux host with Zig 0.15.2); CI runs it on Zig 0.15.2 plus a
`-Dcpu=baseline` build.

## Versioning

semver. MAJOR for any wire-format or public-API break; MINOR is additive only. Extend ops
with optional trailing bytes; never fork an op number. Published tags are never moved (the
dependency hash is content-addressed). See [`CLAUDE.md`](CLAUDE.md) and
[`.claude/rules/`](.claude/rules/) for the engineering contract.

## License

[MIT](LICENSE) © 2026 Junaid Ahmed
