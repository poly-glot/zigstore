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

A complete embeddable **and** networked record store, validated on **Zig 0.16.0** (215 tests
plus a `-Dcpu=baseline` build). Every layer is implemented:

| Surface | State |
|---|---|
| `codec`: `FixedString`, `CompositeKey`, `encodeU64`/`decodeU64`, `Serializable`, `hash` | implemented |
| `schema` / `Engine(schema)`: typed `Header`, named trees, persisted counters | implemented |
| Paged storage (16 KB slotted pages, sharded LRU cache, on-page freelist) + a `[]const u8`-keyed B+Tree | implemented |
| WAL durability + replay, memtables, bloom filter, atomic journaled checkpoint, crash recovery | implemented |
| `recover` / `spawnWorker` / `spawnCheckpointer` / seq-gated `commit` / `SnapshotHost` runtime seams | implemented |
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
zig fetch --save "https://github.com/poly-glot/zigstore/archive/refs/tags/v1.1.0.tar.gz"
```

## API at a glance

```zig
const Store = zigstore.Engine(schema);
const store = try Store.init(io, allocator, .{ .data_dir = dir });   // heap-stable *Store
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
_ = try store.checkpoint();                       // drain + write pages/header atomically + truncate WAL
```

The dynamic seams stay runtime callbacks the consumer supplies: `store.recover(ctx, .{ .apply_entry, .on_replayed, .bootstrap })`, `store.spawnWorker(ctx, .{ .interval_ns, .tick })`, `zigstore.commit(Record, store, op_code, record, ctx, serialize_fn, apply_fn)`, snapshot over a `SnapshotHost`, health/readiness facts via `store.healthStatus()`, replication over `store.primaryHost()` / `store.replicaHost(ctx, apply_entry)` (`zigstore.replication.Hub.start` on the leader, `Receiver.start` on a standby — see [`docs/design/replication-streaming-standby.md`](docs/design/replication-streaming-standby.md)), `zigstore.protocol.processFrames(ctx, conn, dispatch_fn, op_latency, response_reserve)`, and `zigstore.run(Store, ctx, handler, config)`.

See [`examples/basic.zig`](examples/basic.zig) for a full directory-shaped store driven end to end.

### `std.Io`

Every public constructor takes a `std.Io` first (`Store.init`, `ShardSet.init`, `WalWriter.init`,
`PageCache.init`, `Hub.start`, `Receiver.start`, `fetchBaseBackup`, `SnapshotManager`) and
`ServerConfig.fromEnv` reads a `*const std.process.Environ.Map`; `main` gets both from
`std.process.Init`, or builds a `std.Io.Threaded` (as `examples/basic.zig` does), and tests use
`std.testing.io`. The `Io` runs the locks and the cold path (create, rename, copy, stat, the
replay reader); the hot path — the `O_DIRECT` WAL, page I/O, the checkpoint journal, and the
epoll reactor — stays on raw Linux syscalls (`src/sys.zig`) so the on-disk and wire formats are
byte-identical to 2.x.

### Durability model

- **A WAL append is acknowledged after `fdatasync`.** `commit` returns once its entry is in
  `wal.bin` and the group-commit flusher has `fdatasync`ed it (and, with a `CommitGate`, once
  the quorum has acked).
- **The page cache is no-steal.** Eviction only reclaims clean frames; a dirty page is never
  written to `store.dat` by eviction. The only writer of tree pages is `PageCache.checkpoint`.
- **A checkpoint is atomic.** `store.checkpoint()` takes `mt_drain_mutex` then `apply_mutex`,
  pauses WAL appends and waits for every appended entry to apply, drains the memtables into
  their trees, then writes every dirty page plus the superblock first to `checkpoint.journal`
  (fsynced, CRC-covered) and only then in place (header page last), `fdatasync`s `store.dat`,
  empties the journal, and truncates the WAL. A crash at any point leaves `store.dat` either
  entirely pre- or entirely post-checkpoint: `Store.init` redoes a complete journal and discards
  an incomplete one before reading the header. The journal file is created once per store and
  stays open and empty between checkpoints.
- **`header.seq`** is the WAL sequence up to which every entry is fully contained in the on-disk
  tree. `recover` replays only entries above it, and the WAL sequence resumes from it on open
  even when `snapshot.meta` is absent.
- **The pressure valve.** When at least half the cache frames are dirty, `store.flushPages()`
  writes them through the same journal without draining memtables or advancing `header.seq`.
  `commit`, `drainMemtables`, and `recover` pull the valve themselves; `spawnCheckpointer(interval_s)`
  spawns a worker that pulls it every second and runs a full `checkpoint` every `interval_s`.
- **Lock order** everywhere: `mt_drain_mutex` → `apply_mutex`. A consumer's `apply_fn` runs
  under `apply_mutex` and must not call `drainMemtables`, `flushPages`, or `checkpoint`.
- **What two errors mean for a consumer.** `error.WalFlushFailed`: a WAL `fdatasync` failed;
  the store refuses every further commit, and the in-memory effect of the failed entry cannot be
  undone, so crash and restart (recovery rebuilds from what is durable). `error.WalCorrupt`
  (from `Store.init`): `wal.bin` has valid records more than one batch beyond a corrupt one,
  which a torn tail cannot produce; the store refuses to start rather than silently drop data.
  A torn tail of at most one batch is trimmed on open.

### Scale writes: `ShardSet`

A single `Store` serializes every write through one WAL-append lock, one apply mutex, the
per-tree write lock, and one free-list mutex. `ShardSet` partitions an opaque keyspace across
N independent `Store`s under one `data_dir`, each its own serialization domain, routed by
`hash(key) % shard_count`:

```zig
const Set = zigstore.ShardSet(zigstore.Engine(schema));
const set = try Set.init(io, allocator, .{ .data_dir = dir, .shard_count = 8 });
defer set.deinit();

const key = codec.encodeU64(id);
try set.commit(Record, &key, op_code, record, serialize_fn, apply_fn); // routes by key
const store = set.shardFor(&key);                                       // same shard for reads
```

`shard_count` is recorded in `shardset.meta` and a reopen with a different count fails rather
than silently re-routing keys. The routing key must be the canonical key a record is stored
and read under; counters are per-shard; a commit is atomic only within its shard; whole-keyspace
range scans merge the shards. When the write path is **apply/lock-bound**, throughput scales with
the shard count (≈5× at 8 shards, 8 fixed writer threads, on a 10-core box). When it is
**`fdatasync`-bound on a single device**, the existing single-WAL group commit already amortizes
the device and sharding does not help — the win needs apply-bound work or independent devices/nodes
(the Tier-3b production shape in [`scale.md`](scale.md)). Measure your own workload:

```bash
zig build bench -- --indexes 4 --threads 8        # sweeps shards 1,2,4,8; see bench/write_bench.zig
zig build bench -- --no-direct --dir /dev/shm/zsb # buffered WAL on tmpfs: isolate the apply-bound regime
```

## Develop

The repo ships a devcontainer that pins Zig 0.16.0 and ZLS:

```bash
zig build test            # engine + example tests
zig build run-example     # build and run examples/basic.zig
zig build -Dcpu=baseline  # the Ampere/OKE baseline build
zig fmt --check src/ examples/ build.zig
```

The paged storage uses Linux `O_DIRECT`/`fallocate`, so `zig build test` runs in the Linux
devcontainer (or any Linux host with Zig 0.16.0); CI runs it on Zig 0.16.0 on both x86_64 and
arm64 (`ubuntu-24.04-arm`) plus a `-Dcpu=baseline` build.

## Versioning

semver. MAJOR for any wire-format or public-API break; MINOR is additive only. Extend ops
with optional trailing bytes; never fork an op number. Published tags are never moved (the
dependency hash is content-addressed). See [`CLAUDE.md`](CLAUDE.md) and
[`.claude/rules/`](.claude/rules/) for the engineering contract.

## License

[MIT](LICENSE) © 2026 Junaid Ahmed
