//! zigstore: a fast embeddable and networked ordered byte-key/byte-value record store.
//!
//! The public surface, re-exported here:
//!
//!   - `schema` / `Schema` / `IndexSpec` / `KeyKind`: declare a store's indexes, counters,
//!     and identity as a comptime value.
//!   - `Engine(schema)`: generate the typed storage layer (superblock `Header`, named
//!     ordered trees, persisted counters) from that schema.
//!   - `codec`: the application-neutral byte-codec toolkit (`FixedString`, `CompositeKey`,
//!     `encodeU64`/`decodeU64`, `Serializable`, `hash`).
//!   - `wire_codec`: generic `@typeInfo`-driven struct/field marshalling for record encoding.
//!   - `BPlusTree` / `MemTable`: the paged tree an index is built on (point ops + range scans)
//!     and the write memtable fronting it.
//!   - `Config` / `Worker` / `ReplayEntry`: the per-store open config, the background-worker
//!     handle, and one replayed WAL entry handed to a `recover` hook.
//!
//! The networked half of the surface attaches to this same barrel as it lands: the
//! binary-protocol framing/TLV codec, the epoll reactor, the generic `run` bootstrap with its
//! `ServerConfig`, and the reflection-driven TypeScript-client emitter. See the extraction
//! design under `docs/`.

const engine = @import("engine.zig");

pub const codec = @import("codec.zig");
pub const wire_codec = @import("wire_codec.zig");

pub const schema = engine.schema;
pub const Schema = engine.Schema;
pub const IndexSpec = engine.IndexSpec;
pub const KeyKind = engine.KeyKind;
pub const Engine = engine.Engine;
pub const BPlusTree = engine.BPlusTree;
pub const MemTable = engine.MemTable;
pub const ReplayEntry = engine.ReplayEntry;
pub const Worker = engine.Worker;

test {
    _ = @import("codec.zig");
    _ = @import("engine.zig");
    _ = @import("wire_codec.zig");
    _ = @import("file_header.zig");

    _ = @import("page.zig");
    _ = @import("page_cache.zig");
    _ = @import("freelist.zig");
    _ = @import("memtable.zig");
    _ = @import("bloom.zig");
    _ = @import("inverted_index.zig");
    _ = @import("histogram.zig");
    _ = @import("connection.zig");
    _ = @import("signal.zig");

    _ = @import("wal.zig");
    _ = @import("wal_replay.zig");

    _ = @import("btree/btree.zig");
    _ = @import("btree/btree_insert.zig");
    _ = @import("btree/btree_delete.zig");
    _ = @import("btree/btree_search.zig");
    _ = @import("btree/btree_repair.zig");
    _ = @import("btree/btree_helpers.zig");
}
