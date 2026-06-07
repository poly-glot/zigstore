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
//!   - `BPlusTree` / `BTreeError` / `MemTable`: the paged tree an index is built on (point ops +
//!     range scans), its error set, and the write memtable fronting it.
//!   - `Worker` / `ReplayEntry`: the background-worker handle and one replayed WAL entry handed
//!     to a `recover` hook (`Store.Config` is the per-store open config, reached as a member of
//!     the generated `Store`).
//!   - `commit` / `snapshot` / `SnapshotHost`: the durable write path (serialize, WAL-append,
//!     in-order apply, durability wait) over a caller-supplied record type, and point-in-time
//!     snapshots driven through the `SnapshotHost` interface a `Store` satisfies via
//!     `Store.snapshotHost()`.
//!
//!   - `BloomFilter`: the lock-free probabilistic set fronting a uniqueness check.
//!   - `inverted_index`: the token-stream toolkit (`TokenIterator`, `MIN_TOKEN_LEN`,
//!     `MAX_TOKEN_LEN`) a consumer drives to build its own search indexes.
//!   - `signal`: the process-wide shutdown flag and SIGTERM/SIGINT installer.
//!   - `MEMTABLE_SHARDS`: the fixed shard count of a `MemTable`, for callers that iterate its
//!     shards directly when draining outside `Store.drainMemtables`.
//!
//!   - `protocol`: the application-neutral binary framing/TLV codec (`Status`, `writeResp`,
//!     `parsePayload`, `writeRowList`, and the pipelined `processFrames` loop with an injected
//!     raw-`op_byte` dispatch callback and a caller-supplied `response_reserve`).
//!   - `connection` / `histogram`: the per-connection request/response buffers a `Handler`
//!     receives, and the latency-histogram type `processFrames` records op timings into.
//!   - `ServerConfig` / `EpollServer` / `Handler` / `run`: the generic networked bootstrap — the
//!     per-server config with its trust-list, the epoll reactor over an opaque `ctx` and runtime
//!     `Handler`, and the `run` entry that spawns and joins the reactor pool.
//!
//!   - `tsgen`: the reflection-driven TypeScript-client emitters (`writeStructInterface`,
//!     `writeStructReader`, `writeOpEnum`, `writeStatusEnum`, `writeStatusMap`), parameterized
//!     over a caller-supplied comptime `FieldTable` that decides each field's TS type and
//!     decode expression. Names no application record.

const engine = @import("engine.zig");

pub const codec = @import("codec.zig");
pub const wire_codec = @import("wire_codec.zig");
pub const tsgen = @import("tsgen.zig");

pub const schema = engine.schema;
pub const Schema = engine.Schema;
pub const IndexSpec = engine.IndexSpec;
pub const KeyKind = engine.KeyKind;
pub const Engine = engine.Engine;
pub const BPlusTree = engine.BPlusTree;
pub const BTreeError = @import("btree/btree.zig").BTreeError;
pub const MemTable = engine.MemTable;
pub const ReplayEntry = engine.ReplayEntry;
pub const Worker = engine.Worker;

pub const snapshot = @import("snapshot.zig");
pub const SnapshotHost = snapshot.SnapshotHost;
pub const SnapshotResult = snapshot.SnapshotResult;
pub const SnapshotManager = snapshot.SnapshotManager;
pub const forceSnapshot = snapshot.forceSnapshot;
pub const commit = @import("commit.zig").commit;

pub const protocol = @import("protocol/framing.zig");
pub const connection = @import("connection.zig");
pub const histogram = @import("histogram.zig");

pub const BloomFilter = @import("bloom.zig").BloomFilter;
pub const inverted_index = @import("inverted_index.zig");
pub const signal = @import("signal.zig");
pub const MEMTABLE_SHARDS = @import("memtable.zig").NUM_SHARDS;
pub const ServerConfig = @import("server_config.zig").ServerConfig;
pub const EpollServer = @import("epoll.zig").EpollServer;
pub const Handler = @import("epoll.zig").Handler;
pub const run = @import("run.zig").run;

test {
    _ = @import("codec.zig");
    _ = @import("engine.zig");
    _ = @import("wire_codec.zig");
    _ = @import("tsgen.zig");
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
    _ = @import("server_config.zig");
    _ = @import("protocol/framing.zig");
    _ = @import("epoll.zig");
    _ = @import("run.zig");

    _ = @import("wal.zig");
    _ = @import("wal_replay.zig");
    _ = @import("snapshot.zig");
    _ = @import("commit.zig");

    _ = @import("btree/btree.zig");
    _ = @import("btree/btree_insert.zig");
    _ = @import("btree/btree_delete.zig");
    _ = @import("btree/btree_search.zig");
    _ = @import("btree/btree_repair.zig");
    _ = @import("btree/btree_helpers.zig");
}
