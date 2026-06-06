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
//!   - `OrderedTree`: the ordered map an index is built on (point ops + range scans).
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
pub const OrderedTree = engine.OrderedTree;

test {
    _ = @import("codec.zig");
    _ = @import("engine.zig");
    _ = @import("wire_codec.zig");
}
