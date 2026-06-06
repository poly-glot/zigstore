# Protocol Conventions (binary wire format and generated client)

`zigstore` speaks a binary protocol: a framing/TLV codec the engine owns, plus a
reflection-driven TypeScript-client emitter parameterized over the consumer's field-type
mapping. The engine owns framing, pipelining, and latency; the consumer owns the op switch and
its status codes.

- DO: **Treat any wire-format change as a semver event.** Adding, removing, or reordering an op
  or a record field is a protocol change. MAJOR for a break; MINOR only for additive changes.
- DO: **Extend ops with optional trailing bytes.** To extend an op without breaking older
  callers, append an optional trailing byte or field. Follow that pattern instead of forking a
  new op number.
- DO: **Keep the status split.** The engine owns the base `Status` low codes
  (`ok`/`not_found`/`duplicate`/`invalid`/`err`); a consumer's extended statuses and
  sub-statuses are app extensions written to the wire as the raw `u8` the consumer's handler
  returns. The engine never learns a consumer's status names.
- DO: **Regenerate, never hand-edit, generated clients.** A generated TypeScript client is
  output, not source. Regenerate it from the Zig source of truth and commit the result in the
  **same commit** as the server-side change. A manual edit silently diverges the client from
  the server and is overwritten on the next regeneration.
- DO: **Keep hierarchy semantics out of the protocol** (see [architecture.md](architecture.md)).
  New recursive or aggregate needs are consumer ops composed over engine primitives, not engine
  features.
