# Architecture Conventions

## The boundary points one way: apps depend on zigstore, never the reverse

`zigstore` is the generic half — storage, durability, server, framing, codegen. It must not
know about any consumer's domain. The B+Tree compares opaque `[]const u8` keys; the hierarchy,
slugs, and statuses that define a *product* are application semantics layered on top.

- NEVER: **Import or reference an app file or type from an engine file.** No `@import` of a
  consumer module, no consumer type names in engine code, not even in a comment. A dependency
  cycle back to a consumer is the one unforgivable regression. The engine's tests use neutral
  fixtures, never a consumer's records.
- DO: **Invert every engine→app need into a seam.** When the engine must call into
  domain logic (request dispatch, post-replay recompute, a periodic tick, snapshot host
  access), express it as an injected callback or a runtime interface the consumer satisfies —
  not an import. `recover` takes hooks; `spawnWorker` takes a tick fn; `processFrames` takes a
  `dispatch_fn`; snapshot is generic over a `SnapshotHost`.
- DO: **Keep the comptime/runtime split deliberate.** The data/codec/codegen plane is
  comptime-generic (`Engine(schema)`); the dynamic seams stay runtime. Pushing comptime into a
  dynamic seam over-couples the consumer to the engine's internals.

## Hierarchy and aggregate semantics belong to the consumer

The engine provides the primitives (ordered trees, range scans, the tokenizer/inverted-index);
it does not assemble hierarchical answers.

- DO: **Expose cursor/range primitives**; let the consumer compose subtree counts, breadcrumbs,
  and search over them. A consumer that needs a hierarchical query writes it as one walk over
  the engine's trees, not as N round-trips merged client-side.
- DO: **Write dual indexes atomically** inside the engine's mutex when a write maintains more
  than one index, so the consumer never sees a half-updated key space.

## Smallest change first; no abstraction without migration

- DO: **State the minimal fix.** After naming a root cause, name the smallest change that makes
  the bug impossible, and justify any machinery beyond it against the current scale rather than
  hypothetical load.
- DO: **No abstraction without migration.** When you extract a shared primitive, the same
  change deletes every copy it replaces. A primitive adopted in one of several call sites is
  dead weight stacked on the debt it was meant to remove. A reuse abstraction is justified only
  by a real second consumer, not a hypothetical one.
