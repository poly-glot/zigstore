# Examples

Each example imports `zigstore` as a module (wired in [`../build.zig`](../build.zig)) and is
built and tested by the repo's `zig build test`.

## `basic.zig`

Declares a directory-shaped schema (categories and links, with composite secondary indexes),
generates the typed `Store` with `zigstore.Engine(schema)`, and drives it end to end:

- allocates ids from persisted counters (`store.nextId(...)`),
- writes `extern struct` records into named trees via the `codec` toolkit,
- reads one back by primary key,
- range-scans a composite index to list the children of one parent and the links under one
  category, in key order.

Run it:

```bash
zig build run-example
```

It also carries a `test` block (`zig build test` runs it) asserting the range scans and
counters behave, under `std.testing.allocator` so a leak fails the test.
