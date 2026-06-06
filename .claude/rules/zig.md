# Zig Conventions (`src/`, the zigstore engine)

- DO: **`zig fmt` clean.** Every `.zig` file. The auto-format hook runs it on save.
- DO: **Pair every acquire with `defer`/`errdefer`.** Allocations, file handles, locks. Use
  `errdefer` for cleanup on the error path and `defer` for unconditional teardown. No leaks.
- DO: **Use `std.testing.allocator` in unit tests.** It fails the test on a leak. Don't reach
  for the page allocator or an arena to hide a leak in a test.
- DO: **Return errors, don't panic.** Recoverable conditions use error unions (`!T`) with
  `try`/`catch`. Reserve `unreachable` and `@panic` for genuinely impossible states; pick an
  assertion/error name that makes the impossibility self-evident.
- PREFER: **`comptime` over runtime** when a value or shape is known at compile time. The
  schema-to-storage generation (`Engine(schema)`, the generated `Header`) is comptime by
  design; reach for the same tool for op-code tables and field maps. It is the idiom and it
  costs nothing.
- DO: **Treat wire-format changes as protocol changes.** Adding or reordering an op or field
  touches the binary protocol and the semver contract. Update the codegen source and
  regenerate the client (see [protocol.md](protocol.md)). Never edit a generated file by hand.

## Comments — the public-API carve-out (this repo overrides the global no-comment rule)

`zigstore` is a public library. Its **exported declarations are documented**; everything else
keeps the strict no-comment discipline.

- DO: **Doc-comment the exported surface.** `///` on every `pub` declaration that forms the
  API (`schema`, `Engine`, `codec`, the protocol/server/codegen entry points and their public
  methods/types), and a `//!` module header on each public file. Write them so `zig autodoc`
  and IDE hovers read well: what it is, what the caller must uphold, what it returns.
- NEVER: **Comment internal/private code.** No `//` WHY comments, no `///` on private decls,
  no section dividers, no step labels. The only `//` in a non-doc position is inside a string
  literal. Make private code self-documenting: rename the symbol or extract a well-named
  function instead of explaining it.
- DO: **Encode the "why" in tests.** An invariant a comment would have explained becomes a
  named test that fails when the invariant breaks — name the test for the property it locks
  (e.g. `test "range scan returns big-endian keys in ascending numeric order"`).

| Instead of | Use |
|---|---|
| `// keys are big-endian so byte order == numeric order` | a test: `test "encodeU64 big-endian ordering"` |
| `// looks wrong but is correct because X` | a test that fails if X stops holding |
| `/// Gets the name` over a **private** `fn name()` | nothing |
| no doc over `pub fn Engine(...)` | a `///` describing the generated `Store` surface |
