# Common Conventions

Cross-cutting rules for every file type.

- DO: **Trailing newline at EOF.** Every text file ends with a single `\n`. Without it,
  `git diff` shows `\ No newline at end of file`, and `zig fmt` adds it back. The auto-format
  hook handles it on save, so never strip it by hand.
- DO: **Fix the root cause, not the symptom.** Find why a bug happens before patching where it
  shows. If you must ship a band-aid to unblock, say so and name the real fix.
- NEVER: **Leave commented-out code.** Delete it. Git history is the archive.
