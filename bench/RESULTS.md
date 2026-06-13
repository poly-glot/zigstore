# Multi-sharded writer: measured write throughput

Evidence that `zigstore.ShardSet` raises write throughput, and an honest statement of **when**
it does not. Produced by [`write_bench.zig`](write_bench.zig) on the pinned Zig 0.15.2 Linux
devcontainer (aarch64, 10 cores). Every number below is the median of ≥7 measured trials
(2 warmups discarded, fresh data directory per trial); the headline result is reproduced across
two independent process invocations.

## Method (and how it resists a fake speedup)

- **Workload:** one durable WAL commit + N direct B+Tree inserts per op (no memtable — every
  index write happens inside the timed window, nothing deferred to drain). Keys are a fixed,
  seeded set, identical across every configuration, so all configs commit the same work and only
  routing differs. Per-shard offered load is printed and is uniform (max/mean ≤ 1.02 at 8 shards).
- **Total page cache held constant:** each shard gets `total_cache_mb / shard_count`, so an
  8-shard run does **not** silently get 8× the cache.
- **Two baselines:** a raw `*Store` (no wrapper) and a `ShardSet` of one. They match to within
  noise (16,507 vs 16,501 ops/s), so the wrapper adds ~0 per-commit overhead and the speedup
  denominator is honest, not handicapped.
- **`fsyncs/op` is reported** (real `fdatasync` count via a WAL accessor, not a guess). It is the
  group-commit batch-occupancy signal that separates a lock-parallelism win from a change in
  `fdatasync` amortization.
- **Distribution non-overlap** is checked: a speedup is only called clean when the N-shard min
  exceeds the 1-shard max across trials.
- **Fails closed on commit errors:** a single failed commit in any trial (warmup or measured) aborts
  the whole run with a non-zero exit, and throughput is computed from *completed* ops, not offered
  ops. So a run that exits 0 provably had zero commit errors — an errored trial can never be printed
  as a number, let alone feed a speedup ratio.
- Two sweeps: **A** holds the offered load fixed (8 writer threads for every shard count, so only
  the shard count varies); **B** scales threads with shards (1 writer/shard).

## Result 1 — apply/lock-bound regime: sharding scales ~5×

Buffered WAL on tmpfs (`--no-direct`, `/dev/shm`), so `fdatasync` is ~free and the bottleneck is
the apply path (apply mutex + per-tree `RwLock` + free-list mutex). 4 indexes/op, 8 fixed threads,
100k ops/trial.

| shards | threads | median ops/s | speedup | fsyncs/op | distributions |
|-------:|--------:|-------------:|--------:|----------:|---------------|
| 1 | 8 | 16,783 | 1.00× | 0.125 | baseline |
| 2 | 8 | 36,753 | 2.19× | 0.394 | non-overlapping |
| 4 | 8 | 67,423 | 4.02× | 0.574 | non-overlapping |
| 8 | 8 | 82,119 | **4.89×** | 0.733 | non-overlapping |

`fsyncs/op` **rose** 0.125 → 0.733 as shards grew (batching got *worse*), yet throughput rose ~5×.
The gain therefore comes from removing the apply-path serialization, **not** from `fdatasync`
amortization. Reproduced across three independent runs: **4.89× / 4.99× / 5.02×** at 8 shards.
The `ShardSet`-of-one (16,783) matches the raw `*Store` (16,768) → the wrapper adds ~0 overhead.

## Result 2 — `fdatasync`-bound regime on one device: sharding does **not** help

Same workload (4 indexes/op, 8 threads), default `O_DIRECT` on a real-`fdatasync` mount (`/tmp`),
50k ops/trial.

| shards | threads | median ops/s | speedup | fsyncs/op |
|-------:|--------:|-------------:|--------:|----------:|
| 1 | 8 | 16,344 | 1.00× | 0.170 |
| 2 | 8 | 12,675 | 0.78× | 0.349 |
| 4 | 8 | 11,726 | 0.72× | 0.596 |
| 8 | 8 | 11,663 | **0.71×** | 0.790 |

Here a single device's `fdatasync` bandwidth is the cap, and one shared WAL with group commit
amortizes it across all 8 concurrent writers (0.17 fsyncs/op ≈ one sync per ~6 commits). Splitting
those writers across 8 WALs **fragments** the batching (0.79 fsyncs/op ≈ one sync per commit),
so sharding is *slower*. The best single-store config (16,344) beats the best sharded config
(11,523). This is the honest negative.

## What it means

`ShardSet` raises write throughput when the write path is **apply/CPU/lock-bound** — multiple
secondary indexes per record, heavier per-write work, or many concurrent writers contending the
apply mutex. It does **not** raise throughput for `fdatasync`-bound writes on a single shared
device, where the existing single-WAL group commit already saturates the device better than
independent per-shard WALs. The production write-scale win of sharding (Tier 3b in
[`scale.md`](../scale.md)) is the combination of apply parallelism **and** N independent
devices/volumes/nodes — the second of which an in-process single-disk benchmark cannot exhibit,
and which this report does not claim.

Reproduce:

```bash
zig build bench -- --no-direct --dir /dev/shm/zsb --indexes 4 --threads 8 --ops 100000   # Result 1
zig build bench --                --indexes 4 --threads 8 --ops 50000                     # Result 2
```
