# Scaling zigstore: deployment, read replicas, and multi-replica writers

What it takes to scale `zigstore` from its current single-node form to a Kubernetes
(OKE) + OCI Block Volume deployment, then to multi-replica readers and writers. Grounded
in the shipped code; cites the seams each tier extends. Distribution is a v1.0 non-goal
(see the extraction design §10), so everything below is net-new and post-v1.0, and is
designed to stay additive to the wire format (MINOR ops with optional trailing bytes).

## Tiers at a glance

| Tier | Goal | Engine change | Effort |
|---|---|---|---|
| **0** | Run one node well on OKE + OCI | none | low |
| **1** | Read replicas (horizontal read scale, async) | WAL follow API + follower mode | moderate |
| **2** | Writer HA (failover; availability, one writer) | lease/election + WAL ack gate | moderate–large |
| **3a** | Consensus-replicated writer (Raft) | replace local WAL append with replicated log | large |
| **3b** | Sharding (horizontal **write** scale) | routing layer; engine unchanged per shard | moderate–large |

## Baseline: where zigstore is today

A **single-node store that already scales across cores but has zero distribution
primitives.**

- `run.zig:35` spawns `max(thread_count/2, 1)` epoll reactors, all sharing **one** `Store`
  and one app ctx, kernel-load-balanced via `SO_REUSEPORT` (`epoll.zig:84`).
- Reads are concurrent: each B+Tree holds a `std.Thread.RwLock` (`btree.zig:167`);
  `search`/`rangeScan` take `lockShared` (`btree_search.zig:19`), and the range iterator
  holds that shared lock for its whole lifetime (`btree.zig:98`). The page cache is sharded
  with per-shard mutexes (`page_cache.zig`).
- Writes are a **single serialized pipeline**: `commit.zig:38-48` appends to the local WAL
  for a monotonic `seq`, applies under `apply_mutex` strictly in `seq` order
  (`while last_applied_seq+1 < seq: wait`), then `awaitDurable`. WAL is group-commit with
  `fdatasync` (`wal.zig:267`). ID counters are non-atomic single-writer
  (`engine.zig:454-467`).

So one node = **many concurrent readers + one logical writer**. There is no WAL shipping,
no replication, no leader election, and no safe cross-process sharing of `store.dat`
(in-place page mutation + private page cache ⇒ a second reader process would see torn
pages).

## Tier 0 — Run the single node well on OKE + OCI

Zero engine changes. Vertical scale only (bigger instance, higher `thread_count`, larger
`cache_size_mb`). HPA does **not** help a single writer.

| Concern | Decision | Rationale (code) |
|---|---|---|
| Workload | **StatefulSet** + `volumeClaimTemplates` (one PVC per pod) | Store owns one `data_dir` (`store.dat` + `wal.bin`); needs stable identity + stable volume |
| Storage class | OCI **Block Volume** CSI (`blockvolume.csi.oraclecloud.com`), **RWO**, ext4/xfs | `wal.zig` uses `O_DIRECT` + `fallocate`; both work on a real block device. **Not** NFS/File Storage — there `O_DIRECT` fails and it degrades to buffered (`wal.zig:84-86`) |
| Volume layout | High VPU tier; put `wal.bin` on its **own** volume, separate from `store.dat` | The WAL `fdatasync` is the latency-critical path (`wal.zig:267`); isolating it from page-cache/header flush cuts write tail latency |
| Single-attach | Keep RWO; **never** OCI multi-attach | Two pods on one volume = two in-place writers = corruption; no cross-host locking exists |
| Arch | arm64 image, `-Dcpu=baseline` (already in `build.zig`) | OKE Ampere A1 target |
| Shutdown | `terminationGracePeriodSeconds` > drain+flush time | SIGTERM → `signal.zig` pipe → reactor exits → `on_shutdown` flush (`epoll.zig:219`); `deinit` drains memtables + flushes header |
| Probes | Add a cheap app-level `ping`/`health` op | No built-in health op (ops are app-defined); TCP check suffices for liveness, readiness wants a real op. `op_latency` histogram already feeds HPA custom metrics |
| Placement | Anti-affinity across fault domains; PDB = 1 | Protecting availability, not adding capacity |

## Tier 1 — Read replicas (horizontal read scale, async)

Highest leverage per unit work, because the substrate exists: the WAL is an ordered,
CRC'd, sequence-numbered log, and `Store.recover` (`engine.zig:530`) already applies a WAL
stream through the app's `apply_entry` hook. A read replica is "recover, forever."

**Build:**
1. **WAL follow/tail API** — `wal_replay.zig` reads a *finished* file; add a "follow from
   LSN" mode that yields new entries as they are appended.
2. **Streaming endpoint** on the leader (a new protocol op, or ship segments to OCI Object
   Storage) serving `entries since LSN`.
3. **Follower mode** — base-restore from a snapshot (`forceSnapshot` in `snapshot.zig`
   gives a consistent `store.dat` + `snapshot.meta` at a known `wal_sequence`), then
   continuously pull and apply through the existing `apply_entry` path; mark the store
   read-only.
4. **Routing** — writes → leader Service, reads → replica Service; the generated TS client
   (`tsgen`) grows a read/write split.

**Consistency:** async ⇒ eventually consistent. Read-your-writes needs sticky routing or
fencing reads on the leader's LSN. Replica readiness = "applied LSN within N of leader."

**Blocker (shared by every tier below):** `truncateAfterCheckpoint` resets `sequence` to 0
(`wal.zig:154`). Replication needs a **monotonic LSN that survives checkpoints** — decouple
"log segment offset" from "global LSN," and gate truncation on replica ack.

## Tier 2 — Writer HA (failover; availability, still one writer)

Surviving leader death, not adding write throughput.

- **(a) Block-volume failover (cheapest HA).** One pod holds the RWO volume; on failure,
  fence → reattach to a standby → replay WAL tail → resume. RWO single-attach provides most
  of the fencing. Needs a lease (k8s `Lease`, or the volume attach *as* the lock) and
  partition handling. RPO ≈ 0 if `fdatasync` is honored (OCI replicates block volumes
  within an AD). RTO = detect + reattach + replay.
- **(b) Streaming-standby promotion (Postgres-style).** Builds on Tier 1: standbys follow
  the WAL; on leader loss, promote the most-caught-up standby, flip it writable, clients
  re-resolve. For RPO = 0, add a **quorum-ack gate in `awaitDurable`** (`wal.zig:194`) —
  commit waits for a standby ack (synchronous replication, at a latency cost). Needs leader
  election (k8s `Lease` or a small embedded election).

## Tier 3 — Multi-writer

Two designs solving different problems.

### 3a — Consensus-replicated single logical writer (Raft)

Strong consistency + auto-failover + zero data loss. Does **not** raise write throughput
(one leader still applies). The fit is unusually clean: `commit.zig` is already local
state-machine replication — monotonic seq, "apply only after seq−1," broadcast. Replace
*"append to local WAL → get seq"* with *"propose to Raft → get committed index"*; the apply
loop barely changes, `wal.bin` becomes the Raft log, and `forceSnapshot` is already the Raft
snapshot. Large effort (Raft itself), but the seam is the cleanest available.

### 3b — Sharding / partitioning (true write scale-out)

Partition the opaque `[]const u8` keyspace across N independent single-writer instances
(hash or range; composite keys route by leading component — `KeyKind.composite`). A smart
client or stateless proxy maps key → shard; each shard is its own StatefulSet pod
(optionally each shard HA via Tier 2/3a). **Writes scale linearly.** Cost: the engine's
"dual indexes written atomically inside the mutex" guarantee (architecture.md) holds **only
within a shard** — any op spanning shards needs app-level 2PC/saga, or key design that
colocates co-accessed records. For most workloads this, not Raft, is the right answer for
write scale.

## Cross-cutting prerequisites

Most tiers depend on these regardless of which path is chosen:

- **Monotonic, checkpoint-surviving LSN** — the single most important enabling change
  (`wal.zig:154` currently resets it).
- **WAL streaming/tailing** with resume-from-LSN — `wal_replay.zig` reads a finished file
  today; add a follow mode.
- **Consistent base backup** — package `store.dat` + `snapshot.meta` at the
  `forceSnapshot` point for new-replica bootstrap.
- **Replication-status / health op** in the protocol — for k8s probes, leader election, and
  staleness gating.
- **Stream backpressure / flow control.**

## Recommended sequencing

1. **Tier 0** now — StatefulSet + RWO block volume + split WAL volume + probes. No engine
   change.
2. **Monotonic LSN fix + WAL follow API** — the shared prerequisite.
3. **Tier 1 read replicas** — biggest payoff per unit work; reuses
   `recover`/`apply_entry`/`forceSnapshot` almost verbatim.
4. Then choose: **Tier 2a** for cheap HA; **Tier 3b sharding** for write throughput;
   **Tier 3a Raft** only if strong consistency + no-data-loss failover is required and the
   build cost is acceptable.

All tiers are additive to the wire format (new ops with optional trailing bytes, MINOR per
the versioning contract); none forces a v2.0.
