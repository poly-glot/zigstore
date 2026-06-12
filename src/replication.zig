//! Streaming-standby replication: a leader-side `Hub` that streams durable WAL entries to
//! connected followers over a dedicated blocking-I/O port, and a follower-side `Receiver`
//! that appends each entry to the replica's own WAL, applies it through a consumer-supplied
//! hook, and acks durability back to the leader.
//!
//! The engine ships mechanism only. Leader election and fencing belong to the consumer
//! (e.g. a Kubernetes Lease): fence the old leader before calling `promote` on a replica.
//! Synchronous replication is opt-in via the store-owned `CommitGate` — pass
//! `Store.syncGate()` to both `HubConfig.commit_gate` and `Store.setCommitGate` and set
//! `sync_standbys > 0`, and `commit` blocks until the quorum of followers has acked the
//! entry durable.

const std = @import("std");
const posix = std.posix;
const wal = @import("wal.zig");
const wal_follow = @import("wal_follow.zig");
const wal_replay = @import("wal_replay.zig");
const snapshot = @import("snapshot.zig");

const log = std.log.scoped(.replication);

/// First four bytes of every replication handshake, on both directions.
pub const HANDSHAKE_MAGIC: u32 = 0x5A524550;

/// Alternate handshake magic requesting a base backup instead of a stream: the leader
/// replies with `accepted` (`durable_lsn` = the backup's WAL sequence) followed by
/// `u64 len + snapshot.meta bytes + u32 crc32`, then `u64 len + store.dat bytes +
/// u32 crc32`, and closes. The CRC32 trailers are end-to-end checks over each file's
/// bytes. A leader without a snapshot host (or one predating backups) replies `rejected`.
pub const BACKUP_MAGIC: u32 = 0x5A42414B;

/// Replication wire-protocol version; bumped on any breaking change to the stream framing.
pub const PROTOCOL_VERSION: u32 = 1;

/// Fixed byte length of a follower's name in the handshake (zero-padded).
pub const REPLICA_NAME_LEN = 32;

/// Follower → leader handshake: `start_lsn` is the highest sequence the follower already
/// holds durably; the leader streams from `start_lsn + 1`.
pub const HandshakeRequest = extern struct {
    magic: u32,
    version: u32,
    start_lsn: u64,
    replica_name: [REPLICA_NAME_LEN]u8,
};

/// Leader → follower handshake reply carrying the leader's current durable LSN.
pub const HandshakeResponse = extern struct {
    magic: u32,
    status: u8,
    _pad: [3]u8 = .{0} ** 3,
    durable_lsn: u64,
};

/// Handshake verdict: `lsn_too_old` means the leader's WAL no longer covers the follower's
/// position (re-bootstrap from a base backup); `diverged` means the follower is ahead of
/// the leader (fencing violation or split brain — operator intervention).
pub const HandshakeStatus = enum(u8) {
    accepted = 0,
    lsn_too_old = 1,
    diverged = 2,
    rejected = 3,
};

comptime {
    if (@sizeOf(HandshakeRequest) != 48) @compileError("HandshakeRequest size mismatch");
    if (@sizeOf(HandshakeResponse) != 16) @compileError("HandshakeResponse size mismatch");
}

const MSG_ENTRY: u8 = 'E';
const MSG_HEARTBEAT: u8 = 'H';
const MSG_ACK: u8 = 'A';

const MAX_STREAM_DATA_LEN: u32 = 16 * 1024 * 1024;

const MAX_FOLLOWER_SLOTS = 64;

const HANDSHAKE_TIMEOUT_MS: u64 = 5000;

const ACCEPT_POLL_MS: u64 = 100;

const STOP_POLL_NS: u64 = 20 * std.time.ns_per_ms;

/// Quorum watermark for synchronous replication. `commit` blocks in `awaitQuorum` until the
/// hub advances the watermark past the entry's LSN, or fails with
/// `error.ReplicationStopped` once the hub closes the gate. The gate's memory is owned by
/// the caller, not the hub — a generated store provides one with a store-long lifetime via
/// `Store.syncGate()` — so commits can never touch a freed gate, even after `Hub.stop`.
pub const CommitGate = struct {
    mutex: std.Thread.Mutex = .{},
    cond: std.Thread.Condition = .{},
    watermark: u64 = 0,
    closed: bool = false,

    /// Raises the watermark monotonically and wakes every waiter.
    pub fn advance(self: *CommitGate, lsn: u64) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (lsn > self.watermark) {
            self.watermark = lsn;
            self.cond.broadcast();
        }
    }

    /// Releases all current and future waiters with `error.ReplicationStopped`, until
    /// `reopen`.
    pub fn close(self: *CommitGate) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.closed = true;
        self.cond.broadcast();
    }

    /// Re-arms a gate a previous hub closed; the watermark persists (LSNs are monotonic
    /// across hubs on the same store). Called by `Hub.start`.
    pub fn reopen(self: *CommitGate) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.closed = false;
    }

    /// Blocks until the watermark reaches `lsn`; fails once the gate is closed.
    pub fn awaitQuorum(self: *CommitGate, lsn: u64) error{ReplicationStopped}!void {
        self.mutex.lock();
        defer self.mutex.unlock();
        while (self.watermark < lsn) {
            if (self.closed) return error.ReplicationStopped;
            self.cond.wait(&self.mutex);
        }
    }

    /// The current quorum-acked LSN.
    pub fn current(self: *CommitGate) u64 {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.watermark;
    }
};

/// The leader-side seam a `Hub` streams from: the store's WAL writer, data directory, and
/// (when base backups are served) its snapshot host. Obtain one from a generated store via
/// `Store.primaryHost()`. With `snapshot_host = null` the hub rejects backup requests.
pub const PrimaryHost = struct {
    wal: *wal.WalWriter,
    data_dir: []const u8,
    snapshot_host: ?snapshot.SnapshotHost = null,
};

/// The follower-side seam a `Receiver` applies through: the replica's WAL writer, the
/// store's apply ordering state, the read-only and streaming flags, and the consumer's
/// apply hook (the same shape `recover` takes). Obtain one via
/// `Store.replicaHost(ctx, apply_entry)`. `streaming` is held true for the receiver's
/// lifetime so the store's `promote` can refuse while streamed appends are still possible.
pub const ReplicaHost = struct {
    wal: *wal.WalWriter,
    apply_mutex: *std.Thread.Mutex,
    apply_cond: *std.Thread.Condition,
    last_applied_seq: *u64,
    read_only: *std.atomic.Value(bool),
    streaming: *std.atomic.Value(bool),
    ctx: *anyopaque,
    apply_entry: *const fn (ctx: *anyopaque, entry: wal_replay.ReplayEntry) anyerror!void,
};

/// Leader listener configuration. `port = 0` binds an ephemeral port (read it back via
/// `Hub.port()`). `sync_standbys = 0` keeps replication asynchronous; `n > 0` advances
/// `commit_gate` to the n-th highest follower durable ack. The gate pointer must outlive
/// the hub — pass `Store.syncGate()` (store-owned) and wire the same pointer into
/// `Store.setCommitGate` to make commits wait on it.
pub const HubConfig = struct {
    bind_address: [4]u8 = .{ 0, 0, 0, 0 },
    port: u16 = 0,
    sync_standbys: u8 = 0,
    commit_gate: ?*CommitGate = null,
    max_followers: u8 = 8,
    heartbeat_interval_ms: u64 = 1000,
    ack_timeout_ms: u64 = 10_000,
};

/// One connected follower's acks, as reported by `Hub.status`.
pub const FollowerStatus = struct {
    name: [REPLICA_NAME_LEN]u8,
    durable_ack: u64,
    applied_ack: u64,
};

/// Aggregate leader-side replication state returned by `Hub.status`.
pub const HubStatus = struct {
    durable_lsn: u64,
    quorum_lsn: u64,
    follower_count: usize,
};

/// The leader's replication listener: accepts follower handshakes on its own blocking port
/// (the epoll data plane is untouched), streams durable WAL entries with one sender thread
/// per follower, tracks acks, holds the WAL retain floor at the slowest connected
/// follower's durable ack, and drives the optional `CommitGate`.
pub const Hub = struct {
    allocator: std.mem.Allocator,
    host: PrimaryHost,
    cfg: HubConfig,
    gate: ?*CommitGate,
    listener_fd: posix.socket_t,
    bound_port: u16,
    shutdown: std.atomic.Value(bool),
    accept_thread: std.Thread,
    followers_mutex: std.Thread.Mutex,
    followers: [MAX_FOLLOWER_SLOTS]?*Follower,
    backup_mutex: std.Thread.Mutex,
    backup_wg: std.Thread.WaitGroup,

    /// Binds the listener, spawns the accept thread, and returns the heap-allocated hub.
    /// Release with `stop`.
    pub fn start(allocator: std.mem.Allocator, host: PrimaryHost, cfg: HubConfig) !*Hub {
        std.debug.assert(cfg.max_followers <= MAX_FOLLOWER_SLOTS);

        const address = std.net.Address.initIp4(cfg.bind_address, cfg.port);
        var server = try address.listen(.{ .reuse_address = true });
        errdefer server.deinit();

        setSockTimeout(server.stream.handle, posix.SO.RCVTIMEO, ACCEPT_POLL_MS);

        const self = try allocator.create(Hub);
        errdefer allocator.destroy(self);

        if (cfg.commit_gate) |gate| gate.reopen();

        self.* = Hub{
            .allocator = allocator,
            .host = host,
            .cfg = cfg,
            .gate = cfg.commit_gate,
            .listener_fd = server.stream.handle,
            .bound_port = server.listen_address.getPort(),
            .shutdown = std.atomic.Value(bool).init(false),
            .accept_thread = undefined,
            .followers_mutex = .{},
            .followers = .{null} ** MAX_FOLLOWER_SLOTS,
            .backup_mutex = .{},
            .backup_wg = .{},
        };

        self.accept_thread = try std.Thread.spawn(.{}, acceptLoop, .{self});
        return self;
    }

    /// Stops accepting, closes the gate (releasing blocked synchronous commits with
    /// `error.ReplicationStopped`), disconnects and joins every follower, restores the WAL
    /// retain floor, and frees the hub. The pointer is invalid afterward.
    pub fn stop(self: *Hub) void {
        self.shutdown.store(true, .release);
        if (self.gate) |gate| gate.close();

        posix.shutdown(self.listener_fd, .both) catch {};
        self.accept_thread.join();
        posix.close(self.listener_fd);

        self.backup_wg.wait();

        var live: [MAX_FOLLOWER_SLOTS]?*Follower = .{null} ** MAX_FOLLOWER_SLOTS;
        {
            self.followers_mutex.lock();
            defer self.followers_mutex.unlock();
            for (&self.followers, 0..) |*slot, i| {
                if (slot.*) |f| {
                    f.closing.store(true, .release);
                    if (!f.done.load(.acquire)) posix.shutdown(f.fd, .both) catch {};
                    live[i] = f;
                    slot.* = null;
                }
            }
        }
        for (live) |maybe| {
            if (maybe) |f| {
                f.sender_thread.join();
                self.allocator.destroy(f);
            }
        }

        self.host.wal.setRetainFloor(std.math.maxInt(u64));

        const allocator = self.allocator;
        allocator.destroy(self);
    }

    /// The bound listener port (useful with an ephemeral `port = 0`).
    pub fn port(self: *Hub) u16 {
        return self.bound_port;
    }

    /// Snapshot of leader durability, the quorum watermark, and per-follower acks (up to
    /// `out.len` entries).
    pub fn status(self: *Hub, out: []FollowerStatus) HubStatus {
        self.followers_mutex.lock();
        defer self.followers_mutex.unlock();

        var count: usize = 0;
        for (self.followers[0..self.cfg.max_followers]) |maybe| {
            const f = maybe orelse continue;
            if (f.done.load(.acquire)) continue;
            if (count < out.len) {
                out[count] = .{
                    .name = f.name,
                    .durable_ack = f.durable_ack.load(.acquire),
                    .applied_ack = f.applied_ack.load(.acquire),
                };
            }
            count += 1;
        }

        return .{
            .durable_lsn = self.host.wal.durableBoundary().sequence,
            .quorum_lsn = if (self.gate) |gate| gate.current() else 0,
            .follower_count = count,
        };
    }

    fn acceptLoop(self: *Hub) void {
        while (!self.shutdown.load(.acquire)) {
            const fd = posix.accept(self.listener_fd, null, null, posix.SOCK.CLOEXEC) catch |err| switch (err) {
                error.WouldBlock => continue,
                else => {
                    if (self.shutdown.load(.acquire)) return;
                    continue;
                },
            };
            self.handleConnection(fd);
        }
    }

    fn handleConnection(self: *Hub, fd: posix.socket_t) void {
        setSockTimeout(fd, posix.SO.RCVTIMEO, HANDSHAKE_TIMEOUT_MS);
        setSockTimeout(fd, posix.SO.SNDTIMEO, self.cfg.ack_timeout_ms);

        var req_buf: [@sizeOf(HandshakeRequest)]u8 = undefined;
        readFull(fd, &req_buf) catch {
            posix.close(fd);
            return;
        };
        const req = std.mem.bytesToValue(HandshakeRequest, &req_buf);

        if (req.magic == BACKUP_MAGIC and req.version == PROTOCOL_VERSION) {
            self.backup_wg.start();
            const t = std.Thread.spawn(.{}, serveBackupThread, .{ self, fd }) catch |err| {
                self.backup_wg.finish();
                log.err("replication: backup thread spawn failed: {}", .{err});
                respond(fd, .rejected, 0);
                posix.close(fd);
                return;
            };
            t.detach();
            return;
        }

        const boundary = self.host.wal.durableBoundary();
        const verdict = self.judgeHandshake(req, boundary);

        if (verdict != .accepted) {
            respond(fd, verdict, boundary.sequence);
            posix.close(fd);
            return;
        }

        const follower = self.register(fd, req) catch |err| {
            log.warn("replication: rejecting follower: {}", .{err});
            respond(fd, .rejected, boundary.sequence);
            posix.close(fd);
            return;
        };

        respond(fd, .accepted, boundary.sequence);
        setSockTimeout(fd, posix.SO.RCVTIMEO, self.cfg.ack_timeout_ms);

        follower.ack_thread = std.Thread.spawn(.{}, Follower.ackLoop, .{follower}) catch |err| {
            log.err("replication: ack thread spawn failed: {}", .{err});
            self.unregister(follower);
            return;
        };
        follower.sender_thread = std.Thread.spawn(.{}, Follower.sendLoop, .{follower}) catch |err| {
            log.err("replication: sender thread spawn failed: {}", .{err});
            follower.closing.store(true, .release);
            posix.shutdown(follower.fd, .both) catch {};
            follower.ack_thread.join();
            self.unregister(follower);
            return;
        };
    }

    fn serveBackupThread(self: *Hub, fd: posix.socket_t) void {
        defer self.backup_wg.finish();
        defer posix.close(fd);

        const shost = self.host.snapshot_host orelse {
            respond(fd, .rejected, 0);
            return;
        };

        self.backup_mutex.lock();
        defer self.backup_mutex.unlock();

        const result = snapshot.forceBaseBackup(shost) catch |err| {
            log.warn("replication: base backup failed: {}", .{err});
            respond(fd, .rejected, 0);
            return;
        };

        respond(fd, .accepted, result.wal_sequence);

        self.streamBackupFiles(fd) catch |err| {
            log.warn("replication: base backup transfer failed: {}", .{err});
        };
        self.deleteBaseCopy();
    }

    fn streamBackupFiles(self: *Hub, fd: posix.socket_t) !void {
        const meta_path = try std.fs.path.join(self.allocator, &.{ self.host.data_dir, "snapshot.meta" });
        defer self.allocator.free(meta_path);
        try sendFile(fd, meta_path);

        const base_path = try std.fs.path.join(self.allocator, &.{ self.host.data_dir, snapshot.BASE_BACKUP_FILE });
        defer self.allocator.free(base_path);
        try sendFile(fd, base_path);
    }

    fn deleteBaseCopy(self: *Hub) void {
        const base_path = std.fs.path.join(self.allocator, &.{ self.host.data_dir, snapshot.BASE_BACKUP_FILE }) catch return;
        defer self.allocator.free(base_path);
        std.fs.cwd().deleteFile(base_path) catch {};
    }

    fn judgeHandshake(self: *Hub, req: HandshakeRequest, boundary: wal.DurableBoundary) HandshakeStatus {
        if (req.magic != HANDSHAKE_MAGIC or req.version != PROTOCOL_VERSION) return .rejected;
        if (req.start_lsn > boundary.sequence) return .diverged;
        if (req.start_lsn < boundary.sequence) {
            var probe = wal_follow.FollowReader.init(self.allocator, self.host.data_dir, req.start_lsn) catch return .rejected;
            defer probe.deinit();
            const first = probe.firstSequence(boundary) catch return .rejected;
            if (first == null or first.? > req.start_lsn + 1) return .lsn_too_old;
        }
        return .accepted;
    }

    fn respond(fd: posix.socket_t, verdict: HandshakeStatus, durable_lsn: u64) void {
        const resp = HandshakeResponse{
            .magic = HANDSHAKE_MAGIC,
            .status = @intFromEnum(verdict),
            .durable_lsn = durable_lsn,
        };
        writeFull(fd, std.mem.asBytes(&resp)) catch {};
    }

    fn register(self: *Hub, fd: posix.socket_t, req: HandshakeRequest) !*Follower {
        self.followers_mutex.lock();
        defer self.followers_mutex.unlock();

        self.reapLocked();

        const slot = blk: {
            for (self.followers[0..self.cfg.max_followers], 0..) |maybe, i| {
                if (maybe == null) break :blk i;
            }
            return error.FollowersFull;
        };

        var reader = try wal_follow.FollowReader.init(self.allocator, self.host.data_dir, req.start_lsn);
        errdefer reader.deinit();

        const follower = try self.allocator.create(Follower);
        follower.* = Follower{
            .hub = self,
            .fd = fd,
            .name = req.replica_name,
            .start_lsn = req.start_lsn,
            .durable_ack = std.atomic.Value(u64).init(req.start_lsn),
            .applied_ack = std.atomic.Value(u64).init(req.start_lsn),
            .closing = std.atomic.Value(bool).init(false),
            .done = std.atomic.Value(bool).init(false),
            .reader = reader,
            .sender_thread = undefined,
            .ack_thread = undefined,
            .slot = slot,
        };
        self.followers[slot] = follower;
        self.recomputeLocked();
        return follower;
    }

    fn unregister(self: *Hub, follower: *Follower) void {
        posix.close(follower.fd);
        follower.reader.deinit();

        self.followers_mutex.lock();
        defer self.followers_mutex.unlock();
        self.followers[follower.slot] = null;
        self.allocator.destroy(follower);
        self.recomputeLocked();
    }

    fn reapLocked(self: *Hub) void {
        for (&self.followers) |*slot| {
            const f = slot.* orelse continue;
            if (!f.done.load(.acquire)) continue;
            f.sender_thread.join();
            self.allocator.destroy(f);
            slot.* = null;
        }
    }

    fn recompute(self: *Hub) void {
        self.followers_mutex.lock();
        defer self.followers_mutex.unlock();
        self.recomputeLocked();
    }

    fn recomputeLocked(self: *Hub) void {
        var floor: u64 = std.math.maxInt(u64);
        var acks: [MAX_FOLLOWER_SLOTS]u64 = undefined;
        var n: usize = 0;

        for (self.followers[0..self.cfg.max_followers]) |maybe| {
            const f = maybe orelse continue;
            if (f.done.load(.acquire)) continue;
            const ack = f.durable_ack.load(.acquire);
            floor = @min(floor, ack);
            acks[n] = ack;
            n += 1;
        }

        self.host.wal.setRetainFloor(floor);

        if (self.gate) |gate| {
            if (self.cfg.sync_standbys > 0 and n >= self.cfg.sync_standbys) {
                std.mem.sort(u64, acks[0..n], {}, std.sort.desc(u64));
                gate.advance(acks[self.cfg.sync_standbys - 1]);
            }
        }
    }
};

const Follower = struct {
    hub: *Hub,
    fd: posix.socket_t,
    name: [REPLICA_NAME_LEN]u8,
    start_lsn: u64,
    durable_ack: std.atomic.Value(u64),
    applied_ack: std.atomic.Value(u64),
    closing: std.atomic.Value(bool),
    done: std.atomic.Value(bool),
    reader: wal_follow.FollowReader,
    sender_thread: std.Thread,
    ack_thread: std.Thread,
    slot: usize,

    fn sendLoop(self: *Follower) void {
        defer self.finish();

        const heartbeat_ns = self.hub.cfg.heartbeat_interval_ms * std.time.ns_per_ms;

        while (!self.closing.load(.acquire) and !self.hub.shutdown.load(.acquire)) {
            const boundary = self.hub.host.wal.durableBoundary();

            var sent = false;
            while (true) {
                const entry = (self.reader.next(boundary) catch |err| {
                    log.warn("replication: follower stream ended: {}", .{err});
                    return;
                }) orelse break;
                self.sendEntry(entry) catch return;
                sent = true;
            }
            if (sent) continue;

            if (!self.hub.host.wal.waitDurableBeyond(boundary.sequence, heartbeat_ns)) {
                self.sendHeartbeat(boundary.sequence) catch return;
            }
        }
    }

    fn sendEntry(self: *Follower, entry: wal_follow.FollowedEntry) !void {
        const header = wal.WalEntryHeader{
            .sequence = entry.sequence,
            .op_code = entry.op_code,
            .data_len = @intCast(entry.data.len),
            .checksum = entry.checksum,
        };
        var frame: [1 + wal.HEADER_SIZE]u8 = undefined;
        frame[0] = MSG_ENTRY;
        @memcpy(frame[1..], std.mem.asBytes(&header));
        try writeFull(self.fd, &frame);
        try writeFull(self.fd, entry.data);
    }

    fn sendHeartbeat(self: *Follower, durable_lsn: u64) !void {
        var frame: [9]u8 = undefined;
        frame[0] = MSG_HEARTBEAT;
        std.mem.writeInt(u64, frame[1..9], durable_lsn, .little);
        try writeFull(self.fd, &frame);
    }

    fn ackLoop(self: *Follower) void {
        while (!self.closing.load(.acquire)) {
            var frame: [17]u8 = undefined;
            readFull(self.fd, &frame) catch break;
            if (frame[0] != MSG_ACK) break;

            const durable = std.mem.readInt(u64, frame[1..9], .little);
            const durable_advanced = durable != self.durable_ack.load(.acquire);
            self.durable_ack.store(durable, .release);
            self.applied_ack.store(std.mem.readInt(u64, frame[9..17], .little), .release);
            if (durable_advanced) self.hub.recompute();
        }

        self.closing.store(true, .release);
        posix.shutdown(self.fd, .both) catch {};
    }

    fn finish(self: *Follower) void {
        self.closing.store(true, .release);
        posix.shutdown(self.fd, .both) catch {};
        self.ack_thread.join();

        {
            self.hub.followers_mutex.lock();
            defer self.hub.followers_mutex.unlock();
            self.done.store(true, .release);
            posix.close(self.fd);
            self.hub.recomputeLocked();
        }

        self.reader.deinit();
    }
};

/// Follower connection configuration. `replica_name` is copied (≤ `REPLICA_NAME_LEN`
/// bytes); reconnect backoff doubles from `reconnect_min_ms` up to `reconnect_max_ms`.
pub const ReceiverConfig = struct {
    leader_address: [4]u8,
    leader_port: u16,
    replica_name: []const u8,
    reconnect_min_ms: u64 = 200,
    reconnect_max_ms: u64 = 5000,
    ack_timeout_ms: u64 = 10_000,
};

/// Receiver lifecycle phase. `needs_rebootstrap`, `diverged`, and `failed` are terminal:
/// the supervisor thread ends and an operator (or the consumer's control loop) must
/// intervene — re-seed from a base backup, resolve the split brain, or restart.
pub const ReplicaPhase = enum(u8) {
    connecting,
    streaming,
    stopped,
    needs_rebootstrap,
    diverged,
    failed,
};

/// Point-in-time receiver state from `Receiver.status`.
pub const ReceiverStatus = struct {
    phase: ReplicaPhase,
    last_applied_lsn: u64,
    last_durable_lsn: u64,
    leader_durable_lsn: u64,
};

/// The follower's streaming client: connects to the leader's `Hub`, appends each received
/// entry to the replica's own WAL, applies it under the store's apply lock, and acks
/// durability. Transient failures reconnect with backoff; fatal verdicts park the receiver
/// in a terminal phase. `start` marks the store read-only; `promote` it only after the old
/// leader is fenced.
pub const Receiver = struct {
    allocator: std.mem.Allocator,
    host: ReplicaHost,
    cfg: ReceiverConfig,
    name: [REPLICA_NAME_LEN]u8,
    phase: std.atomic.Value(ReplicaPhase),
    stop_flag: std.atomic.Value(bool),
    fd_mutex: std.Thread.Mutex,
    current_fd: ?posix.socket_t,
    last_applied: std.atomic.Value(u64),
    last_durable: std.atomic.Value(u64),
    leader_durable: std.atomic.Value(u64),
    thread: std.Thread,

    /// Marks the replica read-only, spawns the supervisor thread, and returns the
    /// heap-allocated receiver. Release with `stop`.
    pub fn start(allocator: std.mem.Allocator, host: ReplicaHost, cfg: ReceiverConfig) !*Receiver {
        const name = try buildReplicaName(cfg.replica_name);

        host.read_only.store(true, .release);
        host.streaming.store(true, .release);
        errdefer host.streaming.store(false, .release);

        const start_seq = host.wal.getSequence();

        const self = try allocator.create(Receiver);
        errdefer allocator.destroy(self);

        self.* = Receiver{
            .allocator = allocator,
            .host = host,
            .cfg = cfg,
            .name = name,
            .phase = std.atomic.Value(ReplicaPhase).init(.connecting),
            .stop_flag = std.atomic.Value(bool).init(false),
            .fd_mutex = .{},
            .current_fd = null,
            .last_applied = std.atomic.Value(u64).init(start_seq),
            .last_durable = std.atomic.Value(u64).init(start_seq),
            .leader_durable = std.atomic.Value(u64).init(0),
            .thread = undefined,
        };

        self.thread = try std.Thread.spawn(.{}, supervise, .{self});
        return self;
    }

    /// Disconnects, joins the supervisor, and frees the receiver. Does not clear the
    /// store's read-only flag — that is `promote`'s job, after fencing. The pointer is
    /// invalid afterward.
    pub fn stop(self: *Receiver) void {
        self.stop_flag.store(true, .release);
        {
            self.fd_mutex.lock();
            defer self.fd_mutex.unlock();
            if (self.current_fd) |fd| posix.shutdown(fd, .both) catch {};
        }
        self.thread.join();

        const allocator = self.allocator;
        allocator.destroy(self);
    }

    /// The receiver's phase and LSN positions.
    pub fn status(self: *Receiver) ReceiverStatus {
        return .{
            .phase = self.phase.load(.acquire),
            .last_applied_lsn = self.last_applied.load(.acquire),
            .last_durable_lsn = self.last_durable.load(.acquire),
            .leader_durable_lsn = self.leader_durable.load(.acquire),
        };
    }

    fn supervise(self: *Receiver) void {
        defer self.host.streaming.store(false, .release);

        const address = std.net.Address.initIp4(self.cfg.leader_address, self.cfg.leader_port);
        var backoff = self.cfg.reconnect_min_ms;

        while (!self.stop_flag.load(.acquire)) {
            self.phase.store(.connecting, .release);

            const stream = std.net.tcpConnectToAddress(address) catch {
                self.backoffSleep(&backoff);
                continue;
            };
            self.setFd(stream.handle);

            const result = self.session(stream.handle);

            self.setFd(null);
            posix.close(stream.handle);

            if (result) |_| {
                break;
            } else |err| switch (err) {
                error.LsnTooOld => {
                    self.phase.store(.needs_rebootstrap, .release);
                    return;
                },
                error.Diverged => {
                    self.phase.store(.diverged, .release);
                    return;
                },
                error.ApplyFailed, error.LocalWalFailed => {
                    self.phase.store(.failed, .release);
                    return;
                },
                else => {
                    if (self.phase.load(.acquire) == .streaming) backoff = self.cfg.reconnect_min_ms;
                    log.warn("replication: receiver session ended: {} — reconnecting", .{err});
                    self.backoffSleep(&backoff);
                },
            }
        }

        self.phase.store(.stopped, .release);
    }

    fn session(self: *Receiver, fd: posix.socket_t) !void {
        setSockTimeouts(fd, self.cfg.ack_timeout_ms);

        const req = HandshakeRequest{
            .magic = HANDSHAKE_MAGIC,
            .version = PROTOCOL_VERSION,
            .start_lsn = self.host.wal.getSequence(),
            .replica_name = self.name,
        };
        try writeFull(fd, std.mem.asBytes(&req));

        var resp_buf: [@sizeOf(HandshakeResponse)]u8 = undefined;
        try readFull(fd, &resp_buf);
        const resp = std.mem.bytesToValue(HandshakeResponse, &resp_buf);
        if (resp.magic != HANDSHAKE_MAGIC) return error.BadHandshake;

        switch (resp.status) {
            @intFromEnum(HandshakeStatus.accepted) => {},
            @intFromEnum(HandshakeStatus.lsn_too_old) => return error.LsnTooOld,
            @intFromEnum(HandshakeStatus.diverged) => return error.Diverged,
            else => return error.Rejected,
        }

        self.leader_durable.store(resp.durable_lsn, .release);
        self.phase.store(.streaming, .release);

        var data_buf: std.ArrayList(u8) = .{};
        defer data_buf.deinit(self.allocator);

        var pending: u64 = 0;
        while (!self.stop_flag.load(.acquire)) {
            if (pending != 0) {
                const more_buffered = try inputPending(fd);
                if (!more_buffered) {
                    try self.settleAndAck(fd, pending);
                    pending = 0;
                }
            }

            var tag: [1]u8 = undefined;
            try readFull(fd, &tag);

            switch (tag[0]) {
                MSG_ENTRY => pending = try self.receiveEntry(fd, &data_buf),
                MSG_HEARTBEAT => {
                    var lsn_buf: [8]u8 = undefined;
                    try readFull(fd, &lsn_buf);
                    self.leader_durable.store(std.mem.readInt(u64, &lsn_buf, .little), .release);
                    try self.settleAndAck(fd, self.host.wal.getSequence());
                    pending = 0;
                },
                else => return error.BadFrame,
            }
        }
    }

    fn receiveEntry(self: *Receiver, fd: posix.socket_t, data_buf: *std.ArrayList(u8)) !u64 {
        var header_buf: [wal.HEADER_SIZE]u8 = undefined;
        try readFull(fd, &header_buf);
        const header = std.mem.bytesToValue(wal.WalEntryHeader, &header_buf);

        if (header.data_len > MAX_STREAM_DATA_LEN) return error.BadFrame;
        try data_buf.resize(self.allocator, header.data_len);
        try readFull(fd, data_buf.items);

        if (std.hash.crc.Crc32.hash(data_buf.items) != header.checksum) return error.ChecksumMismatch;

        const expected = self.host.wal.getSequence() + 1;
        if (header.sequence != expected) return error.SequenceGap;

        const seq = self.host.wal.append(header.op_code, data_buf.items) catch return error.LocalWalFailed;
        if (seq != header.sequence) return error.LocalWalFailed;

        {
            self.host.apply_mutex.lock();
            defer self.host.apply_mutex.unlock();
            self.host.apply_entry(self.host.ctx, .{
                .sequence = seq,
                .op_code = header.op_code,
                .data = data_buf.items,
            }) catch |err| {
                log.err("replication: apply of entry {d} failed: {}", .{ seq, err });
                return error.ApplyFailed;
            };
            self.host.last_applied_seq.* = seq;
            self.host.apply_cond.broadcast();
        }
        self.last_applied.store(seq, .release);

        return seq;
    }

    fn settleAndAck(self: *Receiver, fd: posix.socket_t, lsn: u64) !void {
        if (lsn > 0) {
            self.host.wal.awaitDurable(lsn) catch return error.LocalWalFailed;
            self.last_durable.store(lsn, .release);
        }

        var frame: [17]u8 = undefined;
        frame[0] = MSG_ACK;
        std.mem.writeInt(u64, frame[1..9], lsn, .little);
        std.mem.writeInt(u64, frame[9..17], self.last_applied.load(.acquire), .little);
        try writeFull(fd, &frame);
    }

    fn setFd(self: *Receiver, fd: ?posix.socket_t) void {
        self.fd_mutex.lock();
        defer self.fd_mutex.unlock();
        self.current_fd = fd;
    }

    fn backoffSleep(self: *Receiver, backoff: *u64) void {
        var remaining_ns = backoff.* * std.time.ns_per_ms;
        while (remaining_ns > 0 and !self.stop_flag.load(.acquire)) {
            const chunk = @min(remaining_ns, STOP_POLL_NS);
            std.Thread.sleep(chunk);
            remaining_ns -= chunk;
        }
        backoff.* = @min(backoff.* * 2, self.cfg.reconnect_max_ms);
    }
};

/// Result of `fetchBaseBackup`: the WAL sequence the fetched base is consistent up to.
pub const BaseBackupInfo = struct {
    wal_sequence: u64,
};

/// Bootstrap (or re-bootstrap, after `needs_rebootstrap`) a replica's data directory from
/// the leader's base backup: connects to the leader's replication port, requests a
/// page-consistent copy, removes any stale `wal.bin`, writes `store.dat` and then
/// `snapshot.meta` into `data_dir`, and returns the backup's WAL sequence. Call it with no
/// store open on `data_dir`; opening the store afterwards resumes the WAL sequence from the
/// snapshot metadata, so a `Receiver.start` continues streaming exactly past the base.
pub fn fetchBaseBackup(allocator: std.mem.Allocator, cfg: ReceiverConfig, data_dir: []const u8) !BaseBackupInfo {
    const name = try buildReplicaName(cfg.replica_name);

    const address = std.net.Address.initIp4(cfg.leader_address, cfg.leader_port);
    const stream = try std.net.tcpConnectToAddress(address);
    defer stream.close();
    setSockTimeouts(stream.handle, cfg.ack_timeout_ms);

    const req = HandshakeRequest{
        .magic = BACKUP_MAGIC,
        .version = PROTOCOL_VERSION,
        .start_lsn = 0,
        .replica_name = name,
    };
    try writeFull(stream.handle, std.mem.asBytes(&req));

    var resp_buf: [@sizeOf(HandshakeResponse)]u8 = undefined;
    try readFull(stream.handle, &resp_buf);
    const resp = std.mem.bytesToValue(HandshakeResponse, &resp_buf);
    if (resp.magic != HANDSHAKE_MAGIC) return error.BadHandshake;
    if (resp.status != @intFromEnum(HandshakeStatus.accepted)) return error.BackupRefused;

    const meta_len = try readLen(stream.handle);
    if (meta_len < @sizeOf(snapshot.SnapshotHeader) or meta_len > 4096) return error.BadFrame;
    var meta_buf: [4096]u8 = undefined;
    try readFull(stream.handle, meta_buf[0..meta_len]);
    if (try readCrc(stream.handle) != std.hash.crc.Crc32.hash(meta_buf[0..meta_len])) {
        return error.BackupChecksumMismatch;
    }

    const meta = std.mem.bytesToValue(snapshot.SnapshotHeader, meta_buf[0..@sizeOf(snapshot.SnapshotHeader)]);
    if (meta.magic != snapshot.SNAP_MAGIC) return error.BadFrame;

    try std.fs.cwd().makePath(data_dir);

    const wal_path = try std.fs.path.join(allocator, &.{ data_dir, "wal.bin" });
    defer allocator.free(wal_path);
    std.fs.cwd().deleteFile(wal_path) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };

    const data_len = try readLen(stream.handle);
    const store_path = try std.fs.path.join(allocator, &.{ data_dir, "store.dat" });
    defer allocator.free(store_path);
    try receiveFile(stream.handle, store_path, data_len);

    const meta_path = try std.fs.path.join(allocator, &.{ data_dir, "snapshot.meta" });
    defer allocator.free(meta_path);
    {
        const meta_file = try std.fs.cwd().createFile(meta_path, .{ .truncate = true });
        defer meta_file.close();
        try meta_file.writeAll(meta_buf[0..meta_len]);
        try meta_file.sync();
    }

    return .{ .wal_sequence = meta.wal_sequence };
}

fn readLen(fd: posix.socket_t) !u64 {
    var len_buf: [8]u8 = undefined;
    try readFull(fd, &len_buf);
    return std.mem.readInt(u64, &len_buf, .little);
}

fn readCrc(fd: posix.socket_t) !u32 {
    var crc_buf: [4]u8 = undefined;
    try readFull(fd, &crc_buf);
    return std.mem.readInt(u32, &crc_buf, .little);
}

fn writeCrc(fd: posix.socket_t, value: u32) !void {
    var crc_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &crc_buf, value, .little);
    try writeFull(fd, &crc_buf);
}

fn writeLen(fd: posix.socket_t, len: u64) !void {
    var len_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &len_buf, len, .little);
    try writeFull(fd, &len_buf);
}

fn sendFile(fd: posix.socket_t, path: []const u8) !void {
    const file = try std.fs.cwd().openFile(path, .{ .mode = .read_only });
    defer file.close();

    const size = (try file.stat()).size;
    try writeLen(fd, size);

    var crc = std.hash.crc.Crc32.init();
    var chunk: [64 * 1024]u8 = undefined;
    var remaining: u64 = size;
    while (remaining > 0) {
        const want: usize = @intCast(@min(remaining, chunk.len));
        const n = try file.readAll(chunk[0..want]);
        if (n == 0) return error.ConnectionClosed;
        crc.update(chunk[0..n]);
        try writeFull(fd, chunk[0..n]);
        remaining -= n;
    }

    try writeCrc(fd, crc.final());
}

fn receiveFile(fd: posix.socket_t, path: []const u8, len: u64) !void {
    const file = try std.fs.cwd().createFile(path, .{ .truncate = true });
    defer file.close();

    var crc = std.hash.crc.Crc32.init();
    var chunk: [64 * 1024]u8 = undefined;
    var remaining: u64 = len;
    while (remaining > 0) {
        const want: usize = @intCast(@min(remaining, chunk.len));
        try readFull(fd, chunk[0..want]);
        crc.update(chunk[0..want]);
        try file.writeAll(chunk[0..want]);
        remaining -= want;
    }

    if (try readCrc(fd) != crc.final()) return error.BackupChecksumMismatch;

    try file.sync();
}

fn setSockTimeout(fd: posix.socket_t, opt: u32, ms: u64) void {
    const tv = posix.timeval{
        .sec = @intCast(ms / 1000),
        .usec = @intCast((ms % 1000) * 1000),
    };
    posix.setsockopt(fd, posix.SOL.SOCKET, opt, std.mem.asBytes(&tv)) catch {};
}

fn setSockTimeouts(fd: posix.socket_t, ms: u64) void {
    setSockTimeout(fd, posix.SO.RCVTIMEO, ms);
    setSockTimeout(fd, posix.SO.SNDTIMEO, ms);
}

fn buildReplicaName(replica_name: []const u8) error{ReplicaNameTooLong}![REPLICA_NAME_LEN]u8 {
    if (replica_name.len > REPLICA_NAME_LEN) return error.ReplicaNameTooLong;
    var name: [REPLICA_NAME_LEN]u8 = .{0} ** REPLICA_NAME_LEN;
    @memcpy(name[0..replica_name.len], replica_name);
    return name;
}

fn writeFull(fd: posix.socket_t, bytes: []const u8) !void {
    var off: usize = 0;
    while (off < bytes.len) {
        const n = try posix.write(fd, bytes[off..]);
        if (n == 0) return error.ConnectionClosed;
        off += n;
    }
}

fn readFull(fd: posix.socket_t, buf: []u8) !void {
    var off: usize = 0;
    while (off < buf.len) {
        const n = try posix.read(fd, buf[off..]);
        if (n == 0) return error.ConnectionClosed;
        off += n;
    }
}

fn inputPending(fd: posix.socket_t) !bool {
    var fds = [_]posix.pollfd{.{ .fd = fd, .events = posix.POLL.IN, .revents = 0 }};
    const n = try posix.poll(&fds, 0);
    return n > 0;
}

const engine = @import("engine.zig");
const commit_path = @import("commit.zig");
const codec = @import("codec.zig");

const test_schema = engine.schema(.{
    .magic = 0x5A52504C,
    .format_version = 1,
    .indexes = .{
        .{ .name = "by_id", .key = .u64 },
    },
    .memtable_indexes = &.{},
    .counters = &.{},
});

const TestStore = engine.Engine(test_schema);

const TestRecord = struct {
    id: u64,
    value: u8,
};

const test_op: u8 = 1;

fn serializeTestRecord(allocator: std.mem.Allocator, rec: TestRecord) anyerror![]u8 {
    const buf = try allocator.alloc(u8, 9);
    @memcpy(buf[0..8], &codec.encodeU64(rec.id));
    buf[8] = rec.value;
    return buf;
}

fn applyLeaderRecord(ctx: *anyopaque, rec: TestRecord) anyerror!void {
    const store: *TestStore = @ptrCast(@alignCast(ctx));
    try store.tree("by_id").insert(&codec.encodeU64(rec.id), &.{rec.value});
}

fn applyReplicaEntry(ctx: *anyopaque, entry: wal_replay.ReplayEntry) anyerror!void {
    const store: *TestStore = @ptrCast(@alignCast(ctx));
    if (entry.op_code != test_op or entry.data.len < 9) return error.UnknownReplicatedOp;
    try store.tree("by_id").insert(entry.data[0..8], entry.data[8..]);
}

fn commitTestRecord(store: *TestStore, id: u64, value: u8) !void {
    try commit_path.commit(TestRecord, store, test_op, .{ .id = id, .value = value }, store, serializeTestRecord, applyLeaderRecord);
}

fn openStore(allocator: std.mem.Allocator, tmp: *std.testing.TmpDir) !*TestStore {
    const path = try tmp.dir.realpathAlloc(allocator, ".");
    errdefer allocator.free(path);
    return TestStore.init(allocator, .{ .data_dir = path, .cache_size_mb = 16 });
}

fn closeStore(store: *TestStore) void {
    const allocator = store.allocator;
    const data_dir = store.config.data_dir;
    store.deinit();
    allocator.free(data_dir);
}

const POLL_INTERVAL_NS = 10 * std.time.ns_per_ms;
const MAX_POLLS = 1000;

fn localReceiverConfig(hub: *Hub) ReceiverConfig {
    return .{
        .leader_address = .{ 127, 0, 0, 1 },
        .leader_port = hub.port(),
        .replica_name = "test-replica",
    };
}

fn waitForPhase(recv: *Receiver, phase: ReplicaPhase) !void {
    var polls: usize = 0;
    while (recv.status().phase != phase) : (polls += 1) {
        if (polls >= MAX_POLLS) return error.PhaseTimeout;
        std.Thread.sleep(POLL_INTERVAL_NS);
    }
}

fn waitForApplied(recv: *Receiver, lsn: u64) !void {
    var polls: usize = 0;
    while (recv.status().last_applied_lsn < lsn) : (polls += 1) {
        if (polls >= MAX_POLLS) return error.ApplyTimeout;
        std.Thread.sleep(POLL_INTERVAL_NS);
    }
}

test "leader streams committed entries to a connected follower" {
    const allocator = std.testing.allocator;
    var tmp_leader = std.testing.tmpDir(.{});
    defer tmp_leader.cleanup();
    var tmp_follower = std.testing.tmpDir(.{});
    defer tmp_follower.cleanup();

    const leader = try openStore(allocator, &tmp_leader);
    defer closeStore(leader);
    const hub = try Hub.start(allocator, try leader.primaryHost(), .{});
    defer hub.stop();

    const follower = try openStore(allocator, &tmp_follower);
    defer closeStore(follower);
    const recv = try Receiver.start(allocator, try follower.replicaHost(follower, applyReplicaEntry), localReceiverConfig(hub));
    defer recv.stop();

    const total: u64 = 50;
    var i: u64 = 1;
    while (i <= total) : (i += 1) {
        try commitTestRecord(leader, i, @intCast(i % 251));
    }

    try waitForApplied(recv, total);

    var value_buf: [16]u8 = undefined;
    i = 1;
    while (i <= total) : (i += 1) {
        const found = try follower.tree("by_id").search(&codec.encodeU64(i), &value_buf);
        try std.testing.expect(found != null);
        try std.testing.expectEqual(@as(u8, @intCast(i % 251)), found.?[0]);
    }

    var follower_stats: [4]FollowerStatus = undefined;
    var polls: usize = 0;
    while (true) : (polls += 1) {
        const hub_status = hub.status(&follower_stats);
        if (hub_status.follower_count == 1 and follower_stats[0].durable_ack >= total) break;
        if (polls >= MAX_POLLS) return error.AckTimeout;
        std.Thread.sleep(POLL_INTERVAL_NS);
    }
}

test "commit on a read-only replica fails with ReadOnlyReplica" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const store = try openStore(allocator, &tmp);
    defer closeStore(store);

    store.demote();
    try std.testing.expectError(error.ReadOnlyReplica, commitTestRecord(store, 1, 1));

    try store.promote();
    try commitTestRecord(store, 1, 1);
}

test "promotion: a caught-up follower accepts commits with a continuing LSN" {
    const allocator = std.testing.allocator;
    var tmp_leader = std.testing.tmpDir(.{});
    defer tmp_leader.cleanup();
    var tmp_follower = std.testing.tmpDir(.{});
    defer tmp_follower.cleanup();

    const leader = try openStore(allocator, &tmp_leader);
    defer closeStore(leader);
    const hub = try Hub.start(allocator, try leader.primaryHost(), .{});
    defer hub.stop();

    const follower = try openStore(allocator, &tmp_follower);
    defer closeStore(follower);
    const recv = try Receiver.start(allocator, try follower.replicaHost(follower, applyReplicaEntry), localReceiverConfig(hub));

    const total: u64 = 5;
    var i: u64 = 1;
    while (i <= total) : (i += 1) {
        try commitTestRecord(leader, i, @intCast(i));
    }
    waitForApplied(recv, total) catch |err| {
        recv.stop();
        return err;
    };

    recv.stop();
    try follower.promote();

    try commitTestRecord(follower, 100, 42);
    try std.testing.expectEqual(total + 1, follower.wal_writer.?.getSequence());
}

test "sync_standbys=1: commit blocks until a follower acks through the gate" {
    const allocator = std.testing.allocator;
    var tmp_leader = std.testing.tmpDir(.{});
    defer tmp_leader.cleanup();
    var tmp_follower = std.testing.tmpDir(.{});
    defer tmp_follower.cleanup();

    const leader = try openStore(allocator, &tmp_leader);
    defer closeStore(leader);
    const hub = try Hub.start(allocator, try leader.primaryHost(), .{
        .sync_standbys = 1,
        .commit_gate = leader.syncGate(),
    });
    defer hub.stop();
    leader.setCommitGate(leader.syncGate());
    defer leader.setCommitGate(null);

    const GatedCommit = struct {
        store: *TestStore,
        done: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
        failed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

        fn run(self: *@This()) void {
            commitTestRecord(self.store, 1, 1) catch {
                self.failed.store(true, .release);
            };
            self.done.store(true, .release);
        }
    };

    var gated = GatedCommit{ .store = leader };
    const committer = try std.Thread.spawn(.{}, GatedCommit.run, .{&gated});

    std.Thread.sleep(150 * std.time.ns_per_ms);
    try std.testing.expect(!gated.done.load(.acquire));

    const follower = try openStore(allocator, &tmp_follower);
    defer closeStore(follower);
    const recv = try Receiver.start(allocator, try follower.replicaHost(follower, applyReplicaEntry), localReceiverConfig(hub));
    defer recv.stop();

    var polls: usize = 0;
    while (!gated.done.load(.acquire)) : (polls += 1) {
        if (polls >= MAX_POLLS) {
            leader.syncGate().close();
            committer.join();
            return error.QuorumTimeout;
        }
        std.Thread.sleep(POLL_INTERVAL_NS);
    }
    committer.join();

    try std.testing.expect(!gated.failed.load(.acquire));
    try std.testing.expect(leader.syncGate().current() >= 1);
}

test "a follower ahead of the leader parks in the diverged phase" {
    const allocator = std.testing.allocator;
    var tmp_leader = std.testing.tmpDir(.{});
    defer tmp_leader.cleanup();
    var tmp_follower = std.testing.tmpDir(.{});
    defer tmp_follower.cleanup();

    const leader = try openStore(allocator, &tmp_leader);
    defer closeStore(leader);
    const hub = try Hub.start(allocator, try leader.primaryHost(), .{});
    defer hub.stop();

    const follower = try openStore(allocator, &tmp_follower);
    defer closeStore(follower);
    try commitTestRecord(follower, 1, 1);
    try commitTestRecord(follower, 2, 2);
    try commitTestRecord(follower, 3, 3);

    const recv = try Receiver.start(allocator, try follower.replicaHost(follower, applyReplicaEntry), localReceiverConfig(hub));
    defer recv.stop();

    try waitForPhase(recv, .diverged);
}

test "a follower behind a truncated WAL parks in needs_rebootstrap" {
    const allocator = std.testing.allocator;
    var tmp_leader = std.testing.tmpDir(.{});
    defer tmp_leader.cleanup();
    var tmp_follower = std.testing.tmpDir(.{});
    defer tmp_follower.cleanup();

    const leader = try openStore(allocator, &tmp_leader);
    defer closeStore(leader);

    try commitTestRecord(leader, 1, 1);
    try commitTestRecord(leader, 2, 2);
    try commitTestRecord(leader, 3, 3);
    _ = try snapshot.forceSnapshot(leader.snapshotHost());
    try leader.wal_writer.?.truncateAfterCheckpoint();

    const hub = try Hub.start(allocator, try leader.primaryHost(), .{});
    defer hub.stop();

    const follower = try openStore(allocator, &tmp_follower);
    defer closeStore(follower);
    const recv = try Receiver.start(allocator, try follower.replicaHost(follower, applyReplicaEntry), localReceiverConfig(hub));
    defer recv.stop();

    try waitForPhase(recv, .needs_rebootstrap);
}

test "retain floor blocks checkpoint truncation while a connected follower lags" {
    const allocator = std.testing.allocator;
    var tmp_leader = std.testing.tmpDir(.{});
    defer tmp_leader.cleanup();

    const leader = try openStore(allocator, &tmp_leader);
    defer closeStore(leader);
    const hub = try Hub.start(allocator, try leader.primaryHost(), .{});
    defer hub.stop();

    const address = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, hub.port());
    const conn = try std.net.tcpConnectToAddress(address);
    defer conn.close();

    var name: [REPLICA_NAME_LEN]u8 = .{0} ** REPLICA_NAME_LEN;
    @memcpy(name[0..4], "lazy");
    const req = HandshakeRequest{
        .magic = HANDSHAKE_MAGIC,
        .version = PROTOCOL_VERSION,
        .start_lsn = 0,
        .replica_name = name,
    };
    try writeFull(conn.handle, std.mem.asBytes(&req));

    var resp_buf: [@sizeOf(HandshakeResponse)]u8 = undefined;
    try readFull(conn.handle, &resp_buf);
    const resp = std.mem.bytesToValue(HandshakeResponse, &resp_buf);
    try std.testing.expectEqual(@intFromEnum(HandshakeStatus.accepted), resp.status);

    try commitTestRecord(leader, 1, 1);
    try commitTestRecord(leader, 2, 2);

    try std.testing.expectError(error.WalRetainedByReplica, leader.wal_writer.?.truncateAfterCheckpoint());
}

test "base backup bootstraps a fresh replica that then streams" {
    const allocator = std.testing.allocator;
    var tmp_leader = std.testing.tmpDir(.{});
    defer tmp_leader.cleanup();
    var tmp_follower = std.testing.tmpDir(.{});
    defer tmp_follower.cleanup();

    const leader = try openStore(allocator, &tmp_leader);
    defer closeStore(leader);
    const hub = try Hub.start(allocator, try leader.primaryHost(), .{});
    defer hub.stop();

    const base_total: u64 = 20;
    var i: u64 = 1;
    while (i <= base_total) : (i += 1) {
        try commitTestRecord(leader, i, @intCast(i));
    }

    const follower_dir = try tmp_follower.dir.realpathAlloc(allocator, ".");
    var follower_dir_owned = true;
    defer if (follower_dir_owned) allocator.free(follower_dir);

    const info = try fetchBaseBackup(allocator, localReceiverConfig(hub), follower_dir);
    try std.testing.expectEqual(base_total, info.wal_sequence);

    const follower = try TestStore.init(allocator, .{ .data_dir = follower_dir, .cache_size_mb = 16 });
    follower_dir_owned = false;
    defer closeStore(follower);

    try std.testing.expectEqual(base_total, follower.wal_writer.?.getSequence());

    var value_buf: [16]u8 = undefined;
    i = 1;
    while (i <= base_total) : (i += 1) {
        const found = try follower.tree("by_id").search(&codec.encodeU64(i), &value_buf);
        try std.testing.expect(found != null);
    }

    const recv = try Receiver.start(allocator, try follower.replicaHost(follower, applyReplicaEntry), localReceiverConfig(hub));
    defer recv.stop();

    const total: u64 = base_total + 5;
    i = base_total + 1;
    while (i <= total) : (i += 1) {
        try commitTestRecord(leader, i, @intCast(i));
    }

    try waitForApplied(recv, total);

    i = base_total + 1;
    while (i <= total) : (i += 1) {
        const found = try follower.tree("by_id").search(&codec.encodeU64(i), &value_buf);
        try std.testing.expect(found != null);
    }
}

test "base backup is refused by a hub without a snapshot host" {
    const allocator = std.testing.allocator;
    var tmp_leader = std.testing.tmpDir(.{});
    defer tmp_leader.cleanup();
    var tmp_follower = std.testing.tmpDir(.{});
    defer tmp_follower.cleanup();

    const leader = try openStore(allocator, &tmp_leader);
    defer closeStore(leader);

    var host = try leader.primaryHost();
    host.snapshot_host = null;
    const hub = try Hub.start(allocator, host, .{});
    defer hub.stop();

    const follower_dir = try tmp_follower.dir.realpathAlloc(allocator, ".");
    defer allocator.free(follower_dir);

    try std.testing.expectError(error.BackupRefused, fetchBaseBackup(allocator, localReceiverConfig(hub), follower_dir));
}

test "CommitGate advances monotonically, releases waiters, and fails closed" {
    var gate = CommitGate{};

    try std.testing.expectEqual(@as(u64, 0), gate.current());

    gate.advance(5);
    gate.advance(3);
    try std.testing.expectEqual(@as(u64, 5), gate.current());
    try gate.awaitQuorum(5);

    const Waiter = struct {
        gate: *CommitGate,
        target: u64,
        result: ?error{ReplicationStopped} = null,

        fn run(self: *@This()) void {
            self.gate.awaitQuorum(self.target) catch |err| {
                self.result = err;
                return;
            };
        }
    };

    var released = Waiter{ .gate = &gate, .target = 10 };
    const t1 = try std.Thread.spawn(.{}, Waiter.run, .{&released});
    gate.advance(10);
    t1.join();
    try std.testing.expectEqual(@as(?error{ReplicationStopped}, null), released.result);

    var stopped = Waiter{ .gate = &gate, .target = 20 };
    const t2 = try std.Thread.spawn(.{}, Waiter.run, .{&stopped});
    gate.close();
    t2.join();
    try std.testing.expectEqual(@as(?error{ReplicationStopped}, error.ReplicationStopped), stopped.result);

    try std.testing.expectError(error.ReplicationStopped, gate.awaitQuorum(11));
}

test "promote refuses while the receiver is streaming" {
    const allocator = std.testing.allocator;
    var tmp_leader = std.testing.tmpDir(.{});
    defer tmp_leader.cleanup();
    var tmp_follower = std.testing.tmpDir(.{});
    defer tmp_follower.cleanup();

    const leader = try openStore(allocator, &tmp_leader);
    defer closeStore(leader);
    const hub = try Hub.start(allocator, try leader.primaryHost(), .{});
    defer hub.stop();

    const follower = try openStore(allocator, &tmp_follower);
    defer closeStore(follower);
    const recv = try Receiver.start(allocator, try follower.replicaHost(follower, applyReplicaEntry), localReceiverConfig(hub));

    waitForPhase(recv, .streaming) catch |err| {
        recv.stop();
        return err;
    };

    try std.testing.expectError(error.ReplicaStillStreaming, follower.promote());

    recv.stop();
    try follower.promote();
    try commitTestRecord(follower, 1, 1);
}

test "commits against a stopped hub fail with ReplicationStopped on the store-owned gate" {
    const allocator = std.testing.allocator;
    var tmp_leader = std.testing.tmpDir(.{});
    defer tmp_leader.cleanup();

    const leader = try openStore(allocator, &tmp_leader);
    defer closeStore(leader);

    const hub = try Hub.start(allocator, try leader.primaryHost(), .{
        .sync_standbys = 1,
        .commit_gate = leader.syncGate(),
    });
    leader.setCommitGate(leader.syncGate());
    defer leader.setCommitGate(null);

    const GatedCommit = struct {
        store: *TestStore,
        result: std.atomic.Value(u8) = std.atomic.Value(u8).init(0),

        fn run(self: *@This()) void {
            commitTestRecord(self.store, 1, 1) catch |err| {
                self.result.store(if (err == error.ReplicationStopped) 1 else 2, .release);
                return;
            };
            self.result.store(3, .release);
        }
    };

    var gated = GatedCommit{ .store = leader };
    const committer = try std.Thread.spawn(.{}, GatedCommit.run, .{&gated});

    std.Thread.sleep(100 * std.time.ns_per_ms);
    try std.testing.expectEqual(@as(u8, 0), gated.result.load(.acquire));

    hub.stop();
    committer.join();
    try std.testing.expectEqual(@as(u8, 1), gated.result.load(.acquire));

    try std.testing.expectError(error.ReplicationStopped, commitTestRecord(leader, 2, 2));
}

test "a reaped follower does not block the next registration" {
    const allocator = std.testing.allocator;
    var tmp_leader = std.testing.tmpDir(.{});
    defer tmp_leader.cleanup();

    const leader = try openStore(allocator, &tmp_leader);
    defer closeStore(leader);
    const hub = try Hub.start(allocator, try leader.primaryHost(), .{});
    defer hub.stop();

    const address = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, hub.port());
    const req = HandshakeRequest{
        .magic = HANDSHAKE_MAGIC,
        .version = PROTOCOL_VERSION,
        .start_lsn = 0,
        .replica_name = try buildReplicaName("first"),
    };

    {
        const conn = try std.net.tcpConnectToAddress(address);
        try writeFull(conn.handle, std.mem.asBytes(&req));
        var resp_buf: [@sizeOf(HandshakeResponse)]u8 = undefined;
        try readFull(conn.handle, &resp_buf);
        conn.close();
    }

    var follower_stats: [4]FollowerStatus = undefined;
    var polls: usize = 0;
    while (hub.status(&follower_stats).follower_count != 0) : (polls += 1) {
        if (polls >= MAX_POLLS) return error.ReapTimeout;
        std.Thread.sleep(POLL_INTERVAL_NS);
    }

    const conn2 = try std.net.tcpConnectToAddress(address);
    defer conn2.close();
    try writeFull(conn2.handle, std.mem.asBytes(&req));
    var resp_buf: [@sizeOf(HandshakeResponse)]u8 = undefined;
    try readFull(conn2.handle, &resp_buf);
    const resp = std.mem.bytesToValue(HandshakeResponse, &resp_buf);
    try std.testing.expectEqual(@intFromEnum(HandshakeStatus.accepted), resp.status);
}
