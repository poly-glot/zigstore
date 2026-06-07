const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;
const connection = @import("connection.zig");
const signal = @import("signal.zig");
const server_config = @import("server_config.zig");

const ServerConfig = server_config.ServerConfig;

const log = std.log.scoped(.epoll);

pub const MAX_CONNECTIONS = 4096;
const MAX_EVENTS = 64;

pub const BUFFER_POOL_SIZE: u16 = 256;

const CONNECTION_TIMEOUT_S: i64 = 30;

/// The application-supplied protocol hooks the reactor drives per connection.
///
///   - `process_frames`: decode the connection's buffered request bytes and
///     write framed responses, setting `conn.response_len`. The `ctx` is the
///     opaque application context handed to the reactor at construction.
///   - `header_size`: the minimum number of buffered bytes before a request
///     frame's length is known; the reactor calls `process_frames` only once
///     this many bytes have arrived.
///   - `on_shutdown` (optional): invoked once with `ctx` when the primary
///     reactor's event loop exits, for the application to flush durable state.
pub const Handler = struct {
    process_frames: *const fn (ctx: *anyopaque, conn: *connection.Connection) void,
    header_size: usize,
    on_shutdown: ?*const fn (ctx: *anyopaque) void = null,
};

pub const EpollServer = struct {
    allocator: std.mem.Allocator,
    ctx: *anyopaque,
    handler: Handler,
    config: ServerConfig,
    epoll_fd: posix.fd_t,
    listen_fd: posix.fd_t,
    connections: [MAX_CONNECTIONS]connection.Connection,

    fd_to_slot: std.AutoHashMap(posix.fd_t, u16),

    free_slot_stack: []u16,
    free_slot_count: u32,

    buffer_pairs: []connection.BufferPair,
    free_stack: []u16,
    free_count: u32,
    buf_pool_size: u32,

    binary_write_fds: [MAX_CONNECTIONS]posix.fd_t = undefined,
    binary_write_count: u32 = 0,

    const Self = @This();

    /// Construct `count` reactors sharing `ctx` and `handler`; only the first
    /// registers the shutdown pipe with its epoll instance.
    pub fn createMulti(allocator: std.mem.Allocator, ctx: *anyopaque, handler: Handler, config: ServerConfig, count: u32) ![]*Self {
        const reactors = try allocator.alloc(*Self, count);
        errdefer allocator.free(reactors);
        for (reactors, 0..) |*rp, i| {
            rp.* = try createOne(allocator, ctx, handler, config, i == 0);
        }
        return reactors;
    }

    /// Construct a single reactor that owns the shutdown-pipe registration.
    pub fn create(allocator: std.mem.Allocator, ctx: *anyopaque, handler: Handler, config: ServerConfig) !*Self {
        return createOne(allocator, ctx, handler, config, true);
    }

    fn createOne(allocator: std.mem.Allocator, ctx: *anyopaque, handler: Handler, config: ServerConfig, register_shutdown: bool) !*Self {
        const listen_fd = try posix.socket(
            posix.AF.INET,
            @as(u32, posix.SOCK.STREAM | posix.SOCK.NONBLOCK | posix.SOCK.CLOEXEC),
            0,
        );
        errdefer posix.close(listen_fd);

        try posix.setsockopt(listen_fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
        try posix.setsockopt(listen_fd, posix.SOL.SOCKET, posix.SO.REUSEPORT, &std.mem.toBytes(@as(c_int, 1)));

        const addr = std.net.Address.initIp4(config.bind_address, config.port);
        try posix.bind(listen_fd, &addr.any, addr.getOsSockLen());

        try posix.listen(listen_fd, 4096);

        const epoll_fd = try posix.epoll_create1(linux.EPOLL.CLOEXEC);
        errdefer posix.close(epoll_fd);

        var listen_event = linux.epoll_event{
            .events = linux.EPOLL.IN,
            .data = .{ .fd = listen_fd },
        };
        try posix.epoll_ctl(epoll_fd, linux.EPOLL.CTL_ADD, listen_fd, &listen_event);

        if (register_shutdown) {
            const shutdown_fd = signal.getShutdownPipeFd();
            if (shutdown_fd != -1) {
                var shutdown_event = linux.epoll_event{
                    .events = linux.EPOLL.IN,
                    .data = .{ .fd = shutdown_fd },
                };
                try posix.epoll_ctl(epoll_fd, linux.EPOLL.CTL_ADD, shutdown_fd, &shutdown_event);
            }
        }

        const buf_pool_size: u32 = @max(BUFFER_POOL_SIZE, config.thread_count * 64);
        const buffer_pairs = try allocator.alloc(connection.BufferPair, buf_pool_size);
        errdefer allocator.free(buffer_pairs);
        const buf_free_stack = try allocator.alloc(u16, buf_pool_size);
        errdefer allocator.free(buf_free_stack);

        const slot_stack = try allocator.alloc(u16, MAX_CONNECTIONS);
        errdefer allocator.free(slot_stack);

        const server = try allocator.create(Self);
        errdefer allocator.destroy(server);

        server.allocator = allocator;
        server.ctx = ctx;
        server.handler = handler;
        server.config = config;
        server.epoll_fd = epoll_fd;
        server.listen_fd = listen_fd;
        server.buffer_pairs = buffer_pairs;
        server.free_stack = buf_free_stack;
        server.buf_pool_size = buf_pool_size;
        server.fd_to_slot = std.AutoHashMap(posix.fd_t, u16).init(allocator);
        server.free_slot_stack = slot_stack;
        server.binary_write_count = 0;

        for (&server.connections) |*c| {
            c.* = connection.Connection{};
        }

        server.free_slot_count = MAX_CONNECTIONS;
        for (0..MAX_CONNECTIONS) |i| {
            server.free_slot_stack[i] = @intCast(i);
        }

        server.free_count = buf_pool_size;
        for (0..buf_pool_size) |i| {
            server.free_stack[i] = @intCast(i);
        }

        log.info("Listening on {d}.{d}.{d}.{d}:{d} (epoll fd={d}, listen fd={d}, buffer pool={d})", .{
            config.bind_address[0], config.bind_address[1], config.bind_address[2], config.bind_address[3],
            config.port,            epoll_fd,               listen_fd,              buf_pool_size,
        });

        if (config.isProtectedMode()) {
            log.warn("PROTECTED MODE: listening on non-loopback address with no trusted IPs configured. " ++
                "Non-loopback connections will be rejected. Set ZIGSTORE_TRUSTED to allow specific IPs.", .{});
        }

        return server;
    }

    pub fn destroy(self: *Self) void {
        for (&self.connections) |*c| {
            if (c.isActive() and c.fd != -1) {
                posix.close(c.fd);
                if (c.buf) |bp| self.releaseBuffer(bp);
                c.reset();
            }
        }

        posix.close(self.epoll_fd);
        posix.close(self.listen_fd);
        signal.closeShutdownPipe();

        self.fd_to_slot.deinit();
        const allocator = self.allocator;
        allocator.free(self.free_slot_stack);
        allocator.free(self.free_stack);
        allocator.free(self.buffer_pairs);
        allocator.destroy(self);
    }

    /// Run the event loop until shutdown is signalled. On exit the primary
    /// reactor invokes the handler's `on_shutdown` hook, if any.
    pub fn run(self: *Self) !void {
        var events: [MAX_EVENTS]linux.epoll_event = undefined;
        const shutdown_fd = signal.getShutdownPipeFd();

        log.info("Entering event loop", .{});

        while (!signal.shutdownRequested()) {
            const n = posix.epoll_wait(self.epoll_fd, &events, 1000);

            self.binary_write_count = 0;
            var got_shutdown = false;

            for (events[0..n]) |ev| {
                const fd = ev.data.fd;

                if (fd == self.listen_fd) {
                    self.acceptConnection();
                } else if (fd == shutdown_fd) {
                    log.info("Shutdown signal received via pipe", .{});
                    got_shutdown = true;
                    break;
                } else {
                    self.handleEvent(fd, ev.events);
                }
            }
            if (got_shutdown) break;

            self.flushBinaryWrites();

            self.sweepIdleConnections();
        }

        log.info("Event loop exiting", .{});
        if (self.handler.on_shutdown) |on_shutdown| {
            on_shutdown(self.ctx);
        }
    }

    fn acceptConnection(self: *Self) void {
        while (true) {
            var peer_addr: posix.sockaddr = undefined;
            var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr);
            const client_fd = posix.accept(self.listen_fd, &peer_addr, &addr_len, @as(u32, posix.SOCK.NONBLOCK | posix.SOCK.CLOEXEC)) catch |err| {
                if (err == error.WouldBlock) return;
                log.warn("Accept failed: {}", .{err});
                return;
            };

            if (peer_addr.family != posix.AF.INET) {
                log.warn("Rejected connection: unexpected address family {d}", .{peer_addr.family});
                posix.close(client_fd);
                continue;
            }
            const in_addr: *const posix.sockaddr.in = @ptrCast(@alignCast(&peer_addr));
            const octets: [4]u8 = @bitCast(in_addr.addr);
            if (!self.config.isAllowed(octets)) {
                log.warn("Protected mode: rejected connection from {d}.{d}.{d}.{d}", .{
                    octets[0], octets[1], octets[2], octets[3],
                });
                posix.close(client_fd);
                continue;
            }

            const free = self.findFreeSlot() orelse {
                if (self.evictIdleConnection()) |_| {
                    const retry_free = self.findFreeSlot() orelse {
                        posix.close(client_fd);
                        continue;
                    };
                    self.setupConnection(client_fd, retry_free.conn, retry_free.idx);
                    continue;
                }
                log.warn("No free connection slots, rejecting fd={d}", .{client_fd});
                posix.close(client_fd);
                continue;
            };

            self.setupConnection(client_fd, free.conn, free.idx);
        }
    }

    fn setupConnection(self: *Self, client_fd: posix.fd_t, slot: *connection.Connection, slot_idx: u16) void {
        posix.setsockopt(client_fd, posix.IPPROTO.TCP, std.os.linux.TCP.NODELAY, &std.mem.toBytes(@as(c_int, 1))) catch {};

        const bp = self.acquireBuffer() orelse {
            log.warn("Buffer pool exhausted, rejecting fd={d} (pool={d})", .{ client_fd, self.buf_pool_size });
            posix.close(client_fd);
            return;
        };

        slot.fd = client_fd;
        slot.phase = .reading_request;
        slot.buf = bp;
        slot.bytes_read = 0;
        slot.last_activity = std.time.timestamp();

        self.fd_to_slot.put(client_fd, slot_idx) catch {
            log.warn("fd_to_slot map full for fd={d}", .{client_fd});
            posix.close(client_fd);
            self.releaseBuffer(bp);
            slot.reset();
            return;
        };

        var ev = linux.epoll_event{
            .events = linux.EPOLL.IN | linux.EPOLL.ET | linux.EPOLL.HUP | linux.EPOLL.ERR,
            .data = .{ .fd = client_fd },
        };
        posix.epoll_ctl(self.epoll_fd, linux.EPOLL.CTL_ADD, client_fd, &ev) catch {
            log.warn("epoll_ctl ADD failed for fd={d}", .{client_fd});
            posix.close(client_fd);
            _ = self.fd_to_slot.remove(client_fd);
            self.releaseBuffer(bp);
            slot.reset();
        };
    }

    fn handleEvent(self: *Self, fd: posix.fd_t, events: u32) void {
        const conn = self.findConnection(fd) orelse {
            posix.epoll_ctl(self.epoll_fd, linux.EPOLL.CTL_DEL, fd, null) catch {};
            posix.close(fd);
            return;
        };

        if (events & (linux.EPOLL.ERR | linux.EPOLL.HUP) != 0) {
            self.closeConnection(fd);
            return;
        }

        if (events & linux.EPOLL.IN != 0 and conn.phase == .reading_request) {
            conn.last_activity = std.time.timestamp();
            if (self.drainAndProcess(fd, conn)) return;
            if (conn.response_len > 0) {
                conn.bytes_written = 0;
                conn.phase = .writing_response;
                self.binary_write_fds[self.binary_write_count] = fd;
                self.binary_write_count += 1;
            }
        }

        if (events & linux.EPOLL.OUT != 0 and conn.phase == .writing_response) {
            self.finishBinaryWrite(fd, conn);
        }
    }

    fn flushBinaryWrites(self: *Self) void {
        for (self.binary_write_fds[0..self.binary_write_count]) |fd| {
            const conn = self.findConnection(fd) orelse continue;
            if (conn.phase != .writing_response) continue;
            self.finishBinaryWrite(fd, conn);
        }
        self.binary_write_count = 0;
    }

    fn drainAndProcess(self: *Self, fd: posix.fd_t, conn: *connection.Connection) bool {
        const bp = conn.buf orelse {
            self.closeConnection(fd);
            return true;
        };
        while (true) {
            const remaining = bp.request_buf[conn.bytes_read..];
            if (remaining.len == 0) break;
            const n = posix.read(fd, remaining) catch |err| {
                if (err == error.WouldBlock) break;
                self.closeConnection(fd);
                return true;
            };
            if (n == 0) {
                self.closeConnection(fd);
                return true;
            }
            conn.bytes_read += n;
            if (conn.response_len == 0 and conn.bytes_read >= self.handler.header_size) {
                self.handler.process_frames(self.ctx, conn);
            }
        }
        if (conn.response_len == 0 and conn.bytes_read >= self.handler.header_size) {
            self.handler.process_frames(self.ctx, conn);
        }
        if (conn.response_len == 0 and conn.bytes_read >= bp.request_buf.len) {
            log.warn("Request frame exceeds buffer for fd={d}, closing", .{fd});
            self.closeConnection(fd);
            return true;
        }
        return false;
    }

    fn finishBinaryWrite(self: *Self, fd: posix.fd_t, conn: *connection.Connection) void {
        while (true) {
            var done = false;
            while (!done) {
                done = conn.writeChunk() catch {
                    self.closeConnection(fd);
                    return;
                };
                if (!done) {
                    self.armForWrite(conn);
                    return;
                }
            }

            conn.bytes_written = 0;
            conn.response_len = 0;
            conn.phase = .reading_request;
            conn.last_activity = std.time.timestamp();

            if (self.drainAndProcess(fd, conn)) return;
            if (conn.response_len > 0) {
                conn.bytes_written = 0;
                conn.phase = .writing_response;
                continue;
            }

            if (conn.armed_for_write) {
                self.armForRead(conn);
                conn.armed_for_write = false;
            }
            return;
        }
    }

    fn closeConnection(self: *Self, fd: posix.fd_t) void {
        posix.epoll_ctl(self.epoll_fd, linux.EPOLL.CTL_DEL, fd, null) catch {};

        if (self.fd_to_slot.get(fd)) |slot_idx| {
            const conn = &self.connections[slot_idx];
            if (conn.buf) |bp| self.releaseBuffer(bp);
            conn.reset();
            self.returnSlot(slot_idx);
        }

        _ = self.fd_to_slot.remove(fd);
        posix.close(fd);
    }

    fn armForWrite(self: *Self, conn: *connection.Connection) void {
        var ev = linux.epoll_event{
            .events = linux.EPOLL.OUT | linux.EPOLL.ET | linux.EPOLL.HUP | linux.EPOLL.ERR,
            .data = .{ .fd = conn.fd },
        };
        conn.armed_for_write = true;
        posix.epoll_ctl(self.epoll_fd, linux.EPOLL.CTL_MOD, conn.fd, &ev) catch {
            self.closeConnection(conn.fd);
        };
    }

    fn armForRead(self: *Self, conn: *connection.Connection) void {
        var ev = linux.epoll_event{
            .events = linux.EPOLL.IN | linux.EPOLL.ET | linux.EPOLL.HUP | linux.EPOLL.ERR,
            .data = .{ .fd = conn.fd },
        };
        posix.epoll_ctl(self.epoll_fd, linux.EPOLL.CTL_MOD, conn.fd, &ev) catch {
            self.closeConnection(conn.fd);
        };
    }

    fn sweepIdleConnections(self: *Self) void {
        const now = std.time.timestamp();
        for (&self.connections) |*c| {
            if (!c.isActive() or c.fd == -1) continue;
            if (c.phase != .reading_request) continue;
            if (now - c.last_activity > CONNECTION_TIMEOUT_S) {
                self.closeConnection(c.fd);
            }
        }
    }

    fn evictIdleConnection(self: *Self) ?u16 {
        var oldest_time: i64 = std.math.maxInt(i64);
        var oldest_fd: posix.fd_t = -1;

        for (&self.connections) |*c| {
            if (c.phase == .reading_request and c.last_activity < oldest_time) {
                oldest_time = c.last_activity;
                oldest_fd = c.fd;
            }
        }

        if (oldest_fd != -1) {
            const slot_idx = self.fd_to_slot.get(oldest_fd);
            self.closeConnection(oldest_fd);
            return slot_idx;
        }
        return null;
    }

    fn findConnection(self: *Self, fd: posix.fd_t) ?*connection.Connection {
        const slot_idx = self.fd_to_slot.get(fd) orelse return null;
        const conn = &self.connections[slot_idx];
        if (conn.isActive()) return conn;
        return null;
    }

    fn findFreeSlot(self: *Self) ?struct { conn: *connection.Connection, idx: u16 } {
        if (self.free_slot_count == 0) return null;
        self.free_slot_count -= 1;
        const idx = self.free_slot_stack[self.free_slot_count];
        return .{ .conn = &self.connections[idx], .idx = idx };
    }

    fn returnSlot(self: *Self, idx: u16) void {
        self.free_slot_stack[self.free_slot_count] = idx;
        self.free_slot_count += 1;
    }

    fn acquireBuffer(self: *Self) ?*connection.BufferPair {
        if (self.free_count == 0) return null;
        self.free_count -= 1;
        const idx = self.free_stack[self.free_count];
        return &self.buffer_pairs[idx];
    }

    fn releaseBuffer(self: *Self, pair: *connection.BufferPair) void {
        const base = @intFromPtr(self.buffer_pairs.ptr);
        const addr = @intFromPtr(pair);
        const idx: u32 = @intCast((addr - base) / @sizeOf(connection.BufferPair));
        if (idx >= self.buf_pool_size) return;

        @memset(std.mem.asBytes(pair), 0);

        self.free_stack[self.free_count] = @intCast(idx);
        self.free_count += 1;
    }
};

test "EpollServer MAX_CONNECTIONS is reasonable" {
    try std.testing.expect(MAX_CONNECTIONS >= 1024);
    try std.testing.expect(MAX_CONNECTIONS <= 65536);
}

test "Buffer pool size fits in u16" {
    try std.testing.expect(BUFFER_POOL_SIZE <= std.math.maxInt(u16));
}

test "Connection timeout constants are reasonable" {
    try std.testing.expect(CONNECTION_TIMEOUT_S > 0);
    try std.testing.expect(CONNECTION_TIMEOUT_S <= 300);
}
