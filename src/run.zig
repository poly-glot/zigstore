const std = @import("std");
const epoll = @import("epoll.zig");
const signal = @import("signal.zig");
const server_config = @import("server_config.zig");

const ServerConfig = server_config.ServerConfig;

const log = std.log.scoped(.run);

fn runReactor(reactor: *epoll.EpollServer) void {
    reactor.run() catch |err| {
        log.err("Reactor error: {}", .{err});
    };
}

/// Optional lifecycle hooks for `run`, letting a consumer wire the reactor set into the store's
/// deferred-response doorbell. `on_reactors_up` fires once, after the reactors exist and before any
/// runs, to register a durability notifier that rings them. `on_reactors_down` fires once, after all
/// reactors have stopped and BEFORE they are freed, to clear that notifier — so a late WAL-flusher
/// or hub fire can never ring a freed reactor.
pub const RunHooks = struct {
    on_reactors_up: ?*const fn (ctx: *anyopaque, reactors: []*epoll.EpollServer) void = null,
    on_reactors_down: ?*const fn (ctx: *anyopaque) void = null,
};

/// Generic server bootstrap: install signal handlers, spawn
/// `max(config.thread_count / 2, 1)` epoll reactors over the opaque `ctx` and
/// `handler`, run the primary reactor on the calling thread, and join the rest
/// on shutdown. The `Store` type is threaded through for the caller's benefit;
/// the reactor itself drives only the runtime `handler`.
pub fn run(
    comptime Store: type,
    ctx: *anyopaque,
    handler: epoll.Handler,
    config: ServerConfig,
    hooks: RunHooks,
) !void {
    _ = Store;
    const allocator = std.heap.page_allocator;

    signal.setupSignalHandlers() catch |err| {
        log.err("Failed to setup signal handlers: {}", .{err});
        return err;
    };

    const num_reactors: u32 = @max(config.thread_count / 2, 1);
    log.info("Starting {d} reactor(s)...", .{num_reactors});

    const reactors = try epoll.EpollServer.createMulti(allocator, ctx, handler, config, num_reactors);
    defer {
        for (reactors) |r| r.destroy();
        allocator.free(reactors);
    }

    if (hooks.on_reactors_up) |up| up(ctx, reactors);
    defer if (hooks.on_reactors_down) |down| down(ctx);

    var reactor_threads = try allocator.alloc(std.Thread, num_reactors - 1);
    defer allocator.free(reactor_threads);

    for (reactors[1..], 0..) |r, i| {
        reactor_threads[i] = std.Thread.spawn(.{}, runReactor, .{r}) catch |err| {
            log.err("Failed to spawn reactor thread: {}", .{err});
            return err;
        };
    }

    reactors[0].run() catch |err| {
        log.err("Primary reactor error: {}", .{err});
        return err;
    };

    for (reactor_threads) |t| t.join();

    log.info("Shutdown complete.", .{});
}
