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
