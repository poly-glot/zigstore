const std = @import("std");
const posix = std.posix;

var shutdown_flag: std.atomic.Value(bool) = std.atomic.Value(bool).init(false);

var shutdown_pipe: [2]posix.fd_t = .{ -1, -1 };

pub fn setupSignalHandlers() !void {
    shutdown_pipe = try posix.pipe2(.{
        .CLOEXEC = true,
        .NONBLOCK = true,
    });

    const sa = posix.Sigaction{
        .handler = .{ .handler = handleSignal },
        .mask = posix.sigemptyset(),
        .flags = std.os.linux.SA.RESTART,
    };

    posix.sigaction(posix.SIG.INT, &sa, null);
    posix.sigaction(posix.SIG.TERM, &sa, null);
}

fn handleSignal(_: c_int) callconv(.c) void {
    shutdown_flag.store(true, .release);
    const write_fd = shutdown_pipe[1];
    if (write_fd != -1) {
        _ = posix.write(write_fd, &[_]u8{1}) catch {};
    }
}

pub fn shutdownRequested() bool {
    return shutdown_flag.load(.acquire);
}

pub fn getShutdownPipeFd() posix.fd_t {
    return shutdown_pipe[0];
}

pub fn closeShutdownPipe() void {
    if (shutdown_pipe[0] != -1) {
        posix.close(shutdown_pipe[0]);
        shutdown_pipe[0] = -1;
    }
    if (shutdown_pipe[1] != -1) {
        posix.close(shutdown_pipe[1]);
        shutdown_pipe[1] = -1;
    }
}

test "shutdownRequested default is false" {
    try std.testing.expect(!shutdownRequested());
}
