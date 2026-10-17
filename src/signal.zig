const std = @import("std");
const linux = std.os.linux;
const sys = @import("sys.zig");

var shutdown_flag: std.atomic.Value(bool) = std.atomic.Value(bool).init(false);

var shutdown_pipe: [2]sys.fd_t = .{ -1, -1 };

pub fn setupSignalHandlers() !void {
    shutdown_pipe = try sys.pipe2(.{
        .CLOEXEC = true,
        .NONBLOCK = true,
    });

    const sa = linux.Sigaction{
        .handler = .{ .handler = handleSignal },
        .mask = linux.sigemptyset(),
        .flags = linux.SA.RESTART,
    };

    _ = linux.sigaction(.INT, &sa, null);
    _ = linux.sigaction(.TERM, &sa, null);
}

fn handleSignal(_: linux.SIG) callconv(.c) void {
    shutdown_flag.store(true, .release);
    const write_fd = shutdown_pipe[1];
    if (write_fd != -1) {
        _ = sys.write(write_fd, &[_]u8{1}) catch {};
    }
}

pub fn shutdownRequested() bool {
    return shutdown_flag.load(.acquire);
}

pub fn getShutdownPipeFd() sys.fd_t {
    return shutdown_pipe[0];
}

pub fn closeShutdownPipe() void {
    if (shutdown_pipe[0] != -1) {
        sys.close(shutdown_pipe[0]);
        shutdown_pipe[0] = -1;
    }
    if (shutdown_pipe[1] != -1) {
        sys.close(shutdown_pipe[1]);
        shutdown_pipe[1] = -1;
    }
}

test "shutdownRequested default is false" {
    try std.testing.expect(!shutdownRequested());
}
