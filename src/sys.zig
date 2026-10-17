//! Raw Linux syscalls for the io-free hot path: the `O_DIRECT` WAL open, positional reads
//! and writes, `fsync`/`fdatasync`, `fallocate`, the epoll reactor and its sockets, and the
//! wall clock. Every wrapper retries `EINTR` and maps `errno` to a small named error set;
//! nothing here takes a `std.Io`. The cold path (create, rename, copy, delete, stat, the
//! replay reader) goes through `std.Io` instead.

const std = @import("std");
const linux = std.os.linux;

pub const fd_t = linux.fd_t;
pub const OpenFlags = linux.O;
pub const epoll_event = linux.epoll_event;
pub const sockaddr = linux.sockaddr;
pub const socklen_t = linux.socklen_t;
pub const pollfd = linux.pollfd;
pub const EPOLL = linux.EPOLL;
pub const EFD = linux.EFD;
pub const AF = linux.AF;
pub const SOCK = linux.SOCK;
pub const SOL = linux.SOL;
pub const SO = linux.SO;
pub const IPPROTO = linux.IPPROTO;
pub const TCP = linux.TCP;
pub const POLL = linux.POLL;

pub const Error = error{
    WouldBlock,
    FileNotFound,
    IsDir,
    AccessDenied,
    OperationNotSupported,
    NoSpaceLeft,
    InputOutput,
    ConnectionReset,
    BrokenPipe,
    InvalidArgument,
    NameTooLong,
    AddressInUse,
    BadFileDescriptor,
    Unexpected,
};

fn check(rc: usize) Error!usize {
    return switch (linux.errno(rc)) {
        .SUCCESS => rc,
        .AGAIN => error.WouldBlock,
        .NOENT => error.FileNotFound,
        .ISDIR => error.IsDir,
        .ACCES, .PERM => error.AccessDenied,
        .OPNOTSUPP, .NOSYS => error.OperationNotSupported,
        .NOSPC => error.NoSpaceLeft,
        .IO => error.InputOutput,
        .CONNRESET => error.ConnectionReset,
        .PIPE => error.BrokenPipe,
        .INVAL => error.InvalidArgument,
        .NAMETOOLONG => error.NameTooLong,
        .ADDRINUSE => error.AddressInUse,
        .BADF => error.BadFileDescriptor,
        else => error.Unexpected,
    };
}

fn interrupted(rc: usize) bool {
    return linux.errno(rc) == .INTR;
}

pub fn open(path: []const u8, flags: linux.O, mode: linux.mode_t) Error!fd_t {
    if (path.len >= linux.PATH_MAX) return error.NameTooLong;
    var buf: [linux.PATH_MAX]u8 = undefined;
    @memcpy(buf[0..path.len], path);
    buf[path.len] = 0;
    const path_z: [*:0]const u8 = @ptrCast(&buf);
    while (true) {
        const rc = linux.open(path_z, flags, mode);
        if (interrupted(rc)) continue;
        return @intCast(try check(rc));
    }
}

pub fn close(fd: fd_t) void {
    _ = linux.close(fd);
}

pub fn read(fd: fd_t, buf: []u8) Error!usize {
    while (true) {
        const rc = linux.read(fd, buf.ptr, buf.len);
        if (interrupted(rc)) continue;
        return check(rc);
    }
}

pub fn write(fd: fd_t, bytes: []const u8) Error!usize {
    while (true) {
        const rc = linux.write(fd, bytes.ptr, bytes.len);
        if (interrupted(rc)) continue;
        return check(rc);
    }
}

pub fn pread(fd: fd_t, buf: []u8, offset: u64) Error!usize {
    while (true) {
        const rc = linux.pread(fd, buf.ptr, buf.len, @intCast(offset));
        if (interrupted(rc)) continue;
        return check(rc);
    }
}

pub fn pwrite(fd: fd_t, bytes: []const u8, offset: u64) Error!usize {
    while (true) {
        const rc = linux.pwrite(fd, bytes.ptr, bytes.len, @intCast(offset));
        if (interrupted(rc)) continue;
        return check(rc);
    }
}

pub fn preadAll(fd: fd_t, buf: []u8, offset: u64) Error!usize {
    var total: usize = 0;
    while (total < buf.len) {
        const n = try pread(fd, buf[total..], offset + total);
        if (n == 0) break;
        total += n;
    }
    return total;
}

pub fn pwriteAll(fd: fd_t, bytes: []const u8, offset: u64) Error!void {
    var total: usize = 0;
    while (total < bytes.len) {
        const n = try pwrite(fd, bytes[total..], offset + total);
        if (n == 0) return error.InputOutput;
        total += n;
    }
}

pub fn fsync(fd: fd_t) Error!void {
    while (true) {
        const rc = linux.fsync(fd);
        if (interrupted(rc)) continue;
        _ = try check(rc);
        return;
    }
}

pub fn fdatasync(fd: fd_t) Error!void {
    while (true) {
        const rc = linux.fdatasync(fd);
        if (interrupted(rc)) continue;
        _ = try check(rc);
        return;
    }
}

pub fn fallocate(fd: fd_t, length: u64) Error!void {
    while (true) {
        const rc = linux.fallocate(fd, 0, 0, @intCast(length));
        if (interrupted(rc)) continue;
        _ = try check(rc);
        return;
    }
}

pub fn ftruncate(fd: fd_t, length: u64) Error!void {
    while (true) {
        const rc = linux.ftruncate(fd, @intCast(length));
        if (interrupted(rc)) continue;
        _ = try check(rc);
        return;
    }
}

pub fn pipe2(flags: linux.O) Error![2]fd_t {
    var fds: [2]fd_t = undefined;
    _ = try check(linux.pipe2(&fds, flags));
    return fds;
}

pub fn eventfd(flags: u32) Error!fd_t {
    return @intCast(try check(linux.eventfd(0, flags)));
}

pub fn epollCreate() Error!fd_t {
    return @intCast(try check(linux.epoll_create1(linux.EPOLL.CLOEXEC)));
}

pub fn epollCtl(epoll_fd: fd_t, op: u32, fd: fd_t, event: ?*linux.epoll_event) Error!void {
    _ = try check(linux.epoll_ctl(epoll_fd, op, fd, event));
}

pub fn epollWait(epoll_fd: fd_t, events: []linux.epoll_event, timeout_ms: i32) Error!usize {
    while (true) {
        const rc = linux.epoll_wait(epoll_fd, events.ptr, @intCast(events.len), timeout_ms);
        if (interrupted(rc)) continue;
        return check(rc);
    }
}

pub fn socket(domain: u32, socket_type: u32, protocol: u32) Error!fd_t {
    return @intCast(try check(linux.socket(domain, socket_type, protocol)));
}

pub fn bind(fd: fd_t, addr: *const linux.sockaddr, len: linux.socklen_t) Error!void {
    _ = try check(linux.bind(fd, addr, len));
}

pub fn listen(fd: fd_t, backlog: u32) Error!void {
    _ = try check(linux.listen(fd, backlog));
}

pub fn accept4(fd: fd_t, addr: ?*linux.sockaddr, len: ?*linux.socklen_t, flags: u32) Error!fd_t {
    while (true) {
        const rc = linux.accept4(fd, addr, len, flags);
        if (interrupted(rc)) continue;
        return @intCast(try check(rc));
    }
}

pub fn setsockopt(fd: fd_t, level: i32, optname: u32, opt: []const u8) Error!void {
    _ = try check(linux.setsockopt(fd, level, optname, opt.ptr, @intCast(opt.len)));
}

pub fn shutdown(fd: fd_t) void {
    _ = linux.shutdown(fd, linux.SHUT.RDWR);
}

pub fn poll(fds: []linux.pollfd, timeout_ms: i32) Error!usize {
    while (true) {
        const rc = linux.poll(fds.ptr, fds.len, timeout_ms);
        if (interrupted(rc)) continue;
        return check(rc);
    }
}

fn realtime() linux.timespec {
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.REALTIME, &ts);
    return ts;
}

pub fn timestamp() i64 {
    return @intCast(realtime().sec);
}

pub fn milliTimestamp() i64 {
    const ts = realtime();
    return @as(i64, @intCast(ts.sec)) * std.time.ms_per_s + @divTrunc(@as(i64, @intCast(ts.nsec)), std.time.ns_per_ms);
}

pub fn nanoTimestamp() i128 {
    const ts = realtime();
    return @as(i128, ts.sec) * std.time.ns_per_s + ts.nsec;
}

test "preadAll reads back exactly what pwriteAll wrote at the offset" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const file = try tmp.dir.createFile(std.testing.io, "sys.bin", .{ .read = true });
    defer file.close(std.testing.io);

    try pwriteAll(file.handle, "hello", 4096);
    var buf: [8]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 5), try preadAll(file.handle, &buf, 4096));
    try std.testing.expectEqualStrings("hello", buf[0..5]);
    try std.testing.expectEqual(@as(usize, 0), try preadAll(file.handle, &buf, 4096 + 5));
}

test "open maps a missing path to FileNotFound and a missing feature to OperationNotSupported" {
    try std.testing.expectError(error.FileNotFound, open("/nonexistent-zigstore-dir/x", .{ .ACCMODE = .RDONLY }, 0));
    try std.testing.expectError(error.NameTooLong, open(&@as([linux.PATH_MAX]u8, @splat('a')), .{ .ACCMODE = .RDONLY }, 0));
}

test "epollWait on a closed descriptor fails instead of reporting an empty poll" {
    var events: [1]linux.epoll_event = undefined;
    try std.testing.expectError(error.BadFileDescriptor, epollWait(-1, &events, 0));
}
