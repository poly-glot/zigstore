//! A condition variable with a timed wait over the `std.Io` futex. Zig 0.16's
//! `Io.Condition` has no timed wait; the WAL flusher, the background worker, and the quorum
//! gate need one, and on 0.17 this collapses into `Io.Condition.waitTimeout`.

const std = @import("std");
const Io = std.Io;

pub const TimedCondition = struct {
    epoch: std.atomic.Value(u32) = .init(0),

    pub fn wait(self: *TimedCondition, io: Io, mutex: *Io.Mutex) void {
        const seen = self.epoch.load(.acquire);
        mutex.unlock(io);
        defer mutex.lockUncancelable(io);
        io.futexWaitUncancelable(u32, &self.epoch.raw, seen);
    }

    pub fn timedWait(self: *TimedCondition, io: Io, mutex: *Io.Mutex, timeout_ns: u64) void {
        const seen = self.epoch.load(.acquire);
        mutex.unlock(io);
        defer mutex.lockUncancelable(io);
        const timeout: Io.Timeout = .{ .duration = .{ .raw = .fromNanoseconds(@intCast(timeout_ns)), .clock = .awake } };
        io.futexWaitTimeout(u32, &self.epoch.raw, seen, timeout) catch {};
    }

    pub fn signal(self: *TimedCondition, io: Io) void {
        _ = self.epoch.fetchAdd(1, .release);
        io.futexWake(u32, &self.epoch.raw, 1);
    }

    pub fn broadcast(self: *TimedCondition, io: Io) void {
        _ = self.epoch.fetchAdd(1, .release);
        io.futexWake(u32, &self.epoch.raw, std.math.maxInt(u32));
    }
};

test "timedWait returns on timeout and wait returns on broadcast" {
    const io = std.testing.io;
    var mutex: Io.Mutex = .init;
    var cond: TimedCondition = .{};

    const started = Io.Clock.awake.now(io);
    mutex.lockUncancelable(io);
    cond.timedWait(io, &mutex, 5 * std.time.ns_per_ms);
    mutex.unlock(io);
    try std.testing.expect(started.untilNow(io, .awake).toNanoseconds() >= 4 * std.time.ns_per_ms);

    const Waker = struct {
        fn run(c: *TimedCondition, m: *Io.Mutex, flag: *bool) void {
            m.lockUncancelable(std.testing.io);
            flag.* = true;
            m.unlock(std.testing.io);
            c.broadcast(std.testing.io);
        }
    };
    var flag = false;
    mutex.lockUncancelable(io);
    const t = try std.Thread.spawn(.{}, Waker.run, .{ &cond, &mutex, &flag });
    while (!flag) cond.wait(io, &mutex);
    mutex.unlock(io);
    t.join();
}
