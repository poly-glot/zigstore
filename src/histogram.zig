const std = @import("std");

const NUM_OCTAVES: usize = 32;
const SUB_BUCKETS: usize = 64;

pub const AtomicHistogram = struct {
    buckets: [NUM_OCTAVES][SUB_BUCKETS]std.atomic.Value(u64) =
        @splat(@splat(std.atomic.Value(u64).init(0))),
    total_count: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    sum: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    max_recorded: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    pub fn recordValue(self: *AtomicHistogram, ns: u64) void {
        _ = self.total_count.fetchAdd(1, .monotonic);
        _ = self.sum.fetchAdd(ns, .monotonic);

        while (true) {
            const cur = self.max_recorded.load(.monotonic);
            if (ns <= cur) break;
            if (self.max_recorded.cmpxchgWeak(cur, ns, .monotonic, .monotonic) == null) break;
        }

        const o: usize = if (ns == 0) 0 else blk: {
            const octave = @as(usize, @intCast(63 - @clz(ns)));
            break :blk @min(octave, NUM_OCTAVES - 1);
        };
        const sub: usize = if (ns == 0) 0 else blk: {
            const base: u64 = @as(u64, 1) << @intCast(o);
            const offset = ns - base;
            break :blk @min(@as(usize, @intCast((offset * SUB_BUCKETS) / base)), SUB_BUCKETS - 1);
        };
        _ = self.buckets[o][sub].fetchAdd(1, .monotonic);
    }

    pub fn percentile(self: *const AtomicHistogram, pct: f64) u64 {
        const total = self.total_count.load(.monotonic);
        if (total == 0) return 0;
        const target = @as(u64, @intFromFloat(@as(f64, @floatFromInt(total)) * pct / 100.0));
        var cum: u64 = 0;
        var o: usize = 0;
        while (o < NUM_OCTAVES) : (o += 1) {
            var s: usize = 0;
            while (s < SUB_BUCKETS) : (s += 1) {
                cum += self.buckets[o][s].load(.monotonic);
                if (cum >= target) {
                    const base: u64 = if (o == 0) 0 else @as(u64, 1) << @intCast(o);
                    const span: u64 = if (o == 0) SUB_BUCKETS else @as(u64, 1) << @intCast(o);
                    return base + (@as(u64, s) * span) / SUB_BUCKETS;
                }
            }
        }
        return self.max_recorded.load(.monotonic);
    }

    pub fn samples(self: *const AtomicHistogram) u64 {
        return self.total_count.load(.monotonic);
    }

    pub fn maxValue(self: *const AtomicHistogram) u64 {
        return self.max_recorded.load(.monotonic);
    }

    pub fn mean(self: *const AtomicHistogram) u64 {
        const total = self.total_count.load(.monotonic);
        if (total == 0) return 0;
        return self.sum.load(.monotonic) / total;
    }
};

test "AtomicHistogram: percentile resolution within 2% of true value" {
    var h: AtomicHistogram = .{};
    var i: u64 = 0;
    while (i < 1000) : (i += 1) {
        h.recordValue(1_000 + i * 1_000);
    }
    const p50 = h.percentile(50.0);
    try std.testing.expect(p50 > 490_000 and p50 < 510_000);
    const p99 = h.percentile(99.0);
    try std.testing.expect(p99 > 970_000 and p99 < 1_010_000);
}

test "AtomicHistogram: zero samples yields zero percentiles" {
    const h: AtomicHistogram = .{};
    try std.testing.expectEqual(@as(u64, 0), h.percentile(50));
    try std.testing.expectEqual(@as(u64, 0), h.percentile(99));
    try std.testing.expectEqual(@as(u64, 0), h.mean());
    try std.testing.expectEqual(@as(u64, 0), h.samples());
}

test "AtomicHistogram: concurrent recordValue from multiple threads" {
    var h: AtomicHistogram = .{};

    const Worker = struct {
        fn run(hist: *AtomicHistogram, base: u64) void {
            var i: u64 = 0;
            while (i < 1000) : (i += 1) {
                hist.recordValue(base + i);
            }
        }
    };

    var t1 = try std.Thread.spawn(.{}, Worker.run, .{ &h, @as(u64, 1_000) });
    var t2 = try std.Thread.spawn(.{}, Worker.run, .{ &h, @as(u64, 2_000) });
    var t3 = try std.Thread.spawn(.{}, Worker.run, .{ &h, @as(u64, 3_000) });
    t1.join();
    t2.join();
    t3.join();

    try std.testing.expectEqual(@as(u64, 3_000), h.samples());
    try std.testing.expect(h.maxValue() >= 3_999);
}
