const std = @import("std");
const zigstore = @import("zigstore");
const codec = zigstore.codec;

const index_names = [_][:0]const u8{ "by_key0", "by_key1", "by_key2", "by_key3", "by_key4", "by_key5" };
const MAX_INDEXES = index_names.len;

const bench_schema = zigstore.schema(.{
    .magic = 0x42454E43,
    .format_version = 1,
    .indexes = .{
        .{ .name = "by_key0", .key = .bytes },
        .{ .name = "by_key1", .key = .bytes },
        .{ .name = "by_key2", .key = .bytes },
        .{ .name = "by_key3", .key = .bytes },
        .{ .name = "by_key4", .key = .bytes },
        .{ .name = "by_key5", .key = .bytes },
    },
    .memtable_indexes = &.{},
    .counters = &.{},
});

const Store = zigstore.Engine(bench_schema);
const Set = zigstore.ShardSet(Store);

var g_indexes_per_op: usize = 4;

const Record = struct {
    key: u64,
    val: u64,
};

fn serialize(allocator: std.mem.Allocator, rec: Record) anyerror![]u8 {
    const buf = try allocator.alloc(u8, 16);
    std.mem.writeInt(u64, buf[0..8], rec.key, .big);
    std.mem.writeInt(u64, buf[8..16], rec.val, .big);
    return buf;
}

fn apply(ctx: *anyopaque, rec: Record) anyerror!void {
    const store: *Store = @ptrCast(@alignCast(ctx));
    const vb = codec.encodeU64(rec.val);
    inline for (index_names, 0..) |name, i| {
        if (i < g_indexes_per_op) {
            const kb = codec.encodeU64(rec.key ^ (@as(u64, i) *% 0x9E3779B97F4A7C15));
            try store.tree(name).insert(&kb, &vb);
        }
    }
}

const Options = struct {
    ops: usize = 200_000,
    threads: usize = 8,
    trials: usize = 7,
    warmup: usize = 2,
    total_cache_mb: u32 = 256,
    wal_batch_size: u32 = 32,
    wal_direct_io: bool = true,
    seed: u64 = 0x5A49_4753_7368_6172,
    dir: []const u8 = "/tmp/zigstore-bench",
    shard_counts: []const usize = &.{ 1, 2, 4, 8 },
};

const SetWorker = struct {
    set: *Set,
    keys: []const u64,
    start: *std.atomic.Value(bool),
    errors: u64 = 0,

    fn run(self: *SetWorker) void {
        while (!self.start.load(.acquire)) std.atomic.spinLoopHint();
        for (self.keys) |k| {
            const kb = codec.encodeU64(k);
            self.set.commit(Record, &kb, 1, .{ .key = k, .val = k }, serialize, apply) catch {
                self.errors += 1;
            };
        }
    }
};

const StoreWorker = struct {
    store: *Store,
    keys: []const u64,
    start: *std.atomic.Value(bool),
    errors: u64 = 0,

    fn run(self: *StoreWorker) void {
        while (!self.start.load(.acquire)) std.atomic.spinLoopHint();
        for (self.keys) |k| {
            zigstore.commit(Record, self.store, 1, .{ .key = k, .val = k }, self.store, serialize, apply) catch {
                self.errors += 1;
            };
        }
    }
};

const TrialResult = struct {
    ops_per_sec: f64,
    fsyncs: u64,
    errors: u64,
    used_direct_io: bool,
};

const Stats = struct {
    min: f64,
    max: f64,
    mean: f64,
    median: f64,
    stddev: f64,
};

fn computeStats(samples: []f64) Stats {
    std.mem.sort(f64, samples, {}, std.sort.asc(f64));
    var sum: f64 = 0;
    for (samples) |s| sum += s;
    const mean = sum / @as(f64, @floatFromInt(samples.len));
    var var_sum: f64 = 0;
    for (samples) |s| var_sum += (s - mean) * (s - mean);
    const stddev = std.math.sqrt(var_sum / @as(f64, @floatFromInt(samples.len)));
    const mid = samples.len / 2;
    const median = if (samples.len % 2 == 1) samples[mid] else (samples[mid - 1] + samples[mid]) / 2.0;
    return .{ .min = samples[0], .max = samples[samples.len - 1], .mean = mean, .median = median, .stddev = stddev };
}

fn keyPartitions(keys: []const u64, threads: usize, out: [][]const u64) void {
    const chunk = keys.len / threads;
    var offset: usize = 0;
    for (0..threads) |i| {
        const end = if (i == threads - 1) keys.len else offset + chunk;
        out[i] = keys[offset..end];
        offset = end;
    }
}

fn shardLoad(allocator: std.mem.Allocator, keys: []const u64, shard_count: usize) ![]u64 {
    const counts = try allocator.alloc(u64, shard_count);
    @memset(counts, 0);
    for (keys) |k| {
        const kb = codec.encodeU64(k);
        const idx = std.hash.Wyhash.hash(0, &kb) % shard_count;
        counts[idx] += 1;
    }
    return counts;
}

fn runSetTrial(allocator: std.mem.Allocator, opts: Options, dir: []const u8, shard_count: usize, threads: usize, keys: []const u64) !TrialResult {
    const set = try Set.init(allocator, .{
        .data_dir = dir,
        .shard_count = shard_count,
        .cache_size_mb = opts.total_cache_mb,
        .wal_batch_size = opts.wal_batch_size,
        .wal_direct_io = opts.wal_direct_io,
    });
    defer set.deinit();

    const parts = try allocator.alloc([]const u64, threads);
    defer allocator.free(parts);
    keyPartitions(keys, threads, parts);

    const workers = try allocator.alloc(SetWorker, threads);
    defer allocator.free(workers);
    const handles = try allocator.alloc(std.Thread, threads);
    defer allocator.free(handles);

    var start = std.atomic.Value(bool).init(false);
    for (workers, 0..) |*w, i| {
        w.* = .{ .set = set, .keys = parts[i], .start = &start };
    }
    for (handles, 0..) |*h, i| {
        h.* = try std.Thread.spawn(.{}, SetWorker.run, .{&workers[i]});
    }

    var timer = try std.time.Timer.start();
    start.store(true, .release);
    for (handles) |h| h.join();
    const elapsed_ns = timer.read();

    var errors: u64 = 0;
    for (workers) |w| errors += w.errors;

    const ops_done: f64 = @floatFromInt(keys.len - errors);
    const secs = @as(f64, @floatFromInt(elapsed_ns)) / std.time.ns_per_s;

    return .{
        .ops_per_sec = ops_done / secs,
        .fsyncs = set.walFsyncCount(),
        .errors = errors,
        .used_direct_io = set.walUsingDirectIo(),
    };
}

fn runRawStoreTrial(allocator: std.mem.Allocator, opts: Options, dir: []const u8, threads: usize, keys: []const u64) !TrialResult {
    const store = try Store.init(allocator, .{
        .data_dir = dir,
        .cache_size_mb = opts.total_cache_mb,
        .wal_batch_size = opts.wal_batch_size,
        .wal_direct_io = opts.wal_direct_io,
    });
    defer store.deinit();

    const parts = try allocator.alloc([]const u64, threads);
    defer allocator.free(parts);
    keyPartitions(keys, threads, parts);

    const workers = try allocator.alloc(StoreWorker, threads);
    defer allocator.free(workers);
    const handles = try allocator.alloc(std.Thread, threads);
    defer allocator.free(handles);

    var start = std.atomic.Value(bool).init(false);
    for (workers, 0..) |*w, i| {
        w.* = .{ .store = store, .keys = parts[i], .start = &start };
    }
    for (handles, 0..) |*h, i| {
        h.* = try std.Thread.spawn(.{}, StoreWorker.run, .{&workers[i]});
    }

    var timer = try std.time.Timer.start();
    start.store(true, .release);
    for (handles) |h| h.join();
    const elapsed_ns = timer.read();

    var errors: u64 = 0;
    for (workers) |w| errors += w.errors;

    const ops_done: f64 = @floatFromInt(keys.len - errors);
    const secs = @as(f64, @floatFromInt(elapsed_ns)) / std.time.ns_per_s;

    return .{
        .ops_per_sec = ops_done / secs,
        .fsyncs = store.walFsyncCount(),
        .errors = errors,
        .used_direct_io = store.walUsingDirectIo(),
    };
}

const ConfigSummary = struct {
    shard_count: usize,
    threads: usize,
    stats: Stats,
    fsyncs_per_op: f64,
    used_direct_io: bool,
};

fn trialDir(buf: []u8, base: []const u8, tag: []const u8, shard_count: usize, trial: usize) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}/{s}-sc{d}-t{d}", .{ base, tag, shard_count, trial });
}

fn runSweep(
    allocator: std.mem.Allocator,
    opts: Options,
    keys: []const u64,
    tag: []const u8,
    threadsFor: *const fn (Options, usize) usize,
    summaries: *std.ArrayList(ConfigSummary),
) !void {
    var dir_buf: [512]u8 = undefined;

    for (opts.shard_counts) |sc| {
        const threads = threadsFor(opts, sc);
        const total = opts.warmup + opts.trials;

        const samples = try allocator.alloc(f64, opts.trials);
        defer allocator.free(samples);
        var measured: usize = 0;
        var fsyncs_accum: u64 = 0;
        var direct_io = false;

        for (0..total) |trial| {
            const dir = try trialDir(&dir_buf, opts.dir, tag, sc, trial);
            std.fs.cwd().deleteTree(dir) catch {};
            const r = try runSetTrial(allocator, opts, dir, sc, threads, keys);
            std.fs.cwd().deleteTree(dir) catch {};
            if (r.errors > 0) return error.BenchCommitErrors;

            if (trial >= opts.warmup) {
                samples[measured] = r.ops_per_sec;
                measured += 1;
                fsyncs_accum += r.fsyncs;
                direct_io = r.used_direct_io;
            }
        }

        const stats = computeStats(samples);
        const fsyncs_per_op = @as(f64, @floatFromInt(fsyncs_accum)) /
            (@as(f64, @floatFromInt(opts.trials)) * @as(f64, @floatFromInt(keys.len)));

        try summaries.append(allocator, .{
            .shard_count = sc,
            .threads = threads,
            .stats = stats,
            .fsyncs_per_op = fsyncs_per_op,
            .used_direct_io = direct_io,
        });

        std.debug.print(
            "  shards={d:<2} threads={d:<2}  median={d:>10.0} ops/s  mean={d:>10.0}  min={d:>10.0}  max={d:>10.0}  stddev={d:>8.0}  fsyncs/op={d:.4}\n",
            .{ sc, threads, stats.median, stats.mean, stats.min, stats.max, stats.stddev, fsyncs_per_op },
        );
    }
}

fn fixedThreads(opts: Options, sc: usize) usize {
    _ = sc;
    return opts.threads;
}

fn scaledThreads(opts: Options, sc: usize) usize {
    _ = opts;
    return sc;
}

fn parseUsize(s: []const u8) !usize {
    return std.fmt.parseInt(usize, s, 10);
}

pub fn main() !void {
    const allocator = std.heap.smp_allocator;

    var opts = Options{};
    var args = std.process.args();
    _ = args.next();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--ops")) {
            opts.ops = try parseUsize(args.next() orelse return error.MissingArg);
        } else if (std.mem.eql(u8, arg, "--threads")) {
            opts.threads = try parseUsize(args.next() orelse return error.MissingArg);
        } else if (std.mem.eql(u8, arg, "--trials")) {
            opts.trials = try parseUsize(args.next() orelse return error.MissingArg);
        } else if (std.mem.eql(u8, arg, "--warmup")) {
            opts.warmup = try parseUsize(args.next() orelse return error.MissingArg);
        } else if (std.mem.eql(u8, arg, "--total-cache-mb")) {
            opts.total_cache_mb = @intCast(try parseUsize(args.next() orelse return error.MissingArg));
        } else if (std.mem.eql(u8, arg, "--dir")) {
            opts.dir = args.next() orelse return error.MissingArg;
        } else if (std.mem.eql(u8, arg, "--indexes")) {
            const n = try parseUsize(args.next() orelse return error.MissingArg);
            if (n < 1 or n > MAX_INDEXES) return error.InvalidIndexCount;
            g_indexes_per_op = n;
        } else if (std.mem.eql(u8, arg, "--batch")) {
            opts.wal_batch_size = @intCast(try parseUsize(args.next() orelse return error.MissingArg));
        } else if (std.mem.eql(u8, arg, "--no-direct")) {
            opts.wal_direct_io = false;
        }
    }

    const cpus = std.Thread.getCpuCount() catch 1;

    std.fs.cwd().makePath(opts.dir) catch {};
    defer std.fs.cwd().deleteTree(opts.dir) catch {};

    const keys = try allocator.alloc(u64, opts.ops);
    defer allocator.free(keys);
    var prng = std.Random.DefaultPrng.init(opts.seed);
    const rng = prng.random();
    for (keys) |*k| k.* = rng.int(u64);

    var distinct = std.AutoHashMap(u64, void).init(allocator);
    defer distinct.deinit();
    for (keys) |k| try distinct.put(k, {});

    std.debug.print("=== zigstore multi-sharded write benchmark ===\n", .{});
    std.debug.print("cores={d}  ops/trial={d}  distinct keys={d}  trials={d} (+{d} warmup)  seed=0x{x}\n", .{
        cpus, opts.ops, distinct.count(), opts.trials, opts.warmup, opts.seed,
    });
    std.debug.print("workload: 1 durable WAL commit + {d} direct B+Tree insert(s) per op (no memtable; all index work in-window)\n", .{g_indexes_per_op});
    std.debug.print("total page cache held constant at {d} MiB across configs (per-shard = {d} MiB / shard_count)\n", .{ opts.total_cache_mb, opts.total_cache_mb });
    std.debug.print("wal_batch_size={d}  wal_direct_io requested={}\n\n", .{ opts.wal_batch_size, opts.wal_direct_io });

    std.debug.print("per-shard offered load (deterministic, same keyset every config):\n", .{});
    for (opts.shard_counts) |sc| {
        const counts = try shardLoad(allocator, keys, sc);
        defer allocator.free(counts);
        var lo: u64 = std.math.maxInt(u64);
        var hi: u64 = 0;
        for (counts) |c| {
            lo = @min(lo, c);
            hi = @max(hi, c);
        }
        const mean = @as(f64, @floatFromInt(keys.len)) / @as(f64, @floatFromInt(sc));
        const imbalance = @as(f64, @floatFromInt(hi)) / mean;
        std.debug.print("  shards={d:<2}  min={d}  max={d}  mean={d:.0}  max/mean={d:.3}\n", .{ sc, lo, hi, mean, imbalance });
    }

    std.debug.print("\n--- raw single Store baseline (no ShardSet wrapper), threads={d} ---\n", .{opts.threads});
    {
        var dir_buf: [512]u8 = undefined;
        const total = opts.warmup + opts.trials;
        const samples = try allocator.alloc(f64, opts.trials);
        defer allocator.free(samples);
        var measured: usize = 0;
        var direct_io = false;
        for (0..total) |trial| {
            const dir = try std.fmt.bufPrint(&dir_buf, "{s}/raw-t{d}", .{ opts.dir, trial });
            std.fs.cwd().deleteTree(dir) catch {};
            std.fs.cwd().makePath(dir) catch {};
            const r = try runRawStoreTrial(allocator, opts, dir, opts.threads, keys);
            std.fs.cwd().deleteTree(dir) catch {};
            if (r.errors > 0) return error.BenchCommitErrors;
            if (trial >= opts.warmup) {
                samples[measured] = r.ops_per_sec;
                measured += 1;
                direct_io = r.used_direct_io;
            }
        }
        const stats = computeStats(samples);
        std.debug.print("  raw Store     threads={d:<2}  median={d:>10.0} ops/s  mean={d:>10.0}  min={d:>10.0}  max={d:>10.0}  O_DIRECT={}\n", .{ opts.threads, stats.median, stats.mean, stats.min, stats.max, direct_io });
    }

    var fixed = std.ArrayList(ConfigSummary){};
    defer fixed.deinit(allocator);
    std.debug.print("\n--- sweep A: fixed offered load (threads={d} for every shard count) ---\n", .{opts.threads});
    try runSweep(allocator, opts, keys, "fixed", fixedThreads, &fixed);

    var scaled = std.ArrayList(ConfigSummary){};
    defer scaled.deinit(allocator);
    std.debug.print("\n--- sweep B: fixed per-shard concurrency (threads = shard_count, 1 writer/shard) ---\n", .{});
    try runSweep(allocator, opts, keys, "scaled", scaledThreads, &scaled);

    std.debug.print("\n=== speedup vs shards=1 (median ops/s; non-overlap = N-shard min exceeds 1-shard max) ===\n", .{});
    printSpeedup("sweep A (fixed load)", fixed.items);
    printSpeedup("sweep B (scaled load)", scaled.items);

    const direct = if (fixed.items.len > 0) fixed.items[0].used_direct_io else false;
    std.debug.print("\nNotes (honest interpretation):\n", .{});
    std.debug.print("  - WAL O_DIRECT engaged: {} — on this mount it likely fell back to BUFFERED + fdatasync.\n", .{direct});
    std.debug.print("    The durability cost being parallelized here is buffered fdatasync on this filesystem,\n", .{});
    std.debug.print("    NOT O_DIRECT on an OCI block volume; do not generalize the fsync-lane scaling to production.\n", .{});
    std.debug.print("  - fsyncs/op shows group-commit batch occupancy; compare it across shard counts to see whether\n", .{});
    std.debug.print("    a speedup came from lock parallelism vs a change in fdatasync amortization.\n", .{});
    std.debug.print("  - Sweep A holds offered load constant (same threads); the gain is in-memory serialization-domain\n", .{});
    std.debug.print("    parallelism (WAL append lock, apply mutex, per-tree RwLock, free-list mutex) plus N fsync lanes.\n", .{});
}

fn printSpeedup(title: []const u8, items: []const ConfigSummary) void {
    if (items.len == 0) return;
    const base_median = items[0].stats.median;
    const base_max = items[0].stats.max;
    std.debug.print("  {s}:\n", .{title});
    for (items) |c| {
        const speedup = c.stats.median / base_median;
        const clean = c.stats.min > base_max;
        std.debug.print("    shards={d:<2}  {d:.2}x   {s}\n", .{
            c.shard_count,
            speedup,
            if (c.shard_count == 1) "(baseline)" else if (clean) "(distributions do not overlap)" else "(ranges overlap — treat as within noise)",
        });
    }
}
