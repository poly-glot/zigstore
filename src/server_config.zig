const std = @import("std");

const log = std.log.scoped(.server_config);

const Cidr = struct { network: u32, prefix: u6 };

/// Generic server bootstrap configuration: bind address, thread/cache sizing,
/// durability cadence, and the trust-list that gates non-loopback connections.
///
/// Reads only the engine-neutral `ZIGSTORE_*` environment keys. Application
/// knobs (data-dir semantics, repair/subtree thresholds) stay in the app's own
/// config; this struct never sees them.
pub const ServerConfig = struct {
    port: u16 = 8080,
    bind_address: [4]u8 = .{ 127, 0, 0, 1 },
    cache_size_mb: u32 = 256,
    thread_count: u32 = 0,
    snapshot_interval_s: u32 = 300,
    wal_sync_interval_ms: u32 = 50,
    wal_batch_size: u32 = 256,
    trusted_ips: [MAX_TRUSTED_IPS][4]u8 = undefined,
    trusted_count: u8 = 0,
    trusted_cidrs: [MAX_TRUSTED_IPS]Cidr = undefined,
    trusted_cidr_count: u8 = 0,

    const MAX_TRUSTED_IPS = 16;

    /// Build a config from the environment, reading keys under `prefix` (e.g.
    /// `"ZIGSTORE_"` reads `ZIGSTORE_PORT`). Falls back to the struct defaults
    /// on absent or unparseable values; a zero thread count resolves to the
    /// host CPU count, capped at 32.
    pub fn fromEnv(comptime prefix: []const u8) ServerConfig {
        var config = ServerConfig{};
        if (std.posix.getenv(prefix ++ "PORT")) |v| {
            config.port = std.fmt.parseInt(u16, v, 10) catch 8080;
        }
        if (std.posix.getenv(prefix ++ "CACHE_SIZE_MB")) |v| {
            config.cache_size_mb = std.fmt.parseInt(u32, v, 10) catch 256;
        }
        if (std.posix.getenv(prefix ++ "THREAD_COUNT")) |v| {
            config.thread_count = std.fmt.parseInt(u32, v, 10) catch 0;
        }
        if (config.thread_count == 0) {
            const max_threads: u32 = 32;
            config.thread_count = @intCast(@min(std.Thread.getCpuCount() catch 8, max_threads));
        }
        if (std.posix.getenv(prefix ++ "SNAPSHOT_INTERVAL_S")) |v| {
            config.snapshot_interval_s = std.fmt.parseInt(u32, v, 10) catch 300;
        }
        if (std.posix.getenv(prefix ++ "WAL_SYNC_INTERVAL_MS")) |v| {
            config.wal_sync_interval_ms = std.fmt.parseInt(u32, v, 10) catch 50;
        }
        if (std.posix.getenv(prefix ++ "WAL_BATCH_SIZE")) |v| {
            config.wal_batch_size = std.fmt.parseInt(u32, v, 10) catch 256;
        }
        if (std.posix.getenv(prefix ++ "BIND")) |v| {
            config.bind_address = parseIpv4(v) orelse .{ 127, 0, 0, 1 };
        }
        if (std.posix.getenv(prefix ++ "TRUSTED")) |v| {
            config.parseTrustedIps(v);
        }
        return config;
    }

    fn parseTrustedIps(self: *ServerConfig, raw: []const u8) void {
        var ip_count: u8 = 0;
        var cidr_count: u8 = 0;
        var rest = raw;
        while (rest.len > 0) {
            var end: usize = 0;
            while (end < rest.len and rest[end] != ',') : (end += 1) {}
            const token = std.mem.trim(u8, rest[0..end], " \t");
            rest = if (end < rest.len) rest[end + 1 ..] else &.{};

            if (token.len == 0) continue;

            if (std.mem.indexOfScalar(u8, token, '/') != null) {
                if (cidr_count >= MAX_TRUSTED_IPS) {
                    log.warn("trusted-ip list: dropping CIDR '{s}' — exceeds MAX_TRUSTED_IPS={d}", .{ token, MAX_TRUSTED_IPS });
                    continue;
                }
                if (parseCidr(token)) |cidr| {
                    self.trusted_cidrs[cidr_count] = cidr;
                    cidr_count += 1;
                } else {
                    log.warn("trusted-ip list: ignoring unparseable CIDR token '{s}'", .{token});
                }
            } else {
                if (ip_count >= MAX_TRUSTED_IPS) {
                    log.warn("trusted-ip list: dropping '{s}' — exceeds MAX_TRUSTED_IPS={d}", .{ token, MAX_TRUSTED_IPS });
                    continue;
                }
                if (parseIpv4(token)) |ip| {
                    self.trusted_ips[ip_count] = ip;
                    ip_count += 1;
                } else {
                    log.warn("trusted-ip list: ignoring unparseable IPv4 token '{s}'", .{token});
                }
            }
        }
        self.trusted_count = ip_count;
        self.trusted_cidr_count = cidr_count;
    }

    /// True when `addr` is loopback, an exact trusted IP, or inside a trusted CIDR.
    pub fn isAllowed(self: *const ServerConfig, addr: [4]u8) bool {
        if (addr[0] == 127) return true;
        for (self.trusted_ips[0..self.trusted_count]) |trusted| {
            if (std.mem.eql(u8, &addr, &trusted)) return true;
        }
        const addr_u32 = octetsToU32(addr);
        for (self.trusted_cidrs[0..self.trusted_cidr_count]) |cidr| {
            if ((addr_u32 & maskU32(cidr.prefix)) == cidr.network) return true;
        }
        return false;
    }

    /// True when bound to a non-loopback address with no trust-list, so every
    /// non-loopback peer would be rejected.
    pub fn isProtectedMode(self: *const ServerConfig) bool {
        return self.bind_address[0] != 127 and self.trusted_count == 0 and self.trusted_cidr_count == 0;
    }
};

fn octetsToU32(a: [4]u8) u32 {
    return (@as(u32, a[0]) << 24) | (@as(u32, a[1]) << 16) | (@as(u32, a[2]) << 8) | @as(u32, a[3]);
}

fn maskU32(prefix: u6) u32 {
    if (prefix == 0) return 0;
    const shift: u5 = @intCast(32 - @as(u8, prefix));
    return @as(u32, 0xFFFFFFFF) << shift;
}

fn parseCidr(s: []const u8) ?Cidr {
    const slash = std.mem.indexOfScalar(u8, s, '/') orelse return null;
    const ip = parseIpv4(s[0..slash]) orelse return null;
    const prefix = std.fmt.parseInt(u6, s[slash + 1 ..], 10) catch return null;
    if (prefix > 32) return null;
    return Cidr{ .network = octetsToU32(ip) & maskU32(prefix), .prefix = prefix };
}

fn parseIpv4(s: []const u8) ?[4]u8 {
    var octets: [4]u8 = undefined;
    var idx: u8 = 0;
    var start: usize = 0;
    for (s, 0..) |c, i| {
        if (c == '.') {
            if (idx >= 3) return null;
            octets[idx] = std.fmt.parseInt(u8, s[start..i], 10) catch return null;
            idx += 1;
            start = i + 1;
        }
    }
    if (idx != 3) return null;
    octets[3] = std.fmt.parseInt(u8, s[start..], 10) catch return null;
    return octets;
}

test "parseIpv4 valid" {
    const ip = parseIpv4("192.168.1.10").?;
    try std.testing.expectEqual([4]u8{ 192, 168, 1, 10 }, ip);
}

test "parseIpv4 loopback" {
    const ip = parseIpv4("127.0.0.1").?;
    try std.testing.expectEqual([4]u8{ 127, 0, 0, 1 }, ip);
}

test "parseIpv4 invalid" {
    try std.testing.expect(parseIpv4("not.an.ip") == null);
    try std.testing.expect(parseIpv4("1.2.3") == null);
    try std.testing.expect(parseIpv4("1.2.3.4.5") == null);
    try std.testing.expect(parseIpv4("256.0.0.1") == null);
    try std.testing.expect(parseIpv4("") == null);
}

test "ServerConfig.isAllowed loopback always permitted" {
    const config = ServerConfig{};
    try std.testing.expect(config.isAllowed(.{ 127, 0, 0, 1 }));
    try std.testing.expect(config.isAllowed(.{ 127, 0, 0, 2 }));
    try std.testing.expect(config.isAllowed(.{ 127, 255, 0, 1 }));
}

test "ServerConfig.isAllowed rejects non-loopback by default" {
    const config = ServerConfig{};
    try std.testing.expect(!config.isAllowed(.{ 192, 168, 1, 1 }));
    try std.testing.expect(!config.isAllowed(.{ 10, 0, 0, 1 }));
}

test "ServerConfig.isAllowed with trusted IPs" {
    var config = ServerConfig{};
    config.trusted_ips[0] = .{ 10, 0, 0, 5 };
    config.trusted_ips[1] = .{ 192, 168, 1, 100 };
    config.trusted_count = 2;
    try std.testing.expect(config.isAllowed(.{ 10, 0, 0, 5 }));
    try std.testing.expect(config.isAllowed(.{ 192, 168, 1, 100 }));
    try std.testing.expect(!config.isAllowed(.{ 10, 0, 0, 6 }));
    try std.testing.expect(config.isAllowed(.{ 127, 0, 0, 1 }));
}

test "ServerConfig.isProtectedMode" {
    const default_config = ServerConfig{};
    try std.testing.expect(!default_config.isProtectedMode());

    var wildcard = ServerConfig{};
    wildcard.bind_address = .{ 0, 0, 0, 0 };
    try std.testing.expect(wildcard.isProtectedMode());

    var configured = ServerConfig{};
    configured.bind_address = .{ 0, 0, 0, 0 };
    configured.trusted_ips[0] = .{ 10, 0, 0, 5 };
    configured.trusted_count = 1;
    try std.testing.expect(!configured.isProtectedMode());
}

test "ServerConfig.parseTrustedIps" {
    var config = ServerConfig{};
    config.parseTrustedIps("10.0.0.1,192.168.1.50, 172.16.0.1");
    try std.testing.expectEqual(@as(u8, 3), config.trusted_count);
    try std.testing.expectEqual([4]u8{ 10, 0, 0, 1 }, config.trusted_ips[0]);
    try std.testing.expectEqual([4]u8{ 192, 168, 1, 50 }, config.trusted_ips[1]);
    try std.testing.expectEqual([4]u8{ 172, 16, 0, 1 }, config.trusted_ips[2]);
}

test "ServerConfig.parseTrustedIps separates CIDRs from exact IPs" {
    var config = ServerConfig{};
    config.parseTrustedIps("10.244.0.0/16, 10.0.1.0/24, 192.168.1.5");
    try std.testing.expectEqual(@as(u8, 1), config.trusted_count);
    try std.testing.expectEqual(@as(u8, 2), config.trusted_cidr_count);
    try std.testing.expectEqual([4]u8{ 192, 168, 1, 5 }, config.trusted_ips[0]);
}

test "isAllowed accepts an address inside a trusted CIDR" {
    var config = ServerConfig{};
    config.parseTrustedIps("10.244.0.0/16,10.0.1.0/24");
    try std.testing.expect(config.isAllowed(.{ 10, 244, 2, 14 }));
    try std.testing.expect(config.isAllowed(.{ 10, 244, 1, 200 }));
    try std.testing.expect(config.isAllowed(.{ 10, 0, 1, 53 }));
}

test "isAllowed rejects an address outside every trusted CIDR" {
    var config = ServerConfig{};
    config.parseTrustedIps("10.244.0.0/16,10.0.1.0/24");
    try std.testing.expect(!config.isAllowed(.{ 10, 245, 0, 1 }));
    try std.testing.expect(!config.isAllowed(.{ 10, 0, 2, 1 }));
    try std.testing.expect(!config.isAllowed(.{ 192, 168, 1, 1 }));
}

test "isProtectedMode is false when only a CIDR is trusted" {
    var config = ServerConfig{};
    config.bind_address = .{ 0, 0, 0, 0 };
    config.parseTrustedIps("10.244.0.0/16");
    try std.testing.expect(!config.isProtectedMode());
}

test "parseCidr rejects malformed tokens" {
    try std.testing.expect(parseCidr("10.0.0.0/33") == null);
    try std.testing.expect(parseCidr("10.0.0.0/abc") == null);
    try std.testing.expect(parseCidr("not-an-ip/24") == null);
}
