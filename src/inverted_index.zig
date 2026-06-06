const std = @import("std");

pub const MIN_TOKEN_LEN: usize = 2;
pub const MAX_TOKEN_LEN: usize = 64;

pub const TokenIterator = struct {
    text: []const u8,
    pos: usize = 0,

    pub fn init(text: []const u8) TokenIterator {
        return .{ .text = text };
    }

    pub fn next(self: *TokenIterator, buf: *[MAX_TOKEN_LEN]u8) ?[]const u8 {
        while (self.pos < self.text.len) {
            while (self.pos < self.text.len and !isAlphaNum(self.text[self.pos])) : (self.pos += 1) {}
            if (self.pos >= self.text.len) return null;

            const start = self.pos;
            var out_len: usize = 0;
            while (self.pos < self.text.len and isAlphaNum(self.text[self.pos])) : (self.pos += 1) {
                if (out_len < MAX_TOKEN_LEN) {
                    buf[out_len] = toLower(self.text[self.pos]);
                    out_len += 1;
                }
            }

            if (self.pos - start < MIN_TOKEN_LEN) continue;
            return buf[0..out_len];
        }
        return null;
    }

    fn isAlphaNum(c: u8) bool {
        return (c >= 'a' and c <= 'z') or
            (c >= 'A' and c <= 'Z') or
            (c >= '0' and c <= '9');
    }

    fn toLower(c: u8) u8 {
        return if (c >= 'A' and c <= 'Z') c + 32 else c;
    }
};

test "TokenIterator basic split" {
    var it = TokenIterator.init("Hello World");
    var buf: [64]u8 = undefined;
    try std.testing.expectEqualSlices(u8, "hello", it.next(&buf).?);
    try std.testing.expectEqualSlices(u8, "world", it.next(&buf).?);
    try std.testing.expect(it.next(&buf) == null);
}

test "TokenIterator punctuation and url" {
    var it = TokenIterator.init("https://ziglang.org/learn?q=zig+lang");
    var buf: [64]u8 = undefined;
    try std.testing.expectEqualSlices(u8, "https", it.next(&buf).?);
    try std.testing.expectEqualSlices(u8, "ziglang", it.next(&buf).?);
    try std.testing.expectEqualSlices(u8, "org", it.next(&buf).?);
    try std.testing.expectEqualSlices(u8, "learn", it.next(&buf).?);
    try std.testing.expectEqualSlices(u8, "zig", it.next(&buf).?);
    try std.testing.expectEqualSlices(u8, "lang", it.next(&buf).?);
    try std.testing.expect(it.next(&buf) == null);
}

test "TokenIterator skips short tokens" {
    var it = TokenIterator.init("a bb ccc d ee");
    var buf: [64]u8 = undefined;
    try std.testing.expectEqualSlices(u8, "bb", it.next(&buf).?);
    try std.testing.expectEqualSlices(u8, "ccc", it.next(&buf).?);
    try std.testing.expectEqualSlices(u8, "ee", it.next(&buf).?);
    try std.testing.expect(it.next(&buf) == null);
}

test "TokenIterator empty and whitespace only" {
    var it1 = TokenIterator.init("");
    var buf: [64]u8 = undefined;
    try std.testing.expect(it1.next(&buf) == null);

    var it2 = TokenIterator.init("   \t\n  ");
    try std.testing.expect(it2.next(&buf) == null);
}

test "TokenIterator single character below MIN_TOKEN_LEN" {
    var it = TokenIterator.init("a");
    var buf: [64]u8 = undefined;
    try std.testing.expect(it.next(&buf) == null);
}

test "TokenIterator all-punctuation input yields nothing" {
    var it = TokenIterator.init("...///!!!&&&***");
    var buf: [64]u8 = undefined;
    try std.testing.expect(it.next(&buf) == null);
}

test "TokenIterator mixed unicode treats non-ASCII bytes as separators" {
    var it = TokenIterator.init("café réseau zigzag");
    var buf: [64]u8 = undefined;
    try std.testing.expectEqualSlices(u8, "caf", it.next(&buf).?);
    try std.testing.expectEqualSlices(u8, "seau", it.next(&buf).?);
    try std.testing.expectEqualSlices(u8, "zigzag", it.next(&buf).?);
    try std.testing.expect(it.next(&buf) == null);
}

test "TokenIterator truncates tokens longer than MAX_TOKEN_LEN" {
    var input_buf: [200]u8 = undefined;
    @memset(input_buf[0..120], 'x');
    input_buf[120] = ' ';
    @memset(input_buf[121..130], 'y');
    const text = input_buf[0..130];

    var it = TokenIterator.init(text);
    var buf: [MAX_TOKEN_LEN]u8 = undefined;

    const t1 = it.next(&buf).?;
    try std.testing.expectEqual(MAX_TOKEN_LEN, t1.len);
    for (t1) |c| try std.testing.expectEqual(@as(u8, 'x'), c);

    const t2 = it.next(&buf).?;
    try std.testing.expectEqualSlices(u8, "yyyyyyyyy", t2);

    try std.testing.expect(it.next(&buf) == null);
}

test "TokenIterator back-to-back single-separator tokens" {
    var it = TokenIterator.init("foo bar baz");
    var buf: [MAX_TOKEN_LEN]u8 = undefined;
    try std.testing.expectEqualSlices(u8, "foo", it.next(&buf).?);
    try std.testing.expectEqualSlices(u8, "bar", it.next(&buf).?);
    try std.testing.expectEqualSlices(u8, "baz", it.next(&buf).?);
    try std.testing.expect(it.next(&buf) == null);
}

test "TokenIterator idempotent on identical input" {
    const text = "Hello, world! https://example.com/path?q=zig";

    var seq_a: [16][]u8 = undefined;
    var len_a: usize = 0;
    var bufs_a: [16][MAX_TOKEN_LEN]u8 = undefined;
    {
        var it = TokenIterator.init(text);
        while (it.next(&bufs_a[len_a])) |tok| {
            seq_a[len_a] = bufs_a[len_a][0..tok.len];
            len_a += 1;
        }
    }

    var seq_b: [16][]u8 = undefined;
    var len_b: usize = 0;
    var bufs_b: [16][MAX_TOKEN_LEN]u8 = undefined;
    {
        var it = TokenIterator.init(text);
        while (it.next(&bufs_b[len_b])) |tok| {
            seq_b[len_b] = bufs_b[len_b][0..tok.len];
            len_b += 1;
        }
    }

    try std.testing.expectEqual(len_a, len_b);
    for (seq_a[0..len_a], seq_b[0..len_b]) |a, b| {
        try std.testing.expectEqualSlices(u8, a, b);
    }
}
