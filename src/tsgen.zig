//! Reflection-driven TypeScript-client emitters. Each emitter walks `@typeInfo`
//! of an `extern struct` or enum and writes idiomatic TypeScript to a
//! `*std.Io.Writer`. The per-field TypeScript-type and decode-expression
//! decisions are delegated to a caller-supplied comptime `FieldTable`, so this
//! module names no application record, field, or enum.
//!
//! A `FieldTable` is any type exposing two comptime functions:
//!
//!   pub fn tsType(comptime T: type, comptime field_name: []const u8) []const u8
//!   pub fn reader(comptime T: type, comptime field_name: []const u8) []const u8
//!
//! `tsType` returns the TypeScript type for a field; `reader` returns the
//! TypeScript expression that decodes one field from a `BufferReader`. The
//! structural emission — interface braces, reader scaffolding, alignment-gap and
//! padding skips driven by `@offsetOf`/`@sizeOf`, op/status enum bodies — is
//! application-neutral and lives here.

const std = @import("std");

/// Emit `export const enum {name} { Pascal = value, ... }` over an enum type,
/// mapping each `snake_case` field name to `PascalCase`.
pub fn writeOpEnum(w: *std.Io.Writer, comptime name: []const u8, comptime E: type) !void {
    try w.print("export const enum {s} {{\n", .{name});
    inline for (@typeInfo(E).@"enum".fields) |f| {
        const pascal = comptime snakeToPascal(f.name);
        try w.print("    {s} = {d},\n", .{ pascal, f.value });
    }
    try w.writeAll("}\n\n");
}

/// Emit `export const enum {name} { Pascal = value, ... }` over a status enum.
/// Identical structure to `writeOpEnum`; named separately so an app reads its
/// status emission at the call site.
pub fn writeStatusEnum(w: *std.Io.Writer, comptime name: []const u8, comptime E: type) !void {
    try writeOpEnum(w, name, E);
}

/// Classification a `KindTable` assigns each op for the routed client: `read`
/// ops may be served by a replica, `write` ops must reach the leader.
pub const OpKind = enum { read, write };

/// Emit `export const {name}: Record<number, "read" | "write"> = { ... }` over
/// an op enum. The per-op classification comes from a caller-supplied comptime
/// `KindTable` exposing:
///
///   pub fn kind(comptime E: type, comptime op_name: []const u8) OpKind
///
/// so this module names no application op. Feed the result to the router class
/// `writeReadWriteRouter` emits.
pub fn writeOpKindMap(
    w: *std.Io.Writer,
    comptime KindTable: type,
    comptime name: []const u8,
    comptime E: type,
) !void {
    try w.print("export const {s}: Record<number, \"read\" | \"write\"> = {{\n", .{name});
    inline for (@typeInfo(E).@"enum".fields) |f| {
        const kind = comptime KindTable.kind(E, f.name);
        try w.print("    {d}: \"{s}\",\n", .{ f.value, @tagName(kind) });
    }
    try w.writeAll("};\n\n");
}

/// Emit the application-neutral routed client `{name}`: a class over two
/// `Transport`s (leader and replica) that routes each op by its kind map.
/// With `readYourWrites` enabled, reads fall back to the leader until the
/// consumer reports (via `noteReplicaAppliedLsn`, fed from the replica's
/// status/health op) that the replica has applied the session's last write LSN
/// (recorded via `noteWriteLsn`). Unknown ops route to the leader.
pub fn writeReadWriteRouter(w: *std.Io.Writer, comptime name: []const u8) !void {
    try w.writeAll("export type Transport = (op: number, payload: Uint8Array) => Promise<Uint8Array>;\n\n");
    try w.print("export class {s} {{\n", .{name});
    try w.writeAll(
        \\    private lastWriteLsn = 0n;
        \\    private replicaAppliedLsn = 0n;
        \\
        \\    constructor(
        \\        private readonly leader: Transport,
        \\        private readonly replica: Transport,
        \\        private readonly opKind: Record<number, "read" | "write">,
        \\        private readonly readYourWrites: boolean = false,
        \\    ) {}
        \\
        \\    noteWriteLsn(lsn: bigint): void {
        \\        if (lsn > this.lastWriteLsn) this.lastWriteLsn = lsn;
        \\    }
        \\
        \\    noteReplicaAppliedLsn(lsn: bigint): void {
        \\        if (lsn > this.replicaAppliedLsn) this.replicaAppliedLsn = lsn;
        \\    }
        \\
        \\    send(op: number, payload: Uint8Array): Promise<Uint8Array> {
        \\        if (this.opKind[op] !== "read") return this.leader(op, payload);
        \\        if (this.readYourWrites && this.replicaAppliedLsn < this.lastWriteLsn) return this.leader(op, payload);
        \\        return this.replica(op, payload);
        \\    }
        \\}
        \\
        \\
    );
}

/// Emit `export const {name}: Record<number, string> = { value: "human", ... }`
/// over an enum, mapping each `snake_case` field name to a space-separated
/// human-readable string.
pub fn writeStatusMap(w: *std.Io.Writer, comptime name: []const u8, comptime E: type) !void {
    try w.print("export const {s}: Record<number, string> = {{\n", .{name});
    inline for (@typeInfo(E).@"enum".fields) |f| {
        const human = comptime snakeToHuman(f.name);
        try w.print("    {d}: \"{s}\",\n", .{ f.value, human });
    }
    try w.writeAll("};\n\n");
}

/// Emit `export interface {name} { camelField: tsType; ... }` over an
/// `extern struct`. Fields whose name begins with `_pad` are skipped. The
/// TypeScript type of each field comes from `FieldTable.tsType`.
pub fn writeStructInterface(
    w: *std.Io.Writer,
    comptime FieldTable: type,
    comptime name: []const u8,
    comptime T: type,
) !void {
    try w.print("export interface {s} {{\n", .{name});
    inline for (@typeInfo(T).@"struct".fields) |f| {
        if (comptime std.mem.startsWith(u8, f.name, "_pad")) continue;
        const camel = comptime fieldNameToCamel(f.name);
        const ts_type = comptime FieldTable.tsType(f.type, f.name);
        try w.print("    {s}: {s};\n", .{ camel, ts_type });
    }
    try w.writeAll("}\n\n");
}

/// Emit `export function read{name}(r: BufferReader): {name} { ... }` over an
/// `extern struct`. Alignment gaps and `_pad` fields are skipped via
/// `r.skip(n)` driven by `@offsetOf`/`@sizeOf`; each real field's decode
/// expression comes from `FieldTable.reader`.
pub fn writeStructReader(
    w: *std.Io.Writer,
    comptime FieldTable: type,
    comptime name: []const u8,
    comptime T: type,
) !void {
    try w.print("export function read{s}(r: BufferReader): {s} {{\n", .{ name, name });
    var cursor: usize = 0;
    inline for (@typeInfo(T).@"struct".fields) |f| {
        const target = @offsetOf(T, f.name);
        if (target > cursor) {
            try w.print("    r.skip({d}); // alignment gap before {s}\n", .{ target - cursor, f.name });
            cursor = target;
        }
        if (comptime std.mem.startsWith(u8, f.name, "_pad")) {
            try w.print("    r.skip({d}); // {s}\n", .{ @sizeOf(f.type), f.name });
            cursor += @sizeOf(f.type);
            continue;
        }
        const reader = comptime FieldTable.reader(f.type, f.name);
        try w.print("    const {s} = {s};\n", .{ fieldNameToCamel(f.name), reader });
        cursor += @sizeOf(f.type);
    }
    if (cursor < @sizeOf(T)) {
        try w.print("    r.skip({d}); // tail padding to @sizeOf({s})\n", .{ @sizeOf(T) - cursor, name });
    }
    try w.writeAll("    return {\n");
    inline for (@typeInfo(T).@"struct".fields) |f| {
        if (comptime std.mem.startsWith(u8, f.name, "_pad")) continue;
        const camel = comptime fieldNameToCamel(f.name);
        try w.print("        {s},\n", .{camel});
    }
    try w.writeAll("    };\n");
    try w.writeAll("}\n\n");
}

/// Convert `snake_case` to `PascalCase`.
pub fn snakeToPascal(comptime s: []const u8) []const u8 {
    @setEvalBranchQuota(10000);
    const result = comptime blk: {
        var out: [s.len]u8 = undefined;
        var oi: usize = 0;
        var capitalise_next = true;
        for (s) |c| {
            if (c == '_') {
                capitalise_next = true;
                continue;
            }
            if (capitalise_next and c >= 'a' and c <= 'z') {
                out[oi] = c - ('a' - 'A');
            } else {
                out[oi] = c;
            }
            oi += 1;
            capitalise_next = false;
        }
        const f: [oi]u8 = out[0..oi].*;
        break :blk f;
    };
    return &result;
}

/// Convert `snake_case` to `camelCase`.
pub fn fieldNameToCamel(comptime s: []const u8) []const u8 {
    @setEvalBranchQuota(10000);
    const result = comptime blk: {
        var out: [s.len]u8 = undefined;
        var oi: usize = 0;
        var capitalise_next = false;
        for (s) |c| {
            if (c == '_') {
                capitalise_next = true;
                continue;
            }
            if (capitalise_next and c >= 'a' and c <= 'z') {
                out[oi] = c - ('a' - 'A');
            } else {
                out[oi] = c;
            }
            oi += 1;
            capitalise_next = false;
        }
        const f: [oi]u8 = out[0..oi].*;
        break :blk f;
    };
    return &result;
}

/// Convert `snake_case` to a space-separated human-readable string.
pub fn snakeToHuman(comptime s: []const u8) []const u8 {
    @setEvalBranchQuota(10000);
    const result = comptime blk: {
        var out: [s.len]u8 = undefined;
        for (s, 0..) |c, i| {
            out[i] = if (c == '_') ' ' else c;
        }
        const f: [s.len]u8 = out;
        break :blk f;
    };
    return &result;
}

const codec = @import("codec.zig");

const StubFieldTable = struct {
    pub fn tsType(comptime T: type, comptime field_name: []const u8) []const u8 {
        _ = field_name;
        if (T == u64) return "number";
        return "string";
    }

    pub fn reader(comptime T: type, comptime field_name: []const u8) []const u8 {
        _ = field_name;
        if (T == u64) return "r.u64()";
        return "r.fixedString(8)";
    }
};

test "writeStructInterface emits camelCase fields with FieldTable TS types" {
    const Rec = extern struct {
        id: u64 = 0,
        display_name: codec.FixedString(8) = .{},
    };

    var buf: [1024]u8 = undefined;
    var fw: std.Io.Writer = .fixed(&buf);
    try writeStructInterface(&fw, StubFieldTable, "Rec", Rec);
    const out = fw.buffered();

    try std.testing.expect(std.mem.indexOf(u8, out, "export interface Rec {") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "    id: number;") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "    displayName: string;") != null);
}

test "writeStructReader emits per-field decode expressions from the FieldTable" {
    const Rec = extern struct {
        id: u64 = 0,
        display_name: codec.FixedString(8) = .{},
    };

    var buf: [1024]u8 = undefined;
    var fw: std.Io.Writer = .fixed(&buf);
    try writeStructReader(&fw, StubFieldTable, "Rec", Rec);
    const out = fw.buffered();

    try std.testing.expect(std.mem.indexOf(u8, out, "export function readRec(r: BufferReader): Rec {") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "    const id = r.u64();") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "    const displayName = r.fixedString(8);") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "        id,") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "        displayName,") != null);
}

test "writeOpEnum maps snake_case enum fields to PascalCase members" {
    const E = enum(u8) { create_link = 1, get_link = 3, ping = 255 };

    var buf: [1024]u8 = undefined;
    var fw: std.Io.Writer = .fixed(&buf);
    try writeOpEnum(&fw, "Op", E);
    const out = fw.buffered();

    try std.testing.expect(std.mem.indexOf(u8, out, "export const enum Op {") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "    CreateLink = 1,") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "    GetLink = 3,") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "    Ping = 255,") != null);
}

test "writeStatusMap maps enum values to human-readable strings" {
    const E = enum(u8) { not_found = 1, has_children = 6 };

    var buf: [1024]u8 = undefined;
    var fw: std.Io.Writer = .fixed(&buf);
    try writeStatusMap(&fw, "STATUS_MSG", E);
    const out = fw.buffered();

    try std.testing.expect(std.mem.indexOf(u8, out, "export const STATUS_MSG: Record<number, string> = {") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "    1: \"not found\",") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "    6: \"has children\",") != null);
}

const StubKindTable = struct {
    pub fn kind(comptime E: type, comptime op_name: []const u8) OpKind {
        _ = E;
        if (std.mem.startsWith(u8, op_name, "get_") or std.mem.eql(u8, op_name, "ping")) return .read;
        return .write;
    }
};

test "writeOpKindMap classifies each op through the KindTable" {
    const E = enum(u8) { create_link = 1, get_link = 3, ping = 255 };

    var buf: [1024]u8 = undefined;
    var fw: std.Io.Writer = .fixed(&buf);
    try writeOpKindMap(&fw, StubKindTable, "OP_KIND", E);
    const out = fw.buffered();

    try std.testing.expect(std.mem.indexOf(u8, out, "export const OP_KIND: Record<number, \"read\" | \"write\"> = {") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "    1: \"write\",") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "    3: \"read\",") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "    255: \"read\",") != null);
}

test "writeReadWriteRouter emits the neutral routed client with the LSN fence" {
    var buf: [4096]u8 = undefined;
    var fw: std.Io.Writer = .fixed(&buf);
    try writeReadWriteRouter(&fw, "RoutedClient");
    const out = fw.buffered();

    try std.testing.expect(std.mem.indexOf(u8, out, "export type Transport = (op: number, payload: Uint8Array) => Promise<Uint8Array>;") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "export class RoutedClient {") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "if (this.opKind[op] !== \"read\") return this.leader(op, payload);") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "if (this.readYourWrites && this.replicaAppliedLsn < this.lastWriteLsn) return this.leader(op, payload);") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "return this.replica(op, payload);") != null);
}
