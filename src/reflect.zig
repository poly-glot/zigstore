//! The one comptime view over a struct's fields. Zig 0.16 reports them as `fields`; 0.17
//! reports them struct-of-arrays as `field_names`/`field_types`. Every reflection site in the
//! engine (`wire_codec`, `tsgen`) reads this and names the layout nowhere else.

/// One struct field: its name and type.
pub const Field = struct {
    name: [:0]const u8,
    type: type,
};

/// The fields of struct `T` in declaration order. Evaluate at comptime.
pub fn structFields(comptime T: type) []const Field {
    comptime {
        const info = @typeInfo(T).@"struct";
        if (@hasField(@TypeOf(info), "fields")) {
            var out: [info.fields.len]Field = undefined;
            for (info.fields, 0..) |f, i| out[i] = .{ .name = f.name, .type = f.type };
            const frozen = out;
            return &frozen;
        } else {
            var out: [info.field_names.len]Field = undefined;
            for (info.field_names, info.field_types, 0..) |name, field_type, i| {
                out[i] = .{ .name = name, .type = field_type };
            }
            const frozen = out;
            return &frozen;
        }
    }
}

test "structFields lists every field in declaration order with its type" {
    const Rec = struct { id: u64, name: []const u8, flag: bool };
    const fields = comptime structFields(Rec);
    try @import("std").testing.expectEqual(@as(usize, 3), fields.len);
    try @import("std").testing.expectEqualStrings("name", fields[1].name);
    try @import("std").testing.expect(fields[2].type == bool);
}
