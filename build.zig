const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const zigstore = b.addModule("zigstore", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    const lib_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/root.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_lib_tests = b.addRunArtifact(lib_tests);

    const example_mod = b.createModule(.{
        .root_source_file = b.path("examples/basic.zig"),
        .target = target,
        .optimize = optimize,
    });
    example_mod.addImport("zigstore", zigstore);

    const example = b.addExecutable(.{
        .name = "basic",
        .root_module = example_mod,
    });
    b.installArtifact(example);

    const run_example = b.addRunArtifact(example);
    run_example.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_example.addArgs(args);
    const run_example_step = b.step("run-example", "Build and run the basic example");
    run_example_step.dependOn(&run_example.step);

    const example_tests = b.addTest(.{ .root_module = example_mod });
    const run_example_tests = b.addRunArtifact(example_tests);

    const examples_step = b.step("examples", "Build the examples");
    examples_step.dependOn(&example.step);

    const test_step = b.step("test", "Run engine + example tests");
    test_step.dependOn(&run_lib_tests.step);
    test_step.dependOn(&run_example_tests.step);
}
