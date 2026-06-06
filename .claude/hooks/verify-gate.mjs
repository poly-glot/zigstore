#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import path from "node:path";

const REPO_ROOT = path.resolve(import.meta.dirname, "../..");
const ZIG = process.env.ZIG || "zig";

let raw = "";
for await (const chunk of process.stdin) raw += chunk;
const payload = JSON.parse(raw || "{}");

if (payload.stop_hook_active) process.exit(0);

const status = spawnSync("git", ["status", "--porcelain"], { cwd: REPO_ROOT, encoding: "utf8" });
const zigTouched = (status.stdout || "")
  .split("\n")
  .map((line) => line.slice(3).trim())
  .filter(Boolean)
  .some((f) => f.endsWith(".zig"));

if (!zigTouched) process.exit(0);

function parseSemver(s) {
  const m = /(\d+)\.(\d+)\.(\d+)/.exec(s || "");
  return m ? [Number(m[1]), Number(m[2]), Number(m[3])] : null;
}

function olderThan(a, b) {
  for (let i = 0; i < 3; i++) {
    if (a[i] < b[i]) return true;
    if (a[i] > b[i]) return false;
  }
  return false;
}

function minimumZigVersion() {
  try {
    const zon = readFileSync(path.join(REPO_ROOT, "build.zig.zon"), "utf8");
    return parseSemver(/\.minimum_zig_version\s*=\s*"([^"]+)"/.exec(zon)?.[1]);
  } catch {
    return null;
  }
}

const versionRun = spawnSync(ZIG, ["version"], { encoding: "utf8" });
const installed = parseSemver(versionRun.stdout);
const minimum = minimumZigVersion();

if (versionRun.status !== 0 || !installed) {
  console.log("verify-gate: no usable zig on PATH — gate deferred to the Linux devcontainer / CI.");
  process.exit(0);
}

if (minimum && olderThan(installed, minimum)) {
  console.log(
    `verify-gate: host zig ${installed.join(".")} < required ${minimum.join(".")} — ` +
      `gate deferred to the Linux devcontainer / CI (set ZIG=/path/to/zig-${minimum.join(".")} to run it here).`,
  );
  process.exit(0);
}

const res = spawnSync(ZIG, ["build", "test"], { cwd: REPO_ROOT, encoding: "utf8" });
if (res.status === 0) process.exit(0);

const out = ((res.stdout || "") + (res.stderr || "")).slice(-3000);
console.error(
  `verify-gate: zig build test FAILED — do not declare done until it passes.\n` +
    `(The on-disk WAL path needs Linux O_DIRECT/fallocate; that part only gates in the devcontainer.)\n${out}`,
);
process.exit(2);
