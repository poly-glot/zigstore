#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import path from "node:path";

const REPO_ROOT = path.resolve(import.meta.dirname, "../..");
const ZIG = process.env.ZIG || "zig";

let raw = "";
for await (const chunk of process.stdin) raw += chunk;

const filePath = JSON.parse(raw || "{}").tool_input?.file_path;
if (typeof filePath !== "string" || filePath.length === 0 || filePath.includes("\0")) {
  process.exit(0);
}

const resolved = path.resolve(filePath);
if (!resolved.startsWith(REPO_ROOT + path.sep)) process.exit(0);

if (path.extname(resolved) === ".zig") {
  spawnSync(ZIG, ["fmt", resolved], { stdio: "ignore", shell: false, cwd: REPO_ROOT });
  console.log(`Auto-formatted ${resolved} (zig fmt).`);
}

process.exit(0);
