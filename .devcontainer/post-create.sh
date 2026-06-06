#!/usr/bin/env bash
set -euo pipefail

echo "=== zigstore Development Container Setup ==="

sudo apt-get update -qq
sudo apt-get install -y -qq --no-install-recommends \
  curl wget jq xz-utils unzip

# Pinned Zig — the engine's gate runs on this exact version (see build.zig.zon).
# ZLS is coupled to Zig by minor version, so pin it to the 0.15.x line and bump the two
# together. A floating `latest` tag pulls a newer ZLS that refuses to run against this Zig.
ZIG_VERSION="0.15.2"
ZLS_VERSION="0.15.0"
ZIG_ARCH="$(uname -m)"
if [ "$ZIG_ARCH" = "x86_64" ]; then
  ZIG_TARGET="x86_64-linux"
elif [ "$ZIG_ARCH" = "aarch64" ]; then
  ZIG_TARGET="aarch64-linux"
else
  echo "Unsupported architecture: $ZIG_ARCH"
  exit 1
fi

echo "Installing Zig ${ZIG_VERSION} for ${ZIG_TARGET}..."
curl -sL "https://ziglang.org/download/${ZIG_VERSION}/zig-${ZIG_TARGET}-${ZIG_VERSION}.tar.xz" | sudo tar -xJ -C /usr/local
sudo ln -sf "/usr/local/zig-${ZIG_TARGET}-${ZIG_VERSION}/zig" /usr/local/bin/zig

echo "Installing ZLS ${ZLS_VERSION}..."
curl -sL "https://github.com/zigtools/zls/releases/download/${ZLS_VERSION}/zls-${ZIG_TARGET}.tar.xz" \
  | sudo tar -xJ -C /usr/local/bin zls

# Upgrade Claude Code to the latest release. The node feature's global npm prefix
# (/usr/local) is root-owned, so `npm install -g` as the vscode user fails with EACCES.
# Point npm's global prefix at a user-owned directory and put its bin first on PATH so the
# upgraded claude wins over the feature's copy — never sudo-install (that leaves root-owned
# files in ~/.npm cache and breaks later user-level npm).
echo "Upgrading Claude Code to latest..."
NPM_GLOBAL="$HOME/.npm-global"
mkdir -p "$NPM_GLOBAL"
npm config set prefix "$NPM_GLOBAL"
export PATH="$NPM_GLOBAL/bin:$PATH"
npm install -g @anthropic-ai/claude-code@latest

echo ""
echo "=== Installed versions ==="
zig version
zls --version 2>/dev/null || echo "ZLS: installed (version check may not be supported)"
git --version
echo -n "claude " && claude --version 2>/dev/null || echo "claude: installed"

git config --global core.autocrlf input
git config --global init.defaultBranch main
git config --global --add safe.directory /workspaces/zigstore

cat >> /home/vscode/.zshrc << 'ALIASES'

# User-owned npm global prefix (so `npm install -g` and the upgraded claude work without sudo)
export PATH="$HOME/.npm-global/bin:$PATH"

# Zig shortcuts
alias zb="zig build"
alias zt="zig build test"
alias zfmt="zig fmt src/ examples/ build.zig"
alias zex="zig build run-example"
alias zbaseline="zig build -Dcpu=baseline"

# Claude shortcut
alias c="claude"

# Git shortcuts
alias gs="git status"
alias gd="git diff"
alias gl="git log --oneline -20"
ALIASES

echo ""
echo "=== Setup complete ==="
echo "Run 'zig build test' to test, 'zig build run-example' to drive the basic example,"
echo "and 'zig build -Dcpu=baseline' to verify the Ampere/OKE baseline build."
