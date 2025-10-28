#!/usr/bin/env bash
set -euo pipefail

echo "=== VM setup starting ==="

# ---------------------------
# 1) Packages
# ---------------------------
echo "[1/4] Installing system packages..."
if command -v dnf >/dev/null 2>&1; then
  sudo dnf install -y git tmux openssh-clients wget vim
elif command -v apt-get >/dev/null 2>&1; then
  sudo apt-get update -y
  sudo apt-get install -y git tmux openssh-client wget vim
else
  echo "Warning: no known package manager found; skipping installs."
fi

# ---------------------------
# 2) Ensure GitLab host key is trusted
# ---------------------------
echo "[2/4] Priming known_hosts for GitLab..."
mkdir -p "$HOME/.ssh"
chmod 700 "$HOME/.ssh"
touch "$HOME/.ssh/known_hosts"
chmod 600 "$HOME/.ssh/known_hosts"
if command -v ssh-keyscan >/dev/null 2>&1; then
  ssh-keyscan gitlab.engr.illinois.edu >> "$HOME/.ssh/known_hosts" || true
fi

# ---------------------------
# 3) Load the GitLab SSH key (must be at $HOME/key.txt)
# ---------------------------
echo "[3/4] Loading GitLab SSH key..."
SSH_KEY="$HOME/key.txt"
if [[ ! -f "$SSH_KEY" ]]; then
  echo "ERROR: Expected GitLab key at $SSH_KEY. Did you run vm_setup.bash?"
  exit 1
fi
chmod 600 "$SSH_KEY"

# Start agent if needed and add key
if ! pgrep -u "$USER" ssh-agent >/dev/null 2>&1; then
  eval "$(ssh-agent -s)"
fi
ssh-add "$SSH_KEY"

# ---------------------------
# 4) Clone or refresh repo in /home/mp3
# ---------------------------
echo "[4/4] Cloning/updating repo..."
TARGET_DIR="/home/mp3"
REPO_DIR="hydfs-g33"
REPO_URL="git@gitlab.engr.illinois.edu:distributed-systems/hydfs-g33.git"

sudo mkdir -p "$TARGET_DIR"
sudo chmod 777 "$TARGET_DIR"

cd "$TARGET_DIR"
if [[ -d "$REPO_DIR/.git" ]]; then
  echo "Repo exists; resetting to origin/main..."
  git -C "$REPO_DIR" fetch --all --prune
  git -C "$REPO_DIR" checkout -f main || true
  git -C "$REPO_DIR" reset --hard origin/main
else
  echo "Cloning fresh..."
  git clone "$REPO_URL" "$REPO_DIR"
  ( cd "$REPO_DIR" && git checkout -f main || true )
fi

echo "=== VM setup completed ==="
