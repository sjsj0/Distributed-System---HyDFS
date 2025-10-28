#!/usr/bin/env bash
set -euo pipefail

echo "=== Starting app in tmux ==="

# ---------------------------
# 1) Make Git use $HOME/key.txt directly (no agent)
# ---------------------------
SSH_KEY="$HOME/key.txt"
if [[ ! -f "$SSH_KEY" ]]; then
  echo "ERROR: Expected GitLab key at $SSH_KEY. Did you run vm_setup.bash?"
  exit 1
fi
chmod 600 "$SSH_KEY"

# Prime ~/.ssh and known_hosts for GitLab, and write a minimal SSH config
mkdir -p "$HOME/.ssh"
chmod 700 "$HOME/.ssh"
touch "$HOME/.ssh/known_hosts"
chmod 600 "$HOME/.ssh/known_hosts"
if command -v ssh-keyscan >/dev/null 2>&1; then
  ssh-keyscan gitlab.engr.illinois.edu >> "$HOME/.ssh/known_hosts" || true
fi

# Force all git SSH calls in this script to use the key (no agent needed)
export GIT_SSH_COMMAND="ssh -i \"$SSH_KEY\" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new"

# (Also nice to have for future interactive shells)
CONFIG="$HOME/.ssh/config"
if ! grep -q "Host gitlab.engr.illinois.edu" "$CONFIG" 2>/dev/null; then
  {
    echo "Host gitlab.engr.illinois.edu"
    echo "  IdentityFile $SSH_KEY"
    echo "  IdentitiesOnly yes"
    echo "  StrictHostKeyChecking accept-new"
  } >> "$CONFIG"
  chmod 600 "$CONFIG"
fi

# ---------------------------
# 2) Ensure repo is current
# ---------------------------
TARGET_DIR="/home/mp3"
REPO_DIR="hydfs-g33"
REPO_URL="git@gitlab.engr.illinois.edu:distributed-systems/hydfs-g33.git"

sudo mkdir -p "$TARGET_DIR"
sudo chmod 777 "$TARGET_DIR"
cd "$TARGET_DIR"

# Clean rebuild to guarantee a fresh start (matches your intent)
if [[ -d "$REPO_DIR" ]]; then
  echo "Removing existing repo..."
  sudo rm -rf "$REPO_DIR"
fi

echo "Cloning repo..."
git clone "$REPO_URL" "$REPO_DIR"
cd "$REPO_DIR"
git checkout -f main || true

# ---------------------------
# 3) Start tmux session for running the main daemon
# ---------------------------
TMUX_SESSION="cs-425-shared-mp3"
TMUX_SOCKET="/tmp/tmux-cs-425-mp3.sock"
SRC_DIR="$TARGET_DIR/$REPO_DIR/src/main"

echo "Killing any prior tmux server (if present)..."
sudo tmux -S "$TMUX_SOCKET" kill-server 2>/dev/null || true

echo "Creating new tmux session: $TMUX_SESSION"
# If this node is the introducer host, start with -is-introducer; else point to it.
FQDN="$(hostname -f 2>/dev/null || hostname)"
if [[ "$FQDN" == "fa25-cs425-3301.cs.illinois.edu" ]]; then
  echo "This is the introducer node."
#   sudo env "PATH=$PATH" tmux -S "$TMUX_SOCKET" new-session -d -s "$TMUX_SESSION" \
  sudo tmux -S $TMUX_SOCKET new-session -d -s $TMUX_SESSION \
    "cd '$SRC_DIR' && go run main.go -is-introducer 2>&1 | tee out.log"
else
  echo "This is a regular node; connecting to introducer."
  sudo tmux -S $TMUX_SOCKET new-session -d -s $TMUX_SESSION \
    "cd '$SRC_DIR' && go run main.go -introducer=fa25-cs425-3301.cs.illinois.edu:6000 2>&1 | tee out.log"
#   sudo env "PATH=$PATH" tmux -S "$TMUX_SOCKET" new-session -d -s "$TMUX_SESSION" \
fi

echo "Tmux session '$TMUX_SESSION' created."
echo "Attach with: sudo tmux -S $TMUX_SOCKET attach -t $TMUX_SESSION"


# ---------------------------
# 4) Start tmux session for running the ctl daemon
# ---------------------------
TMUX_SESSION="cs-425-shared-mp3-ctl"
TMUX_SOCKET="/tmp/tmux-cs-425-mp3-ctl.sock"
SRC_DIR="$TARGET_DIR/$REPO_DIR/src/ctl"

echo "Killing any prior tmux server (if present)..."
sudo tmux -S "$TMUX_SOCKET" kill-server 2>/dev/null || true

echo "Creating new tmux session: $TMUX_SESSION"

sudo tmux -S $TMUX_SOCKET new-session -d -s $TMUX_SESSION \
    "cd '$SRC_DIR' && go run hydfsctl_daemon.go -is-ctl-client 2>&1 | tee out.log"

echo "Tmux session '$TMUX_SESSION' created."
echo "Attach with: sudo tmux -S $TMUX_SOCKET attach -t $TMUX_SESSION"
