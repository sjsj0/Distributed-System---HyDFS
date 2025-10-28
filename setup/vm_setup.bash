#!/usr/bin/env bash
set -euo pipefail

# --- load .env from this script's directory ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"
[[ -f "$ENV_FILE" ]] || { echo "Error: .env not found at $ENV_FILE"; exit 1; }

# Load .env (supports multi-line PRIVATE_KEY_CONTENT if quoted in .env)
# shellcheck source=/dev/null
set -a
source "$ENV_FILE"
set +a

: "${SSH_USER:?Missing SSH_USER in .env}"
: "${PRIVATE_KEY_CONTENT:?Missing PRIVATE_KEY_CONTENT in .env}"

# Optional: local key used just to reach the VMs. If you've already run
# `ssh-copy-id` or have an agent, you can omit this in .env.
SSH_OPTS=(-o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new)
if [[ -n "${SSH_KEY_LOCAL:-}" && -f "${SSH_KEY_LOCAL:-/dev/null}" ]]; then
  SSH_OPTS=(-i "$SSH_KEY_LOCAL" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new)
fi

# Always place the GitLab private key at $HOME/key.txt on the VM
REMOTE_KEY_PATH="${REMOTE_KEY_PATH:-~/key.txt}"

# Which local script to send and run remotely
if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <setup|start|kill>"
  exit 1
fi

ACTION="$1"
case "$ACTION" in
  setup) LOCAL_SCRIPT="$SCRIPT_DIR/setup.bash" ;;
  start) LOCAL_SCRIPT="$SCRIPT_DIR/start_server.bash" ;;
  kill)  LOCAL_SCRIPT="$SCRIPT_DIR/kill.bash" ;;
  *) echo "Invalid action: $ACTION"; echo "Usage: $0 <setup|start|kill>"; exit 1 ;;
esac
[[ -f "$LOCAL_SCRIPT" ]] || { echo "Error: $LOCAL_SCRIPT not found"; exit 1; }
REMOTE_SCRIPT="/home/${SSH_USER}/$(basename "$LOCAL_SCRIPT")"

# Hosts (without usernames)
hosts=(
  "fa25-cs425-3301.cs.illinois.edu"
  "fa25-cs425-3302.cs.illinois.edu"
  "fa25-cs425-3303.cs.illinois.edu"
  "fa25-cs425-3304.cs.illinois.edu"
  "fa25-cs425-3305.cs.illinois.edu"
  "fa25-cs425-3306.cs.illinois.edu"
  "fa25-cs425-3307.cs.illinois.edu"
  "fa25-cs425-3308.cs.illinois.edu"
  "fa25-cs425-3309.cs.illinois.edu"
  "fa25-cs425-3310.cs.illinois.edu"
)

# --- stage the private key to a secure temp file (no encoding) ---
umask 077
tmpkey="$(mktemp)"
trap 'rm -f "$tmpkey"' EXIT
printf '%s\n' "$PRIVATE_KEY_CONTENT" > "$tmpkey"
chmod 600 "$tmpkey"

# --- loop over servers ---
for host in "${hosts[@]}"; do
  server="${SSH_USER}@${host}"
  echo "==> $server"

  # Ensure remote ~/.ssh exists, tight perms
  ssh "${SSH_OPTS[@]}" "$server" 'umask 077; mkdir -p ~/.ssh'

  # Copy GitLab key to $HOME/key.txt and secure it
  echo "   -> installing private key to $REMOTE_KEY_PATH"
  scp "${SSH_OPTS[@]}" "$tmpkey" "$server:$REMOTE_KEY_PATH"
  ssh "${SSH_OPTS[@]}" "$server" "chmod 600 '$REMOTE_KEY_PATH'"

  # Add GitLab host key to known_hosts (avoid interactive prompt on first git clone)
  ssh "${SSH_OPTS[@]}" "$server" \
    'umask 077; mkdir -p ~/.ssh; touch ~/.ssh/known_hosts;
     command -v ssh-keyscan >/dev/null 2>&1 &&
     ssh-keyscan gitlab.engr.illinois.edu >> ~/.ssh/known_hosts || true'

  # Copy and run the requested script
  echo "   -> copying $(basename "$LOCAL_SCRIPT")"
  scp "${SSH_OPTS[@]}" "$LOCAL_SCRIPT" "$server:$REMOTE_SCRIPT"

  echo "   -> running $REMOTE_SCRIPT"
  ssh -t "${SSH_OPTS[@]}" "$server" "bash '$REMOTE_SCRIPT'"
done

