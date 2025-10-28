````markdown
# Hybrid Distributed File System (HyDFS)

## Overview
HyDFS is a distributed systems project implementing group membership, failure detection, and gossip-based communication. The system is built in Go and designed to run across multiple virtual machines (VMs). It supports dynamic membership, failure detection using ping/ack and gossip heartbeat protocols along with piggyback dissemination of membership information.
## Features
* **Group Membership:** Each VM maintains a membership list and updates it using gossip/pingack protocols.
* **Failure Detection:** Nodes periodically ping/gossip to each other and use suspicion mechanisms to detect failures.
* **Gossip/PingACK Protocol:** Membership and state changes are propagated efficiently using piggy-back communication.
* **Configurable:** System parameters are controlled via a central `config.json` file and also from command-line flags.


## Directory Structure

```
config.json
README.md
setup/
  setup.bash
  vm_setup.bash
  start_server.bash
  kill.bash
src/
  go.mod
  config/
    config.go
  ctl/
    hydfsctl_daemon.go
  hydfs/
    cmd/
      hydfsd/
        hydfs_daemon.go
      server/
        http_server.go
    logging/
      logger.go
    merge/
      ring/
        replica_manager.go
        ring_manager.go
        ring.go
    routing/
      router.go
    storage/
      utils/
  main/
    main.go
  membership/
    daemon/
      daemon.go
      config_diff.go
    node/
      nodeid.go
    protocol/
      store/
    suspicion/
    transport/
    wire/
  utils/
```


## Getting Started

To get started with hydfs-g33, follow these steps:

### Clone the Repository
```
git clone <your-repo-url>
cd hydfs-g33
```

### Installation & Setup

#### Prerequisites
- Go 1.18+
- Bash (for setup scripts)
- Access to multiple VMs or containers (recommended for distributed testing)

#### Install Dependencies
```
cd src
# Install Go dependencies
go mod tidy
```
## How to Run

### 1. VM Setup & Run
Use the provided scripts in the `setup/` directory to initialize and start the system:
```
# Setup VMs and environment
bash setup/vm_setup.bash
# Start the server
bash setup/start_server.bash
```

On each VM, run the setup script to install dependencies and clone the repo:

```
cd setup
bash setup.bash
```

Or, to automate setup and startup across all VMs from one machine:

```
bash vm_setup.bash setup   # Setup all VMs
bash vm_setup.bash start   # Start servers on all VMs
```

### 2. Start the Membership Daemon

If the automation scripts were run, the membership daemon will already be running on each VM. You can attach to the session (if using tmux):

```
sudo tmux -S /tmp/tmux-cs-425-mp2.sock attach-session -t cs-425-shared-mp2
```

Or run directly:

```
cd src/membership/memberd
--> if introducer:
    sudo tmux -S /tmp/tmux-cs-425-mp2.sock new-session -d -s cs-425-shared-mp2 "cd /home/mp2/hydfs-g33/src/membership/memberd && go run main.go -is-introducer"
--> else:
    sudo tmux -S /tmp/tmux-cs-425-mp2.sock new-session -d -s cs-425-shared-mp2 "cd /home/mp2/hydfs-g33/src/membership/memberd && go run main.go -introducer=fa25-cs425-3301.cs.illinois.edu:6000"
```

### 3. Monitor Membership & Failures

Membership and failure events are logged and can be monitored via the daemon output. Configuration is managed via `config.json` and command-line flags.

## Usage

- Configuration is managed via `config.json`.
- Main entry point is in `src/membership/memberd/main.go`.
- Membership, gossip, and failure detection logic are in `src/membership/protocol/`, `src/membership/store/`, and `src/membership/suspicion/`.

## Ports Information

- **5000:** Application port (UDP). Each node runs its main membership and gossip protocol on this port.
- **6000:** Introducer port (TCP). Used for new nodes joining the group; the introducer listens here for join requests.
- **8080:** HTTP server port (TCP). Used for changing configuration parameters at runtime via HTTP requests.

## API Endpoints

The HTTP server (port 8080) exposes several endpoints for monitoring and controlling the distributed group membership system. Example usage with `curl`:

--> Example VM address used here: `http://fa25-cs425-3301.cs.illinois.edu:8080`

- **Get current configuration:**
  ```sh
  curl http://fa25-cs425-3301.cs.illinois.edu:8080/get | python -m json.tool --sort-keys
  ```
- **Set configuration (from deployed config.json file):**
  ```sh
  curl http://fa25-cs425-3301.cs.illinois.edu:8080/set | python -m json.tool --sort-keys
  ```
- **List all members:**
  ```sh
  curl http://fa25-cs425-3301.cs.illinois.edu:8080/list_mem | python -m json.tool --sort-keys
  ```
- **List self information:**
  ```sh
  curl http://fa25-cs425-3301.cs.illinois.edu:8080/list_self | python -m json.tool --sort-keys
  ```
- **Display suspected failed nodes:**
  ```sh
  curl http://fa25-cs425-3301.cs.illinois.edu:8080/display_suspects | python -m json.tool --sort-keys
  ```
- **Display protocol information:**
  ```sh
  curl http://fa25-cs425-3301.cs.illinois.edu:8080/display_protocol | python -m json.tool --sort-keys
  ```
- **Leave the group:**
  ```sh
  curl http://fa25-cs425-3301.cs.illinois.edu:8080/leave | python -m json.tool --sort-keys
  ```
- **Switch protocol/suspicion settings:**
  **To switch to pingack protocol with suspicion disabled:**
  ```sh
  curl -s -X POST http://fa25-cs425-3301.cs.illinois.edu:8080/switch -d '{"protocol":"pingack","suspicion":"disabled"}' -H 'content-type: application/json' | python -m json.tool --sort-keys
  ```
  To switch to gossip protocol with suspicion disabled:
  ```sh
  curl -s -X POST http://fa25-cs425-3301.cs.illinois.edu:8080/switch -d '{"protocol":"gossip","suspicion":"disabled"}' -H 'content-type: application/json' | python -m json.tool --sort-keys
  ```
  To enable suspicion with a custom suspicion time (e.g., 1s) for pingack protocol:
  ```sh
  curl -s -X POST http://fa25-cs425-3301.cs.illinois.edu:8080/switch -d '{"protocol":"pingack","suspicion":"enabled","suspicion_time":"1s"}' -H 'content-type: application/json' | python -m json.tool --sort-keys
  ```
  To enable suspicion with a custom suspicion time (e.g., 1s) for gossip protocol:
  ```sh
  curl -s -X POST http://fa25-cs425-3301.cs.illinois.edu:8080/switch -d '{"protocol":"gossip","suspicion":"enabled","suspicion_time":"1s"}' -H 'content-type: application/json' | python -m json.tool --sort-keys
  ```
These endpoints allow you to query and modify the system state, view membership and suspicion lists, and control protocol settings at runtime.

## Contributing

Contributions are welcome!

````

# HyDFS: extended module overview

This companion file contains a concise tour of the HyDFS (file storage) components that complement the existing membership README in `README.md`.

## HyDFSDaemon
- Location: `src/ctl/hydfsctl_daemon.go` (and related command packages under `src/hydfs/cmd/`)
- Responsibility: boots the HyDFS node, starts the HTTP server, subscribes to the membership store, and wires the ring manager + router. The daemon constructs a `FileStore`, `Ring.Manager` (if a membership store is available) and an HTTP API that exposes user-facing and replica endpoints.

## HTTP API / Server
- Location: `src/hydfs/cmd/server/http_server.go`
- Responsibility: exposes the user-facing endpoints and replica endpoints used for file create/append/get. Key user endpoints:
  - `POST /v1/user/create_with_data` — create a HyDFS file (streams file bytes)
  - `POST /v1/user/append` — append bytes to a HyDFS file
  - `GET  /v1/user/files/content` — download a file to a local path (uses manifest quorum to choose replica)

  Replica (node→node) endpoints are used during replication and reads:
  - `POST /v1/replica/create_with_data`, `POST /v1/replica/append`, `GET /v1/replica/files/content`, `GET /v1/replica/files/manifest`.

  The server implements multipart responses for replica reads (file bytes + JSON metadata) and performs quorum-based fetches when serving user GETs.

## Storage (chunks, manifest, file store)
- Location: `src/hydfs/storage/` (files: `chunk_store.go`, `file_store.go`, `manifest.go`, etc.)
- Responsibilities:
  - Chunking: large uploads are split into chunk files (max chunk size = 64 MB). See `WriteChunks`/`ReadChunks` in `chunk_store.go`.
  - Manifest model: each HyDFS file has a manifest (versioned list of append operations). The manifest contains `AppendOp` records and deterministic ordering/merge rules.
  - FileStore: manages creating files, appending data (writes chunks, updates manifest), and reading by concatenating chunks. It also exposes helpers to create local files from stored data.

## Ring & Replica Management
- Location: `src/hydfs/ring/` (e.g., `ring_manager.go`) and `src/hydfs/ring/replica_manager.go`
- Responsibility: maintain the consistent-hashing ring built from the membership store, plan local rebalancing (pushes and garbage-collection), and (optionally) execute moves using an HTTP mover when the ring changes. The ring manager subscribes to membership updates and rebuilds the ring automatically.

## Routing
- Location: `src/hydfs/routing/router.go`
- Responsibility: translate a HyDFS filename into a replica set by computing a file token and selecting successors on the ring. This encapsulates the replication factor used by the system.

## Merge & Conflict Ordering
- Location: `src/hydfs/merge/merge.go`
- Responsibility: provide deterministic ordering of append operations (SortOps) using timestamps, client IDs, and client sequence numbers so manifests converge across replicas.

## Logging
- Location: `src/hydfs/logging/logger.go`
- Responsibility: node-local structured logging for file operations (RECV / DONE / INFO messages) written to a per-node log file.

## Client / CLI
- Location: `src/ctl` and related CLI helper in the repository (e.g., a small CLI under `src/ctl` or `src/hydfs/cmd/`)
- Responsibility: a simple REPL-style CLI that talks to the HyDFS HTTP API to create/append/get files. The CLI maps human consistency levels (one/quorum/all) to `min_replies` for quorum requests.

## Quick run notes
- The HyDFS daemon boots the HTTP API and subscribes to the membership store when available. Configurable values (HTTP bind address, file directories, replication factor) are in the repository config (see `config.json` and `src/config/config.go`).

Simple example (adjust paths/config as appropriate):

```powershell
# from project root
# cd src
# go mod tidy
# Run the hydfs daemon (exact package depends on layout; see src/ctl and src/hydfs/cmd)
# Example (may need to adapt to your environment):
go run ./src/ctl
# or
go run ./src/hydfs/cmd/hydfsd
```

And run the client/ctl REPL (if present) to exercise the API:

```powershell
# from project root
go run ./src/ctl  # if the CLI entrypoint is in src/ctl
# then in the REPL:
# create <localfile> <HyDFSfilename> [one|quorum|all]
# append <localfile> <HyDFSfilename> [one|quorum|all]
# get <HyDFSfilename> <localfile> [one|quorum|all]
```

## Notes & next steps
- The HyDFS daemon tightly integrates with the membership subsystem (it subscribes to membership updates to build the ring). If you want to run HyDFS in a single-node mode, the Daemon code detects a nil membership store and will run without ring-based replication.
- This extended documentation is purposely concise — open the referenced files for implementation details and examples.
