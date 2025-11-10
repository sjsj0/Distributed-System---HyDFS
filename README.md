# Hybrid Distributed File System (HyDFS)

## Overview
hydfs-g33 is a hybrid distributed file system (HyDFS) and key-value store inspired by ideas from Cassandra (consistent-hash ring + replication) and HDFS (chunked storage with manifests). The implementation is in Go and is intended to run across multiple VMs. The project combines two coordinated subsystems:

- A membership and failure-detection subsystem that provides dynamic group membership, protocol switching, and an HTTP admin interface.
- A HyDFS storage and routing subsystem that stores logical files as manifests + chunked data, uses a consistent-hash ring for replica placement, and performs re-replication and GC when the ring changes.

Key implementation points:
- Membership daemon (`src/membership/daemon`) supports two protocols: gossip and ping/ack. The active protocol is configurable at runtime and piggybacks cluster config updates on protocol messages.
- Failure detection is handled via a `suspicion.Manager` that can be enabled/disabled and tuned (TSuspect/TFail/TCleanup). The Ping/Ack loop uses an inflight tracker to observe ping timeouts.
- An introducer TCP server assists new nodes to join the cluster; nodes can also be configured as introducers.
- The membership store maintains member entries, incarnation numbers, and state transitions and exposes a simple HTTP API for inspection and admin operations.
- HyDFS routing and replication are implemented with a token ring (`src/hydfs/ring`) and a `ReplicaManager` that computes local push/GC plans on ring changes and executes transfers via an HTTP mover.
- Storage is chunk/manifest-based (`src/hydfs/storage`): manifests record ordered append operations (with client IDs, sequences, timestamps and chunk lists) and are used to deterministically merge concurrent appends.
- A control client/daemon (`src/ctl/hydfsctl_daemon.go`) provides a CLI-like interface to issue file operations (create, append, get, list, merge) against the HyDFS daemon.

## Features
* Protocols: supports both gossip and ping/ack membership protocols with runtime switching. Both protocols piggyback cluster config updates onto normal messages.
* Failure detection & suspicion: a configurable suspicion manager (TSuspect/TFail/TCleanup) detects suspect/failed nodes; ping/ack mode uses timeout-based inflight tracking.
* Introducer-based bootstrapping: nodes can join via an introducer TCP server; the introducer is optional and runs in its own goroutine in the daemon.
* Membership store & HTTP admin: membership state is stored and exposed via an HTTP admin API (endpoints: /get, /set, /list_mem, /list_self, /display_suspects, /display_protocol, /switch, /leave).
* Consistent-hash ring & replica management: ring tokens determine replica sets; `ReplicaManager` computes per-node local plans (pushes + GC) when nodes join/leave and executes them via an HTTP mover.
* Chunked storage + manifests: logical files are represented by manifests (ordered append ops) and streaming chunk data; manifests provide deterministic merge and versioning semantics for concurrent appends.
* Re-replication & garbage collection: when the ring changes the node planner computes which token ranges to push to peers and which ranges to GC locally to restore the target replication factor.
* Hydfs control client: `hydfsctl` (in `src/ctl`) can connect to the daemon for file operations (create, append, get, liststore, getfromreplica, multicreate/multiappend, merge, etc.).
* Runtime-configurable knobs: many parameters (PingEvery, GossipPeriod, PingFanout/GossipFanout, DropRateRecv, TSuspect/TFail/TCleanup, ports) are exposed through `config.json` and can be live-tuned via the HTTP API.


## Directory Structure

```
config.json                  # System configuration file (cluster knobs and runtime parameters)
README.md                    # Project documentation
dataset/                     # Test datasets and sample files used for experiments
  business_*.txt             # sample input files used by evaluation scripts
  text_*.txt                 # text files of various sizes for throughput/latency tests
llm/                         # LLM notes and chat logs
  chat.txt                   # conversation / notes
setup/                       # Scripts for VM setup and orchestration
  setup.bash                 # Local single-VM setup/install script
  vm_setup.bash              # Multi-VM orchestration script (setup/start/stop)
  start_server.bash          # Starts servers/daemons across nodes
  kill.bash                  # Kills running test processes / daemons

src/                         # Source code (Go)
  go.mod                     # Go module file (dependencies)
  config/
    config.go                # Configuration structures, parsing, DTO and live-apply logic
  ctl/
    hydfsctl_daemon.go       # hydfs control client/daemon: CLI for file ops and test orchestration
  hydfs/
    cmd/
      hydfsd/
        hydfs_daemon.go      # HyDFS daemon entrypoint for storage node (starts HTTP + replica manager)
      server/
        http_server.go       # HTTP API used for client/replica endpoints (upload, download, replica management)
    ring/
      replica_manager.go     # Plans local push/GC on ring changes and executes re-replication via HTTPMover
      ring_manager.go        # Builds/updates the logical token ring from membership information
      ring.go                # Ring utilities: tokens, predecessor/successor, node iteration
    routing/
      router.go              # Maps file tokens to replica set (replica selection logic)
    storage/
      chunk_store.go         # Low-level chunk storage (read/write chunk blobs)
      file_enumerator.go     # Enumerates files & tokens for re-replication and GC
      file_store.go          # High-level file store: manifest+chunks management and stream IO
      fs_paths.go            # Filesystem layout and path helpers for manifests/chunks
      manifest.go            # Manifest structure and deterministic append/merge logic (append ops)
    utils/
      ascii_ring.go          # ASCII helpers for pretty-printing ring/token maps
      ids.go                 # ID/token utilities and hashing helpers
    main/
      main.go                # Local hydfs/membership binary (convenience runner)
    membership/
      daemon/
        config_diff.go       # Utilities to compute and log config diffs
        daemon.go            # Membership daemon: starts gossip/pingack loops, introducer, HTTP admin
      node/
        nodeid.go            # NodeID, endpoint and timestamp helpers
      protocol/
        common/
          inflight.go        # Inflight tracker used by ping/ack for nonce/timeouts
          peers.go           # Peer selection helpers used by both gossip and ping/ack
        gossip/     
          loop.go            # Gossip protocol loop: periodic gossip + recv and merge
        pingack/     
          loop.go            # Ping/Ack protocol loop: periodic ping, ACK handling, inflight tracking
      store/     
        membership.go        # Membership store: entries, incarnation, merge rules and snapshot
        subscriber.go        # Subscriber interface for components that react to membership changes
      suspicion/     
        manager.go           # Suspicion manager: scanner that marks suspect/fail and issues GC/cleanup
      transport/     
        udp.go               # UDP wrapper used by both protocols (Read/Write helpers)
      utils/     
        colors.go            # Terminal color constants used in logging
      wire/     
        codec.go             # Wire encoding/decoding of membership messages (ping/ack/gossip)
  utils/
    utils.go                 # Generic helpers (DNS resolution, string utilities)
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

### 2. Start the HyDFS Daemon

If the automation scripts were run, the hydfs daemon will already be running on each VM. You can attach to the session (if using tmux):

```
sudo tmux -S /tmp/tmux-cs-425-mp3.sock attach-session -t cs-425-shared-mp3
```

Or run directly:

```
--> if introducer:
    sudo tmux -S /tmp/tmux-cs-425-mp3.sock new-session -d -s cs-425-shared-mp3 "cd /home/mp3/hydfs-g33/src/main && go run main.go -is-introducer"
--> else:
    sudo tmux -S /tmp/tmux-cs-425-mp3.sock new-session -d -s cs-425-shared-mp3 "cd /home/mp3/hydfs-g33/src/main && go run main.go -introducer=fa25-cs425-3301.cs.illinois.edu:6000"
```

### 3. Hydfs CTL Daemon
To interact with the hydfs daemon, you can use the hydfsctl daemon located in `src/ctl/hydfsctl_daemon.go`. This tool allows you to send commands and queries to the running hydfs daemon.

```
sudo tmux -S /tmp/tmux-cs-425-mp3-ctl.sock attach-session -t cs-425-shared-mp3-ctl
```

Or run directly:

```
sudo tmux -S /tmp/tmux-cs-425-mp3-ctl.sock new-session -d -s cs-425-shared-mp3-ctl "cd /home/mp3/hydfs-g33/src/ctl && go run hydfsctl_daemon.go -is-ctl-client"
```

### 4. Hydfs Daemon Features
The hydfs daemon provides the following features: 
 - Ring management, data storage, routing, and an HTTP server for configuration and monitoring.
 - Uses membership management, gossip/pingack protocols, and failure detection.

### 5. Monitor Membership & Failures

Membership and failure events are logged and can be monitored via the daemon output. Configuration is managed via `config.json` and command-line flags.

## Usage

- Configuration is managed via `config.json`.
- Main entry point for the hydfs daemon is in `src/hydfs/`.
- Main entry point for membership is in `src/membership/`.
- Membership, gossip, and failure detection logic are in `src/membership/protocol/`, `src/membership/store/`, and `src/membership/suspicion/`.

## Ports Information

- **5000:** Application port (UDP). Each node runs its main membership and gossip protocol on this port.
- **6000:** Introducer port (TCP). Used for new nodes joining the group; the introducer listens here for join requests.
- **8080:** HTTP server port (TCP). Used for changing configuration parameters at runtime via HTTP requests.
- **10010**: Hydfs Daemon port (TCP). Used for hydfs daemon operations.

## Hydfs Daemon Commands

The hydfs daemon supports various commands for file operations and system management. Use the hydfsctl daemon to send commands to the hydfs daemon.
## Example Commands
- **Create a file:**
  ```
  create <localfilename> <HyDFSfilename>
  ```
- **Append to a file:**
  ``` 				
  append <localfilename> <HyDFSfilename>
  ```
- **Download a file:**
  ```
  get <HyDFSfilename> <localfilename>
  ```
- **Merge file replicas:**
  ```
  merge <HyDFSfilename>
  ```
- **List file replicas:**
  ```
  ls <HyDFSfilename>
  ```
- **List all stored files:**
  ```
  liststore
  ```
- **Download from a specific replica:**
  ```
  getfromreplica <VMAddress> <HyDFSfilename> <localfilename>
  ```
- **List ring node IDs and tokens:**
  ```
  list_mem_ids
  ```
- **Multiple file operations:**
  ```
  multiappend <HyDFSfilename> <VMi> <VMj> ... <localfilei> <localfilej> ...
  ```
- **Create multiple files:**
  ```
  multicreate n <localfile> ...
  ```
- **Help command:**
  ```
  help
  ```
- **Exit the hydfsctl daemon:**
  ``` 
  exit | quit
  ```

## Membership API Endpoints

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

## Authors & Acknowledgments
- Distributed Systems (CS 425), Fall 2025