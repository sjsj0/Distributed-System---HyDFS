# hydfs-g33

Distributed Group Membership (DGM) - Group 33

## Overview
hydfs-g33 is a distributed systems project implementing group membership, failure detection, and gossip-based communication. The system is built in Go and designed to run across multiple virtual machines (VMs). It supports dynamic membership, failure detection using ping/ack and gossip heartbeat protocols along with piggyback dissemination of membership information.
## Features
* **Group Membership:** Each VM maintains a membership list and updates it using gossip/pingack protocols.
* **Failure Detection:** Nodes periodically ping/gossip to each other and use suspicion mechanisms to detect failures.
* **Gossip/PingACK Protocol:** Membership and state changes are propagated efficiently using piggy-back communication.
* **Configurable:** System parameters are controlled via a central `config.json` file and also from command-line flags.


## Directory Structure

```
src/                   # Source code
  pkg/
    memberd/           # Main membership daemon
    protocol/          # Gossip and ping/ack protocols
    store/             # Membership store
    suspicion/         # Failure suspicion manager
    transport/         # UDP transport layer
    utils/             # Utility functions
    wire/              # Wire protocol and encoding
  go.mod               # Go module file

setup/                 # Scripts for VM setup and orchestration
  setup.bash           # Local setup script
  vm_setup.bash        # VM setup automation
  start_server.bash    # Start membership server
  kill.bash            # Kill all running processes

config.json            # System configuration file
README.md              # Project documentation
MP2_Report_G33         # Project Report
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

## Authors & Acknowledgments
- Group 33, Distributed Systems (CS 425), Fall 2025
- Special thanks to course staff and contributors

## License
This project is for educational use only.

## Project Status
Actively maintained for the Fall 2025 semester.