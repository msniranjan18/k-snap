# k-snap
K-Snap Delta is a space-efficient "DVR" for Kubernetes resources. It records object changes as incremental deltas and provides a side-by-side terminal UI to play back history.

## Key Features
 - **Targeted Recording:** Use kind/name syntax to record a specific instance and ignore noise from other resources.
 - **CRD-Aware:** Intentionally preserves the .status block so you can track Custom Resource health and transitions.
 - **Delta Storage:** Saves 1 full YAML every 10 changes; others are tiny .patch files using difflib.
 - **Smart Reconnection:** Tracks resourceVersion to resume watches exactly where they left off without missing events.
 - **TUI Browser:** Side-by-side "VimDiff" style view with live update support and highlighted changes.
 - **Zero-Cluster Footprint:** Runs on your deployer or laptop using standard kubectl binaries.

## Prerequisites
Before running K-Snap, ensure your environment has:
- Python 3.10+: Uses modern type hinting and pathlib.
- kubectl: Configured with access to your cluster (~/.kube/config).
- Standard Libraries: Uses curses for the TUI (included in most Linux/macOS Python distributions).

## Installation
Run this command from inside the k-snap project folder:
```bash
# Clone the project and install via pip
pip install --user .
```
The `--user` flag is important on production servers so you don't need sudo permissions and don't mess with system-wide Python libraries.
After installation the `ksnap` command is available system-wide.

## Usage
### 1. Record History
You can record all resources of a certain type, or target a specific one by name.
```Bash
# All deployments in a namespace
ksnap record deployments -n ns1

# A single named resource
ksnap record deployments/my-api -n ns2

# Override the kind label used for the storage directory
ksnap record mycrds.example.com -n ns1 --kind MyCRD
```
Press `Ctrl+C` to stop recording and see a summary of captured events.

### 2. Browse History
Open the interactive side-by-side browser. If multiple resources were recorded in the same directory, k-snap will prompt you to choose one.
```Bash
# Browse by namespace and kind
ksnap browse -n ns1 -k Deployment

# Jump straight to a specific named resource
ksnap browse -n ns2 -k Deployment --name my-api

# Point directly at a history directory
ksnap browse --dir ./history/production_deployment
```

#### Browser Controls
- `Ctrl + N` Move to Next (newer) version
- `Ctrl + P` Move to Previous (older) version
- `Ctrl + Q` Quit Browser
- `Arrows keys` Scroll up/down within panes (synchronized)
  
Changed lines are highlighted in yellow. The version count in the header updates live as the recorder writes new files.


## Storage Structure
Snapshots are stored in `./history/<namespace>_<kind>/` using filesystem-safe sanitized names and ISO timestamps.
```
history/
└── ns1_deployment/
    ├── my-api_20240615T120000_000000_FULL.yaml   # full snapshot (index 0, 10, 20 …)
    ├── my-api_20240615T120005_123456_DIFF.patch  # unified diff (index 1–9)
    └── …
```
- `*_FULL.yaml`: Base checkpoint (every 10th event).
- `*_DIFF.patch`: Incremental change (Unified Diff).

## Notes
- `status` is preserved by default so CRD state transitions are recorded.
- Volatile metadata (`managedFields`, `resourceVersion`, `generation`) is stripped before diffing to avoid noise.
- On reconnection after a dropped watch, the recorder resumes from the last seen `resourceVersion` using `--watch-only` to avoid replaying already-captured events.

