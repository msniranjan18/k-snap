# k-snap
K-Snap Delta is a space-efficient "DVR" for Kubernetes resources. It records object changes as incremental deltas and provides a side-by-side terminal UI to play back history.

## Key Features
 - **Targeted Recording:** Use kind/name syntax to record a specific instance and ignore noise from other resources.
 - **CRD-Aware:** Intentionally preserves the .status block so you can track Custom Resource health and transitions.
 - **Delta Storage:** Saves 1 full YAML every 10 changes; others are tiny .patch files using difflib.
 - **Smart Reconnection:** Tracks resourceVersion to resume watches exactly where they left off without missing events.
 - **TUI Browser:** Side-by-side "VimDiff" style view with live update support and highlighted changes.
 - **Zero-Cluster Footprint:** Runs on your deployer or laptop using standard kubectl binaries.

## Usage
### 1. Record History
You can record all resources of a certain type, or target a specific one by name.
```Bash
# Record all deployments in a namespace
ksnap record deployments -n prod

# Record a specific CRD instance (Recommended for noise reduction)
ksnap record postgresclusters.acid.zalan.do/db-main -n db-layer
```
Press `Ctrl+C` to stop recording and see a summary of captured events.

### 2. Browse History
Open the interactive side-by-side browser. If multiple resources were recorded in the same directory, k-snap will prompt you to choose one.
```Bash
# Browse by namespace and kind
ksnap browse -n prod -k Deployment

# Jump straight to a specific named resource
ksnap browse -n prod -k Deployment --name api-gateway
```

#### Browser Controls
- `Ctrl + N` Move to Next (newer) version
- `Ctrl + P` Move to Previous (older) version
- `Ctrl + Q` Quit Browser
- `Arrows keys` Scroll up/down within panes (synchronized)


## Storage Structure
Snapshots are stored in `./history/<namespace>_<kind>/` using filesystem-safe sanitized names and ISO timestamps.
- `*_FULL.yaml`: Base checkpoint (every 10th event).
- `*_DIFF.patch`: Incremental change (Unified Diff).
