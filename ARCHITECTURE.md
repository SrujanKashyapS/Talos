# Talos Architecture

## Components
- `talosd`: control plane (Raft, registry, cache index, scheduler, reconciliation)
- `talos-agent`: node runtime (container lifecycle, logs, heartbeats)
- `talos`: CLI for build, image transfer, and workload operations

## Data Model
- Image manifests and layers stay content-addressed; state root is `~/.talos/`.
- Raft FSM replicates nodes, workloads, cache keys, and replica assignments.
- Agents persist container metadata/log paths in local Talos state.

## Runtime Flow
```text
talos CLI -> talosd leader (gRPC) -> Raft FSM -> scheduler -> talos-agent
      \-> registry/cache RPCs ------------------------------^
```

## Failure Handling
- Followers redirect mutating CLI operations to the current leader.
- Dead nodes are detected from missed heartbeats and reconciled by the controller.
- Snapshots/restore keep cluster state across leader changes.
