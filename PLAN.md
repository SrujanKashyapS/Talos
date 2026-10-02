# Talos implementation plan
1. Rename module/CLI/state paths and preserve image/layer formats.
2. Add registry and agent RPCs with streamed, hash-verified layers.
3. Add namespaces, cgroups v2 limits, and tar extraction hardening.
4. Add node membership, heartbeats, and pluggable scheduling.
5. Add Raft control state, snapshots, restore, and leader redirect.
6. Add YAML workloads and reconciliation CLI/controller.
7. Add distributed build cache, metrics/logging, and demo.
