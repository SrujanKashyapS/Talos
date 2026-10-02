# Decisions
- gRPC uses a registered JSON codec with checked-in proto contracts; protoc-generated code is not required.
- Existing image manifest/layer/cache composition is preserved; state moves to ~/.talos; the cache-key composition is unchanged.
- Namespace isolation uses SysProcAttr; chroot is opt-in; cgroup v2 requests fail loudly when unavailable.
- Membership defaults to three missed heartbeat intervals; scheduler combines resource headroom, container load, layer affinity, and deterministic IDs.
- Raft uses HashiCorp raft + raft-boltdb/v2, file snapshots, deterministic JSON FSM commands, and bootstrap-plus-join startup.
- Workload desired state is YAML-validated and stored in Raft; assignments are replicated before reconciliation advances.
- Build clients may use the shared Raft cache; a hit fetches a missing layer by its existing digest from the registry.
- Extra dependencies are limited to gRPC, protobuf, Raft/Raft-BoltDB, bbolt, Prometheus, and YAML.
- Metrics are exposed from `talosd` via Prometheus `/metrics`; cache hit rate is derived from cache lookup/hit counters.
