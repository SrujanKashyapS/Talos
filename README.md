# Talos

Talos is an **educational** distributed container-orchestration runtime built around the repository's content-addressed image and layer engine. It is intentionally small: it demonstrates a Raft-backed control plane, gRPC services, scheduling, and agent-managed processes without presenting itself as a production container platform.

## Architecture

```text
                  +-------------------------------+
 talos CLI  -----> | talosd control plane          |
                  | gRPC API • Raft FSM • registry |
                  | scheduler • reconciler • stats |
                  +---------------+---------------+
                                  |
                    Raft replication between talosd nodes
                                  |
             +--------------------+--------------------+
             |                                         |
+------------v-----------+                 +-----------v------------+
| talos-agent             |                 | talos-agent            |
| image/layer store        |                 | image/layer store       |
| process + optional       |                 | process + optional      |
| chroot/cgroup handling   |                 | chroot/cgroup handling  |
+--------------------------+                 +-------------------------+
```

## What it implements

- Content-addressed local image builds, layer storage, and registry push/pull.
- A Raft-replicated control-plane state machine for nodes, workloads, assignments, and build-cache metadata.
- gRPC control, registry, membership, and agent APIs using the repository's JSON codec.
- Least-loaded scheduling that filters dead nodes and enforces advertised CPU and memory capacity.
- Periodic reconciliation that replaces replicas assigned to dead or missing nodes.
- Workload apply, scale, delete, node/workload listing, and container-log retrieval through `talos`.
- Prometheus metrics from `talosd` (default `:9090`).

## Prerequisites

- Go 1.25 or newer.
- Linux for the agent runtime.
- `bash` for the demo script.
- Root privileges and a cgroup v2 host only when exercising `--chroot` or CPU/memory limits.

## Quickstart

```bash
go build ./...
./scripts/demo.sh
```

The demo builds `talos`, `talosd`, and `talos-agent` in `.talos-bin`, starts a three-node control plane and three agents, deploys `deploy.yaml`, and prints workload and node status. Before running it, make `demo:latest` available in the local image store used by the agents; the script does not build or distribute that image. It then stops an agent and a control-plane node to exercise failure handling. The script cleans up the processes it started on exit.

To run the commands manually after starting the services:

```bash
.talos-bin/talos deploy -f deploy.yaml
.talos-bin/talos ps
.talos-bin/talos nodes
.talos-bin/talos logs demo 0
.talos-bin/talos scale demo 2
.talos-bin/talos delete demo
```

## Workload specification

```yaml
name: web
image: hello:latest
replicas: 3
env: ["KEY=VALUE"]
resources:
  request: { cpu: "250m", memory: "128Mi" }
  limit:   { cpu: "500m", memory: "256Mi" }
```

Requests are used for placement. Limits are passed to the agent for cgroup configuration when cgroup support is available.

## Runtime and security limitations

Talos is for local experimentation and education, **not** for hosting untrusted workloads or operating a production cluster.

- gRPC has no authentication, authorization, TLS, tenancy, or network policy.
- Image provenance, signature verification, admission control, secrets management, and production-grade audit logging are absent.
- Without `--chroot`, an agent runs image commands as ordinary host processes rooted in the extracted image directory; it does not create Linux namespaces. With `--chroot`, namespace creation and cgroup use require suitable Linux privileges and are not a complete sandbox.
- Cgroup limits rely on a writable cgroup v2 hierarchy. The runtime does not manage all host resource controllers.
- Node capacity and liveness are agent-reported; this project does not provide hardened failure detection or transactional cleanup across node failures.
- The generated-looking RPC Go code is hand-written for the JSON codec; `proto/workload.proto` documents that implemented control API rather than driving code generation.

## Development validation

```bash
gofmt -w $(find . -name '*.go' -not -path './.git/*')
go build ./...
go vet ./...
go test ./...
```
