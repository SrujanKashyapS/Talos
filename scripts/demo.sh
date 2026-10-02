#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-$(pwd)}"
BIN_DIR="${ROOT}/.talos-bin"
mkdir -p "${BIN_DIR}"

go build -o "${BIN_DIR}/talos" ./cmd/talos
go build -o "${BIN_DIR}/talosd" ./cmd/talosd
go build -o "${BIN_DIR}/talos-agent" ./cmd/talos-agent

pids=()
cleanup() {
  for p in "${pids[@]:-}"; do kill "${p}" 2>/dev/null || true; done
}
trap cleanup EXIT

"${BIN_DIR}/talosd" --id node1 --listen 127.0.0.1:5000 --raft-listen 127.0.0.1:7000 --bootstrap --peers "node1=127.0.0.1:7000,node2=127.0.0.1:7001,node3=127.0.0.1:7002" --metrics-listen 127.0.0.1:9090 &
pids+=($!)
"${BIN_DIR}/talosd" --id node2 --listen 127.0.0.1:5002 --raft-listen 127.0.0.1:7001 --join 127.0.0.1:5000 --metrics-listen 127.0.0.1:9092 &
pids+=($!)
"${BIN_DIR}/talosd" --id node3 --listen 127.0.0.1:5004 --raft-listen 127.0.0.1:7002 --join 127.0.0.1:5000 --metrics-listen 127.0.0.1:9094 &
pids+=($!)

"${BIN_DIR}/talos-agent" --node-id agent1 --listen 127.0.0.1:5001 --control 127.0.0.1:5000 &
pids+=($!)
"${BIN_DIR}/talos-agent" --node-id agent2 --listen 127.0.0.1:5003 --control 127.0.0.1:5002 &
pids+=($!)
"${BIN_DIR}/talos-agent" --node-id agent3 --listen 127.0.0.1:5005 --control 127.0.0.1:5004 &
pids+=($!)

sleep 5
"${BIN_DIR}/talos" deploy -f deploy.yaml
"${BIN_DIR}/talos" ps
"${BIN_DIR}/talos" nodes

echo "Kill one agent node to trigger rescheduling"
kill "${pids[4]}"
sleep 5
"${BIN_DIR}/talos" ps

echo "Kill current leader talosd to trigger re-election"
kill "${pids[0]}"
sleep 7
"${BIN_DIR}/talos" leader || true
"${BIN_DIR}/talos" ps || true
