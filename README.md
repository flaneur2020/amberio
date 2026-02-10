# Amberio

Amberio is a lightweight write-back cache layer for small on-premise clusters.

It features:

- Works with S3-based systems (SlateDB, Greptime, Databend, etc.) for on-prem and edge deployments.
- Simple setup and configuration.
- Minimal architecture with low operational overhead.
- TLA+ backed design with extensive property-based testing.

Non-goals (important):

- No full AWS S3 API compatibility (basic CRUD only).
- No full AWS IAM/ACL support.
- No multi-tenant support.
- Not built for dynamic cluster autoscaling or rebalancing (node addition and removal are not supported).

## Quick guide: bootstrap a local cluster

Prerequisites:

- Redis reachable at `redis://127.0.0.1:6379` (will remove redis dependency soon)
- Rust toolchain installed

1) Build:

```bash
cargo build --release -p amberio-server --bin amberio
```

2) Prepare config + local disks:

```bash
cp config.example.yaml config.yaml
mkdir -p demo/node1/disk demo/node2/disk demo/node3/disk
```

3) Initialize cluster state once (first wins):

```bash
./target/release/amberio server --config config.yaml --current-node node-1 --init
```

4) Start all nodes from the same config (using CLI override):

```bash
./target/release/amberio server --config config.yaml --current-node node-1
./target/release/amberio server --config config.yaml --current-node node-2
./target/release/amberio server --config config.yaml --current-node node-3
```

5) Verify:

```bash
curl http://127.0.0.1:19080/_/api/v1/healthz
curl http://127.0.0.1:19080/_/api/v1/nodes
```

## Integration check

```bash
uv run --project integration integration/run_all.py \
  --binary target/release/amberio \
  --redis-url redis://127.0.0.1:6379
```

## License

MIT
