# Rimio TLA+ Specs

This folder contains TLA+ specs aligned with the **current implementation**.

## Files

- `RimioWriteSemantics.tla`
  - Models external `PUT`/idempotent `PUT retry`/`DELETE` behavior.
  - Captures generation monotonicity per path.
  - Captures write idempotency cache semantics.
  - Includes invariants for monotonic generations and cache consistency.

- `RimioReplicaSelection.tla`
  - Models slot replica selection in `resolve_replica_nodes`.
  - Rotation by `slot_id % node_count`, then `take(min(3, node_count))`.

- `RimioTraceReplay.tla`
  - Deterministic replay checker for **observed integration traces**.
  - Intended to be fed by `scripts/tla/check_trace.py`.

- `*.cfg`
  - Example TLC configs for exploration.

## Running TLC (exploration)

```bash
java -cp /path/to/tla2tools.jar tlc2.TLC -config tla/RimioWriteSemantics.cfg tla/RimioWriteSemantics.tla
java -cp /path/to/tla2tools.jar tlc2.TLC -config tla/RimioReplicaSelection.cfg tla/RimioReplicaSelection.tla
```

## Trace-based checking from integration

1) Generate trace:

```bash
python3 integration/008_tla_trace_check.py --build-if-missing
```

2) Replay trace against the TLA+ checker:

```bash
python3 scripts/tla/check_trace.py \
  --trace integration/artifacts/008_trace.json \
  --tlc-jar /path/to/tla2tools.jar
```

Or do both in one command:

```bash
python3 integration/008_tla_trace_check.py \
  --build-if-missing \
  --tlc-jar /path/to/tla2tools.jar
```

## What this gives you

- Formal state invariants over your write path behavior.
- A bridge from real implementation traces to model-level validation.
- Early detection if runtime behavior drifts from the modeled semantics.

## Current limitations

- Trace capture focuses on external write/delete operations.
- `DELETE` committed replica count is inferred (minimum quorum), because API does
  not currently return committed replica count for deletes.
- This is **trace conformance checking**, not full refinement mapping.
