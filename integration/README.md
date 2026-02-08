# Amberio Integration Tests

These tests are contract tests for external/internal HTTP APIs, including RFC 0002 and RFC 0003 coverage.

## Prerequisites

- Redis is running at `redis://127.0.0.1:6379` (default).
- Amberio binary is available at `target/release/amberio`, or pass `--build-if-missing`.

## Run all cases

```bash
python3 integration/run_all.py --build-if-missing
```

## Run one case

```bash
python3 integration/002_external_blob_crud.py --build-if-missing
```

## TLA+ trace case

Case `008_tla_trace_check.py` generates a real write/delete trace from a live
cluster run and saves it as JSON. You can optionally validate this trace
against a TLA+ replay spec with TLC.

Generate trace only:

```bash
python3 integration/008_tla_trace_check.py --build-if-missing
```

Generate trace + check with TLC:

```bash
python3 integration/008_tla_trace_check.py \
  --build-if-missing \
  --tlc-jar /path/to/tla2tools.jar
```

## Notes

- Each case auto-generates cluster configs and data directories under a temporary folder.
- Redis is not started by scripts.
- Use `--keep-artifacts` to keep generated configs/logs for debugging.
- API prefixes default to:
  - External: `/api/v1`
  - Internal: `/internal/v1`
