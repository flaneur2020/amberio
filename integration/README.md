# AmberBlob Integration Tests

These tests are **contract tests** for RFC 0002 HTTP APIs.

## Prerequisites

- Redis is running at `redis://127.0.0.1:6379` (default).
- AmberBlob binary is available at `target/release/amberblob`, or pass `--build-if-missing`.

## Run all cases

```bash
python3 integration/run_all.py --build-if-missing
```

## Run one case

```bash
python3 integration/002_external_blob_crud.py --build-if-missing
```

## Notes

- Each case auto-generates cluster configs and data directories under a temporary folder.
- Redis is **not** started by scripts.
- Use `--keep-artifacts` to keep generated configs/logs for debugging.
- API prefixes default to:
  - External: `/api/v1`
  - Internal: `/internal/v1`
