#!/usr/bin/env python3
"""Convert an Rimio integration trace JSON into a TLC model run.

This script:
1) reads a trace JSON array,
2) materializes a generated `.cfg` for `tla/RimioTraceReplay.tla`,
3) runs TLC and exits non-zero on spec violation.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set


REPO_ROOT = Path(__file__).resolve().parents[2]
TRACE_SPEC = REPO_ROOT / "tla" / "RimioTraceReplay.tla"

IDENT_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class Event:
    kind: str
    path: str
    write_id: str
    etag: str
    generation: int
    committed_replicas: int
    quorum_reached: bool
    from_cache: bool


def _to_identifier(raw: str, prefix: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", raw)
    if not cleaned:
        cleaned = "x"
    if not re.match(r"^[A-Za-z_]", cleaned):
        cleaned = f"{prefix}_{cleaned}"
    if not IDENT_PATTERN.match(cleaned):
        cleaned = f"{prefix}_{abs(hash(raw))}"
    return cleaned


def _validate_event(index: int, raw: Dict[str, Any]) -> Event:
    required = {
        "kind",
        "path",
        "write_id",
        "etag",
        "generation",
        "committed_replicas",
        "quorum_reached",
        "from_cache",
    }
    missing = required - raw.keys()
    if missing:
        raise ValueError(f"event[{index}] missing fields: {sorted(missing)}")

    kind = str(raw["kind"])
    if kind not in {"put", "put_retry", "delete"}:
        raise ValueError(f"event[{index}] invalid kind: {kind}")

    generation = int(raw["generation"])
    committed = int(raw["committed_replicas"])
    if generation < 0:
        raise ValueError(f"event[{index}] generation must be >= 0")
    if committed < 1:
        raise ValueError(f"event[{index}] committed_replicas must be >= 1")

    return Event(
        kind=kind,
        path=str(raw["path"]),
        write_id=str(raw["write_id"]),
        etag=str(raw["etag"]),
        generation=generation,
        committed_replicas=committed,
        quorum_reached=bool(raw["quorum_reached"]),
        from_cache=bool(raw["from_cache"]),
    )


def _symbol_table(values: Iterable[str], prefix: str) -> Dict[str, str]:
    table: Dict[str, str] = {}
    used: Set[str] = set()

    for value in sorted(set(values)):
        symbol = _to_identifier(value, prefix)
        if symbol in used:
            suffix = 2
            while f"{symbol}_{suffix}" in used:
                suffix += 1
            symbol = f"{symbol}_{suffix}"
        used.add(symbol)
        table[value] = symbol

    return table


def _set_expr(symbols: Iterable[str]) -> str:
    items = sorted(set(symbols))
    return "{" + ", ".join(items) + "}" if items else "{}"


def _bool(value: bool) -> str:
    return "TRUE" if value else "FALSE"


def _render_observed(
    events: List[Event],
    path_symbols: Dict[str, str],
    write_symbols: Dict[str, str],
    etag_symbols: Dict[str, str],
) -> str:
    lines: List[str] = ["<<"]

    for idx, event in enumerate(events):
        event_lines = [
            "  [",
            f"    kind |-> \"{event.kind}\",",
            f"    path |-> {path_symbols[event.path]},",
            f"    writeId |-> {write_symbols[event.write_id]},",
            f"    etag |-> {etag_symbols[event.etag]},",
            f"    generation |-> {event.generation},",
            f"    committedReplicas |-> {event.committed_replicas},",
            f"    quorumReached |-> {_bool(event.quorum_reached)},",
            f"    fromCache |-> {_bool(event.from_cache)}",
            "  ]",
        ]
        if idx + 1 < len(events):
            event_lines[-1] += ","
        lines.extend(event_lines)

    lines.append(">>")
    return "\n".join(lines)


def _render_cfg(
    *,
    events: List[Event],
    paths: Dict[str, str],
    writes: Dict[str, str],
    etags: Dict[str, str],
    replicas: int,
    min_write_replicas: int,
) -> str:
    if replicas < 1:
        raise ValueError("--replicas must be >= 1")
    if min_write_replicas < 1:
        raise ValueError("--min-write-replicas must be >= 1")

    max_generation = max([event.generation for event in events] + [1]) + 2

    replica_symbols = [f"n{i}" for i in range(1, replicas + 1)]
    leader = replica_symbols[0]

    observed_expr = _render_observed(events, paths, writes, etags)

    return (
        "SPECIFICATION Spec\n\n"
        "CONSTANTS\n"
        f"  Paths = {_set_expr(paths.values())}\n"
        f"  WriteIds = {_set_expr(writes.values())}\n"
        f"  Etags = {_set_expr(etags.values())}\n"
        f"  Replicas = {_set_expr(replica_symbols)}\n"
        f"  Leader = {leader}\n"
        f"  MinWriteReplicas = {min_write_replicas}\n"
        f"  MaxGeneration = {max_generation}\n"
        f"  Observed = {observed_expr}\n\n"
        "INVARIANTS\n"
        "  TypeInvariant\n"
        "  ValidNeverDrops\n"
        "  IndexInBounds\n"
        "  CacheEntriesAreMetaWrites\n"
        "  TraceAccepted\n"
    )


def _run_tlc(jar: Path, cfg: Path, workers: int) -> int:
    command = [
        "java",
        "-cp",
        str(jar),
        "tlc2.TLC",
        "-workers",
        str(workers),
        "-config",
        str(cfg),
        str(TRACE_SPEC),
    ]
    print("[tla] running:", " ".join(command))
    completed = subprocess.run(command, cwd=REPO_ROOT)
    return completed.returncode


def main() -> None:
    parser = argparse.ArgumentParser(description="Check an Rimio trace with TLA+/TLC")
    parser.add_argument("--trace", required=True, help="Path to JSON trace file")
    parser.add_argument(
        "--tlc-jar",
        default="",
        help="Path to tla2tools.jar (or set TLA2TOOLS_JAR)",
    )
    parser.add_argument(
        "--min-write-replicas",
        type=int,
        default=2,
        help="Expected write quorum config used by the run",
    )
    parser.add_argument(
        "--replicas",
        type=int,
        default=3,
        help="Number of replicas to model",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="TLC workers",
    )
    parser.add_argument(
        "--keep-cfg",
        action="store_true",
        help="Keep generated cfg file next to trace",
    )
    args = parser.parse_args()

    trace_path = Path(args.trace)
    if not trace_path.exists():
        raise SystemExit(f"trace file not found: {trace_path}")

    tlc_jar = Path(args.tlc_jar or "") if args.tlc_jar else None
    if tlc_jar is None:
        env_value = os.getenv("TLA2TOOLS_JAR", "")
        tlc_jar = Path(env_value) if env_value else None

    if tlc_jar is None or not tlc_jar.exists():
        raise SystemExit(
            "tla2tools.jar not found. Pass --tlc-jar or set TLA2TOOLS_JAR."
        )

    payload = json.loads(trace_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list) or not payload:
        raise SystemExit("trace must be a non-empty JSON array")

    events = [_validate_event(index, item) for index, item in enumerate(payload)]

    path_symbols = _symbol_table((event.path for event in events), "p")
    write_symbols = _symbol_table((event.write_id for event in events), "w")
    etag_symbols = _symbol_table((event.etag for event in events), "e")

    cfg_text = _render_cfg(
        events=events,
        paths=path_symbols,
        writes=write_symbols,
        etags=etag_symbols,
        replicas=args.replicas,
        min_write_replicas=args.min_write_replicas,
    )

    cfg_path = trace_path.with_suffix(".tla.cfg")
    cfg_path.write_text(cfg_text, encoding="utf-8")
    print(f"[tla] generated cfg: {cfg_path}")

    exit_code = _run_tlc(jar=tlc_jar, cfg=cfg_path, workers=args.workers)
    if exit_code == 0:
        print("[tla] trace accepted by RimioTraceReplay")
    else:
        print("[tla] trace rejected (see TLC output above)")

    if not args.keep_cfg and cfg_path.exists():
        cfg_path.unlink()

    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
