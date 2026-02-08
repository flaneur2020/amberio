#!/usr/bin/env python3
"""[006] Range read contract when meta.json has no parts[]."""

from __future__ import annotations

import datetime
import hashlib
import json
import math
import sqlite3
import uuid
from pathlib import Path

from _harness import build_case_parser, cluster_from_args, expect_status, parse_json_body, quote_blob_path


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def utc_now_rfc3339() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def ensure_schema_has_part_no(db_path: Path) -> set[str]:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("PRAGMA table_info(file_entries)").fetchall()
        return {row[1] for row in rows}
    finally:
        conn.close()


def insert_file_entry(db_path: Path, values: dict[str, object]) -> None:
    conn = sqlite3.connect(db_path)
    try:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(file_entries)").fetchall()}
        present_keys = [key for key in values.keys() if key in columns]
        sql = (
            "INSERT OR REPLACE INTO file_entries ("
            + ",".join(present_keys)
            + ") VALUES ("
            + ",".join("?" for _ in present_keys)
            + ")"
        )
        conn.execute(sql, [values[key] for key in present_keys])
        conn.commit()
    finally:
        conn.close()


def main() -> None:
    parser = build_case_parser("006", "RFC003 range read without parts[]")
    parser.set_defaults(nodes=1, min_write_replicas=1)
    args = parser.parse_args()

    with cluster_from_args(args) as cluster:
        blob_path = f"cases/rfc003/003/{uuid.uuid4().hex}.bin"
        encoded_path = quote_blob_path(blob_path)

        resolve_response = cluster.external_request(
            0,
            "GET",
            f"/slots/resolve?path={blob_path}",
        )
        expect_status(resolve_response.status, {200}, "resolve slot")
        resolve_payload = parse_json_body(resolve_response)
        slot_id = resolve_payload.get("slot_id")
        if not isinstance(slot_id, int):
            raise AssertionError(f"resolve payload missing slot_id: {resolve_payload}")

        _ = cluster.internal_request(
            0,
            "GET",
            f"/slots/{slot_id}/heads?path={encoded_path}",
        )

        slot_root = cluster.nodes[0].data_dir / "slots" / str(slot_id)
        db_path = slot_root / "meta.sqlite3"
        if not db_path.exists():
            raise AssertionError(f"meta.sqlite3 not found: {db_path}")

        columns = ensure_schema_has_part_no(db_path)
        if "part_no" not in columns:
            raise AssertionError("RFC003 contract requires file_entries.part_no column")

        generation = 1
        part_size = 256 * 1024
        body = (b"rfc003-range-read-" * 100000)[:1_000_000]
        part_count = math.ceil(len(body) / part_size)

        object_dir = slot_root / "blobs" / blob_path / f"g.{generation}"
        object_dir.mkdir(parents=True, exist_ok=True)

        now = utc_now_rfc3339()

        for part_no in range(part_count):
            start = part_no * part_size
            end = min(start + part_size, len(body))
            part_body = body[start:end]
            part_sha = sha256_hex(part_body)
            file_name = f"part.{part_no:08d}.{part_sha}"
            part_path = object_dir / file_name
            part_path.write_bytes(part_body)

            insert_file_entry(
                db_path,
                {
                    "slot_id": slot_id,
                    "blob_path": blob_path,
                    "file_name": file_name,
                    "file_kind": "part",
                    "storage_kind": "external",
                    "inline_data": None,
                    "external_path": str(part_path),
                    "archive_url": None,
                    "size_bytes": len(part_body),
                    "sha256": part_sha,
                    "generation": generation,
                    "etag": None,
                    "created_at": now,
                    "updated_at": now,
                    "part_no": part_no,
                },
            )

        meta = {
            "path": blob_path,
            "slot_id": slot_id,
            "generation": generation,
            "version": generation,
            "size_bytes": len(body),
            "etag": sha256_hex(body),
            "part_size": part_size,
            "part_count": part_count,
            "part_index_state": "complete",
            "updated_at": now,
        }
        inline_data = json.dumps(meta, separators=(",", ":")).encode("utf-8")
        head_sha = sha256_hex(inline_data)

        insert_file_entry(
            db_path,
            {
                "slot_id": slot_id,
                "blob_path": blob_path,
                "file_name": "meta.json",
                "file_kind": "meta",
                "storage_kind": "inline",
                "inline_data": inline_data,
                "external_path": None,
                "archive_url": None,
                "size_bytes": len(body),
                "sha256": head_sha,
                "generation": generation,
                "etag": meta["etag"],
                "created_at": now,
                "updated_at": now,
            },
        )

        range_start = 111_111
        range_end = 333_333
        range_response = cluster.external_request(
            0,
            "GET",
            f"/blobs/{encoded_path}",
            headers={"range": f"bytes={range_start}-{range_end}"},
        )

        expect_status(range_response.status, {206}, "GET range for meta-without-parts")
        expected = body[range_start : range_end + 1]
        if range_response.body != expected:
            raise AssertionError(
                "range body mismatch: "
                f"expected {len(expected)} bytes, got {len(range_response.body)} bytes"
            )

    print("[006] PASS")


if __name__ == "__main__":
    main()
