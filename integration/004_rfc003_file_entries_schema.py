#!/usr/bin/env python3
"""[004] file_entries schema contract for RFC 0003."""

from __future__ import annotations

import sqlite3
import uuid

from _harness import build_case_parser, cluster_from_args, expect_status, parse_json_body, quote_blob_path


def main() -> None:
    parser = build_case_parser("004", "RFC003 file_entries schema contract")
    parser.set_defaults(nodes=1, min_write_replicas=1)
    args = parser.parse_args()

    with cluster_from_args(args) as cluster:
        blob_path = f"cases/rfc003/001/{uuid.uuid4().hex}.bin"
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

        db_path = cluster.nodes[0].data_dir / "slots" / str(slot_id) / "meta.sqlite3"
        if not db_path.exists():
            raise AssertionError(f"meta.sqlite3 not found: {db_path}")

        conn = sqlite3.connect(db_path)
        try:
            rows = conn.execute("PRAGMA table_info(file_entries)").fetchall()
            columns = {row[1] for row in rows}

            required_columns = {
                "slot_id",
                "blob_path",
                "file_name",
                "file_kind",
                "storage_kind",
                "inline_data",
                "external_path",
                "archive_url",
                "generation",
                "sha256",
                "size_bytes",
                "part_no",
            }
            missing = sorted(required_columns - columns)
            if missing:
                raise AssertionError(
                    "RFC003 requires file_entries columns missing: " + ", ".join(missing)
                )

            index_rows = conn.execute(
                "SELECT name, sql FROM sqlite_master WHERE type='index'"
            ).fetchall()
            has_part_lookup_index = any(
                sql
                and "file_entries" in sql
                and "part_no" in sql
                and "generation" in sql
                and "blob_path" in sql
                for _, sql in index_rows
            )
            if not has_part_lookup_index:
                raise AssertionError(
                    "RFC003 requires an index covering (slot_id, blob_path, generation, part_no)"
                )
        finally:
            conn.close()

    print("[004] PASS")


if __name__ == "__main__":
    main()
