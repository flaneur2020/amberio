#!/usr/bin/env python3
"""[008] Build a write/delete trace and optionally verify with TLA+/TLC."""

from __future__ import annotations

import argparse
import subprocess
import uuid
from pathlib import Path

from _harness import (
    build_case_parser,
    cluster_from_args,
    expect_status,
    quote_blob_path,
)


def main() -> None:
    parser = build_case_parser("008", "TLA trace extraction and optional replay check")
    parser.add_argument(
        "--trace-output",
        default="",
        help="Path to write JSON trace (default: integration/artifacts/008_trace.json)",
    )
    parser.add_argument(
        "--tlc-jar",
        default="",
        help="Path to tla2tools.jar. If set, run TLA trace check.",
    )
    parser.add_argument(
        "--tla-workers",
        type=int,
        default=1,
        help="TLC worker count",
    )
    args = parser.parse_args()

    with cluster_from_args(args) as cluster:
        blob_path = f"cases/008/{uuid.uuid4().hex}.txt"
        encoded = quote_blob_path(blob_path)
        body_v1 = b"rimio-008-v1\n"
        body_v2 = b"rimio-008-v2\n"

        write_id = f"w-{uuid.uuid4()}"
        put_v1 = cluster.external_request(
            0,
            "PUT",
            f"/blobs/{encoded}",
            body=body_v1,
            headers={
                "content-type": "application/octet-stream",
                "x-rimio-write-id": write_id,
            },
        )
        expect_status(put_v1.status, {201}, "PUT v1")

        put_retry = cluster.external_request(
            0,
            "PUT",
            f"/blobs/{encoded}",
            body=body_v1,
            headers={
                "content-type": "application/octet-stream",
                "x-rimio-write-id": write_id,
            },
        )
        expect_status(put_retry.status, {200, 201}, "PUT retry")

        put_v2 = cluster.external_request(
            1 % cluster.node_count,
            "PUT",
            f"/blobs/{encoded}",
            body=body_v2,
            headers={
                "content-type": "application/octet-stream",
                "x-rimio-write-id": f"w-{uuid.uuid4()}",
            },
        )
        expect_status(put_v2.status, {201}, "PUT v2")

        delete_response = cluster.external_request(
            2 % cluster.node_count,
            "DELETE",
            f"/blobs/{encoded}",
            headers={"x-rimio-write-id": f"d-{uuid.uuid4()}"},
        )
        expect_status(delete_response.status, {200, 204}, "DELETE")

        get_deleted = cluster.external_request(0, "GET", f"/blobs/{encoded}")
        expect_status(get_deleted.status, {404, 410}, "GET after delete")

        trace_output = (
            Path(args.trace_output)
            if args.trace_output
            else Path("integration") / "artifacts" / "008_trace.json"
        )
        cluster.write_trace_json(trace_output)

    print(f"[008] trace written to {trace_output}")

    if args.tlc_jar:
        command = [
            "python3",
            "scripts/tla/check_trace.py",
            "--trace",
            str(trace_output),
            "--tlc-jar",
            args.tlc_jar,
            "--min-write-replicas",
            str(args.min_write_replicas),
            "--replicas",
            str(args.nodes),
            "--workers",
            str(args.tla_workers),
        ]
        print("[008] running TLA trace check:", " ".join(command))
        subprocess.run(command, check=True)

    print("[008] PASS")


if __name__ == "__main__":
    main()
