#!/usr/bin/env python3
"""[018] RFC0008 contract probe: write-gate API allow/deny matrix.

Current code does not yet expose write_gate toggles. This case still executes
real API probes to establish baseline behavior and make future deltas obvious.
"""

from __future__ import annotations

from _harness import build_case_parser, cluster_from_args, expect_status


def main() -> None:
    parser = build_case_parser("018", "RFC0008 write gate API matrix placeholder")
    args = parser.parse_args()

    with cluster_from_args(args) as cluster:
        blob_path = "cases/018/rfc0008.txt"

        health = cluster.external_request(0, "GET", "/healthz")
        expect_status(health.status, {200}, "healthz")

        nodes = cluster.external_request(0, "GET", "/nodes")
        expect_status(nodes.status, {200}, "nodes")

        initial_get = cluster.external_request(0, "GET", f"/blobs/{blob_path}")
        expect_status(initial_get.status, {404}, "get before put")

        put = cluster.external_request(0, "PUT", f"/blobs/{blob_path}", body=b"rfc0008")
        expect_status(put.status, {201}, "put baseline")

        got = cluster.external_request(0, "GET", f"/blobs/{blob_path}")
        expect_status(got.status, {200}, "get after put")

        delete = cluster.external_request(0, "DELETE", f"/blobs/{blob_path}")
        expect_status(delete.status, {200, 204}, "delete baseline")

    print("[018] PASS (baseline captured; write_gate-specific assertions activate after implementation)")


if __name__ == "__main__":
    main()
