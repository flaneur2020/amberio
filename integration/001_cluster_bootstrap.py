#!/usr/bin/env python3
"""[001] Cluster bootstrap + control plane API smoke test."""

from __future__ import annotations

from _harness import build_case_parser, cluster_from_args, expect_status, parse_json_body


def main() -> None:
    parser = build_case_parser("001", "Cluster bootstrap and API smoke")
    args = parser.parse_args()

    with cluster_from_args(args) as cluster:
        # 1) Every node should expose external health endpoint.
        for index in range(cluster.node_count):
            response = cluster.external_request(index, "GET", "/healthz")
            expect_status(response.status, {200}, f"node-{index + 1} healthz")
            payload = parse_json_body(response)
            if payload.get("status") not in {"ok", "healthy"}:
                raise AssertionError(f"node-{index + 1} healthz invalid payload: {payload}")

        # 2) Node view should include all peers in this group.
        nodes_response = cluster.external_request(0, "GET", "/nodes")
        expect_status(nodes_response.status, {200}, "list nodes")
        nodes_payload = parse_json_body(nodes_response)
        nodes = nodes_payload.get("nodes")
        if not isinstance(nodes, list):
            raise AssertionError(f"/nodes payload must contain list 'nodes': {nodes_payload}")
        if len(nodes) < cluster.node_count:
            raise AssertionError(
                f"expected at least {cluster.node_count} nodes, got {len(nodes)}: {nodes_payload}"
            )

        # 3) Slot resolve should map any blob path to one slot and replicas.
        resolve_response = cluster.external_request(
            0,
            "GET",
            "/slots/resolve?path=cases/001/bootstrap.txt",
        )
        expect_status(resolve_response.status, {200}, "resolve slot")
        resolve_payload = parse_json_body(resolve_response)
        if not isinstance(resolve_payload.get("slot_id"), int):
            raise AssertionError(f"resolve payload must include integer slot_id: {resolve_payload}")
        replicas = resolve_payload.get("replicas")
        if not isinstance(replicas, list):
            raise AssertionError(f"resolve payload must include replicas list: {resolve_payload}")

    print("[001] PASS")


if __name__ == "__main__":
    main()
