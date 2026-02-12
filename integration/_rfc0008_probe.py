#!/usr/bin/env python3
"""Helpers for RFC0008 contract probe integration cases."""

from __future__ import annotations

import subprocess
from pathlib import Path

from _harness import REPO_ROOT


def ensure_binary(binary_path: Path, *, build_if_missing: bool) -> None:
    if binary_path.exists():
        return

    if not build_if_missing:
        raise RuntimeError(
            f"Rimio binary not found at {binary_path}. "
            "Build it first or use --build-if-missing."
        )

    command = [
        "cargo",
        "build",
        "--release",
        "-p",
        "rimio-server",
        "--bin",
        "rimio",
    ]
    print(f"[RFC0008] building binary: {' '.join(command)}")
    subprocess.run(command, cwd=REPO_ROOT, check=True)


def run_cli(binary: Path, args: list[str], *, timeout: float = 12.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(binary), *args],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        timeout=timeout,
    )


def joined_output(result: subprocess.CompletedProcess[str]) -> str:
    return f"{result.stdout}\n{result.stderr}".strip()


def is_subcommand_unimplemented(result: subprocess.CompletedProcess[str], name: str) -> bool:
    output = joined_output(result).lower()
    patterns = (
        f"unrecognized subcommand '{name}'",
        f"invalid subcommand '{name}'",
        f"unrecognized subcommand \"{name}\"",
        f"invalid subcommand \"{name}\"",
    )
    return any(pattern in output for pattern in patterns)

