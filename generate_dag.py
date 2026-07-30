#!/usr/bin/env python3
"""Backward-compatible wrapper for the installed ``datahub-dag`` command."""

from dag_generator.cli import main


if __name__ == "__main__":
    raise SystemExit(main())
