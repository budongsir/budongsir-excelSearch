#!/usr/bin/env python3
"""macOS app entry point for Excel Archive Search."""

from __future__ import annotations

import sys
from pathlib import Path

from excel_archive_search import make_config, run_gui


def default_config_path() -> Path:
    if getattr(sys, "frozen", False):
        executable = Path(sys.executable).resolve()
        # dist/ExcelArchiveSearch.app/Contents/MacOS/ExcelArchiveSearch
        app_bundle = executable.parents[2]
        return app_bundle.parent / "archive_data" / "config.json"
    return Path.cwd() / "archive_data" / "config.json"


def main() -> int:
    config_path = default_config_path()
    config = make_config(config_path)
    return run_gui(config_path.resolve(), config)


if __name__ == "__main__":
    raise SystemExit(main())
