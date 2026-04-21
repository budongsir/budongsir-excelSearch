#!/usr/bin/env python3
"""Desktop app entry point for Excel Archive Search."""

from __future__ import annotations

import traceback
import sys
from pathlib import Path

from excel_archive_search import make_config, run_gui


def default_config_path() -> Path:
    if getattr(sys, "frozen", False):
        executable = Path(sys.executable).resolve()
        if sys.platform == "darwin" and ".app" in executable.as_posix():
            # dist/ExcelArchiveSearch.app/Contents/MacOS/ExcelArchiveSearch
            app_bundle = executable.parents[2]
            return app_bundle.parent / "archive_data" / "config.json"
        return executable.parent / "archive_data" / "config.json"
    return Path.cwd() / "archive_data" / "config.json"


def write_startup_error(exc: BaseException) -> None:
    candidates: list[Path] = []
    try:
        candidates.append(default_config_path().parent / "startup_error.log")
    except Exception:
        pass
    candidates.append(Path.home() / "Desktop" / "ExcelArchiveSearch_startup_error.log")
    for path in candidates:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(traceback.format_exc(), encoding="utf-8")
            return
        except Exception:
            continue


def main() -> int:
    try:
        config_path = default_config_path()
        config = make_config(config_path)
        return run_gui(config_path.resolve(), config)
    except BaseException as exc:
        write_startup_error(exc)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
