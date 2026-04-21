#!/usr/bin/env python3
"""Search the local Excel archive SQLite database."""

from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from xml.sax.saxutils import escape


DEFAULT_CONFIG = Path("archive_data/config.json")


def open_path(path: Path) -> None:
    if sys.platform == "darwin":
        subprocess.run(["open", str(path)], check=False)
    elif sys.platform.startswith("win"):
        try:
            import os

            os.startfile(path)  # type: ignore[attr-defined]
        except OSError:
            pass
    else:
        subprocess.run(["xdg-open", str(path)], check=False)


def load_config(config_path: Path) -> dict[str, str]:
    if not config_path.exists():
        return {}
    try:
        data = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return {str(key): str(value) for key, value in data.items() if value}


def save_config(config_path: Path, config: dict[str, str]) -> None:
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")


def make_config(
    config_path: Path,
    db_path: Path | None = None,
    images_dir: Path | None = None,
    excel_root: Path | None = None,
) -> dict[str, str]:
    config = load_config(config_path)
    archive_dir = config_path.parent
    if db_path is not None:
        config["database_path"] = str(db_path.resolve())
    else:
        config.setdefault("database_path", str((archive_dir / "archive.sqlite").resolve()))
    if images_dir is not None:
        config["images_dir"] = str(images_dir.resolve())
    else:
        config.setdefault("images_dir", str((Path(config["database_path"]).parent / "images").resolve()))
    if excel_root is not None:
        config["excel_root"] = str(excel_root.resolve())
    else:
        config.setdefault("excel_root", str(Path.cwd().resolve()))
    return config


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(items)")}
    if "matched_at" not in columns:
        conn.execute("ALTER TABLE items ADD COLUMN matched_at TEXT")
    if "matched_note" not in columns:
        conn.execute("ALTER TABLE items ADD COLUMN matched_note TEXT")
    conn.commit()


def beijing_now() -> str:
    return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")


def split_query_terms(query: str) -> list[str]:
    return [term for term in query.split() if term]


def resolve_image_path(stored_path: str, images_dir: Path) -> Path:
    path = Path(stored_path)
    if path.is_absolute() and path.exists():
        return path
    if path.is_absolute():
        parts = path.parts
        if "images" in parts:
            idx = len(parts) - 1 - list(reversed(parts)).index("images")
            candidate = images_dir / Path(*parts[idx + 1 :])
            if candidate.exists():
                return candidate
    return images_dir / stored_path


def resolve_source_path(stored_path: str, excel_root: Path) -> Path:
    path = Path(stored_path)
    return path if path.is_absolute() else excel_root / stored_path


def build_item_filters(
    query: str = "",
    year: str = "",
    category: str = "",
    match_filter: str = "",
) -> tuple[list[str], list[object]]:
    params: list[object] = []
    where: list[str] = []

    for term in split_query_terms(query):
        if len(term) >= 3:
            where.append(
                "(items.id IN (SELECT rowid FROM item_fts WHERE search_text MATCH ?) "
                "OR items.search_text LIKE ?)"
            )
            params.extend([term, f"%{term}%"])
        else:
            where.append("items.search_text LIKE ?")
            params.append(f"%{term}%")
    if year:
        where.append("items.year_label = ?")
        params.append(year)
    if category:
        where.append("items.category_path LIKE ?")
        params.append(f"{category}%")
    if match_filter == "matched":
        where.append("items.matched_at IS NOT NULL AND items.matched_at <> ''")
    elif match_filter == "unmatched":
        where.append("(items.matched_at IS NULL OR items.matched_at = '')")
    return where, params


def search_items(
    conn: sqlite3.Connection,
    query: str = "",
    year: str = "",
    category: str = "",
    match_filter: str = "",
    limit: int = 100,
) -> list[sqlite3.Row]:
    where, params = build_item_filters(query=query, year=year, category=category, match_filter=match_filter)

    sql = """
        SELECT
            items.*,
            source_files.file_name,
            (
                SELECT COUNT(*) FROM item_images WHERE item_images.item_id = items.id
            ) AS image_count
        FROM items
        JOIN source_files ON source_files.id = items.source_file_id
    """
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY items.year_label, items.source_path, items.source_row LIMIT ?"
    params.append(limit)
    return list(conn.execute(sql, params))


def matching_years(conn: sqlite3.Connection, query: str = "", category: str = "", match_filter: str = "") -> list[str]:
    where, params = build_item_filters(query=query, category=category, match_filter=match_filter)
    sql = """
        SELECT DISTINCT items.year_label
        FROM items
        WHERE items.year_label IS NOT NULL AND items.year_label <> ''
    """
    if where:
        sql += " AND " + " AND ".join(where)
    sql += " ORDER BY items.year_label"
    return [row["year_label"] for row in conn.execute(sql, params)]


def item_images(conn: sqlite3.Connection, item_id: int) -> list[sqlite3.Row]:
    return list(conn.execute("SELECT * FROM item_images WHERE item_id = ? ORDER BY id", (item_id,)))


def mark_item_matched(conn: sqlite3.Connection, item_id: int) -> str:
    matched_at = beijing_now()
    conn.execute("UPDATE items SET matched_at = ? WHERE id = ?", (matched_at, item_id))
    conn.commit()
    return matched_at


def clear_item_matched(conn: sqlite3.Connection, item_id: int) -> None:
    conn.execute("UPDATE items SET matched_at = NULL, matched_note = NULL WHERE id = ?", (item_id,))
    conn.commit()
