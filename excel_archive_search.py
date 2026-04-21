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


def toggle_item_matched(conn: sqlite3.Connection, item_id: int) -> str | None:
    row = conn.execute("SELECT matched_at FROM items WHERE id = ?", (item_id,)).fetchone()
    if row is not None and row["matched_at"]:
        clear_item_matched(conn, item_id)
        return None
    return mark_item_matched(conn, item_id)


def update_match_note(conn: sqlite3.Connection, item_id: int, note: str) -> None:
    conn.execute("UPDATE items SET matched_note = ? WHERE id = ?", (note.strip() or None, item_id))
    conn.commit()


def matched_export_rows(conn: sqlite3.Connection, start_date: str, end_date: str) -> list[sqlite3.Row]:
    start = f"{start_date} 00:00:00"
    end = f"{end_date} 23:59:59"
    return list(
        conn.execute(
            """
            SELECT
                items.id,
                items.category_path,
                items.year_label,
                items.source_path,
                items.sheet_name,
                items.source_row,
                items.schema_key,
                items.sequence_no,
                items.item_code,
                items.committee_code,
                items.item_name,
                items.unit,
                items.set_count_raw,
                items.piece_count_raw,
                items.actual_quantity_raw,
                items.size_raw,
                items.accession_no,
                items.item_source,
                items.acquisition_form,
                items.storage_location,
                items.handler_name,
                items.work_date_raw,
                items.inbound_date_raw,
                items.display_part,
                items.material,
                items.manufacturer,
                items.box_no,
                items.committee_formed_time_raw,
                items.handover_unfinished,
                items.handover_finished,
                items.remark,
                items.matched_at,
                items.matched_note,
                items.extra_fields_json,
                items.raw_row_json,
                (
                    SELECT COUNT(*) FROM item_images WHERE item_images.item_id = items.id
                ) AS image_count
            FROM items
            WHERE items.matched_at >= ? AND items.matched_at <= ?
            ORDER BY items.matched_at, items.year_label, items.source_path, items.source_row
            """,
            (start, end),
        )
    )


def write_xlsx(path: Path, headers: list[str], rows: list[list[object]]) -> None:
    def col_name(index: int) -> str:
        name = ""
        while index:
            index, rem = divmod(index - 1, 26)
            name = chr(65 + rem) + name
        return name

    def cell_xml(row_index: int, col_index: int, value: object, style: int | None = None) -> str:
        ref = f"{col_name(col_index)}{row_index}"
        style_attr = f' s="{style}"' if style is not None else ""
        text = "" if value is None else str(value)
        return f'<c r="{ref}" t="inlineStr"{style_attr}><is><t>{escape(text)}</t></is></c>'

    sheet_rows = []
    sheet_rows.append(
        '<row r="1">' + "".join(cell_xml(1, idx, header, 1) for idx, header in enumerate(headers, 1)) + "</row>"
    )
    for row_idx, row in enumerate(rows, 2):
        sheet_rows.append(
            f'<row r="{row_idx}">'
            + "".join(cell_xml(row_idx, col_idx, value) for col_idx, value in enumerate(row, 1))
            + "</row>"
        )

    worksheet = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
 xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheetData>{''.join(sheet_rows)}</sheetData>
</worksheet>"""
    workbook = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
 xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets><sheet name="匹配记录" sheetId="1" r:id="rId1"/></sheets>
</workbook>"""
    workbook_rels = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>"""
    styles = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <fonts count="2"><font><sz val="11"/><name val="Calibri"/></font><font><b/><sz val="11"/><name val="Calibri"/></font></fonts>
  <fills count="1"><fill><patternFill patternType="none"/></fill></fills>
  <borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>
  <cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>
  <cellXfs count="2"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/><xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0"/></cellXfs>
</styleSheet>"""
    content_types = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
</Types>"""
    root_rels = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>"""

    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", content_types)
        zf.writestr("_rels/.rels", root_rels)
        zf.writestr("xl/workbook.xml", workbook)
        zf.writestr("xl/_rels/workbook.xml.rels", workbook_rels)
        zf.writestr("xl/styles.xml", styles)
        zf.writestr("xl/worksheets/sheet1.xml", worksheet)


def print_results(conn: sqlite3.Connection, rows: list[sqlite3.Row]) -> None:
    for row in rows:
        title = row["item_name"] or "(无物品名称)"
        code = row["item_code"] or row["committee_code"] or row["accession_no"] or ""
        date = row["work_date_raw"] or row["inbound_date_raw"] or ""
        print(f"#{row['id']} {title} {code}")
        if row["matched_at"]:
            print(f"  已匹配: {row['matched_at']} 北京时间")
        print(f"  保存地点: {row['storage_location'] or ''}  经手人: {row['handler_name'] or ''}  日期: {date}")
        print(f"  来源: {row['source_path']} / {row['sheet_name']} / 第{row['source_row']}行  图片: {row['image_count']}")
        if row["remark"]:
            print(f"  备注: {row['remark']}")
        print()


def run_gui(config_path: Path, config: dict[str, str]) -> int:
    try:
        import tkinter as tk
        from tkinter import filedialog, messagebox, ttk
        from PIL import Image, ImageTk
    except Exception as exc:
        print(f"当前 Python 不支持 tkinter/Pillow，无法启动图形界面: {exc}", file=sys.stderr)
        return 2

    db_path = Path(config["database_path"])
    images_dir = Path(config["images_dir"])
    excel_root = Path(config["excel_root"])
    conn: sqlite3.Connection | None = None
    startup_error = ""
    if db_path.exists():
        try:
            conn = connect(db_path)
        except Exception as exc:
            startup_error = str(exc)

    root = tk.Tk()
    root.title("Excel 档案全文搜索")
    root.geometry("1200x760")

    query_var = tk.StringVar()
    year_var = tk.StringVar()
    category_var = tk.StringVar()
    match_filter_var = tk.StringVar(value="全部")
    status_var = tk.StringVar()
    current_rows: list[sqlite3.Row] = []
    thumbnail_refs: list[ImageTk.PhotoImage] = []
    sort_column = "id"
    sort_reverse = False

    def load_years() -> list[str]:
        if conn is None:
            return []
        return matching_years(conn)

    def load_categories() -> list[str]:
        if conn is None:
            return []
        return [row["path"] for row in conn.execute("SELECT path FROM categories ORDER BY path")]

    top = ttk.Frame(root, padding=8)
    top.pack(fill="x")
    ttk.Label(top, text="关键词").pack(side="left")
    query_entry = ttk.Entry(top, textvariable=query_var, width=22)
    query_entry.pack(side="left", padx=(4, 4))
    clear_query = ttk.Label(top, text="清除", foreground="#1f6feb", cursor="hand2")
    clear_query.pack(side="left", padx=(0, 10))
    ttk.Label(top, text="年份").pack(side="left")
    year_box = ttk.Combobox(top, textvariable=year_var, values=[""] + load_years(), width=10)
    year_box.pack(side="left", padx=(4, 10))
    ttk.Label(top, text="目录").pack(side="left")
    category_box = ttk.Combobox(top, textvariable=category_var, values=[""] + load_categories(), width=18)
    category_box.pack(side="left", padx=(4, 10))
    ttk.Label(top, text="匹配").pack(side="left")
    match_box = ttk.Combobox(top, textvariable=match_filter_var, values=["全部", "未匹配", "已匹配"], width=7, state="readonly")
    match_box.pack(side="left", padx=(4, 10))

    main = ttk.Panedwindow(root, orient="horizontal")
    main.pack(fill="both", expand=True, padx=8, pady=4)
    left = ttk.Frame(main)
    right = ttk.Frame(main)
    main.add(left, weight=3)
    main.add(right, weight=2)

    columns = ("id", "matched", "has_image", "name", "code", "location", "handler", "date", "source")
    tree = ttk.Treeview(left, columns=columns, show="headings", height=24)
    headings = {
        "id": "ID",
        "matched": "匹配",
        "has_image": "有图",
        "name": "物品名称",
        "code": "编号",
        "location": "保存地点",
        "handler": "经手人",
        "date": "日期",
        "source": "来源文件",
    }
    widths = {
        "id": 60,
        "matched": 64,
        "has_image": 52,
        "name": 250,
        "code": 110,
        "location": 120,
        "handler": 90,
        "date": 90,
        "source": 240,
    }
    for col in columns:
        tree.heading(col, text=headings[col])
        tree.column(col, width=widths[col], anchor="w")
    tree.tag_configure("matched", foreground="#0a7d28")
    tree.pack(side="left", fill="both", expand=True)
    scroll = ttk.Scrollbar(left, orient="vertical", command=tree.yview)
    scroll.pack(side="right", fill="y")
    tree.configure(yscrollcommand=scroll.set)

    detail = tk.Text(right, wrap="word", height=25)
    detail.pack(fill="both", expand=True)

    image_panel = ttk.LabelFrame(right, text="图片")
    image_panel.pack(fill="x", pady=(6, 0))
    image_canvas = tk.Canvas(image_panel, height=190, highlightthickness=0)
    image_scroll = ttk.Scrollbar(image_panel, orient="horizontal", command=image_canvas.xview)
    image_canvas.configure(xscrollcommand=image_scroll.set)
    image_canvas.pack(fill="x", expand=True)
    image_scroll.pack(fill="x")
    image_frame = ttk.Frame(image_canvas)
    image_canvas.create_window((0, 0), window=image_frame, anchor="nw")
    image_frame.bind("<Configure>", lambda _event: image_canvas.configure(scrollregion=image_canvas.bbox("all")))

    image_list = tk.Listbox(right, height=3)
    image_list.pack(fill="x", pady=(6, 0))

    def set_status() -> None:
        if conn is None:
            if startup_error:
                status_var.set(f"数据库打开失败，请点击“设置”重新选择 archive.sqlite：{startup_error}")
            else:
                status_var.set("未找到数据库，请点击“设置”选择 archive.sqlite。")
        else:
            status_var.set(f"数据库: {db_path}    图片: {images_dir}    Excel: {excel_root}")

    def current_match_filter() -> str:
        value = match_filter_var.get()
        if value == "已匹配":
            return "matched"
        if value == "未匹配":
            return "unmatched"
        return ""

    def row_value(row: sqlite3.Row, column: str) -> object:
        if column == "id":
            return int(row["id"])
        if column == "matched":
            return row["matched_at"] or ""
        if column == "has_image":
            return int(row["image_count"] or 0)
        if column == "name":
            return row["item_name"] or ""
        if column == "code":
            return row["item_code"] or row["committee_code"] or row["accession_no"] or ""
        if column == "location":
            return row["storage_location"] or ""
        if column == "handler":
            return row["handler_name"] or ""
        if column == "date":
            return row["work_date_raw"] or row["inbound_date_raw"] or ""
        if column == "source":
            return Path(row["source_path"]).name
        return row[column] if column in row.keys() else ""

    def sorted_rows() -> list[sqlite3.Row]:
        return sorted(
            current_rows,
            key=lambda row: (row_value(row, sort_column) is None, row_value(row, sort_column)),
            reverse=sort_reverse,
        )

    def render_rows() -> None:
        tree.delete(*tree.get_children())
        for row in sorted_rows():
            matched = bool(row["matched_at"])
            tree.insert(
                "",
                "end",
                iid=str(row["id"]),
                tags=("matched",) if matched else (),
                values=(
                    row["id"],
                    "✓" if matched else "☐",
                    "★" if int(row["image_count"] or 0) > 0 else "",
                    row["item_name"] or "",
                    row["item_code"] or row["committee_code"] or row["accession_no"] or "",
                    row["storage_location"] or "",
                    row["handler_name"] or "",
                    row["work_date_raw"] or row["inbound_date_raw"] or "",
                    Path(row["source_path"]).name,
                ),
            )

    def set_sort(column: str) -> None:
        nonlocal sort_column, sort_reverse
        if sort_column == column:
            sort_reverse = not sort_reverse
        else:
            sort_column = column
            sort_reverse = False
        render_rows()

    for col in columns:
        tree.heading(col, text=headings[col], command=lambda c=col: set_sort(c))

    def do_search(*_event: object) -> None:
        nonlocal current_rows
        if conn is None:
            messagebox.showwarning("未找到数据库", "请先点击“设置路径”选择 archive.sqlite。")
            return
        try:
            year_options = matching_years(
                conn,
                query=query_var.get().strip(),
                category=category_var.get().strip(),
                match_filter=current_match_filter(),
            )
        except Exception as exc:
            messagebox.showerror("年份过滤失败", str(exc))
            return
        current_year = year_var.get().strip()
        year_box.configure(values=[""] + year_options)
        if current_year and current_year not in year_options:
            year_var.set("")
        try:
            current_rows = search_items(
                conn,
                query=query_var.get().strip(),
                year=year_var.get().strip(),
                category=category_var.get().strip(),
                match_filter=current_match_filter(),
                limit=500,
            )
        except Exception as exc:
            messagebox.showerror("搜索失败", str(exc))
            return
        render_rows()
        status_var.set(f"找到 {len(current_rows)} 条结果    数据库: {db_path}")

    def selected_row() -> sqlite3.Row | None:
        selected = tree.selection()
        if not selected:
            return None
        item_id = int(selected[0])
        for row in current_rows:
            if int(row["id"]) == item_id:
                return row
        return None

    def show_detail(*_event: object) -> None:
        nonlocal thumbnail_refs
        row = selected_row()
        if row is None or conn is None:
            return
        imgs = item_images(conn, int(row["id"]))
        detail.delete("1.0", "end")
        raw_json = json.dumps(json.loads(row["raw_row_json"] or "{}"), ensure_ascii=False, indent=2)
        lines = [
            f"匹配状态: {'✓ 已匹配 ' + row['matched_at'] if row['matched_at'] else '☐ 未匹配'}",
            f"匹配备注: {row['matched_note'] or ''}",
            "",
            f"物品名称: {row['item_name'] or ''}",
            f"物品编号: {row['item_code'] or ''}",
            f"冬奥组委编号: {row['committee_code'] or ''}",
            f"入库编号: {row['accession_no'] or ''}",
            f"单位/数量: {row['unit'] or ''} {row['set_count_raw'] or ''} {row['piece_count_raw'] or ''} {row['actual_quantity_raw'] or ''}",
            f"尺寸: {row['size_raw'] or ''}",
            f"物品来源: {row['item_source'] or ''}",
            f"物品形式: {row['acquisition_form'] or ''}",
            f"保存地点: {row['storage_location'] or ''}",
            f"经手人: {row['handler_name'] or ''}",
            f"日期: {row['work_date_raw'] or row['inbound_date_raw'] or ''}",
            f"材质: {row['material'] or ''}",
            f"厂家: {row['manufacturer'] or ''}",
            f"箱号: {row['box_no'] or ''}",
            f"备注: {row['remark'] or ''}",
            "",
            f"来源目录: {row['category_path'] or ''}",
            f"来源文件: {row['source_path']}",
            f"Sheet: {row['sheet_name']}",
            f"原始行号: {row['source_row']}",
            "",
            "原始行数据:",
            raw_json,
        ]
        extras = json.loads(row["extra_fields_json"] or "{}")
        if extras:
            lines.extend(["", "扩展字段:", json.dumps(extras, ensure_ascii=False, indent=2)])
        detail.insert("1.0", "\n".join(lines))

        image_list.delete(0, "end")
        for child in image_frame.winfo_children():
            child.destroy()
        thumbnail_refs = []
        if not imgs:
            ttk.Label(image_frame, text="无图片").pack(side="left", padx=8, pady=8)
            return
        for img in imgs:
            path = resolve_image_path(img["image_path"], images_dir)
            image_list.insert("end", str(path))
            if not path.exists():
                ttk.Label(image_frame, text=f"找不到图片: {path.name}").pack(side="left", padx=8, pady=8)
                continue
            try:
                pil_image = Image.open(path)
                pil_image.thumbnail((160, 160))
                photo = ImageTk.PhotoImage(pil_image)
            except Exception:
                continue
            thumbnail_refs.append(photo)
            holder = ttk.Frame(image_frame, padding=4)
            holder.pack(side="left", padx=4, pady=4)
            label = ttk.Label(holder, image=photo, cursor="hand2")
            label.pack()
            label.bind("<Button-1>", lambda _event, image_path=path: open_path(image_path))
            ttk.Label(holder, text=path.name, width=22).pack()

    def open_selected_image(*_event: object) -> None:
        selected = image_list.curselection()
        if selected:
            open_path(Path(image_list.get(selected[0])))

    def open_source() -> None:
        row = selected_row()
        if row is None:
            return
        source_path = resolve_source_path(row["source_path"], excel_root)
        if not source_path.exists():
            messagebox.showwarning("找不到原始 Excel", f"请在“设置路径”中重新指定原始 Excel 根目录。\n\n{source_path}")
            return
        open_path(source_path)

    def refresh_tree_row(item_id: int) -> sqlite3.Row | None:
        if conn is None:
            return None
        updated = conn.execute(
            """
            SELECT
                items.*,
                source_files.file_name,
                (
                    SELECT COUNT(*) FROM item_images WHERE item_images.item_id = items.id
                ) AS image_count
            FROM items
            JOIN source_files ON source_files.id = items.source_file_id
            WHERE items.id = ?
            """,
            (item_id,),
        ).fetchone()
        if updated is None:
            return None
        for index, row in enumerate(current_rows):
            if int(row["id"]) == item_id:
                current_rows[index] = updated
                break
        render_rows()
        return updated

    def toggle_selected_matched() -> None:
        nonlocal current_rows
        if conn is None:
            return
        row = selected_row()
        if row is None:
            messagebox.showinfo("请选择记录", "请先在左侧结果列表中选择一条记录。")
            return
        item_id = int(row["id"])
        matched_at = toggle_item_matched(conn, int(row["id"]))
        if current_match_filter():
            do_search()
        else:
            refresh_tree_row(item_id)
        if tree.exists(str(item_id)):
            tree.selection_set(str(item_id))
            tree.focus(str(item_id))
            show_detail()
        if matched_at:
            status_var.set(f"已标记匹配成功：#{row['id']}  北京时间 {matched_at}")
        else:
            status_var.set(f"已取消匹配标记：#{row['id']}")

    def edit_match_note() -> None:
        if conn is None:
            return
        row = selected_row()
        if row is None:
            messagebox.showinfo("请选择记录", "请先在左侧结果列表中选择一条记录。")
            return
        window = tk.Toplevel(root)
        window.title("匹配备注")
        window.geometry("520x260")
        window.transient(root)
        window.grab_set()
        frame = ttk.Frame(window, padding=12)
        frame.pack(fill="both", expand=True)
        ttk.Label(frame, text=f"记录 #{row['id']}：{row['item_name'] or ''}").pack(anchor="w")
        note_text = tk.Text(frame, height=7, wrap="word")
        note_text.pack(fill="both", expand=True, pady=(8, 8))
        note_text.insert("1.0", row["matched_note"] or "")

        def save_note() -> None:
            update_match_note(conn, int(row["id"]), note_text.get("1.0", "end").strip())
            refresh_tree_row(int(row["id"]))
            if tree.exists(str(row["id"])):
                tree.selection_set(str(row["id"]))
                tree.focus(str(row["id"]))
                show_detail()
            status_var.set(f"已保存匹配备注：#{row['id']}")
            window.destroy()

        buttons = ttk.Frame(frame)
        buttons.pack(fill="x")
        ttk.Button(buttons, text="取消", command=window.destroy).pack(side="right")
        ttk.Button(buttons, text="保存", command=save_note).pack(side="right", padx=(0, 8))

    def export_matched_records() -> None:
        if conn is None:
            return
        today = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
        window = tk.Toplevel(root)
        window.title("导出匹配记录")
        window.geometry("760x430")
        window.transient(root)
        window.grab_set()
        frame = ttk.Frame(window, padding=12)
        frame.pack(fill="both", expand=True)
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(1, weight=1)
        start_var = tk.StringVar(value=today)
        end_var = tk.StringVar(value=today)
        preview_rows: list[sqlite3.Row] = []
        preview_after_id: str | None = None

        controls = ttk.Frame(frame)
        controls.grid(row=0, column=0, sticky="ew")
        ttk.Label(controls, text="开始").pack(side="left")
        ttk.Entry(controls, textvariable=start_var, width=13).pack(side="left", padx=(4, 10))
        ttk.Label(controls, text="结束").pack(side="left")
        ttk.Entry(controls, textvariable=end_var, width=13).pack(side="left", padx=(4, 10))
        preview_status = ttk.Label(controls, text="")
        preview_status.pack(side="left", padx=(6, 0))

        preview_frame = ttk.Frame(frame)
        preview_frame.grid(row=1, column=0, sticky="nsew", pady=(10, 10))
        preview_frame.columnconfigure(0, weight=1)
        preview_frame.rowconfigure(0, weight=1)
        preview = ttk.Treeview(
            preview_frame,
            columns=("name", "code", "matched_at", "note"),
            show="headings",
            height=12,
        )
        preview.heading("name", text="名称")
        preview.heading("code", text="编号")
        preview.heading("matched_at", text="匹配确认时间")
        preview.heading("note", text="备注")
        preview.column("name", width=260, anchor="w")
        preview.column("code", width=130, anchor="w")
        preview.column("matched_at", width=150, anchor="w")
        preview.column("note", width=180, anchor="w")
        preview.grid(row=0, column=0, sticky="nsew")
        preview_scroll = ttk.Scrollbar(preview_frame, orient="vertical", command=preview.yview)
        preview_scroll.grid(row=0, column=1, sticky="ns")
        preview.configure(yscrollcommand=preview_scroll.set)

        def read_export_dates(show_errors: bool = False) -> tuple[str, str] | None:
            start_date = start_var.get().strip()
            end_date = end_var.get().strip()
            try:
                datetime.strptime(start_date, "%Y-%m-%d")
                datetime.strptime(end_date, "%Y-%m-%d")
            except ValueError:
                preview_status.configure(text="日期格式：YYYY-MM-DD")
                if show_errors:
                    messagebox.showerror("日期格式错误", "请使用 YYYY-MM-DD 格式。", parent=window)
                return None
            return start_date, end_date

        def refresh_preview(show_errors: bool = False) -> bool:
            nonlocal preview_rows
            dates = read_export_dates(show_errors=show_errors)
            preview.delete(*preview.get_children())
            if dates is None:
                preview_rows = []
                return False
            start_date, end_date = dates
            preview_rows = matched_export_rows(conn, start_date, end_date)
            for row in preview_rows:
                code = row["item_code"] or row["committee_code"] or row["accession_no"] or ""
                preview.insert(
                    "",
                    "end",
                    values=(
                        row["item_name"] or "",
                        code,
                        row["matched_at"] or "",
                        row["matched_note"] or "",
                    ),
                )
            preview_status.configure(text=f"将导出 {len(preview_rows)} 条记录")
            return True

        def schedule_preview(*_event: object) -> None:
            nonlocal preview_after_id
            if preview_after_id is not None:
                window.after_cancel(preview_after_id)

            def delayed_preview() -> None:
                nonlocal preview_after_id
                preview_after_id = None
                refresh_preview()

            preview_after_id = window.after(350, delayed_preview)

        def do_export() -> None:
            if not refresh_preview(show_errors=True):
                return
            if not preview_rows:
                messagebox.showinfo("没有记录", "所选时间段没有匹配记录。", parent=window)
                return
            start_date = start_var.get().strip()
            end_date = end_var.get().strip()
            output = filedialog.asksaveasfilename(
                title="保存导出文件",
                defaultextension=".xlsx",
                initialfile=f"匹配记录_{start_date}_{end_date}.xlsx",
                filetypes=[("Excel 文件", "*.xlsx")],
                parent=window,
            )
            if not output:
                return
            headers = [
                "ID",
                "目录",
                "年份",
                "来源文件",
                "Sheet",
                "原始行号",
                "字段结构",
                "序号",
                "物品编号",
                "冬奥组委编号",
                "物品名称",
                "单位",
                "件套数",
                "件数",
                "实际数量",
                "尺寸",
                "入库编号",
                "物品来源",
                "物品形式",
                "保存地点",
                "经手人",
                "工作日期",
                "入库日期",
                "上展部位",
                "材质",
                "厂家",
                "箱号",
                "冬奥组委形成时间",
                "未交接",
                "已交接",
                "备注",
                "图片数",
                "扩展字段JSON",
                "原始行JSON",
            ]
            keys = [
                "id",
                "category_path",
                "year_label",
                "source_path",
                "sheet_name",
                "source_row",
                "schema_key",
                "sequence_no",
                "item_code",
                "committee_code",
                "item_name",
                "unit",
                "set_count_raw",
                "piece_count_raw",
                "actual_quantity_raw",
                "size_raw",
                "accession_no",
                "item_source",
                "acquisition_form",
                "storage_location",
                "handler_name",
                "work_date_raw",
                "inbound_date_raw",
                "display_part",
                "material",
                "manufacturer",
                "box_no",
                "committee_formed_time_raw",
                "handover_unfinished",
                "handover_finished",
                "remark",
                "image_count",
                "extra_fields_json",
                "raw_row_json",
            ]
            write_xlsx(Path(output), headers, [[row[key] for key in keys] for row in preview_rows])
            status_var.set(f"已导出 {len(preview_rows)} 条匹配记录：{output}")
            window.destroy()

        buttons = ttk.Frame(frame)
        buttons.grid(row=2, column=0, sticky="e")
        ttk.Button(buttons, text="取消", command=window.destroy).pack(side="right")
        ttk.Button(buttons, text="导出", command=do_export).pack(side="right", padx=(0, 8))
        start_var.trace_add("write", schedule_preview)
        end_var.trace_add("write", schedule_preview)
        refresh_preview()

    def on_tree_click(event: object) -> None:
        if not hasattr(event, "x") or not hasattr(event, "y"):
            return
        region = tree.identify("region", event.x, event.y)
        if region != "cell":
            return
        column = tree.identify_column(event.x)
        row_id = tree.identify_row(event.y)
        if column == "#2" and row_id:
            tree.selection_set(row_id)
            tree.focus(row_id)
            toggle_selected_matched()
            return "break"

    def open_settings() -> None:
        nonlocal conn, db_path, images_dir, excel_root, config, startup_error
        window = tk.Toplevel(root)
        window.title("设置路径")
        window.geometry("760x230")
        window.transient(root)
        window.grab_set()

        db_var = tk.StringVar(value=str(db_path))
        images_var = tk.StringVar(value=str(images_dir))
        excel_var = tk.StringVar(value=str(excel_root))
        form = ttk.Frame(window, padding=12)
        form.pack(fill="both", expand=True)
        form.columnconfigure(1, weight=1)

        def add_row(label: str, variable: tk.StringVar, command: object, row_index: int) -> None:
            ttk.Label(form, text=label, width=14).grid(row=row_index, column=0, sticky="w", pady=6)
            ttk.Entry(form, textvariable=variable, width=78).grid(row=row_index, column=1, sticky="ew", pady=6)
            ttk.Button(form, text="选择", command=command).grid(row=row_index, column=2, padx=(8, 0), pady=6)

        def choose_db() -> None:
            selected = filedialog.askopenfilename(
                title="选择 archive.sqlite",
                filetypes=[("SQLite 数据库", "*.sqlite *.db"), ("所有文件", "*.*")],
            )
            if selected:
                db_var.set(selected)
                candidate_images = Path(selected).parent / "images"
                if candidate_images.exists():
                    images_var.set(str(candidate_images))

        def choose_images() -> None:
            selected = filedialog.askdirectory(title="选择 images 目录")
            if selected:
                images_var.set(selected)

        def choose_excel() -> None:
            selected = filedialog.askdirectory(title="选择原始 Excel 根目录")
            if selected:
                excel_var.set(selected)

        add_row("数据库", db_var, choose_db, 0)
        add_row("图片目录", images_var, choose_images, 1)
        add_row("Excel 根目录", excel_var, choose_excel, 2)

        def apply_settings() -> None:
            nonlocal conn, db_path, images_dir, excel_root, config, startup_error
            new_db = Path(db_var.get()).expanduser()
            new_images = Path(images_var.get()).expanduser()
            new_excel = Path(excel_var.get()).expanduser()
            if not new_db.exists():
                messagebox.showerror("数据库不存在", str(new_db), parent=window)
                return
            if not new_images.exists():
                messagebox.showerror("图片目录不存在", str(new_images), parent=window)
                return
            if not new_excel.exists():
                messagebox.showerror("Excel 根目录不存在", str(new_excel), parent=window)
                return
            if conn is not None:
                conn.close()
            db_path = new_db.resolve()
            images_dir = new_images.resolve()
            excel_root = new_excel.resolve()
            try:
                conn = connect(db_path)
            except Exception as exc:
                startup_error = str(exc)
                messagebox.showerror("数据库打开失败", str(exc), parent=window)
                return
            startup_error = ""
            config = {
                "database_path": str(db_path),
                "images_dir": str(images_dir),
                "excel_root": str(excel_root),
            }
            save_config(config_path, config)
            year_box.configure(values=[""] + load_years())
            category_box.configure(values=[""] + load_categories())
            set_status()
            window.destroy()
            do_search()

        buttons = ttk.Frame(form)
        buttons.grid(row=3, column=0, columnspan=3, sticky="e", pady=(12, 0))
        ttk.Button(buttons, text="取消", command=window.destroy).pack(side="right")
        ttk.Button(buttons, text="保存", command=apply_settings).pack(side="right", padx=(0, 8))

    ttk.Button(top, text="备注", command=edit_match_note).pack(side="left", padx=(2, 0))
    ttk.Button(top, text="导出匹配", command=export_matched_records).pack(side="left", padx=(6, 0))
    ttk.Button(top, text="原Excel", command=open_source).pack(side="left", padx=(6, 0))
    ttk.Button(top, text="设置", width=4, command=open_settings).pack(side="left", padx=(6, 0))
    ttk.Label(root, textvariable=status_var).pack(fill="x", padx=8, pady=(0, 6))

    search_after_id: str | None = None

    def schedule_search(*_event: object) -> None:
        nonlocal search_after_id
        if search_after_id is not None:
            root.after_cancel(search_after_id)

        def delayed_search() -> None:
            nonlocal search_after_id
            search_after_id = None
            do_search()

        search_after_id = root.after(350, delayed_search)

    def clear_query_text(*_event: object) -> None:
        query_var.set("")
        query_entry.focus_set()

    query_entry.bind("<Return>", do_search)
    clear_query.bind("<Button-1>", clear_query_text)
    query_var.trace_add("write", schedule_search)
    year_box.bind("<<ComboboxSelected>>", do_search)
    category_box.bind("<<ComboboxSelected>>", do_search)
    match_box.bind("<<ComboboxSelected>>", do_search)
    tree.bind("<Button-1>", on_tree_click)
    tree.bind("<<TreeviewSelect>>", show_detail)
    image_list.bind("<Double-Button-1>", open_selected_image)

    set_status()
    if conn is None:
        root.after(300, open_settings)
    else:
        do_search()
    root.mainloop()
    if conn is not None:
        conn.close()
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="查询本地 Excel 档案数据库。")
    parser.add_argument("--config", default=DEFAULT_CONFIG, type=Path, help="配置文件路径。")
    parser.add_argument("--db", type=Path, help="SQLite 数据库路径。")
    parser.add_argument("--images", type=Path, help="图片目录路径。")
    parser.add_argument("--excel-root", type=Path, help="原始 Excel 根目录。")
    parser.add_argument("--query", "-q", default="", help="关键词。省略时启动图形界面。")
    parser.add_argument("--year", default="", help="年份分类筛选。")
    parser.add_argument("--category", default="", help="目录路径前缀筛选。")
    parser.add_argument("--limit", type=int, default=50, help="命令行输出条数。")
    parser.add_argument("--gui", action="store_true", help="强制启动图形界面。")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config_path = args.config.resolve()
    config = make_config(config_path, args.db, args.images, args.excel_root)
    db_path = Path(config["database_path"]).resolve()
    if args.gui or not args.query:
        return run_gui(config_path, config)
    if not db_path.exists():
        print(f"数据库不存在: {db_path}", file=sys.stderr)
        return 2
    conn = connect(db_path)
    rows = search_items(conn, query=args.query, year=args.year, category=args.category, limit=args.limit)
    print_results(conn, rows)
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
