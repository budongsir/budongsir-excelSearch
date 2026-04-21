#!/usr/bin/env python3
"""Search the local Excel archive SQLite database."""

from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
from pathlib import Path


DEFAULT_CONFIG = Path("archive_data/config.json")


def open_path(path: Path) -> None:
    subprocess.run(["open", str(path)], check=False)


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
    return conn


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
) -> tuple[list[str], list[object]]:
    params: list[object] = []
    where: list[str] = []

    if query:
        if len(query.strip()) >= 3:
            where.append(
                "(items.id IN (SELECT rowid FROM item_fts WHERE search_text MATCH ?) "
                "OR items.search_text LIKE ?)"
            )
            params.extend([query, f"%{query}%"])
        else:
            where.append("items.search_text LIKE ?")
            params.append(f"%{query}%")
    if year:
        where.append("items.year_label = ?")
        params.append(year)
    if category:
        where.append("items.category_path LIKE ?")
        params.append(f"{category}%")
    return where, params


def search_items(
    conn: sqlite3.Connection,
    query: str = "",
    year: str = "",
    category: str = "",
    limit: int = 100,
) -> list[sqlite3.Row]:
    where, params = build_item_filters(query=query, year=year, category=category)

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


def matching_years(conn: sqlite3.Connection, query: str = "", category: str = "") -> list[str]:
    where, params = build_item_filters(query=query, category=category)
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


def print_results(conn: sqlite3.Connection, rows: list[sqlite3.Row]) -> None:
    for row in rows:
        title = row["item_name"] or "(无物品名称)"
        code = row["item_code"] or row["committee_code"] or row["accession_no"] or ""
        date = row["work_date_raw"] or row["inbound_date_raw"] or ""
        print(f"#{row['id']} {title} {code}")
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
    conn: sqlite3.Connection | None = connect(db_path) if db_path.exists() else None

    root = tk.Tk()
    root.title("Excel 档案全文搜索")
    root.geometry("1200x760")

    query_var = tk.StringVar()
    year_var = tk.StringVar()
    category_var = tk.StringVar()
    status_var = tk.StringVar()
    current_rows: list[sqlite3.Row] = []
    thumbnail_refs: list[ImageTk.PhotoImage] = []

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
    query_entry = ttk.Entry(top, textvariable=query_var, width=32)
    query_entry.pack(side="left", padx=(4, 12))
    ttk.Label(top, text="年份").pack(side="left")
    year_box = ttk.Combobox(top, textvariable=year_var, values=[""] + load_years(), width=14)
    year_box.pack(side="left", padx=(4, 12))
    ttk.Label(top, text="目录").pack(side="left")
    category_box = ttk.Combobox(top, textvariable=category_var, values=[""] + load_categories(), width=34)
    category_box.pack(side="left", padx=(4, 12))

    main = ttk.Panedwindow(root, orient="horizontal")
    main.pack(fill="both", expand=True, padx=8, pady=4)
    left = ttk.Frame(main)
    right = ttk.Frame(main)
    main.add(left, weight=3)
    main.add(right, weight=2)

    columns = ("id", "name", "code", "location", "handler", "date", "source", "images")
    tree = ttk.Treeview(left, columns=columns, show="headings", height=24)
    headings = {
        "id": "ID",
        "name": "物品名称",
        "code": "编号",
        "location": "保存地点",
        "handler": "经手人",
        "date": "日期",
        "source": "来源文件",
        "images": "图片",
    }
    widths = {"id": 60, "name": 260, "code": 110, "location": 120, "handler": 90, "date": 90, "source": 260, "images": 50}
    for col in columns:
        tree.heading(col, text=headings[col])
        tree.column(col, width=widths[col], anchor="w")
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
            status_var.set("未找到数据库，请点击“设置路径”选择 archive.sqlite。")
        else:
            status_var.set(f"数据库: {db_path}    图片: {images_dir}    Excel: {excel_root}")

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
                limit=500,
            )
        except Exception as exc:
            messagebox.showerror("搜索失败", str(exc))
            return
        tree.delete(*tree.get_children())
        for row in current_rows:
            tree.insert(
                "",
                "end",
                iid=str(row["id"]),
                values=(
                    row["id"],
                    row["item_name"] or "",
                    row["item_code"] or row["committee_code"] or row["accession_no"] or "",
                    row["storage_location"] or "",
                    row["handler_name"] or "",
                    row["work_date_raw"] or row["inbound_date_raw"] or "",
                    Path(row["source_path"]).name,
                    row["image_count"],
                ),
            )
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

    def open_settings() -> None:
        nonlocal conn, db_path, images_dir, excel_root, config
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
            nonlocal conn, db_path, images_dir, excel_root, config
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
            conn = connect(db_path)
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

    ttk.Button(top, text="搜索", command=do_search).pack(side="left")
    ttk.Button(top, text="打开原 Excel", command=open_source).pack(side="left", padx=(8, 0))
    ttk.Button(top, text="设置路径", command=open_settings).pack(side="left", padx=(8, 0))
    ttk.Label(root, textvariable=status_var).pack(fill="x", padx=8, pady=(0, 6))

    query_entry.bind("<Return>", do_search)
    year_box.bind("<<ComboboxSelected>>", do_search)
    category_box.bind("<<ComboboxSelected>>", do_search)
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
