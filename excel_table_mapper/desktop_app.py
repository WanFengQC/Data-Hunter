from __future__ import annotations

import json
import os
import queue
import random
import shutil
import socket
import string
import sys
import threading
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from openpyxl import load_workbook

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from excel_table_mapper.processor import (  # noqa: E402
    DEFAULT_STOPWORDS,
    build_table2,
    normalize_input_rows,
    read_input_rows,
    write_output,
)
from excel_table_mapper.tagger import PgConfig, Tagger  # noqa: E402


APP_TITLE = "Excel 表1 -> 表2 转换器"
APP_DIR_NAME = "ExcelTableMapper"
LOCAL_CACHE_NAME = "word_cache.sqlite3"
SEED_CACHE_NAME = "seed_word_cache.sqlite3"
DEFAULT_DB_HOST = "192.168.110.107"
DEFAULT_DB_PORT = 5432
DEFAULT_DB_NAME = "hunter"
DEFAULT_DB_USER = "postgres"
DEFAULT_DB_PASS = "123456"
DEFAULT_DB_SCHEMA = "public"
DEFAULT_DB_TABLE = "seller_sprite_word_cache"
DEFAULT_AI_MODEL = "gpt-5.4"
DEFAULT_AI_BASE_URL = "https://api.wfqc8.cn/v1"
DEFAULT_BATCH_SIZE = 50


def _runtime_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return ROOT


def _resource_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(getattr(sys, "_MEIPASS", _runtime_base_dir()))
    return ROOT


def _app_data_dir() -> Path:
    base = _runtime_base_dir()
    path = base / APP_DIR_NAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def _default_local_cache_db() -> Path:
    cache_dir = _app_data_dir() / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / LOCAL_CACHE_NAME


def _default_output_for_input(input_path: Path | None) -> Path:
    output_dir = _runtime_base_dir() / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = (input_path.stem if input_path else "result").strip() or "result"
    rand = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    return output_dir / f"{stem}_{rand}_output.xlsx"


def _seed_cache_path() -> Path:
    return _resource_dir() / "assets" / SEED_CACHE_NAME


def _ensure_local_cache_seed(local_db: Path) -> None:
    local_db.parent.mkdir(parents=True, exist_ok=True)
    if local_db.exists():
        return
    seed = _seed_cache_path()
    if seed.exists():
        shutil.copy2(seed, local_db)


def _is_lan_ip(ip: str) -> bool:
    return ip.startswith("192.168.") or ip.startswith("10.") or ip.startswith("172.")


def _detect_local_ips() -> list[str]:
    ips: set[str] = set()
    try:
        hostname = socket.gethostname()
        _, _, host_ips = socket.gethostbyname_ex(hostname)
        ips.update(host_ips)
    except Exception:
        pass
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            ips.add(s.getsockname()[0])
    except Exception:
        pass
    return sorted(ip for ip in ips if "." in ip)


def _can_connect(host: str, port: int, timeout_sec: float = 1.5) -> bool:
    try:
        with socket.create_connection((host, int(port)), timeout=timeout_sec):
            return True
    except Exception:
        return False


def _load_json(path: Path, default: dict) -> dict:
    if not path.exists():
        return dict(default)
    try:
        return {**default, **json.loads(path.read_text(encoding="utf-8"))}
    except Exception:
        return dict(default)


def _save_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


class App:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title(APP_TITLE)
        self.root.geometry("1200x760")

        self.queue: queue.Queue[tuple[str, dict]] = queue.Queue()
        self.is_processing = False
        self.sync_in_progress = False
        self.sync_done = False
        self.db_available = False
        self._suppress_mode_event = False

        self.config_path = _app_data_dir() / "settings.json"
        self.local_cache_default = _default_local_cache_db()
        _ensure_local_cache_seed(self.local_cache_default)

        defaults = {
            "mode": "offline",
            "enable_ai": True,
            "ai_model": DEFAULT_AI_MODEL,
            "ai_base_url": DEFAULT_AI_BASE_URL,
            "ai_batch_size": DEFAULT_BATCH_SIZE,
            "openai_api_key": "",
            "local_cache_db": str(self.local_cache_default),
            "db_host": DEFAULT_DB_HOST,
            "db_port": DEFAULT_DB_PORT,
            "db_name": DEFAULT_DB_NAME,
            "db_user": DEFAULT_DB_USER,
            "db_pass": DEFAULT_DB_PASS,
            "db_schema": DEFAULT_DB_SCHEMA,
            "db_table": DEFAULT_DB_TABLE,
        }
        self.settings = _load_json(self.config_path, defaults)
        self.output_user_customized = False

        self.input_var = tk.StringVar(value="")
        self.sheet_var = tk.StringVar(value="")
        self.output_var = tk.StringVar(value=str(_default_output_for_input(None)))

        self.mode_var = tk.StringVar(value=self.settings.get("mode", "offline"))
        self.enable_ai_var = tk.BooleanVar(value=bool(self.settings.get("enable_ai", True)))

        self.status_var = tk.StringVar(value="待处理")
        self.progress_text_var = tk.StringVar(value="")

        self._build_ui()
        self.root.after(80, self._startup_probe_and_set_mode)
        self.root.after(100, self._poll_queue)

    def _build_ui(self) -> None:
        frm = ttk.Frame(self.root, padding=10)
        frm.pack(fill=tk.BOTH, expand=True)

        row = 0
        ttk.Label(frm, text="输入文件").grid(row=row, column=0, sticky="w", padx=(0, 8), pady=4)
        ttk.Entry(frm, textvariable=self.input_var).grid(row=row, column=1, sticky="ew", pady=4)
        ttk.Button(frm, text="选择", command=self._choose_input).grid(row=row, column=2, padx=(8, 0), pady=4)

        row += 1
        ttk.Label(frm, text="输入Sheet").grid(row=row, column=0, sticky="w", padx=(0, 8), pady=4)
        self.sheet_combo = ttk.Combobox(frm, textvariable=self.sheet_var, values=[], state="readonly")
        self.sheet_combo.grid(row=row, column=1, sticky="ew", pady=4)

        row += 1
        ttk.Label(frm, text="输出文件").grid(row=row, column=0, sticky="w", padx=(0, 8), pady=4)
        ttk.Entry(frm, textvariable=self.output_var).grid(row=row, column=1, sticky="ew", pady=4)
        ttk.Button(frm, text="另存为", command=self._choose_output).grid(row=row, column=2, padx=(8, 0), pady=4)

        row += 1
        mode_bar = ttk.Frame(frm)
        mode_bar.grid(row=row, column=0, columnspan=3, sticky="ew", pady=(8, 4))
        ttk.Label(mode_bar, text="运行模式").pack(side=tk.LEFT)
        ttk.Radiobutton(
            mode_bar,
            text="在线模式（本地+数据库同步）",
            value="online",
            variable=self.mode_var,
            command=self._on_mode_changed,
        ).pack(side=tk.LEFT, padx=(12, 0))
        ttk.Radiobutton(
            mode_bar,
            text="离线模式（仅本地）",
            value="offline",
            variable=self.mode_var,
            command=self._on_mode_changed,
        ).pack(side=tk.LEFT, padx=(12, 0))
        ttk.Button(mode_bar, text="配置", command=self._open_settings_dialog).pack(side=tk.RIGHT)

        row += 1
        ai_bar = ttk.Frame(frm)
        ai_bar.grid(row=row, column=0, columnspan=3, sticky="ew", pady=4)
        self.ai_check = ttk.Checkbutton(ai_bar, text="AI打标", variable=self.enable_ai_var)
        self.ai_check.pack(side=tk.LEFT)

        row += 1
        action_bar = ttk.Frame(frm)
        action_bar.grid(row=row, column=0, columnspan=3, sticky="ew", pady=8)
        self.start_btn = ttk.Button(action_bar, text="开始处理", command=self._start_processing)
        self.start_btn.pack(side=tk.LEFT)
        ttk.Label(action_bar, textvariable=self.status_var).pack(side=tk.LEFT, padx=(16, 0))

        row += 1
        ttk.Label(frm, textvariable=self.progress_text_var).grid(row=row, column=0, columnspan=3, sticky="w")

        row += 1
        self.progress = ttk.Progressbar(frm, mode="determinate", maximum=100)
        self.progress.grid(row=row, column=0, columnspan=3, sticky="ew", pady=(2, 10))

        row += 1
        ttk.Label(frm, text="结果预览（前200行）").grid(row=row, column=0, columnspan=3, sticky="w", pady=(0, 4))

        row += 1
        columns = ("单词", "频次", "打标", "mark", "Weight", "Total", "占比")
        self.tree = ttk.Treeview(frm, columns=columns, show="headings", height=20)
        for col in columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=120 if col != "mark" else 360, anchor="center")
        self.tree.grid(row=row, column=0, columnspan=3, sticky="nsew")

        y_scroll = ttk.Scrollbar(frm, orient=tk.VERTICAL, command=self.tree.yview)
        y_scroll.grid(row=row, column=3, sticky="ns")
        self.tree.configure(yscrollcommand=y_scroll.set)

        frm.columnconfigure(1, weight=1)
        frm.rowconfigure(row, weight=1)

    def _choose_input(self) -> None:
        path = filedialog.askopenfilename(filetypes=[("Excel/CSV", "*.xlsx *.xlsm *.csv"), ("All", "*.*")])
        if not path:
            return
        self.input_var.set(path)
        self._load_sheets(Path(path))
        self.output_user_customized = False
        self.output_var.set(str(_default_output_for_input(Path(path))))

    def _choose_output(self) -> None:
        default_name = _default_output_for_input(Path(self.input_var.get()) if self.input_var.get() else None).name
        path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            initialfile=default_name,
            filetypes=[("Excel", "*.xlsx"), ("CSV", "*.csv")],
        )
        if path:
            self.output_user_customized = True
            self.output_var.set(path)

    def _load_sheets(self, path: Path) -> None:
        if path.suffix.lower() not in {".xlsx", ".xlsm"}:
            self.sheet_combo["values"] = []
            self.sheet_var.set("")
            return
        try:
            wb = load_workbook(path, read_only=True, data_only=True)
            sheets = list(wb.sheetnames)
            self.sheet_combo["values"] = sheets
            self.sheet_var.set(sheets[0] if sheets else "")
            wb.close()
        except Exception as exc:
            messagebox.showerror("错误", f"读取Sheet失败: {exc}")

    def _open_settings_dialog(self) -> None:
        win = tk.Toplevel(self.root)
        win.title("配置")
        win.geometry("760x360")
        win.transient(self.root)
        win.grab_set()

        vars_map = {
            "openai_api_key": tk.StringVar(value=str(self.settings.get("openai_api_key", ""))),
            "ai_model": tk.StringVar(value=str(self.settings.get("ai_model", DEFAULT_AI_MODEL))),
            "ai_base_url": tk.StringVar(value=str(self.settings.get("ai_base_url", DEFAULT_AI_BASE_URL))),
            "ai_batch_size": tk.IntVar(value=int(self.settings.get("ai_batch_size", DEFAULT_BATCH_SIZE))),
            "local_cache_db": tk.StringVar(value=str(self.settings.get("local_cache_db", str(self.local_cache_default)))),
            "db_host": tk.StringVar(value=str(self.settings.get("db_host", DEFAULT_DB_HOST))),
            "db_port": tk.IntVar(value=int(self.settings.get("db_port", DEFAULT_DB_PORT))),
            "db_name": tk.StringVar(value=str(self.settings.get("db_name", DEFAULT_DB_NAME))),
            "db_user": tk.StringVar(value=str(self.settings.get("db_user", DEFAULT_DB_USER))),
            "db_pass": tk.StringVar(value=str(self.settings.get("db_pass", DEFAULT_DB_PASS))),
            "db_schema": tk.StringVar(value=str(self.settings.get("db_schema", DEFAULT_DB_SCHEMA))),
            "db_table": tk.StringVar(value=str(self.settings.get("db_table", DEFAULT_DB_TABLE))),
        }

        frame = ttk.Frame(win, padding=10)
        frame.pack(fill=tk.BOTH, expand=True)

        def add_row(r: int, label: str, key: str, secret: bool = False, browse: bool = False) -> None:
            ttk.Label(frame, text=label).grid(row=r, column=0, sticky="w", padx=(0, 8), pady=4)
            entry = ttk.Entry(frame, textvariable=vars_map[key], show="*" if secret else "")
            entry.grid(row=r, column=1, sticky="ew", pady=4)
            if browse:
                ttk.Button(
                    frame,
                    text="选择",
                    command=lambda: self._browse_file_var(vars_map[key]),
                ).grid(row=r, column=2, padx=(8, 0), pady=4)

        r = 0
        add_row(r, "OPENAI API Key", "openai_api_key", secret=True)
        r += 1
        add_row(r, "AI 模型", "ai_model")
        r += 1
        add_row(r, "AI Base URL", "ai_base_url")
        r += 1
        add_row(r, "批量", "ai_batch_size")
        r += 1
        add_row(r, "本地缓存数据库", "local_cache_db", browse=True)
        r += 1
        add_row(r, "DB Host", "db_host")
        r += 1

        db_line = ttk.Frame(frame)
        db_line.grid(row=r, column=0, columnspan=3, sticky="ew", pady=4)
        ttk.Label(db_line, text="Port").pack(side=tk.LEFT)
        ttk.Entry(db_line, textvariable=vars_map["db_port"], width=8).pack(side=tk.LEFT, padx=(6, 14))
        ttk.Label(db_line, text="DB").pack(side=tk.LEFT)
        ttk.Entry(db_line, textvariable=vars_map["db_name"], width=12).pack(side=tk.LEFT, padx=(6, 14))
        ttk.Label(db_line, text="User").pack(side=tk.LEFT)
        ttk.Entry(db_line, textvariable=vars_map["db_user"], width=12).pack(side=tk.LEFT, padx=(6, 14))
        ttk.Label(db_line, text="Pass").pack(side=tk.LEFT)
        ttk.Entry(db_line, textvariable=vars_map["db_pass"], width=14, show="*").pack(side=tk.LEFT, padx=(6, 0))

        r += 1
        schema_line = ttk.Frame(frame)
        schema_line.grid(row=r, column=0, columnspan=3, sticky="ew", pady=4)
        ttk.Label(schema_line, text="Schema").pack(side=tk.LEFT)
        ttk.Entry(schema_line, textvariable=vars_map["db_schema"], width=12).pack(side=tk.LEFT, padx=(6, 14))
        ttk.Label(schema_line, text="Table").pack(side=tk.LEFT)
        ttk.Entry(schema_line, textvariable=vars_map["db_table"], width=26).pack(side=tk.LEFT, padx=(6, 0))

        r += 1

        def save_and_close() -> None:
            self.settings.update({k: v.get() for k, v in vars_map.items()})
            self.settings["ai_batch_size"] = max(1, int(self.settings.get("ai_batch_size", DEFAULT_BATCH_SIZE)))
            self.settings["db_port"] = int(self.settings.get("db_port", DEFAULT_DB_PORT))
            local_db = Path(str(self.settings.get("local_cache_db", str(self.local_cache_default)))).expanduser().resolve()
            self.settings["local_cache_db"] = str(local_db)
            _ensure_local_cache_seed(local_db)
            _save_json(self.config_path, self.settings)
            self.status_var.set("配置已保存")
            self._apply_mode_state(initial=False)
            win.destroy()

        btn_row = ttk.Frame(frame)
        btn_row.grid(row=r, column=0, columnspan=3, sticky="e", pady=(12, 0))
        ttk.Button(btn_row, text="保存", command=save_and_close).pack(side=tk.RIGHT)

        frame.columnconfigure(1, weight=1)

    def _browse_file_var(self, var: tk.StringVar) -> None:
        chosen = filedialog.asksaveasfilename(
            defaultextension=".sqlite3",
            filetypes=[("SQLite", "*.sqlite3"), ("All", "*.*")],
            initialfile=LOCAL_CACHE_NAME,
        )
        if chosen:
            var.set(chosen)

    def _on_mode_changed(self) -> None:
        if self._suppress_mode_event:
            return
        current_mode = str(self.settings.get("mode", "offline"))
        target_mode = self.mode_var.get()
        if target_mode == "offline" and current_mode == "online" and self.db_available:
            confirm = messagebox.askyesno(
                "切换确认",
                "已成功连接数据库，推荐使用在线模式。\n\n"
                "切换到离线模式会导致：\n"
                "1) AI新打标结果不会写入数据库\n"
                "2) 多台电脑缓存无法自动同步\n"
                "3) 其他同事无法复用你新增的缓存\n\n"
                "确认切换到离线模式吗？",
            )
            if not confirm:
                self._set_mode_var("online")
                return
        self._apply_mode_state(initial=False)

    def _apply_mode_state(self, initial: bool) -> None:
        mode = self.mode_var.get()
        self.settings["mode"] = mode
        self.settings["enable_ai"] = bool(self.enable_ai_var.get())
        _save_json(self.config_path, self.settings)

        if mode == "online":
            ok, reason = self._check_online_ready()
            self.db_available = ok
            if not ok:
                if not initial:
                    messagebox.showwarning(
                        "提示",
                        "连接数据库失败，已切换到离线模式；如果要使用在线模式，请检查数据库配置后重试。\n\n"
                        f"失败原因：{reason}",
                    )
                self._set_mode_var("offline")
                self.settings["mode"] = "offline"
                _save_json(self.config_path, self.settings)
                self.sync_done = True
                self.sync_in_progress = False
                self.status_var.set("离线模式")
                self._update_start_button_state()
                return
            self._start_online_sync_if_needed(force=not initial)
        else:
            self.sync_in_progress = False
            self.sync_done = True
            self.status_var.set("离线模式")
            self.progress_text_var.set("")
            self.progress["value"] = 0
            self._update_start_button_state()

    def _startup_probe_and_set_mode(self) -> None:
        ok, reason = self._check_online_ready()
        self.db_available = ok
        if ok:
            self._set_mode_var("online")
            self._apply_mode_state(initial=False)
            return
        self._set_mode_var("offline")
        self._apply_mode_state(initial=True)
        messagebox.showwarning(
            "提示",
            "连接数据库失败，已切换到离线模式；如果要使用在线模式，请检查数据库配置后重试。\n\n"
            f"失败原因：{reason}",
        )

    def _set_mode_var(self, mode: str) -> None:
        self._suppress_mode_event = True
        self.mode_var.set(mode)
        self._suppress_mode_event = False

    def _check_online_ready(self) -> tuple[bool, str]:
        host = str(self.settings.get("db_host", DEFAULT_DB_HOST)).strip() or DEFAULT_DB_HOST
        port = int(self.settings.get("db_port", DEFAULT_DB_PORT))
        ips = _detect_local_ips()
        if not any(_is_lan_ip(ip) for ip in ips):
            return False, "当前网络不在局域网"
        if not _can_connect(host, port):
            return False, f"无法连接数据库 {host}:{port}"
        return True, ""

    def _start_online_sync_if_needed(self, force: bool = False) -> None:
        if self.sync_in_progress:
            return
        if self.sync_done and not force:
            self._update_start_button_state()
            return

        self.sync_in_progress = True
        self.sync_done = False
        self.status_var.set("在线模式初始化中：同步数据库缓存...")
        self.progress_text_var.set("正在后台同步数据库缓存到本地，请稍候")
        self.progress["value"] = 0
        self._update_start_button_state()

        def worker() -> None:
            try:
                local_db = Path(str(self.settings.get("local_cache_db", str(self.local_cache_default)))).expanduser().resolve()
                _ensure_local_cache_seed(local_db)
                tagger = self._build_tagger(enable_ai=False)
                result = tagger.sync_all_from_db_to_local(
                    progress_callback=lambda p: self.queue.put(("sync_progress", p))
                )
                self.queue.put(("sync_done", result))
            except Exception as exc:
                self.queue.put(("sync_error", {"error": str(exc)}))

        threading.Thread(target=worker, daemon=True).start()

    def _build_tagger(self, *, enable_ai: bool) -> Tagger:
        mode = self.mode_var.get()
        local_db = Path(str(self.settings.get("local_cache_db", str(self.local_cache_default)))).expanduser().resolve()
        _ensure_local_cache_seed(local_db)

        pg = PgConfig(
            enabled=mode == "online",
            host=str(self.settings.get("db_host", DEFAULT_DB_HOST)),
            port=int(self.settings.get("db_port", DEFAULT_DB_PORT)),
            dbname=str(self.settings.get("db_name", DEFAULT_DB_NAME)),
            user=str(self.settings.get("db_user", DEFAULT_DB_USER)),
            password=str(self.settings.get("db_pass", DEFAULT_DB_PASS)),
            schema=str(self.settings.get("db_schema", DEFAULT_DB_SCHEMA)),
            table=str(self.settings.get("db_table", DEFAULT_DB_TABLE)),
        )

        return Tagger(
            enable_ai=enable_ai,
            model=str(self.settings.get("ai_model", DEFAULT_AI_MODEL)),
            base_url=str(self.settings.get("ai_base_url", DEFAULT_AI_BASE_URL)),
            batch_size=max(1, int(self.settings.get("ai_batch_size", DEFAULT_BATCH_SIZE))),
            local_cache_db=local_db,
            pg_config=pg,
            persist_local_on_ai=(mode == "online"),
            api_key=str(self.settings.get("openai_api_key", "")).strip() or None,
        )

    def _start_processing(self) -> None:
        if self.is_processing:
            return
        if self.mode_var.get() == "online" and (self.sync_in_progress or not self.sync_done):
            messagebox.showinfo("提示", "在线模式缓存同步尚未完成，暂时不能开始处理。")
            return

        input_path = Path(self.input_var.get().strip()) if self.input_var.get().strip() else None
        if not input_path or not input_path.exists():
            messagebox.showerror("错误", "请输入有效的输入文件路径。")
            return

        output_text = self.output_var.get().strip()
        if not self.output_user_customized:
            output_path = _default_output_for_input(input_path)
            self.output_var.set(str(output_path))
        else:
            output_path = Path(output_text) if output_text else _default_output_for_input(input_path)
        if not output_path.suffix:
            output_path = output_path.with_suffix(".xlsx")
        self.output_var.set(str(output_path))

        self.is_processing = True
        self.status_var.set("处理中...")
        self.progress_text_var.set("准备开始")
        self.progress["value"] = 0
        self._update_start_button_state()
        self.settings["enable_ai"] = bool(self.enable_ai_var.get())
        _save_json(self.config_path, self.settings)

        def worker() -> None:
            try:
                enable_ai = bool(self.enable_ai_var.get())
                tagger = self._build_tagger(enable_ai=enable_ai)

                raw_rows = read_input_rows(input_path, self.sheet_var.get().strip() or None)
                normalized = normalize_input_rows(raw_rows, DEFAULT_STOPWORDS)

                def progress_cb(payload: dict) -> None:
                    self.queue.put(("process_progress", payload))

                result_rows = build_table2(normalized, tagger, progress_callback=progress_cb)
                write_output(result_rows, output_path)
                self.queue.put(
                    (
                        "process_done",
                        {
                            "rows": result_rows,
                            "output": str(output_path),
                            "input_count": len(normalized),
                        },
                    )
                )
            except Exception as exc:
                self.queue.put(("process_error", {"error": str(exc)}))

        threading.Thread(target=worker, daemon=True).start()

    def _update_start_button_state(self) -> None:
        disabled = self.is_processing or (self.mode_var.get() == "online" and not self.sync_done)
        self.start_btn.configure(state=("disabled" if disabled else "normal"))

    def _poll_queue(self) -> None:
        while True:
            try:
                event, data = self.queue.get_nowait()
            except queue.Empty:
                break

            if event == "sync_progress":
                done = int(data.get("done", 0))
                total = max(1, int(data.get("total", 1)))
                pct = min(100, int(done * 100 / total))
                self.progress["value"] = pct
                self.progress_text_var.set(f"数据库同步中：{done}/{total}")

            elif event == "sync_done":
                self.sync_in_progress = False
                self.sync_done = True
                synced = int(data.get("synced_count", 0))
                self.progress["value"] = 100
                self.progress_text_var.set(f"数据库同步完成：{synced} 条")
                self.status_var.set("在线模式已就绪")
                self._update_start_button_state()

            elif event == "sync_error":
                self.sync_in_progress = False
                self.sync_done = False
                self.mode_var.set("offline")
                self.status_var.set("在线模式初始化失败，已切换离线模式")
                self.progress_text_var.set(data.get("error", ""))
                self._apply_mode_state(initial=False)
                messagebox.showerror("错误", f"在线模式初始化失败:\n{data.get('error', '')}")

            elif event == "process_progress":
                self._update_process_progress(data)

            elif event == "process_done":
                self.is_processing = False
                self.progress["value"] = 100
                self.status_var.set("处理完成")
                self.progress_text_var.set(f"输入{data.get('input_count', 0)}行，输出{len(data.get('rows', []))}行")
                self._update_start_button_state()
                self._refresh_preview(data.get("rows", []))
                if not self.output_user_customized and self.input_var.get().strip():
                    self.output_var.set(str(_default_output_for_input(Path(self.input_var.get().strip()))))
                messagebox.showinfo("完成", f"已输出: {data.get('output', '')}")

            elif event == "process_error":
                self.is_processing = False
                self.status_var.set("处理失败")
                self.progress_text_var.set(data.get("error", ""))
                self.progress["value"] = 0
                self._update_start_button_state()
                messagebox.showerror("错误", data.get("error", "处理失败"))

        self.root.after(100, self._poll_queue)

    def _update_process_progress(self, payload: dict) -> None:
        stage = str(payload.get("stage", ""))
        if stage == "cache_checked":
            hit = int(payload.get("cache_hit", 0))
            missing = int(payload.get("missing", 0))
            total = int(payload.get("total_words", 0))
            self.progress["value"] = 8
            self.progress_text_var.set(f"缓存检查完成：总词数{total}，命中{hit}，待AI{missing}")
            return

        if stage == "ai_batch":
            done = int(payload.get("done_words", 0))
            total = max(1, int(payload.get("total_missing", 1)))
            pct = 10 + int(done * 80 / total)
            self.progress["value"] = min(95, pct)
            self.progress_text_var.set(
                f"AI打标中：第{payload.get('batch_index', 0)}/{payload.get('total_batches', 0)}批，{done}/{total}"
            )
            return

        if stage == "ai_done":
            self.progress["value"] = 95
            self.progress_text_var.set("AI打标完成，正在写出文件")

    def _refresh_preview(self, rows: list[dict]) -> None:
        for item in self.tree.get_children():
            self.tree.delete(item)
        preview = rows[:200]
        for row in preview:
            self.tree.insert(
                "",
                "end",
                values=(
                    row.get("单词", ""),
                    row.get("频次", ""),
                    row.get("打标", ""),
                    row.get("mark", ""),
                    row.get("Weight", ""),
                    row.get("Total", ""),
                    row.get("占比", ""),
                ),
            )


def main() -> None:
    root = tk.Tk()
    App(root)
    root.mainloop()


if __name__ == "__main__":
    main()
