from __future__ import annotations

import asyncio
import json
import os
import re
import ssl
import tempfile
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from fastapi import (
    Body,
    FastAPI,
    File,
    Header,
    HTTPException,
    Query,
    Request,
    UploadFile,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from zoneinfo import ZoneInfo
import httpx
import pandas as pd
import uvicorn

# =========================================================
# Configuración base
# =========================================================

load_dotenv()

API_KEY = os.getenv("API_KEY")

# Storage sencillo en disco (si quieres GitHub como MAD luego se extiende)
DATA_DIR = Path(os.getenv("DATA_DIR_BCN", "./data")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)

TASKS_DB = Path(os.getenv("TASKS_DB_BCN", DATA_DIR / "tasks_bcn.json"))
ROSTER_XLSX_PATH = Path(os.getenv("ROSTER_XLSX_PATH_BCN", DATA_DIR / "Informe_diario_bcn.xlsx"))
ROSTER_TZ = os.getenv("ROSTER_TZ_BCN", os.getenv("ROSTER_TZ", "Europe/Madrid"))
ROSTER_POLL_SECONDS = int(os.getenv("ROSTER_POLL_SECONDS_BCN", os.getenv("ROSTER_POLL_SECONDS", "60")))
ROSTER_NIGHT_PREV_DAY = (
    os.getenv("ROSTER_NIGHT_PREV_DAY_BCN", os.getenv("ROSTER_NIGHT_PREV_DAY", "true")).lower() == "true"
)

# Fuente externa (Carga/Planificación BCN)
EXT_URL = os.getenv("EXT_URL_BCN", os.getenv("EXT_URL", "")).strip()
EXT_COOKIE = os.getenv("EXT_COOKIE_BCN", os.getenv("EXT_COOKIE", "")).strip()
EXT_REFERER = os.getenv("EXT_REFERER_BCN", os.getenv("EXT_REFERER", "")).strip()
EXT_USER_AGENT = os.getenv("EXT_USER_AGENT_BCN", os.getenv("EXT_USER_AGENT", "Mozilla/5.0"))
EXT_POLL_SECONDS = int(os.getenv("EXT_POLL_SECONDS_BCN", os.getenv("EXT_POLL_SECONDS", "60")))
EXT_VERIFY_MODE = os.getenv("EXT_VERIFY_MODE_BCN", os.getenv("EXT_VERIFY_MODE", "TRUSTSTORE")).upper()
EXT_CAFILE = os.getenv("EXT_CAFILE_BCN", os.getenv("EXT_CAFILE", "")).strip()
EXT_AUTH_MODE = os.getenv("EXT_AUTH_MODE_BCN", os.getenv("EXT_AUTH_MODE", "COOKIE")).upper()
EXT_LOGIN_URL = os.getenv("EXT_LOGIN_URL_BCN", "").strip()
EXT_LOGIN_PAYLOAD_JSON = os.getenv("EXT_LOGIN_PAYLOAD_JSON_BCN", "").strip()
EXT_LOGIN_CSRF_REGEX = os.getenv("EXT_LOGIN_CSRF_REGEX_BCN", "").strip()
EXT_USERNAME = os.getenv("EXT_USERNAME_BCN", "").strip()
EXT_PASSWORD = os.getenv("EXT_PASSWORD_BCN", "").strip()
EXT_REFRESH_CMD = os.getenv("EXT_REFRESH_CMD_BCN", "").strip()

SPANISH_DAY = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]

# =========================================================
# Utilidades generales
# =========================================================

def _now_local() -> datetime:
    return datetime.now(ZoneInfo(ROSTER_TZ))


def _safe_cell(v: Any) -> Any:
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return str(v)


def _flatten_record(d: dict, parent: str = "", sep: str = ".") -> dict:
    out = {}
    for k, v in (d or {}).items():
        key = f"{parent}{sep}{k}" if parent else k
        if isinstance(v, dict):
            out.update(_flatten_record(v, key, sep))
        elif isinstance(v, list):
            if v and all(isinstance(x, dict) for x in v):
                for i, x in enumerate(v):
                    out.update(_flatten_record(x, f"{key}[{i}]", sep))
            else:
                out[key] = _safe_cell(v)
        else:
            out[key] = v
    return out


def _atomic_write_json(path: Path, data: Any):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".tmp_", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False)
        os.replace(tmp, path)
    finally:
        try:
            if Path(tmp).exists():
                os.remove(tmp)
        except Exception:
            pass


def _read_json(path: Path, default: Any):
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return default


# =========================================================
# Modelos
# =========================================================

class Task(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    task_type: str
    status: Optional[str] = None
    title: str
    assigned_to: Optional[str] = None
    created_at: str = Field(
        default_factory=lambda: datetime.utcnow().isoformat(timespec="seconds") + "Z"
    )
    is_completed: bool = False
    category: Optional[str] = None
    note: Optional[str] = None


class TaskUpdate(BaseModel):
    status: str


class TaskCompletionUpdate(BaseModel):
    is_completed: bool


class TaskNoteUpdate(BaseModel):
    note: Optional[str] = None


# =========================================================
# Estado en memoria
# =========================================================

tasks_in_memory_store: Dict[str, Dict[str, Any]] = {}

latest_external_table: Dict[str, Any] = {
    "columns": [],
    "rows": [],
    "fetched_at": None,
    "version": 0,
}

roster_cache: Dict[str, Any] = {
    "file_mtime": None,
    "sheet_date": None,
    "shift": None,
    "people": [],
    "updated_at": None,
    "window": None,
    "sheet": None,
}

# =========================================================
# Helpers Tareas
# =========================================================

def sanitize_task(raw: dict) -> dict:
    t = dict(raw or {})
    t["id"] = str(t.get("id") or t.get("ID") or t.get("Id") or uuid.uuid4())
    t.setdefault("is_completed", False)
    return t


SERVER_FIELDS = ("is_completed", "note")


def merge_preserve_server(existing: dict | None, incoming: dict | None) -> dict:
    base = dict(existing or {})
    incoming = dict(incoming or {})
    # aplicar cambios
    for k, v in incoming.items():
        base[k] = v
    # preservar campos del servidor si no vienen en el payload
    if existing:
        for f in SERVER_FIELDS:
            if f in existing and f not in incoming:
                base[f] = existing[f]
    return sanitize_task(base)


def save_tasks_to_disk():
    payload = [sanitize_task(t) for t in tasks_in_memory_store.values()]
    _atomic_write_json(TASKS_DB, payload)


def load_tasks_from_disk():
    arr = _read_json(TASKS_DB, [])
    tasks_in_memory_store.clear()
    for t in arr or []:
        t = sanitize_task(t)
        if t.get("id") and t.get("task_type"):
            tasks_in_memory_store[t["id"]] = t
    print(f"🗂️ BCN: cargadas {len(tasks_in_memory_store)} tareas desde {TASKS_DB}")


# =========================================================
# Roster helpers (Excel BCN)
# =========================================================

def _parse_sheet_date(name: str) -> date | None:
    m = re.search(r"(\d{1,2})\D+(\d{1,2})\D+(\d{2,4})", str(name))
    if not m:
        return None
    d, mth, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if y < 100:
        y += 2000
    try:
        return date(y, mth, d)
    except ValueError:
        return None


def _list_sheet_names(path: Path) -> List[str]:
    if not path.exists():
        return []
    try:
        xl = pd.ExcelFile(path)
        return list(xl.sheet_names or [])
    except Exception as e:
        print("⚠️ BCN roster: no se pueden listar hojas:", e)
        return []


def _find_sheet_for_date(xlsx_path: Path, desired: date) -> tuple[Optional[str], List[str]]:
    names = _list_sheet_names(xlsx_path)
    if not names:
        return None, []
    parsed = [(n, _parse_sheet_date(n)) for n in names]
    for n, dte in parsed:
        if dte == desired:
            return n, names
    futures = sorted(
        [(dte, n) for n, dte in parsed if dte and dte >= desired],
        key=lambda x: x[0],
    )
    pasts = sorted(
        [(dte, n) for n, dte in parsed if dte and dte < desired],
        key=lambda x: x[0],
        reverse=True,
    )
    if futures:
        return futures[0][1], names
    if pasts:
        return pasts[0][1], names
    return None, names


def _parse_range_to_tuple(s: str) -> tuple[int, int] | None:
    if not isinstance(s, str):
        return None
    m = re.search(r"(\d{1,2})(?::?(\d{2}))?\s*[-–]\s*(\d{1,2})(?::?(\d{2}))?", s)
    if not m:
        return None
    h1, m1 = int(m.group(1)), int(m.group(2) or 0)
    h2, m2 = int(m.group(3)), int(m.group(4) or 0)
    return h1 * 60 + m1, h2 * 60 + m2


SHIFT_RANGES = {
    "Mañana": (6 * 60, 14 * 60),
    "Tarde": (14 * 60, 22 * 60),
    "Noche": (22 * 60, 6 * 60),
}


def _match_shift(horario: str, shift: str) -> bool:
    rng = _parse_range_to_tuple(horario)
    if not rng:
        return False
    s, e = rng
    S, E = SHIFT_RANGES[shift]
    if shift != "Noche":
        return (s, e) == (S, E)
    # Noche envuelve medianoche
    return (s == S and e == E) or (s == S and e in (0,)) or (s == 22 * 60 and e == 6 * 60)


def _normalize_cols(cols: List[str]) -> Dict[str, Optional[str]]:
    norm = {}
    for c in cols:
        k = c.strip().lower()
        norm[k] = c

    def pick(*cands):
        for cand in cands:
            if cand in norm:
                return norm[cand]
        return None

    return {
        "apellidos": pick("apellidos", "apellido", "apellidos/s"),
        "nombre": pick("nombre", "nombres"),
        "horario": pick("horario", "turno", "franja"),
        "observaciones": pick("observaciones", "observación", "obs"),
    }


def _read_sheet_people(xlsx_path: Path, sheet_name: str, shift: str) -> List[dict]:
    if not xlsx_path.exists():
        return []
    try:
        df = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    except Exception as e:
        print(f"⚠️ BCN roster: no se pudo leer hoja {sheet_name}:", e)
        return []

    cols = _normalize_cols(list(df.columns))
    if not (cols["horario"] and (cols["apellidos"] or cols["nombre"])):
        print("⚠️ BCN roster: columnas no esperadas en", sheet_name, "→", df.columns.tolist())
        return []

    ap_col = cols["apellidos"]
    no_col = cols["nombre"]
    ho_col = cols["horario"]
    ob_col = cols["observaciones"] or ho_col

    people = []
    for _, row in df.iterrows():
        ap = (row.get(ap_col) or "").strip() if ap_col else ""
        no = (row.get(no_col) or "").strip() if no_col else ""
        ho = (row.get(ho_col) or "").strip() if ho_col else ""
        ob = (row.get(ob_col) or "").strip() if ob_col else ""

        if not ap and not no:
            continue
        if not _match_shift(ho, shift):
            continue

        full = f"{ap}, {no}" if ap and no else (ap or no)
        people.append(
            {
                "apellidos": ap,
                "nombre": no,
                "nombre_completo": full,
                "horario": ho,
                "observaciones": ob,
            }
        )

    return people


def _current_shift_info(now: datetime):
    hhmm = now.strftime("%H:%M")
    if "06:00" <= hhmm < "14:00":
        return "Mañana", now.date(), "06:00", "14:00"
    if "14:00" <= hhmm < "22:00":
        return "Tarde", now.date(), "14:00", "22:00"
    # Noche
    if hhmm < "06:00" and ROSTER_NIGHT_PREV_DAY:
        sheet_date = now.date() - timedelta(days=1)
    else:
        sheet_date = now.date()
    return "Noche", sheet_date, "22:00", "06:00"


async def _build_roster_state(force: bool = False) -> Dict[str, Any]:
    p = ROSTER_XLSX_PATH
    mtime = p.stat().st_mtime if p.exists() else None
    now = _now_local()
    shift, sheet_date, start, end = _current_shift_info(now)

    needs_reload = (
        force
        or (mtime != roster_cache.get("file_mtime"))
        or (sheet_date != roster_cache.get("sheet_date"))
        or (shift != roster_cache.get("shift"))
    )

    if needs_reload:
        sheet_real, sheet_names = _find_sheet_for_date(ROSTER_XLSX_PATH, sheet_date)
        if not sheet_real:
            print(
                f"⚠️ BCN roster: sin hoja para {sheet_date}. Hojas detectadas: {sheet_names[:10]}"
            )
            people = []
        else:
            people = _read_sheet_people(ROSTER_XLSX_PATH, sheet_real, shift)

        roster_cache.update(
            {
                "file_mtime": mtime,
                "sheet_date": sheet_date,
                "shift": shift,
                "people": people,
                "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "window": {"from": start, "to": end},
                "sheet": sheet_real,
            }
        )

        # Broadcast WS
        await manager.broadcast(
            {
                "type": "roster_update",
                "shift": shift,
                "sheet": sheet_real,
                "sheet_date": sheet_date.isoformat(),
                "window": {"from": start, "to": end},
                "count": len(people),
                "people": people,
                "source": "excel",
                "updated_at": roster_cache["updated_at"],
            }
        )

    return roster_cache


async def _roster_watcher():
    try:
        await _build_roster_state(force=True)
    except Exception as e:
        print("⚠️ BCN roster initial load error:", e)

    while True:
        await asyncio.sleep(max(15, ROSTER_POLL_SECONDS))
        try:
            await _build_roster_state(force=False)
        except Exception as e:
            print("⚠️ BCN roster watcher error:", e)


# =========================================================
# Tabla externa (Carga/Planificación BCN)
# =========================================================

def _build_ssl_context() -> ssl.SSLContext | bool:
    mode = (EXT_VERIFY_MODE or "").upper()
    if mode == "FALSE":
        return False
    if mode == "TRUSTSTORE":
        try:
            import truststore  # type: ignore
            truststore.inject_into_ssl()
            return ssl.create_default_context()
        except Exception as e:
            print("⚠️ truststore no disponible, uso CA por defecto:", e)
            return ssl.create_default_context()
    if mode == "CAFILE" and EXT_CAFILE:
        return ssl.create_default_context(cafile=EXT_CAFILE)
    return ssl.create_default_context()


def _parse_cookie_header(header: str) -> Dict[str, str]:
    jar: Dict[str, str] = {}
    for part in header.split(";"):
        if "=" in part:
            k, v = part.split("=", 1)
            jar[k.strip()] = v.strip()
    return jar


def _list_of_dicts(obj: Any) -> List[dict]:
    if isinstance(obj, list) and (not obj or isinstance(obj[0], dict)):
        return obj
    if isinstance(obj, dict):
        for key in ("data", "items", "rows", "result", "results", "value"):
            v = obj.get(key)
            if isinstance(v, list) and (not v or isinstance(v[0], dict)):
                return v
        if all(not isinstance(v, (list, dict)) for v in obj.values()):
            return [obj]
    return []


def _table_from_json(payload: Any) -> tuple[List[str], List[List[Any]]]:
    # {"columns":[...], "rows":[...]}
    if isinstance(payload, dict) and "rows" in payload:
        rows = payload.get("rows") or []
        cols = payload.get("columns")
        if rows and isinstance(rows[0], dict):
            flat = [_flatten_record(r) for r in rows]
            seen, cols2 = set(), []
            for r in flat:
                for k in r.keys():
                    if k not in seen:
                        seen.add(k)
                        cols2.append(k)
            return cols2, [[_safe_cell(r.get(c)) for c in cols2] for r in flat]
        if rows and isinstance(rows[0], (list, tuple)):
            if not cols:
                cols = [f"col_{i+1}" for i in range(len(rows[0]))]
            matrix = []
            for r in rows:
                r = list(r)
                if len(r) < len(cols):
                    r += [None] * (len(cols) - len(r))
                matrix.append([_safe_cell(x) for x in r[: len(cols)]])
            return [str(c) for c in cols], matrix

    records = _list_of_dicts(payload)
    if records:
        flat = [_flatten_record(r) for r in records]
        seen, cols = set(), []
        for r in flat:
            for k in r.keys():
                if k not in seen:
                    seen.add(k)
                    cols.append(k)
        return cols, [[_safe_cell(r.get(c)) for c in cols] for r in flat]

    if isinstance(payload, list) and payload and isinstance(payload[0], (list, tuple)):
        cols = [f"col_{i+1}" for i in range(len(payload[0]))]
        return cols, [[_safe_cell(v) for v in r] for r in payload]

    if isinstance(payload, dict):
        flat = _flatten_record(payload)
        if flat:
            cols = list(flat.keys())
            return cols, [[_safe_cell(flat.get(c)) for c in cols]]

    return [], []


async def apply_external_table(payload: Any):
    cols, rows = _table_from_json(payload)
    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    latest_external_table["columns"] = cols
    latest_external_table["rows"] = rows
    latest_external_table["fetched_at"] = ts
    latest_external_table["version"] = int(latest_external_table.get("version", 0)) + 1

    await manager.broadcast(
        {
            "type": "table_ping",
            "table": "external",
            "version": latest_external_table["version"],
            "rows": len(rows),
            "fetched_at": ts,
        }
    )


@dataclass
class ExternalSettings:
    urls: List[str] = field(default_factory=list)
    referer: str = ""
    user_agent: str = "Mozilla/5.0"
    cookie_header: str = ""
    poll_seconds: int = 60
    verify_mode: str = "TRUSTSTORE"
    cafile: str = ""
    auth_mode: str = "COOKIE"
    login_url: str = ""
    login_payload_json: str = ""
    login_csrf_regex: str = ""
    username: str = ""
    password: str = ""
    refresh_cmd: str = ""
    http2: bool = True
    max_keepalive: int = 20
    max_connections: int = 40

    @classmethod
    def from_env(cls) -> "ExternalSettings":
        urls_env = os.getenv("EXT_URLS_BCN", "") or os.getenv("EXT_URLS", "")
        urls = [u.strip() for u in urls_env.split(",") if u.strip()]
        if not urls and EXT_URL:
            urls = [EXT_URL]
        return cls(
            urls=urls,
            referer=EXT_REFERER,
            user_agent=EXT_USER_AGENT,
            cookie_header=EXT_COOKIE,
            poll_seconds=EXT_POLL_SECONDS,
            verify_mode=EXT_VERIFY_MODE,
            cafile=EXT_CAFILE,
            auth_mode=EXT_AUTH_MODE,
            login_url=EXT_LOGIN_URL,
            login_payload_json=EXT_LOGIN_PAYLOAD_JSON,
            login_csrf_regex=EXT_LOGIN_CSRF_REGEX,
            username=EXT_USERNAME,
            password=EXT_PASSWORD,
            refresh_cmd=EXT_REFRESH_CMD,
        )


class ExternalConnector:
    def __init__(self):
        self.settings = ExternalSettings.from_env()
        self._client: httpx.AsyncClient | None = None
        self._url_idx = 0
        self._consec_fail = 0
        self._status: Dict[str, Any] = {
            "ok": False,
            "last_ok": None,
            "last_error": None,
            "fails": 0,
            "url": self.current_url,
        }

    @property
    def current_url(self) -> str:
        return self.settings.urls[self._url_idx] if self.settings.urls else ""

    def _ssl_verify(self):
        return _build_ssl_context()

    def _cookies_jar(self):
        return _parse_cookie_header(self.settings.cookie_header) if self.settings.cookie_header else {}

    async def _ensure_client(self, recycle: bool = False):
        if self._client is not None and not recycle:
            return
        if self._client is not None and recycle:
            try:
                await self._client.aclose()
            except Exception:
                pass

        cookies = httpx.Cookies()
        for k, v in self._cookies_jar().items():
            cookies.set(k, v)

        transport = httpx.AsyncHTTPTransport(
            retries=0,
            http2=self.settings.http2,
            limits=httpx.Limits(
                max_connections=self.settings.max_connections,
                max_keepalive_connections=self.settings.max_keepalive,
                keepalive_expiry=60.0,
            ),
        )

        self._client = httpx.AsyncClient(
            verify=self._ssl_verify(),
            cookies=cookies,
            transport=transport,
            headers={
                "User-Agent": self.settings.user_agent,
                "Accept": "application/json, text/plain, */*",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": self.settings.referer or None,
            },
        )

    async def _fetch_once(self):
        if not self.current_url:
            return
        await self._ensure_client()
        assert self._client is not None

        try:
            resp = await self._client.get(self.current_url, timeout=30.0)
            if resp.status_code in (401, 403, 419, 440) or resp.status_code >= 500:
                raise httpx.HTTPStatusError("auth_or_5xx", request=resp.request, response=resp)

            try:
                data = resp.json()
            except Exception:
                print("⚠️ BCN external: respuesta no JSON, ignorada")
                return

            await apply_external_table(data)

            self._consec_fail = 0
            ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
            self._status.update(ok=True, last_ok=ts, last_error=None, fails=0, url=self.current_url)
            await manager.broadcast(
                {"type": "external_status", "ok": True, "url": self.current_url, "ts": ts}
            )

        except Exception as e:
            self._consec_fail += 1
            self._status.update(
                ok=False,
                last_error=str(e),
                fails=self._consec_fail,
                url=self.current_url,
            )
            await manager.broadcast(
                {
                    "type": "external_status",
                    "ok": False,
                    "url": self.current_url,
                    "error": str(e),
                    "fails": self._consec_fail,
                }
            )
            # rotar URL si hay varias
            if len(self.settings.urls) > 1:
                self._url_idx = (self._url_idx + 1) % len(self.settings.urls)
                print(f"🔀 BCN external rota a {self.current_url}")

    async def run(self):
        await self._ensure_client()
        base_delay = max(5, self.settings.poll_seconds)
        while True:
            try:
                await self._fetch_once()
                delay = base_delay
            except Exception:
                # backoff exponencial suave
                step = min(self._consec_fail, 6)
                delay = max(10, min(300, 2**step))
            await asyncio.sleep(delay)

    def status(self) -> Dict[str, Any]:
        return dict(self._status)


# =========================================================
# WebSocket manager
# =========================================================

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        print(f"🔌 WS BCN conectado. Activos: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            print(f"🔌 WS BCN desconectado. Activos: {len(self.active_connections)}")

    async def broadcast(self, data: Dict[str, Any]):
        for conn in list(self.active_connections):
            try:
                await conn.send_json(data)
            except Exception:
                self.disconnect(conn)

    async def send_one(self, websocket: WebSocket, data: Dict[str, Any]):
        try:
            await websocket.send_json(data)
        except Exception:
            self.disconnect(websocket)


manager = ConnectionManager()

# =========================================================
# Lifespan (como Madrid: un solo sitio con startup/shutdown)
# =========================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 BCN: iniciando sistema...")
    load_tasks_from_disk()

    # lanzar watchers
    app.state._roster_task = asyncio.create_task(_roster_watcher())
    app.state._ext = ExternalConnector()
    if app.state._ext.current_url:
        app.state._ext_task = asyncio.create_task(app.state._ext.run())
    else:
        app.state._ext_task = None

    print("🚀 BCN: sistema iniciado")
    yield
    print("🛑 BCN: deteniendo sistema...")

    for key in ("_roster_task", "_ext_task"):
        task: asyncio.Task = getattr(app.state, key, None)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    print("🛑 BCN: sistema detenido correctamente")


# =========================================================
# FastAPI app
# =========================================================

app = FastAPI(title="WFS BCN Dashboard", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from fastapi import UploadFile, File, Header
from io import BytesIO
import pandas as pd
import uuid
from typing import Optional

# ... aquí ya tienes: tasks_in_memory_store, sanitize_task, save_tasks_to_disk, manager, API_KEY ...

def _norm(s: str) -> str:
    return (s or "").strip().lower()

def _pick(colmap, *names):
    for n in names:
        if _norm(n) in colmap:
            return colmap[_norm(n)]
    return None

@app.post("/api/gw/import-xlsx")
async def import_gw_xlsx(
    file: UploadFile = File(...),
    x_api_key: Optional[str] = Header(None)
):
    # opcional: protege con tu clave del dashboard
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")

    fname = (file.filename or "").lower()
    if not (fname.endswith(".xlsx") or fname.endswith(".xls")):
        raise HTTPException(status_code=400, detail="Sube un .xlsx/.xls")

    raw = await file.read()
    try:
        df = pd.read_excel(BytesIO(raw), dtype=str)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"No se pudo leer el Excel: {e}")

    if df.empty:
        raise HTTPException(status_code=400, detail="El Excel está vacío")

    # Mapa normalizado de columnas
    colmap = {_norm(c): c for c in df.columns}

    col_country   = _pick(colmap, "country")
    col_desc      = _pick(colmap, "gemba walk description")
    col_ap1_due   = _pick(colmap, "ap1 due date")
    col_ap1_days  = _pick(colmap, "ap1 days left/overdue")
    col_ap1_resp  = _pick(colmap, "ap1 responsible")

    required = [col_desc, col_ap1_due, col_ap1_days]
    if not all(required):
        missing = []
        if not col_desc:     missing.append("Gemba Walk description")
        if not col_ap1_due:  missing.append("AP1 Due Date")
        if not col_ap1_days: missing.append("AP1 Days Left/Overdue")
        raise HTTPException(
            status_code=400,
            detail=f"Faltan columnas obligatorias: {', '.join(missing)}"
        )

    # Borra GW antiguos de MAD para no duplicar (solo gw_task de esta estación)
    to_delete = [
        tid for tid, t in tasks_in_memory_store.items()
        if t.get("task_type") == "gw_task"
        and str(t.get("station", "")).upper() == "MAD"
    ]
    for tid in to_delete:
        tasks_in_memory_store.pop(tid, None)

    new_tasks = []
    today = pd.Timestamp.utcnow().normalize()

    for _, row in df.iterrows():
        desc = str(row.get(col_desc, "") or "").strip()
        due_raw = row.get(col_ap1_due)
        days_raw = row.get(col_ap1_days)

        if not desc:
            continue

        # Due date → ISO
        try:
            due = pd.to_datetime(due_raw, dayfirst=True, errors="coerce")
        except Exception:
            due = pd.NaT

        if pd.isna(due):
            # sin fecha no nos sirve como GW accionable
            continue

        due_date = due.date().isoformat()

        # Days left / overdue → int (permite negativos)
        days_left = None
        if days_raw not in (None, "", "nan"):
            try:
                days_left = int(float(str(days_raw).replace(",", ".").strip()))
            except Exception:
                pass

        # Si no viene days_left, lo calculamos
        if days_left is None:
            days_left = int((due.normalize() - today).days)

        resp = (str(row.get(col_ap1_resp)) or "").strip()
        country = (str(row.get(col_country)) or "").strip()

        note_parts = []
        if resp:
            note_parts.append(f"Resp: {resp}")
        note_parts.append(f"Vence: {due_date}")
        note_parts.append(f"Días restantes: {days_left}")
        if country:
            note_parts.append(f"Country: {country}")
        note = " • ".join(note_parts)

        # ID estable por contenido (para que la misma fila no genere IDs nuevos)
        stable_id_seed = f"MAD|{desc}|{due_date}|{country}"
        tid = str(uuid.uuid5(uuid.NAMESPACE_DNS, stable_id_seed))

        task = sanitize_task({
            "id": tid,
            "task_type": "gw_task",
            "title": desc,           # lo que mostraremos en GW
            "station": "MAD",
            "gw_date": due_date,     # usamos para ordenar
            "due_date": due_date,
            "ap1_days_left": days_left,
            "note": note,
            "is_completed": False,
        })

        tasks_in_memory_store[task["id"]] = task
        new_tasks.append(task)

    save_tasks_to_disk()

    # Notifica por WebSocket al dashboard
    for t in new_tasks:
        await manager.broadcast(t)

    return {
        "ok": True,
        "deleted_old": len(to_delete),
        "inserted": len(new_tasks)
    }



# =========================================================
# Endpoints Tareas
# =========================================================

@app.get("/api/tasks")
async def get_all_tasks():
    return [sanitize_task(t) for t in tasks_in_memory_store.values()]


@app.post("/api/tasks", response_model=Task, status_code=201)
async def create_task(task: Task):
    t = sanitize_task(task.dict())
    tasks_in_memory_store[t["id"]] = t
    save_tasks_to_disk()
    await manager.broadcast(t)
    return Task(**t)


@app.put("/api/tasks/{task_id}", response_model=Task)
async def update_task_status(task_id: str, upd: TaskUpdate):
    if task_id not in tasks_in_memory_store:
        raise HTTPException(status_code=404, detail="Tarea no encontrada")
    cur = tasks_in_memory_store[task_id]
    cur["status"] = upd.status
    cur = sanitize_task(cur)
    tasks_in_memory_store[task_id] = cur
    save_tasks_to_disk()
    await manager.broadcast(cur)
    return Task(**cur)


@app.put("/api/tasks/{task_id}/complete", response_model=Task)
async def update_task_completion(task_id: str, upd: TaskCompletionUpdate):
    if task_id not in tasks_in_memory_store:
        raise HTTPException(status_code=404, detail="Tarea no encontrada")
    cur = tasks_in_memory_store[task_id]
    cur["is_completed"] = upd.is_completed
    cur = sanitize_task(cur)
    tasks_in_memory_store[task_id] = cur
    save_tasks_to_disk()
    await manager.broadcast(cur)
    return Task(**cur)


@app.put("/api/tasks/{task_id}/note", response_model=Task)
async def update_task_note(task_id: str, upd: TaskNoteUpdate):
    if task_id not in tasks_in_memory_store:
        raise HTTPException(status_code=404, detail="Tarea no encontrada")
    cur = tasks_in_memory_store[task_id]
    cur["note"] = upd.note or ""
    cur = sanitize_task(cur)
    tasks_in_memory_store[task_id] = cur
    save_tasks_to_disk()
    await manager.broadcast(cur)
    return Task(**cur)


@app.delete("/api/tasks/{task_id}", status_code=204)
async def delete_task(task_id: str):
    if task_id not in tasks_in_memory_store:
        raise HTTPException(status_code=404, detail="Tarea no encontrada")
    task_data = tasks_in_memory_store.pop(task_id)
    save_tasks_to_disk()
    await manager.broadcast(
        {"id": str(task_id), "action": "delete", "task_type": task_data.get("task_type")}
    )
    return {}


# =========================================================
# Webhook SharePoint (BCN)
# =========================================================

def _json_lenient(text: str):
    if not isinstance(text, str) or not text.strip():
        return None
    s = text.lstrip("\ufeff").strip()
    s = s.replace("“", '"').replace("”", '"').replace("’", "'")
    try:
        return json.loads(s)
    except Exception:
        try:
            s2 = re.sub(r",(\s*[}\]])", r"\1", s)
            return json.loads(s2)
        except Exception:
            return None


def _first_list_of_dicts(obj: Any) -> List[dict]:
    if obj is None:
        return []
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        preferred = [
            "body",
            "value",
            "values",
            "items",
            "data",
            "results",
            "records",
            "array",
            "tasks",
            "rows",
        ]
        for k in preferred:
            if k in obj:
                r = _first_list_of_dicts(obj[k])
                if r:
                    return r
        for v in obj.values():
            r = _first_list_of_dicts(v)
            if r:
                return r
        if {"id", "title"} & set(obj.keys()):
            return [obj]
    if isinstance(obj, str):
        j = _json_lenient(obj)
        return _first_list_of_dicts(j)
    return []


def _extract_tasks_flex(data: Any) -> List[dict]:
    candidates = _first_list_of_dicts(data)
    useful_keys = {"id", "title", "task_type", "station", "warehouse", "gw_date"}
    return [t for t in candidates if useful_keys & set(t.keys())]


@app.post("/webhook/sharepoint-bulk-update")
async def sharepoint_bulk_update(request: Request, x_api_key: Optional[str] = Header(None)):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")

    raw = await request.body()
    ct = (request.headers.get("content-type") or "").lower()
    text = raw.decode("utf-8", errors="ignore") if raw else ""

    # intentar JSON directo
    payload: Any = None
    try:
        payload = json.loads(text)
    except Exception:
        # intentar lenient + x-www-form-urlencoded/multipart ya gestionado en _json_lenient
        payload = _json_lenient(text) or text

    tasks_raw = _extract_tasks_flex(payload)
    if not tasks_raw:
        preview = (text or "")[:4000]
        raise HTTPException(
            status_code=400,
            detail=f"No se encontraron tareas en el payload. Preview: {preview!r}",
        )

    def _boolish(v):
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return v != 0
        if isinstance(v, str):
            return v.strip().lower() in {
                "1",
                "true",
                "yes",
                "y",
                "ok",
                "done",
                "si",
                "sí",
            }
        return False

    def _to_task(item: dict) -> dict:
        _id = (
            item.get("id")
            or item.get("ID")
            or item.get("Id")
            or str(uuid.uuid5(uuid.NAMESPACE_URL, json.dumps(item, sort_keys=True)))
        )
        title = (
            item.get("title")
            or item.get("Title")
            or item.get("task")
            or "Tarea"
        )
        completed = (
            item.get("is_completed")
            or item.get("completed")
            or item.get("done")
        )
        return sanitize_task(
            {
                "id": str(_id),
                "task_type": str(item.get("task_type") or "gw_task"),
                "title": str(title),
                "note": item.get("note") or "",
                "is_completed": _boolish(completed),
            }
        )

    upserts = []
    for it in tasks_raw:
        mapped = _to_task(it)
        prev = tasks_in_memory_store.get(mapped["id"])
        merged = merge_preserve_server(prev, mapped)
        tasks_in_memory_store[merged["id"]] = merged
        upserts.append(merged)

    save_tasks_to_disk()
    for t in upserts:
        await manager.broadcast(t)

    # Opcional: también usamos ese payload como tabla externa
    try:
        await apply_external_table(tasks_raw)
    except Exception as e:
        print("⚠️ BCN apply_external_table desde webhook error:", e)

    return {
        "ok": True,
        "received": len(tasks_raw),
        "upserts": len(upserts),
        "ids": [t["id"] for t in upserts][:200],
    }


# =========================================================
# API tabla externa & roster
# =========================================================

@app.get("/api/external-table")
async def get_external_table():
    return latest_external_table


@app.get("/api/external-status")
def api_external_status():
    ext = getattr(app.state, "_ext", None)
    return ext.status() if ext else {"ok": False, "last_error": "not_started"}


@app.get("/api/roster/current")
async def get_roster_current():
    state = await _build_roster_state(force=False)
    return {
        "shift": state.get("shift"),
        "sheet": state.get("sheet"),
        "sheet_date": state.get("sheet_date").isoformat()
        if state.get("sheet_date")
        else None,
        "window": state.get("window"),
        "people": state.get("people", []),
        "updated_at": state.get("updated_at"),
    }


@app.get("/api/roster/sheets")
def api_roster_sheets():
    return {
        "path": str(ROSTER_XLSX_PATH),
        "sheets": _list_sheet_names(ROSTER_XLSX_PATH),
    }


# =========================================================
# WebSocket
# =========================================================

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # estado inicial de tabla externa si existe
        if latest_external_table.get("version", 0) > 0:
            await manager.send_one(
                websocket,
                {
                    "type": "table_state",
                    "table": "external",
                    "version": latest_external_table["version"],
                    "rows": len(latest_external_table.get("rows", [])),
                    "fetched_at": latest_external_table.get("fetched_at"),
                },
            )
        while True:
            await websocket.receive_text()  # mantener vivo
    except WebSocketDisconnect:
        manager.disconnect(websocket)


# =========================================================
# Frontend estático
# =========================================================

BASE_DIR = Path(__file__).resolve().parent
FRONTEND_DIR = BASE_DIR.parent / "frontend_bcn"
app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="static")


# =========================================================
# Local dev
# =========================================================

if __name__ == "__main__":
    uvicorn.run("main_bcn:app", host="0.0.0.0", port=8001, reload=True)

