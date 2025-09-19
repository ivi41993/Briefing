import uvicorn
import os
import uuid
import json
import re
import asyncio  # NUEVO
import ssl      # NUEVO
from datetime import datetime
from typing import Optional, List, Dict, Any

from zoneinfo import ZoneInfo
from pathlib import Path
from datetime import timedelta
import pandas as pd
import re
import shlex
from dataclasses import dataclass, field


ROSTER_XLSX_PATH = os.getenv("ROSTER_XLSX_PATH", "C:/Users/iexposito/briefing/backend/data/Informe diario.xlsx")
ROSTER_TZ = os.getenv("ROSTER_TZ", "Europe/Madrid")
ROSTER_POLL_SECONDS = int(os.getenv("ROSTER_POLL_SECONDS", "60"))
ROSTER_NIGHT_PREV_DAY = os.getenv("ROSTER_NIGHT_PREV_DAY", "true").lower() == "true"

SPANISH_DAY = ["Lunes","Martes","Miércoles","Jueves","Viernes","Sábado","Domingo"]

roster_cache: dict[str, Any] = {
    "file_mtime": None,
    "sheet_date": None,     # date usado para hoja
    "shift": None,          # 'Mañana'|'Tarde'|'Noche'
    "people": [],           # [{apellidos,nombre,nombre_completo,horario,observaciones}]
    "updated_at": None,
}

def _now_local():
    return datetime.now(ZoneInfo(ROSTER_TZ))

def _current_shift_info(now):
    hhmm = now.strftime("%H:%M")
    if "06:00" <= hhmm < "14:00":
        return "Mañana", now.date(), "06:00", "14:00"
    if "14:00" <= hhmm < "22:00":
        return "Tarde", now.date(), "14:00", "22:00"
    # Noche
    if hhmm < "06:00":
        sheet_date = now.date() - timedelta(days=1) if ROSTER_NIGHT_PREV_DAY else now.date()
    else:
        sheet_date = now.date()
    return "Noche", sheet_date, "22:00", "06:00"

def _parse_range_to_tuple(s: str) -> tuple[int,int] | None:
    if not isinstance(s, str): return None
    m = re.search(r'(\d{1,2})(?::?(\d{2}))?\s*[-–]\s*(\d{1,2})(?::?(\d{2}))?', s)
    if not m: return None
    h1 = int(m.group(1)); m1 = int(m.group(2) or 0)
    h2 = int(m.group(3)); m2 = int(m.group(4) or 0)
    return (h1*60 + m1, h2*60 + m2)

SHIFT_RANGES = {
    "Mañana": (6*60, 14*60),
    "Tarde":  (14*60, 22*60),
    "Noche":  (22*60, 6*60),  # envuelve medianoche
}

def _match_shift(horario: str, shift: str) -> bool:
    rng = _parse_range_to_tuple(horario)
    if not rng: return False
    s,e = rng
    S,E = SHIFT_RANGES[shift]
    if shift != "Noche":
        return (s, e) == (S, E)
    # noche: 22:00–06:00
    return (s == S and e == E) or (s == S and e in (0,)) or (s == 22*60 and e == 6*60)

def _normalize_cols(cols):
    norm = {}
    for c in cols:
        k = c.strip().lower()
        norm[k] = c
    # devuelve los nombres reales (tal y como están en el excel)
    def pick(*cands):
        for cand in cands:
            if cand in norm: return norm[cand]
        return None
    return {
        "apellidos": pick("apellidos","apellido","apellidos/s"),
        "nombre": pick("nombre","nombree","nombres"),
        "horario": pick("horario","turno","franja"),
        "observaciones": pick("observaciones","observación","obs","observaciones "),
    }

def _read_sheet_people(xlsx_path: str, sheet_name: str | None, shift: str) -> list[dict]:
    if not sheet_name:  # ← clave: nunca intentamos leer con None
        return []
    if not Path(xlsx_path).exists():
        return []
    try:
        df = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
        # Seguridad extra: si por cualquier motivo devolviera un dict, coge la hoja pedida
        if isinstance(df, dict):
            df = df.get(sheet_name) or next(iter(df.values()), None)
        if df is None:
            print(f"⚠️ Hoja '{sheet_name}' no encontrada o vacía.")
            return []
    except Exception as e:
        print("⚠️ No se pudo leer hoja:", sheet_name, repr(e))
        return []

    cols = _normalize_cols(list(df.columns))
    req = ["apellidos", "nombre", "horario"]
    if any(cols[k] is None for k in req):
        print("⚠️ Hoja sin columnas esperadas:", sheet_name, "→", df.columns.tolist())
        return []

    ap_col = cols["apellidos"]; no_col = cols["nombre"]; ho_col = cols["horario"]
    ob_col = cols["observaciones"] or cols["horario"]  # si no hay observaciones, no fallar

    people = []
    for _, row in df.iterrows():
        ap = (row.get(ap_col) or "").strip()
        no = (row.get(no_col) or "").strip()
        ho = (row.get(ho_col) or "").strip()
        ob = (row.get(ob_col) or "").strip() if cols["observaciones"] else ""
        if not ap and not no:
            continue
        if not _match_shift(ho, shift):
            continue
        full = f"{ap}, {no}" if ap and no else (ap or no)
        people.append({
            "apellidos": ap,
            "nombre": no,
            "nombre_completo": full,
            "horario": ho,
            "observaciones": ob,
        })
    return people



def _sheet_name_for_date(d):
    return SPANISH_DAY[d.weekday()]  # Lunes..Domingo

SPANISH_DAY = ["Lunes","Martes","Miércoles","Jueves","Viernes","Sábado","Domingo"]

roster_cache: dict[str, Any] = {
    "file_mtime": None,
    "sheet_date": None,     # date usado para hoja
    "shift": None,          # 'Mañana'|'Tarde'|'Noche'
    "people": [],           # [{apellidos,nombre,nombre_completo,horario,observaciones}]
    "updated_at": None,
}

def _now_local():
    return datetime.now(ZoneInfo(ROSTER_TZ))

def _current_shift_info(now):
    hhmm = now.strftime("%H:%M")
    if "06:00" <= hhmm < "14:00":
        return "Mañana", now.date(), "06:00", "14:00"
    if "14:00" <= hhmm < "22:00":
        return "Tarde", now.date(), "14:00", "22:00"
    # Noche
    if hhmm < "06:00":
        sheet_date = now.date() - timedelta(days=1) if ROSTER_NIGHT_PREV_DAY else now.date()
    else:
        sheet_date = now.date()
    return "Noche", sheet_date, "22:00", "06:00"

def _parse_range_to_tuple(s: str) -> tuple[int,int] | None:
    if not isinstance(s, str): return None
    m = re.search(r'(\d{1,2})(?::?(\d{2}))?\s*[-–]\s*(\d{1,2})(?::?(\d{2}))?', s)
    if not m: return None
    h1 = int(m.group(1)); m1 = int(m.group(2) or 0)
    h2 = int(m.group(3)); m2 = int(m.group(4) or 0)
    return (h1*60 + m1, h2*60 + m2)

SHIFT_RANGES = {
    "Mañana": (6*60, 14*60),
    "Tarde":  (14*60, 22*60),
    "Noche":  (22*60, 6*60),  # envuelve medianoche
}

def _match_shift(horario: str, shift: str) -> bool:
    rng = _parse_range_to_tuple(horario)
    if not rng: return False
    s,e = rng
    S,E = SHIFT_RANGES[shift]
    if shift != "Noche":
        return (s, e) == (S, E)
    # noche: 22:00–06:00
    return (s == S and e == E) or (s == S and e in (0,)) or (s == 22*60 and e == 6*60)

def _normalize_cols(cols):
    norm = {}
    for c in cols:
        k = c.strip().lower()
        norm[k] = c
    # devuelve los nombres reales (tal y como están en el excel)
    def pick(*cands):
        for cand in cands:
            if cand in norm: return norm[cand]
        return None
    return {
        "apellidos": pick("apellidos","apellido","apellidos/s"),
        "nombre": pick("nombre","nombree","nombres"),
        "horario": pick("horario","turno","franja"),
        "observaciones": pick("observaciones","observación","obs","observaciones "),
    }

def _read_sheet_people(xlsx_path: str, sheet_name: str, shift: str) -> list[dict]:
    if not Path(xlsx_path).exists():
        return []
    try:
        df = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    except Exception as e:
        print("⚠️ No se pudo leer hoja:", sheet_name, repr(e))
        return []

    cols = _normalize_cols(list(df.columns))
    req = ["apellidos","nombre","horario"]
    if any(cols[k] is None for k in req):
        print("⚠️ Hoja sin columnas esperadas:", sheet_name, "→", df.columns.tolist())
        return []

    ap_col = cols["apellidos"]; no_col = cols["nombre"]; ho_col = cols["horario"]
    ob_col = cols["observaciones"] or cols["horario"]  # por si no existe, no fallar

    people = []
    for _, row in df.iterrows():
        ap = (row.get(ap_col) or "").strip()
        no = (row.get(no_col) or "").strip()
        ho = (row.get(ho_col) or "").strip()
        ob = (row.get(ob_col) or "").strip() if cols["observaciones"] else ""
        if not ap and not no: 
            continue
        if not _match_shift(ho, shift):
            continue
        full = f"{ap}, {no}" if ap and no else (ap or no)
        people.append({
            "apellidos": ap,
            "nombre": no,
            "nombre_completo": full,
            "horario": ho,
            "observaciones": ob,
        })
    return people

def _sheet_name_for_date(d):
    return SPANISH_DAY[d.weekday()]  # Lunes..Domingo


from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Header, WebSocket, WebSocketDisconnect, Request
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
import tempfile
from pathlib import Path

TASKS_DB = os.getenv("TASKS_DB", "./data/tasks_bcn.json")

def _atomic_write_json(path: str, data: list[dict]):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(p.parent), prefix=".tasks_", suffix=".json")
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

def save_tasks_to_disk():
    try:
        payload = [sanitize_task(t) for t in tasks_in_memory_store.values()]
        _atomic_write_json(TASKS_DB, payload)
        # print(f"💾 Guardadas {len(payload)} tareas en {TASKS_DB}")
    except Exception as e:
        print("⚠️ Error guardando tareas:", repr(e))

def load_tasks_from_disk():
    try:
        p = Path(TASKS_DB)
        if not p.exists():
            return
        with p.open("r", encoding="utf-8") as fh:
            arr = json.load(fh)
        tasks_in_memory_store.clear()
        for t in arr or []:
            t = sanitize_task(t)
            if t.get("id") and t.get("task_type"):
                tasks_in_memory_store[t["id"]] = t
        print(f"🗂️ Cargadas {len(tasks_in_memory_store)} tareas desde {TASKS_DB}")
    except Exception as e:
        print("⚠️ Error leyendo tareas:", repr(e))

# -----------------------------------
# Utilidades
# -----------------------------------
def _safe_cell(v):
    """Devuelve un escalar imprimible; objetos/listas -> JSON."""
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return str(v)

def _flatten_record(d: dict, parent: str = "", sep: str = ".") -> dict:
    """
    Aplana un dict anidado a claves 'a.b' y listas a 'a[0]'.
    Si encuentra listas de dicts, indexa: items[0].sku, items[1].qty, ...
    """
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
                out[key] = _safe_cell(v)  # lista de escalares -> JSON o join
        else:
            out[key] = v
    return out


def sanitize_task(raw: dict) -> dict:
    """Normaliza la tarea para cumplir con el response_model:
    - id siempre como str (acepta 'id'/'ID')
    - is_completed por defecto False
    """
    t = dict(raw or {})
    t["id"] = str(t.get("id") or t.get("ID") or t.get("Id") or "")
    t.setdefault("is_completed", False)
    return t

SERVER_FIELDS = ("is_completed", "note")

def merge_preserve_server(existing: dict | None, incoming: dict | None) -> dict:
    """Upsert que preserva campos del servidor (ticks, notas) salvo que vengan explícitos."""
    base = dict(existing or {})
    base.update(incoming or {})  # aplica cambios del lote
    if existing:
        for f in SERVER_FIELDS:
            if f in existing and f not in (incoming or {}):
                base[f] = existing[f]  # preserva tick/nota si el lote no los trae
    return sanitize_task(base)

from datetime import date  # si no lo tienes ya

# --- Helpers para hojas nombradas como dd-mm-aaaa (admite 1/2 dígitos y separadores "raros")
DATE_SHEET_RE = re.compile(r'(\d{1,2})\D+(\d{1,2})\D+(\d{2,4})')

def _parse_sheet_date(name: str) -> date | None:
    """
    Intenta extraer una fecha desde el nombre de la hoja (p.ej. '01-09-2025', '1/9/25', '23-08--2025').
    Acepta cualquier separador no numérico, día/mes de 1 o 2 dígitos y año de 2 o 4 dígitos.
    """
    if not isinstance(name, str):
        return None
    m = DATE_SHEET_RE.search(name.strip())
    if not m:
        return None
    d, mth, y = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
    if y < 100:  # normaliza años de 2 dígitos
        y += 2000
    try:
        return date(y, mth, d)
    except ValueError:
        return None

def _list_sheet_names(xlsx_path: str) -> list[str]:
    if not Path(xlsx_path).exists():
        return []
    try:
        xl = pd.ExcelFile(xlsx_path)  # usa openpyxl por defecto
        return list(xl.sheet_names or [])
    except Exception as e:
        print("⚠️ No se pudieron listar hojas:", repr(e))
        return []

def _find_sheet_for_date(xlsx_path: str, desired: date) -> tuple[str | None, list[str]]:
    """
    Devuelve (nombre_hoja, lista_nombres) para la fecha deseada.
    Si no hay exacta, devuelve la más cercana: primero la futura más próxima,
    y si no existe, la pasada más reciente. Si no hay ninguna parseable, devuelve (None, nombres).
    """
    names = _list_sheet_names(xlsx_path)
    if not names:
        return None, []

    parsed = [(n, _parse_sheet_date(n)) for n in names]
    # exacta
    for n, dte in parsed:
        if dte == desired:
            return n, names

    # futuras y pasadas
    futures = sorted([(dte, n) for n, dte in parsed if dte and dte >= desired], key=lambda x: x[0])
    pasts   = sorted([(dte, n) for n, dte in parsed if dte and dte <  desired], key=lambda x: x[0], reverse=True)

    if futures:
        return futures[0][1], names
    if pasts:
        return pasts[0][1], names

    return None, names
# -----------------------------------
# Configuración
# -----------------------------------
load_dotenv()
API_KEY = os.getenv("API_KEY")

# ===== NUEVO: credenciales/fuente interna para Carga/Planificación =====
EXT_URL          = os.getenv("EXT_URL", "").strip()       # URL de la intranet (endpoint que devuelve JSON)
EXT_COOKIE       = os.getenv("EXT_COOKIE", "").strip()    # Cookie copiada de DevTools (header Cookie:)
EXT_REFERER      = os.getenv("EXT_REFERER", "").strip()   # Opcional: Referer si tu backend lo pide
EXT_USER_AGENT   = os.getenv("EXT_USER_AGENT", "Mozilla/5.0")
EXT_POLL_SECONDS = int(os.getenv("EXT_POLL_SECONDS", "60"))

# SSL (elige 1 modo en .env): TRUSTSTORE | CERTIFI | CAFILE | FALSE
EXT_VERIFY_MODE  = os.getenv("EXT_VERIFY_MODE", "TRUSTSTORE").upper()
EXT_CAFILE       = os.getenv("EXT_CAFILE", "").strip()

# -----------------------------------
# Modelos
# -----------------------------------
class Task(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    task_type: str
    status: Optional[str] = None
    title: str
    assigned_to: Optional[str] = None
    due_date: Optional[str] = None   # <- NUEVO
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat(timespec='seconds') + 'Z')
    action: Optional[str] = None
    is_completed: Optional[bool] = False
    
    category: Optional[str] = None
    due_date: Optional[str] = None  # ISO (YYYY-MM-DD)

class TaskUpdate(BaseModel):
    status: str

class TaskCompletionUpdate(BaseModel):
    is_completed: bool

class PowerBIMetric(BaseModel):
    tab: str
    metric: str
    value: float
    timestamp: str

class TaskNoteUpdate(BaseModel):
    note: str | None = None

# -----------------------------------
# Estado & WS
# -----------------------------------
tasks_in_memory_store: Dict[str, Any] = {}

# ===== NUEVO: almacén de tabla externa (para Carga/Planificación) =====
latest_external_table: Dict[str, Any] = {
    "columns": [],
    "rows": [],
    "fetched_at": None,
    "version": 0,
}

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        print(f"🔌 WS conectado. Activos: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            print(f"🔌 WS desconectado. Activos: {len(self.active_connections)}")

    async def broadcast(self, data: Dict[str, Any]):
        # Enviar a todos; si uno falla, lo desconectamos
        for connection in list(self.active_connections):
            try:
                await connection.send_json(data)
            except Exception:
                self.disconnect(connection)

    async def send_one(self, websocket: WebSocket, data: Dict[str, Any]):
        try:
            await websocket.send_json(data)
        except Exception:
            self.disconnect(websocket)

app = FastAPI()
manager = ConnectionManager()

# ======================================================================
# ===== NUEVO: helpers SSL / parsing / polling de la fuente interna =====
# ======================================================================
def _build_ssl_context() -> ssl.SSLContext | bool:
    """
    Construye el verificador SSL según EXT_VERIFY_MODE.
    - TRUSTSTORE: usa el almacén del sistema (útil en entornos corporativos).
    - CERTIFI: usa el bundle por defecto de httpx (pasar None/por defecto).
    - CAFILE: usa un .pem propio en EXT_CAFILE.
    - FALSE: deshabilita la verificación (no recomendado).
    """
    mode = (EXT_VERIFY_MODE or "").upper()
    if mode == "FALSE":
        return False
    if mode == "TRUSTSTORE":
        try:
            import truststore  # pip install truststore
            truststore.inject_into_ssl()
            return ssl.create_default_context()
        except Exception as e:
            print("⚠️ truststore no disponible, caigo a CA por defecto:", e)
            return ssl.create_default_context()
    if mode == "CAFILE" and EXT_CAFILE:
        return ssl.create_default_context(cafile=EXT_CAFILE)
    # CERTIFI / default
    return ssl.create_default_context()

def _parse_cookie_header(header: str) -> Dict[str, str]:
    """Convierte 'k1=v1; k2=v2' en dict {'k1':'v1','k2':'v2'}."""
    jar: Dict[str, str] = {}
    for part in header.split(";"):
        if "=" in part:
            k, v = part.split("=", 1)
            jar[k.strip()] = v.strip()
    return jar

def _list_of_dicts(obj) -> list[dict]:
    """Intenta localizar una lista de registros (dict) dentro de cualquier JSON."""
    if isinstance(obj, list) and (not obj or isinstance(obj[0], dict)):
        return obj
    if isinstance(obj, dict):
        for key in ("data", "items", "rows", "result", "results"):
            if isinstance(obj.get(key), list) and (not obj[key] or isinstance(obj[key][0], dict)):
                return obj[key]
        if all(not isinstance(v, (list, dict)) for v in obj.values()):
            return [obj]
    return []

def _table_from_json(payload: Any) -> tuple[list[str], list[list[Any]]]:
    """
    Devuelve (columns, rows) a partir de:
    - {"columns":[...], "rows":[...]}  (rows puede ser lista de listas o de dicts)
    - lista de dicts                   (aplana claves)
    - lista de listas                  (crea columnas col_1..col_n)
    - dict único                       (una fila)
    """
    # 0) Forma directa {"columns":[...], "rows":[...]}
    if isinstance(payload, dict) and "rows" in payload:
        cols = payload.get("columns")
        rows = payload.get("rows") or []
        # rows de dicts
        if rows and isinstance(rows[0], dict):
            # si no vienen columnas, infiere en orden de primera aparición
            if not cols:
                seen = set()
                cols = []
                for r in rows:
                    for k in r.keys():
                        if k not in seen:
                            seen.add(k); cols.append(k)
            matrix = [[_safe_cell(r.get(c)) for c in cols] for r in rows]
            return [str(c) for c in cols], matrix
        # rows de listas/tuplas
        if isinstance(rows, list) and (not rows or isinstance(rows[0], (list, tuple))):
            # si no vienen columnas, crea col_1..col_n en base a la primera fila
            if not cols:
                n = len(rows[0]) if rows else 0
                cols = [f"col_{i+1}" for i in range(n)]
            matrix = [[_safe_cell(v) for v in (row if isinstance(row, (list, tuple)) else [row])] for row in rows]
            return [str(c) for c in cols], matrix

    # 1) Lista de dicts (posiblemente anidada bajo data/items/rows/result/results)
    records = _list_of_dicts(payload)
    if records:
        flat = [_flatten_record(r) for r in records]
        cols, seen = [], set()
        for r in flat:
            for k in r.keys():
                if k not in seen:
                    seen.add(k); cols.append(k)
        rows = [[_safe_cell(r.get(c)) for c in cols] for r in flat]
        return cols, rows

    # 2) Lista de listas "plana"
    if isinstance(payload, list) and payload and isinstance(payload[0], (list, tuple)):
        cols = [f"col_{i+1}" for i in range(len(payload[0]))]
        rows = [[_safe_cell(v) for v in row] for row in payload]
        return cols, rows

    # 3) Dict único -> 1 fila
    if isinstance(payload, dict):
        flat = _flatten_record(payload)
        cols = list(flat.keys())
        rows = [[_safe_cell(flat.get(c)) for c in cols]]
        return cols, rows

    return [], []



async def apply_external_table(payload: Any):
    """Actualiza el almacén de tabla y notifica al frontend sin romper nada existente."""
    cols, rows = _table_from_json(payload)
    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    latest_external_table["columns"]    = cols
    latest_external_table["rows"]       = rows
    latest_external_table["fetched_at"] = ts
    latest_external_table["version"]    = int(latest_external_table.get("version", 0)) + 1

    # Ping ligero por WS (el frontend hará GET /api/external-table cuando reciba esto)
    await manager.broadcast({
        "type": "table_ping",
        "table": "external",
        "version": latest_external_table["version"],
        "rows": len(rows),
        "fetched_at": ts,
    })
async def push_external_table_state(websocket: WebSocket):
    await manager.send_one(websocket, {
        "type": "table_state",
        "table": "external",
        "version": latest_external_table.get("version", 0),
        "rows": len(latest_external_table.get("rows", [])),
        "fetched_at": latest_external_table.get("fetched_at"),
    })
# ---- httpx (cliente asíncrono) ----
import httpx  # pip install httpx truststore (si usas TRUSTSTORE)

async def _fetch_external_once(client: httpx.AsyncClient) -> None:
    """Hace 1 GET a EXT_URL y actualiza la tabla."""
    if not EXT_URL:
        return

    headers = {
        "User-Agent": EXT_USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "X-Requested-With": "XMLHttpRequest",
    }
    if EXT_REFERER:
        headers["Referer"] = EXT_REFERER

    resp = await client.get(EXT_URL, headers=headers, timeout=30.0)
    resp.raise_for_status()

    # httpx descomprime automáticamente si viene gzip/deflate
    data = None
    try:
        data = resp.json()
    except Exception:
        # Si no es JSON válido, no tocamos nada
        print("⚠️ La respuesta no es JSON; se ignora para la tabla externa.")
        return

    await apply_external_table(data)

# ============================
# ExternalConnector (robusto)
# ============================
@dataclass
class ExternalSettings:
    urls: list[str] = field(default_factory=list)  # admite varias, rota en fallos
    referer: str = ""
    user_agent: str = "Mozilla/5.0"
    cookie_header: str = ""         # "k1=v1; k2=v2"
    poll_seconds: int = 60
    verify_mode: str = "TRUSTSTORE" # TRUSTSTORE|CERTIFI|CAFILE|FALSE
    cafile: str = ""
    auth_mode: str = "COOKIE"       # COOKIE|LOGIN_POST|SCRIPT
    # LOGIN_POST
    login_url: str = ""
    login_payload_json: str = ""    # JSON con campos (puedes usar {username},{password},{csrf})
    login_csrf_regex: str = ""      # regex para extraer token del HTML (opcional)
    username: str = ""
    password: str = ""
    # SCRIPT
    refresh_cmd: str = ""           # comando que debe imprimir JSON con {cookie,url}
    # miscelánea
    http2: bool = True
    max_keepalive: int = 20
    max_connections: int = 40

    @classmethod
    def from_env(cls) -> "ExternalSettings":
        # Soporta una lista separada por comas en EXT_URLS o usa EXT_URL
        urls = [u.strip() for u in os.getenv("EXT_URLS", "").split(",") if u.strip()]
        if not urls and os.getenv("EXT_URL", "").strip():
            urls = [os.getenv("EXT_URL", "").strip()]
        return cls(
            urls=urls,
            referer=os.getenv("EXT_REFERER", "").strip(),
            user_agent=os.getenv("EXT_USER_AGENT", "Mozilla/5.0"),
            cookie_header=os.getenv("EXT_COOKIE", "").strip(),
            poll_seconds=int(os.getenv("EXT_POLL_SECONDS", "60")),
            verify_mode=os.getenv("EXT_VERIFY_MODE", "TRUSTSTORE").upper(),
            cafile=os.getenv("EXT_CAFILE", "").strip(),
            auth_mode=os.getenv("EXT_AUTH_MODE", "COOKIE").upper(),
            login_url=os.getenv("EXT_LOGIN_URL", "").strip(),
            login_payload_json=os.getenv("EXT_LOGIN_PAYLOAD_JSON", "").strip(),
            login_csrf_regex=os.getenv("EXT_LOGIN_CSRF_REGEX", "").strip(),
            username=os.getenv("EXT_USERNAME", "").strip(),
            password=os.getenv("EXT_PASSWORD", "").strip(),
            refresh_cmd=os.getenv("EXT_REFRESH_CMD", "").strip(),
            http2=(os.getenv("EXT_HTTP2", "true").lower() == "true"),
            max_keepalive=int(os.getenv("EXT_MAX_KEEPALIVE", "20")),
            max_connections=int(os.getenv("EXT_MAX_CONNECTIONS", "40")),
        )

class ExternalConnector:
    def __init__(self):
        self.settings = ExternalSettings.from_env()
        self._env_path = Path(os.getenv("ENV_FILE", ".env"))
        self._env_mtime = self._env_path.stat().st_mtime if self._env_path.exists() else None
        self._client: httpx.AsyncClient | None = None
        self._url_idx = 0
        self._consec_fail = 0
        self._last_ok: str | None = None
        self._status: dict[str, Any] = {
            "ok": False, "last_ok": None, "last_error": None, "fails": 0, "url": self.current_url
        }

    @property
    def current_url(self) -> str:
        return self.settings.urls[self._url_idx] if self.settings.urls else ""

    def _ssl_verify(self):
        # reutiliza tu helper existente
        return _build_ssl_context()

    def _cookies_jar(self):
        return _parse_cookie_header(self.settings.cookie_header) if self.settings.cookie_header else {}

    async def _ensure_client(self, recycle: bool = False):
    # si ya existe y no nos piden reciclar, no toques nada
        if self._client is not None and not recycle:
            return

        # siempre inicializamos la variable para evitar NameError
        existing_cookies = None

        if self._client is not None and recycle:
            try:
                # conserva cookies vivas (posibles renovaciones por Set-Cookie)
                existing_cookies = self._client.cookies
                await self._client.aclose()
            except Exception:
                existing_cookies = None

        # cookies base: las existentes si las hay; si no, un jar vacío
        base_cookies = existing_cookies or httpx.Cookies()

        # superpone las del .env sin perder las existentes
        env_jar = self._cookies_jar()
        if env_jar:
            try:
                base_cookies.update(env_jar)
            except Exception:
                for k, v in env_jar.items():
                    base_cookies.set(k, v)

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
            cookies=base_cookies,
            transport=transport,
            headers={
                "User-Agent": self.settings.user_agent,
                "Accept": "application/json, text/plain, */*",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": self.settings.referer or None,
            }
        )

    async def _reload_env_if_changed(self):
        # Hot-reload del .env si cambia en disco
        try:
            if self._env_path.exists():
                m = self._env_path.stat().st_mtime
                if self._env_mtime != m:
                    self._env_mtime = m
                    load_dotenv(override=True)
                    new_settings = ExternalSettings.from_env()
                    if new_settings != self.settings:
                        self.settings = new_settings
                        await self._ensure_client(recycle=True)
                        self._url_idx = 0
                        print("🔄 .env recargado y cliente renovado.")
        except Exception as e:
            print("⚠️ Hot-reload .env error:", e)

    async def _login_if_needed(self) -> bool:
        if self.settings.auth_mode != "LOGIN_POST":
            return True
        if not (self.settings.login_url and self._client):
            return False

        csrf = None
        try:
            r = await self._client.get(self.settings.login_url, timeout=30.0)
            r.raise_for_status()
            if self.settings.login_csrf_regex:
                m = re.search(self.settings.login_csrf_regex, r.text)
                if m:
                    csrf = m.group(1)
        except Exception as e:
            self._status.update(ok=False, last_error=f"login_get:{e}")
            return False

        # Monta payload
        payload = {}
        if self.settings.login_payload_json:
            try:
                payload = json.loads(self.settings.login_payload_json)
            except Exception:
                pass
        if "{username}" in json.dumps(payload) or "{password}" in json.dumps(payload) or "{csrf}" in json.dumps(payload):
            payload = json.loads(json.dumps(payload)
                     .replace("{username}", self.settings.username)
                     .replace("{password}", self.settings.password)
                     .replace("{csrf}", csrf or ""))

        # Fallback simple si no se dio JSON
        if not payload and self.settings.username:
            payload = {"username": self.settings.username, "password": self.settings.password}
            if csrf is not None:
                payload["csrf"] = csrf

        try:
            r = await self._client.post(self.settings.login_url, data=payload, timeout=30.0)
            r.raise_for_status()
            # Si el login pone Set-Cookie, httpx actualiza cookies automáticamente
            return True
        except Exception as e:
            self._status.update(ok=False, last_error=f"login_post:{e}")
            return False

    async def _refresh_via_script(self) -> bool:
        if not self.settings.refresh_cmd:
            return False
        try:
            # Ejecuta el script/comando que debe imprimir JSON con {cookie: "...", url: "..."}
            proc = await asyncio.create_subprocess_shell(
                self.settings.refresh_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            out, err = await proc.communicate()
            if proc.returncode != 0:
                raise RuntimeError(err.decode("utf-8", "ignore"))
            data = json.loads(out.decode("utf-8", "ignore"))
            cookie = data.get("cookie") or data.get("Cookie")
            url = data.get("url") or data.get("URL")
            if cookie:
                self.settings.cookie_header = cookie
            if url:
                if url not in self.settings.urls:
                    self.settings.urls.insert(0, url)
                self._url_idx = 0
            await self._ensure_client(recycle=True)
            return True
        except Exception as e:
            self._status.update(ok=False, last_error=f"refresh_cmd:{e}")
            return False

    async def _reauth(self) -> bool:
        # Intenta en este orden según modo
        if self.settings.auth_mode == "SCRIPT":
            if await self._refresh_via_script():
                return True
        if self.settings.auth_mode == "LOGIN_POST":
            if await self._login_if_needed():
                return True
        # COOKIE: intenta recargar .env (por si has pegado nuevas cookies)
        await self._reload_env_if_changed()
        # Re-inyecta cookies actuales (por si cambiaron desde .env)
        await self._ensure_client(recycle=True)
        return True

    def _rotate_url(self):
        if len(self.settings.urls) <= 1:
            return
        self._url_idx = (self._url_idx + 1) % len(self.settings.urls)
        print(f"🔀 Rotando a URL: {self.current_url}")

    async def _fetch_once(self):
        if not self.current_url:
            return  # nada que hacer
        await self._ensure_client()
        try:
            resp = await self._client.get(self.current_url, timeout=30.0)
            # Auto-actualiza cookie si viene Set-Cookie (httpx lo hace ya)
            if resp.status_code in (401, 403, 419, 440):
                raise httpx.HTTPStatusError("auth", request=resp.request, response=resp)
            if resp.status_code >= 500:
                raise httpx.HTTPStatusError("5xx", request=resp.request, response=resp)

            try:
                data = resp.json()
            except Exception:
                # si no es JSON válido, no tumbar el bucle
                raise ValueError("Respuesta no JSON")

            await apply_external_table(data)
            self._consec_fail = 0
            self._last_ok = datetime.utcnow().isoformat(timespec="seconds") + "Z"
            self._status.update(ok=True, last_ok=self._last_ok, last_error=None, fails=0, url=self.current_url)

            # Emite un latido de estado
            await manager.broadcast({"type":"external_status","ok":True,"url":self.current_url,"ts":self._last_ok})
        except Exception as e:
            self._consec_fail += 1
            self._status.update(ok=False, last_error=str(e), fails=self._consec_fail, url=self.current_url)
            await manager.broadcast({"type":"external_status","ok":False,"url":self.current_url,"error":str(e),"fails":self._consec_fail})
            raise

    async def run(self):
        # Intento de login inicial si aplica
        await self._ensure_client()
        if self.settings.auth_mode == "LOGIN_POST":
            await self._login_if_needed()

        base = max(5, self.settings.poll_seconds)
        # primer intento inmediato
        next_delay = 0

        while True:
            try:
                await self._reload_env_if_changed()
                await self._fetch_once()
                next_delay = self.settings.poll_seconds  # éxito → intervalo normal
            except Exception:
                # Exponencial con jitter y reciclado de cliente cada 3 fallos
                step = min(self._consec_fail, 6)  # cap
                backoff = min(300, (2 ** step))   # 1,2,4,8,16,32,64, capped
                jitter = int(0.2 * backoff * (1 + (os.getpid() % 5)))  # soft jitter
                next_delay = max(10, backoff + (jitter % 7))
                # recicla cliente cada 3 fallos
                if self._consec_fail % 3 == 0:
                    await self._ensure_client(recycle=True)
                # rota URL cada 2 fallos
                if self._consec_fail % 2 == 0:
                    self._rotate_url()
                # reautenticación
                await self._reauth()

            await asyncio.sleep(next_delay)

    def status(self) -> dict[str, Any]:
        return dict(self._status)



from urllib.parse import parse_qs
from fastapi import Body

def _extract_tasks_flex(data: Any) -> list[dict]:
    """Tolera lista en raíz, {'body':[...]} o 'body' como string JSON."""
    def _as_list(x):
        if x is None: return []
        if isinstance(x, list): return [i for i in x if isinstance(i, dict)]
        if isinstance(x, dict): return [x]
        if isinstance(x, str):
            try:
                return _as_list(json.loads(x))
            except Exception:
                return []
        return []
    cur = data
    # desenrolla 'body' hasta 3 veces por si viene anidado o como string
    for _ in range(3):
        if isinstance(cur, dict) and "body" in cur:
            b = cur["body"]
            if isinstance(b, (list, dict)):
                cur = b; continue
            if isinstance(b, str):
                try:
                    cur = json.loads(b); continue
                except Exception:
                    break
        break
    return _as_list(cur)

async def _best_effort_get_payload(request: Request) -> Any:
    """Lee el cuerpo crudo y soporta JSON directo, x-www-form-urlencoded y multipart con 'body'."""
    ctype = (request.headers.get("content-type") or "").lower()
    raw = await request.body()
    text = raw.decode("utf-8", errors="ignore") if raw else ""
    # JSON directo
    try:
        return json.loads(text)
    except Exception:
        pass
    # x-www-form-urlencoded
    if "application/x-www-form-urlencoded" in ctype:
        try:
            qs = parse_qs(text)
            b = qs.get("body", [None])[0]
            if b is not None:
                try:
                    return json.loads(b)
                except Exception:
                    return {"body": b}
        except Exception:
            pass
    # multipart/form-data
    if "multipart/form-data" in ctype:
        try:
            form = await request.form()
            if "body" in form:
                b = form["body"]
                if hasattr(b, "read"):
                    txt = (await b.read()).decode("utf-8", errors="ignore")
                    try: return json.loads(txt)
                    except Exception: return {"body": txt}
                else:
                    s = str(b)
                    try: return json.loads(s)
                    except Exception: return {"body": s}
        except Exception:
            pass
    return text  # lo tratará el extractor

from urllib.parse import parse_qs

def _json_lenient(text: str):
    """Carga JSON desde string admitiendo BOM, comillas “raras” y comas finales."""
    if not isinstance(text, str) or not text.strip():
        return None
    s = text.lstrip("\ufeff").strip()
    # normaliza comillas tipográficas
    s = s.replace("“", '"').replace("”", '"').replace("’", "'")
    # algunos flujos mandan "body=...."
    if s.startswith("body="):
        s = s[5:]
    # primer intento
    try:
        return json.loads(s)
    except Exception:
        pass
    # elimina comas finales antes de } o ]
    try:
        s2 = re.sub(r",(\s*[}\]])", r"\1", s)
        return json.loads(s2)
    except Exception:
        pass
    # intenta extraer el primer array/objeto JSON embebido en texto
    try:
        # busca primer '[' o '{' y balancea
        start = min([i for i in [s.find("["), s.find("{")] if i >= 0], default=-1)
        if start >= 0:
            stack = []
            for i, ch in enumerate(s[start:], start=start):
                if ch in "[{":
                    stack.append(ch)
                elif ch in "]}":
                    if stack and ((stack[-1] == "[" and ch == "]") or (stack[-1] == "{" and ch == "}")):
                        stack.pop()
                        if not stack:
                            frag = s[start:i+1]
                            return json.loads(frag)
            # si no balancea, último intento con eliminación de comas
            frag2 = re.sub(r",(\s*[}\]])", r"\1", s[start:])
            return json.loads(frag2)
    except Exception:
        pass
    return None

def _first_list_of_dicts(obj):
    """Recorre recursivamente y devuelve la primera lista de dicts que encuentre."""
    if obj is None:
        return []
    if isinstance(obj, list):
        if obj and all(isinstance(x, dict) for x in obj):
            return obj
        # lista mixta -> filtra dicts si los hubiera
        dicts = [x for x in obj if isinstance(x, dict)]
        if dicts:
            return dicts
        return []
    if isinstance(obj, dict):
        # claves típicas (case-insensitive)
        preferred = ["body", "Body", "BODY", "value", "values", "items", "data", "results", "result", "records", "array", "tasks"]
        for k in preferred:
            if k in obj:
                r = _first_list_of_dicts(obj.get(k))
                if r:
                    return r
        # cualquier valor que sea lista de dicts
        for v in obj.values():
            r = _first_list_of_dicts(v)
            if r:
                return r
        # si el propio dict parece una tarea suelta
        keys = set(obj.keys())
        if {"id","title"} & keys:
            return [obj]
    if isinstance(obj, str):
        j = _json_lenient(obj)
        return _first_list_of_dicts(j)
    return []

async def _best_effort_get_payload(request: Request) -> Any:
    """Lee el body (JSON, urlencoded, multipart) y devuelve algo parseable."""
    ctype = (request.headers.get("content-type") or "").lower()
    raw = await request.body()
    text = raw.decode("utf-8", errors="ignore") if raw else ""

    # JSON directo
    try:
        return json.loads(text)
    except Exception:
        pass

    # x-www-form-urlencoded
    if "application/x-www-form-urlencoded" in ctype:
        try:
            qs = parse_qs(text)
            b = qs.get("body", [None])[0]
            if b is not None:
                j = _json_lenient(b)
                return j if j is not None else {"body": b}
        except Exception:
            pass

    # multipart/form-data
    if "multipart/form-data" in ctype:
        try:
            form = await request.form()
            if "body" in form:
                b = form["body"]
                if hasattr(b, "read"):
                    txt = (await b.read()).decode("utf-8", errors="ignore")
                else:
                    txt = str(b)
                j = _json_lenient(txt)
                return j if j is not None else {"body": txt}
        except Exception:
            pass

    # último intento leniente contra texto crudo
    j = _json_lenient(text)
    return j if j is not None else text

def _extract_tasks_flex(data: Any) -> list[dict]:
    """Devuelve una lista de tareas a partir de casi cualquier forma razonable."""
    tasks = _first_list_of_dicts(data)
    # filtra por dicts que realmente parezcan tarea (al menos 1 key útil)
    useful_keys = {"id","title","station","warehouse","gw_date","task_type"}
    tasks = [t for t in tasks if isinstance(t, dict) and useful_keys.intersection(t.keys())]
    return tasks



# -----------------------------------
# Webhooks (EXISTENTE, SIN CAMBIOS)
# -----------------------------------
from typing import Any, Dict, List, Optional
from fastapi import Header, HTTPException, Request

from fastapi import Header, HTTPException, Request

@app.post("/webhook/sharepoint-bulk-update")
async def sharepoint_bulk_update(request: Request, x_api_key: Optional[str] = Header(None)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")

    # --- LOG: ver exactamente qué llega y reinyectar el body para poder releerlo
    try:
        cl = request.headers.get("content-length")
        ct = request.headers.get("content-type")
        raw = await request.body()
        print(f"📥 HTTP IN → CT={ct!r} CL={cl} RAW_LEN={len(raw) if raw else 0}")
        async def _receive_again():
            return {"type": "http.request", "body": raw, "more_body": False}
        request._receive = _receive_again
    except Exception as e:
        print("⚠️ No se pudo loguear el body entrante:", repr(e))

    # --- Parseo robusto del body (JSON directo / urlencoded / multipart / 'body' string, etc.)
    incoming_payload = await _best_effort_get_payload(request)

    # --- Extraer lista de tareas (tolera 'body' como string JSON, 'items', 'value', etc.)
    tasks_raw = _extract_tasks_flex(incoming_payload)

    if not tasks_raw:
        # si no hay tareas, devolvemos 400 con preview (para que el túnel NUNCA muestre body:null silencioso)
        try:
            preview = json.dumps(incoming_payload, ensure_ascii=False)[:4000]
        except Exception:
            preview = (str(incoming_payload) or "")[:4000]
        raise HTTPException(status_code=400, detail=f"No se encontraron tareas en el payload. Preview: {preview!r}")

    # --- Helpers locales para mapear a nuestro modelo
    def _boolish(v):
        if isinstance(v, bool): return v
        if isinstance(v, (int, float)): return v != 0
        if isinstance(v, str): return v.strip().lower() in {"1","true","yes","y","ok","done","si","sí"}
        return False

    def _to_gw_task(item: dict) -> dict:
        # id estable si viene; si no, genera uno determinista por el contenido
        _id = (item.get("id")
               or item.get("ID")
               or item.get("Id")
               or str(uuid.uuid5(uuid.NAMESPACE_URL, json.dumps(item, sort_keys=True))))
        title = (item.get("title")
                 or item.get("Title")
                 or item.get("task")
                 or item.get("TaskName")
                 or "Tarea")
        station   = item.get("station")   or item.get("Station")
        warehouse = item.get("warehouse") or item.get("Warehouse")
        gw_date   = (item.get("gw_date")
                     or item.get("date")
                     or item.get("fecha")
                     or item.get("Fecha"))
        note_parts = [p for p in [station, warehouse, gw_date] if p]
        note = " · ".join(note_parts) if note_parts else (item.get("note") or "")

        completed = (item.get("is_completed")
                     or item.get("completed")
                     or item.get("done")
                     or item.get("Completado"))

        return sanitize_task({
            "id": str(_id),
            "task_type": "gw_task",
            "title": str(title),
            "note": note,
            "is_completed": _boolish(completed),
            # deja pasar algunos campos útiles tal cual por si el front los usa
            "station": station,
            "warehouse": warehouse,
            "gw_date": gw_date,
        })

    # --- Upsert en memoria y broadcast
    upserts = []
    for it in tasks_raw:
        mapped = _to_gw_task(it)
        prev = tasks_in_memory_store.get(mapped["id"])
        merged = merge_preserve_server(prev, mapped)
        tasks_in_memory_store[merged["id"]] = merged
        upserts.append(merged)

    # persistimos en disco y notificamos uno a uno (compatible con tu front actual)
    save_tasks_to_disk()
    for t in upserts:
        await manager.broadcast(t)

    # además, si quieres que el widget de "Carga/Planificación" se refresque como tabla:
    try:
        await apply_external_table(tasks_raw)
    except Exception as e:
        print("⚠️ apply_external_table error:", repr(e))

    # --- RESPUESTA (¡nunca 'null'!)
    return {
        "ok": True,
        "received": len(tasks_raw),
        "upserts": len(upserts),
        "ids": [t["id"] for t in upserts][:200],
    }








@app.post("/webhook/powerbi-total-cost")
async def powerbi_total_cost(request: Request, x_api_key: Optional[str] = Header(None)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")

    raw = await request.body()
    raw_text = raw.decode("utf-8", errors="ignore")
    print("📩 RAW /powerbi-total-cost:", raw_text)

    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    tab = str(data.get("tab", "")).strip()
    metric = str(data.get("metric", "")).strip()
    ts = data.get("timestamp") or (datetime.utcnow().isoformat(timespec="seconds") + "Z")
    v_in = data.get("value", None)

    if not tab or not metric:
        raise HTTPException(status_code=422, detail="Missing required fields: 'tab' and 'metric'.")

    # --- extracción robusta del primer número ---
    def try_float(x):
        if isinstance(x, (int, float)): return float(x)
        if isinstance(x, str):
            s = x.strip()
            try: return float(s.replace(",", "."))
            except:
                m = re.search(r"[-+]?\d+(?:[.,]\d+)?", s)
                return float(m.group(0).replace(",", ".")) if m else None
        return None

    def extract_numeric(obj):
        preferred = {"[Total Cost]", "Total Cost", "TotalCost", "total_cost", "value"}
        val = try_float(obj)
        if val is not None: return val
        if isinstance(obj, dict):
            for k in preferred:
                if k in obj:
                    v = try_float(obj[k])
                    if v is not None: return v
            for v in obj.values():
                r = extract_numeric(v)
                if r is not None: return r
        if isinstance(obj, list):
            for item in obj:
                r = extract_numeric(item)
                if r is not None: return r
        return None

    value = extract_numeric(v_in)
    parse_status = "ok" if value is not None else "unparseable"

    payload = {
        "type": "kpi_update",
        "tab": tab,
        "metric": metric,
        "value": value,
        "timestamp": ts,
        "parse_status": parse_status
    }
    if value is None:
        payload["value_raw"] = v_in

    print(f"🟣 PowerBI KPI → {metric} @ {tab} = {value} ({ts})  status={parse_status} raw={v_in!r}")
    await manager.broadcast(payload)
    return {"status": "ok", "parsed": value is not None, "parse_status": parse_status, "value": value}

# -----------------------------------
# API de tareas (EXISTENTE, SIN CAMBIOS)
# -----------------------------------
@app.put("/api/tasks/{task_id}/complete", response_model=Task)
async def update_task_completion(task_id: str, update_data: TaskCompletionUpdate):
    if task_id not in tasks_in_memory_store:
        raise HTTPException(status_code=404, detail="Tarea no encontrada")

    task = tasks_in_memory_store[task_id]
    task["is_completed"] = update_data.is_completed

    sanitized = sanitize_task(task)
    tasks_in_memory_store[task_id] = sanitized
    save_tasks_to_disk()
    await manager.broadcast(sanitized)
    return Task(**sanitized)

@app.get("/api/tasks")
async def get_all_tasks():
    # devuelve ya saneado
    return [sanitize_task(t) for t in tasks_in_memory_store.values()]

@app.get("/api/external-status")
def api_external_status():
    ext = getattr(app.state, "_ext", None)
    return ext.status() if ext else {"ok": False, "last_error": "not_started"}

@app.on_event("startup")
async def _startup_ext():
    load_tasks_from_disk()
    app.state._roster = asyncio.create_task(_roster_watcher())
    app.state._ext = ExternalConnector()
    app.state._poller = asyncio.create_task(app.state._ext.run())

@app.on_event("shutdown")
async def _shutdown_ext():
    for key in ("_poller","_roster"):
        task: asyncio.Task = getattr(app.state, key, None)
        if task and not task.done():
            task.cancel()
            try: await task
            except asyncio.CancelledError: pass

@app.post("/api/tasks", response_model=Task, status_code=201)
async def create_task(task: Task):
    task_dict = task.dict()
    task_dict.pop("action", None)
    sanitized = sanitize_task(task_dict)
    tasks_in_memory_store[sanitized["id"]] = sanitized
    save_tasks_to_disk()
    await manager.broadcast(sanitized)
    return Task(**sanitized)

@app.put("/api/tasks/{task_id}", response_model=Task)
async def update_task_status(task_id: str, task_update: TaskUpdate):
    if task_id not in tasks_in_memory_store:
        raise HTTPException(status_code=404, detail="Tarea no encontrada")
    task_to_update = tasks_in_memory_store[task_id]
    task_to_update["status"] = task_update.status
    sanitized = sanitize_task(task_to_update)
    tasks_in_memory_store[task_id] = sanitized
    save_tasks_to_disk()
    await manager.broadcast(sanitized)
    return Task(**sanitized)

@app.delete("/api/tasks/{task_id}", status_code=204)
async def delete_task(task_id: str):
    if task_id not in tasks_in_memory_store:
        raise HTTPException(status_code=404, detail="Tarea no encontrada")
    task_data = tasks_in_memory_store.pop(task_id)
    save_tasks_to_disk()
    await manager.broadcast({"id": str(task_id), "action": "delete", "task_type": task_data.get("task_type")})
    return {}

@app.put("/api/tasks/{task_id}/note")
async def update_task_note(task_id: str, upd: TaskNoteUpdate):
    if task_id not in tasks_in_memory_store:
        raise HTTPException(status_code=404, detail="Tarea no encontrada")
    task = tasks_in_memory_store[task_id]
    task["note"] = upd.note or ""
    sanitized = sanitize_task(task)
    tasks_in_memory_store[task_id] = sanitized
    save_tasks_to_disk()
    await manager.broadcast(sanitized)
    return sanitized

# -----------------------------------
# NUEVO: API tabla externa para el frontend (Carga/Planificación)
# -----------------------------------
@app.get("/api/external-table")
async def get_external_table():
    return latest_external_table

# -----------------------------------
# WebSocket (EXISTENTE, SIN CAMBIOS)
# -----------------------------------
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # 👉 Si ya hay tabla cargada (version>0), empuja estado inicial
        if latest_external_table.get("version", 0) > 0:
            await push_external_table_state(websocket)

        # Mantén el socket vivo
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# -----------------------------------
# Lifespan: lanzar/limpiar poller (NUEVO)
# -----------------------------------
@app.on_event("startup")
async def _startup():
    # lanza el poller sólo si hay EXT_URL
    if EXT_URL:
        app.state._poller = asyncio.create_task(_poller_task())

@app.on_event("shutdown")
async def _shutdown():
    task: asyncio.Task = getattr(app.state, "_poller", None)
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

@app.on_event("startup")
async def _startup():
    # 1) cargar tareas persistidas
    load_tasks_from_disk()

    # 2) lanzar el poller de la tabla externa (como ya tenías)
    if EXT_URL:
        app.state._poller = asyncio.create_task(_poller_task())
# ====== ROSTER: construir estado desde Excel y publicar por WS ======
roster_cache: dict[str, Any] = {
    "file_mtime": None,
    "sheet_date": None,
    "shift": None,         # 'Mañana'|'Tarde'|'Noche'
    "people": [],
    "updated_at": None,
    "window": None,
    "sheet": None,
}

def _now_local():
    return datetime.now(ZoneInfo(ROSTER_TZ))

def _current_shift_info(now):
    hhmm = now.strftime("%H:%M")
    if "06:00" <= hhmm < "14:00":
        return "Mañana", now.date(), "06:00", "14:00"
    if "14:00" <= hhmm < "22:00":
        return "Tarde", now.date(), "14:00", "22:00"
    # Noche → si son de 00:00 a 05:59, usamos la hoja de AYER
    sheet_date = now.date() - timedelta(days=1) if hhmm < "06:00" and ROSTER_NIGHT_PREV_DAY else now.date()
    return "Noche", sheet_date, "22:00", "06:00"

async def _build_roster_state(force=False) -> dict:
    p = Path(ROSTER_XLSX_PATH)
    mtime = p.stat().st_mtime if p.exists() else None
    now = _now_local()
    shift, sheet_date, start, end = _current_shift_info(now)

    needs_reload = force or (mtime != roster_cache.get("file_mtime")) \
                         or (sheet_date != roster_cache.get("sheet_date")) \
                         or (shift != roster_cache.get("shift"))

    if needs_reload:
        sheet = _sheet_name_for_date(sheet_date)  # <- si todavía lo tienes, ya no lo usaremos
        # NUEVO: elegir hoja real por nombre dd-mm-aaaa (o la más cercana)
        sheet_real, sheet_names = _find_sheet_for_date(ROSTER_XLSX_PATH, sheet_date)

        if not sheet_real:
            # No hay ninguna hoja con fecha parseable. No rompas; devuelve vacío y log legible.
            sample = ", ".join(sheet_names[:15])
            if len(sheet_names) > 15:
                sample += ", …"
            print(f"⚠️ No hay hoja para la fecha {sheet_date} (nombres vistos: {sample})")
            people = []
        else:
            people = _read_sheet_people(ROSTER_XLSX_PATH, sheet_real, shift)

        roster_cache.update({
            "file_mtime": mtime,
            "sheet_date": sheet_date,
            "shift": shift,
            "people": people,
            "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "window": {"from": start, "to": end},
            "sheet": sheet_real,  # <- guarda el nombre real
        })
        await manager.broadcast({
            "type": "roster_update",
            "shift": shift,
            "sheet": sheet_real,
            "sheet_date": sheet_date.isoformat(),
            "window": {"from": start, "to": end},
            "count": len(people),
            "people": people,
            "source": "excel",
            "updated_at": roster_cache["updated_at"],
        })
    return roster_cache

@app.get("/api/roster/current")
async def get_roster_current():
    state = await _build_roster_state(force=False)
    return {
        "shift": state.get("shift"),
        "sheet": state.get("sheet"),
        "sheet_date": state.get("sheet_date").isoformat() if state.get("sheet_date") else None,
        "window": state.get("window"),
        "people": state.get("people", []),
        "updated_at": state.get("updated_at"),
    }
@app.get("/api/roster/sheets")
def api_roster_sheets():
    return {
        "path": ROSTER_XLSX_PATH,
        "sheets": _list_sheet_names(ROSTER_XLSX_PATH),
    }


async def _roster_watcher():
    # primera carga
    try:
        await _build_roster_state(force=True)
    except Exception as e:
        print("⚠️ Roster initial load error:", repr(e))

    # refresco periódico (por si cambia la hora/turno o reemplazas el archivo)
    while True:
        await asyncio.sleep(max(15, ROSTER_POLL_SECONDS))
        try:
            await _build_roster_state(force=False)
        except Exception as e:
            print("⚠️ Roster watcher error:", repr(e))


# -----------------------------------
# Frontend estático
# -----------------------------------
app.mount("/", StaticFiles(directory="../frontend_bcn", html=True), name="static")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8001, reload=True)
