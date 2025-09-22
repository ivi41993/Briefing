from __future__ import annotations
import asyncio
import ssl
import time
import os
import uuid
import json
import re
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any
from pathlib import Path
from zoneinfo import ZoneInfo
import tempfile
from urllib.parse import unquote
from contextlib import asynccontextmanager  # ← ESTA LÍNEA DEBE ESTAR AQUÍ

import pandas as pd
import httpx
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Header, WebSocket, WebSocketDisconnect, Request
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Cargar variables de entorno INMEDIATAMENTE
load_dotenv()

ROSTER_XLSX_PATH = os.getenv("ROSTER_XLSX_PATH", "C:/Users/iexposito/briefing/backend/data/Informe diario.xlsx")
ROSTER_TZ = os.getenv("ROSTER_TZ", "Europe/Madrid")
ROSTER_POLL_SECONDS = int(os.getenv("ROSTER_POLL_SECONDS", "60"))
ROSTER_NIGHT_PREV_DAY = os.getenv("ROSTER_NIGHT_PREV_DAY", "true").lower() == "true"

SPANISH_DAY = ["Lunes","Martes","Miércoles","Jueves","Viernes","Sábado","Domingo"]










def _sheet_name_for_date(d):
    return SPANISH_DAY[d.weekday()]  # Lunes..Domingo

SPANISH_DAY = ["Lunes","Martes","Miércoles","Jueves","Viernes","Sábado","Domingo"]










def _normalize_cols(cols):
    norm = {}
    for c in cols:
        k = c.strip().lower()
        norm[k] = c
    # devuelve los nombres reales (tal y como están en el excel)
    def pick(*cands):
        for cand in cands:
            # Añadimos una limpieza extra para quitar comas y espacios extra
            cand_clean = cand.replace(",", "").replace(" ", "")
            for k in norm:
                k_clean = k.replace(",", "").replace(" ", "")
                if k_clean == cand_clean:
                    return norm[k]
        return None
   
    return {
        "apellidos": pick("apellidos","apellido","apellidos/s"),
        "nombre": pick("nombre","nombree","nombres"),
        # --- NUEVA LÍNEA ---
        # Añadimos una clave para la columna combinada. Buscará "apellidos, nombre", "apellidos nombre", etc.
        "apellidos, nombre": pick("apellidos, nombre", "apellidos nombre", "nombre completo"),
        # --------------------
        "horario": pick("horario","turno","franja"),
        "observaciones": pick("observaciones","observación","obs","observaciones "),
    }
def _match_shift(horario: str, shift: str) -> bool:
    s = (horario or "").lower()
    if shift == "Mañana":
        return ("mañana" in s) or ("manana" in s) or s.startswith(("06:", "07:", "08:", "09:", "10:", "11:", "12:", "13:"))
    if shift == "Tarde":
        return ("tarde" in s) or s.startswith(("14:", "15:", "16:", "17:", "18:", "19:", "20:", "21:"))
    if shift == "Noche":
        return ("noche" in s) or s.startswith(("22:", "23:", "00:", "01:", "02:", "03:", "04:", "05:", "06:"))
    return True


def _read_sheet_people(xlsx_path: str, sheet_name: str, shift: str) -> list[dict]:
    if not Path(xlsx_path).exists():
        return []
    try:
        df = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str, header=2)
    except Exception as e:
        print("⚠️ No se pudo leer hoja:", sheet_name, repr(e))
        return []
 
    cols = _normalize_cols(list(df.columns))
   
    req = ["apellidos, nombre", "horario"]
    if any(cols[k] is None for k in req):
        print("⚠️ Hoja sin columnas esperadas:", sheet_name, "→", df.columns.tolist())
        return []
 
    full_name_col = cols["apellidos, nombre"]
    ho_col = cols["horario"]
    ob_col = cols["observaciones"] or ho_col
 
    people = []
    for _, row in df.iterrows():
        full_name = str(row.get(full_name_col) or "").strip()
        ho = str(row.get(ho_col) or "").strip()
 
        if not full_name or full_name.lower() == "nan":
            continue
        if not _match_shift(ho, shift):
            continue
 
        # --- CAMBIO FINAL Y CLAVE AQUÍ ---
        # Leemos las observaciones y si el resultado es "nan", lo convertimos a texto vacío.
        ob = str(row.get(ob_col) or "").strip()
        if ob.lower() == 'nan':
            ob = ""
        # ------------------------------------
 
        ap, no = "", ""
        if "," in full_name:
            parts = full_name.split(",", 1)
            ap = parts[0].strip()
            no = parts[1].strip()
        else:
            ap = full_name
       
        people.append({
            "apellidos": ap,
            "nombre": no,
            "nombre_completo": full_name,
            "horario": ho,
            "observaciones": ob,
        })
    return people

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Header, WebSocket, WebSocketDisconnect, Request
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
import tempfile
from pathlib import Path

TASKS_DB = os.getenv("TASKS_DB", "./data/tasks.json")

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

def _pick_env(*names, default=""):
    for n in names:
        v = os.getenv(n)
        if v and v.strip():
            return v.strip()
    return default

# ===== ENABLON efectivo (acepta ENA_* o ENABLON_*) =====
ENA_URL        = _pick_env("ENA_URL", "ENABLON_URL")
ENA_COOKIE     = _pick_env("ENA_COOKIE", "ENABLON_COOKIE")
ENA_REFERER    = _pick_env("ENA_REFERER", "ENABLON_REFERER")
ENA_USER_AGENT = _pick_env("ENA_USER_AGENT", "ENABLON_USER_AGENT", default="Mozilla/5.0")
# opcional: bearer/JWT si tu Enablon lo usa
ENA_BEARER     = _pick_env("ENA_BEARER", "ENABLON_BEARER")

# SSL específicos de ENA (si los tienes); si no, caerá a los de EXT más abajo
ENA_VERIFY_MODE  = _pick_env("ENA_VERIFY_MODE", "EXT_VERIFY_MODE", default="TRUSTSTORE").upper()
ENA_CAFILE       = _pick_env("ENA_CAFILE", "EXT_CAFILE", default="")


# -----------------------------------
# Modelos
# -----------------------------------
class Task(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    task_type: str
    status: Optional[str] = None
    title: str
    assigned_to: Optional[str] = None
    due_date: Optional[str] = None  # ISO (YYYY-MM-DD)
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat(timespec='seconds') + 'Z')
    action: Optional[str] = None
    is_completed: bool = False
    category: Optional[str] = None
    note: Optional[str] = None

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

# ===== NUEVO: almacén de INCIDENTES (Enablon) =====
latest_incidents_table: Dict[str, Any] = {
    "columns": [],
    "rows": [],
    "fetched_at": None,
    "version": 0,
}

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self._broadcast_stats = {"success": 0, "failed": 0, "last_error": None}

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        print(f"🔌 WS conectado. Activos: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            print(f"🔌 WS desconectado. Activos: {len(self.active_connections)}")

    async def broadcast(self, data: Dict[str, Any]):
        """Broadcasting mejorado con estadísticas y manejo de errores"""
        if not self.active_connections:
            print("📡 No hay conexiones WebSocket activas para broadcast")
            return

        success_count = 0
        failed_connections = []
        
        # Log del mensaje que se va a enviar (solo para debugging de tareas)
        if data.get('task_type') or data.get('action') == 'delete':
            print(f"📡 Broadcasting: {data.get('task_type', 'unknown')} - {data.get('title', data.get('id', 'unknown'))}")
        
        for connection in list(self.active_connections):  # Copia para modificar durante iteración
            try:
                await connection.send_json(data)
                success_count += 1
            except Exception as e:
                print(f"❌ Error enviando a WebSocket: {str(e)}")
                failed_connections.append(connection)
                self._broadcast_stats["failed"] += 1
                self._broadcast_stats["last_error"] = str(e)

        # Limpiar conexiones fallidas
        for failed_conn in failed_connections:
            self.disconnect(failed_conn)
        
        self._broadcast_stats["success"] += success_count
        
        if success_count > 0:
            print(f"📡 Broadcast exitoso a {success_count}/{len(self.active_connections) + len(failed_connections)} conexiones")
        else:
            print(f"❌ Broadcast falló a todas las conexiones")
        
        return success_count

    async def send_one(self, websocket: WebSocket, data: Dict[str, Any]):
        try:
            await websocket.send_json(data)
        except Exception:
            self.disconnect(websocket)

    def get_stats(self):
        return {
            "active_connections": len(self.active_connections),
            "broadcast_stats": self._broadcast_stats
        }

# Asegúrate de que solo hay UNA instancia de ConnectionManager
manager = ConnectionManager()

# ======================================================================
# ===== NUEVO: helpers SSL / parsing / polling de la fuente interna =====
# ======================================================================
PATH_IDX = re.compile(r"(.*?)\[(\d+)\]$")

def _pluck_by_path(obj, path: str):
    """
    Camino tipo 'a.b[0].c.d' → devuelve obj['a']['b'][0]['c']['d'] o None.
    """
    cur = obj
    for raw in filter(None, [p.strip() for p in (path or "").split(".")]):
        m = PATH_IDX.match(raw)
        key, idx = (raw, None) if not m else (m.group(1), int(m.group(2)))
        if key:
            if not isinstance(cur, dict) or key not in cur:
                return None
            cur = cur[key]
        if idx is not None:
            if not isinstance(cur, list) or idx >= len(cur):
                return None
            cur = cur[idx]
    return cur
def _walk_lists(obj, prefix=""):
    """
    Recorre el payload y yielda (path, list_obj) para cada lista de dicts.
    """
    if isinstance(obj, dict):
        for k, v in obj.items():
            path = f"{prefix}.{k}" if prefix else k
            if isinstance(v, list) and (not v or isinstance(v[0], dict)):
                yield (path, v)
            elif isinstance(v, (dict, list)):
                yield from _walk_lists(v, path)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            path = f"{prefix}[{i}]"
            yield from _walk_lists(v, path)

def _pick_preferred_list(payload):
    """
    Devuelve la 'mejor' lista de dicts dentro de payload, priorizando nombres
    que parezcan de incidentes y evitando 'guides' y compañía.
    Controlable por env: ENA_PREFER_LIST_KEYS / ENA_AVOID_LIST_KEYS
    """
    prefer = [s.strip().lower() for s in os.getenv("ENA_PREFER_LIST_KEYS", "incidents,incident,events,event,records,rows,items,results,issues,alerts,tickets").split(",") if s.strip()]
    avoid  = [s.strip().lower() for s in os.getenv("ENA_AVOID_LIST_KEYS", "guides,guideWidget,features,segmentFlags").split(",") if s.strip()]

    candidates = []
    for path, lst in _walk_lists(payload):
        last_key = path.split(".")[-1].lower()
        score = (prefer.index(last_key) if last_key in prefer else 999)
        bad   = any(a == last_key for a in avoid)
        # penaliza si está en avoid
        if bad:
            score += 1000
        # preferir listas con >0 filas
        size_bonus = -min(len(lst), 100)  # más grande → mejor (ligero)
        candidates.append((score, size_bonus, path, lst))

    if not candidates:
        return None, None

    candidates.sort(key=lambda t: (t[0], t[1], t[2]))
    best = candidates[0]
    return best[3], best[2]  # (lista, ruta)


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

def _build_ssl_context_for(mode: str, cafile: str) -> ssl.SSLContext | bool:
    mode = (mode or "").upper()
    if mode == "FALSE":
        return False
    if mode == "TRUSTSTORE":
        try:
            import truststore
            truststore.inject_into_ssl()
            return ssl.create_default_context()
        except Exception as e:
            print("⚠️ truststore no disponible, CA por defecto:", e)
            return ssl.create_default_context()
    if mode == "CAFILE" and cafile:
        return ssl.create_default_context(cafile=cafile)
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
    """
    Localiza una lista de registros (dict) dentro de cualquier JSON.
    Ahora incluye 'value' (típico de OData).
    """
    if isinstance(obj, list) and (not obj or isinstance(obj[0], dict)):
        return obj

    if isinstance(obj, dict):
        # prioriza contenedores típicos
        for key in ("data", "items", "rows", "result", "results",
                    "all", "records", "entries", "elements", "list",
                    "value"):   # ← ¡aquí el fix!
            v = obj.get(key)
            if isinstance(v, list) and (not v or isinstance(v[0], dict)):
                return v

        # buscar en anidados
        for v in obj.values():
            if isinstance(v, (dict, list)):
                found = _list_of_dicts(v)
                if found:
                    return found

        # dict plano → 1 fila
        if all(not isinstance(v, (list, dict)) for v in obj.values()):
            return [obj]

    return []

def _get_ci(d: dict, *candidates: str):
    # devuelve (key_encontrada, valor) ignorando mayúsculas
    if not isinstance(d, dict):
        return None, None
    low = {k.lower(): k for k in d.keys()}
    for cand in candidates:
        k = low.get(cand.lower())
        if k is not None:
            return k, d[k]
    return None, None


def _table_from_json(payload: Any) -> tuple[list[str], list[list[Any]]]:
    """
    Devuelve (columns, rows) desde payloads MUY variados:
    - {"columns":[...], "rows":[...]}  (rows puede ser lista de dicts o de listas)
    - {"value":[...]} / {"data":[...]} / {"items":[...]} / etc.
    - lista de dicts, lista de listas, lista de escalares
    - dict plano -> 1 fila
    """
    def _ci_get(d: dict, *names: str):
        # devuelve (clave_real, valor) ignorando mayúsculas
        low = {k.lower(): k for k in d.keys()}
        for n in names:
            k = low.get(n.lower())
            if k is not None:
                return k, d[k]
        return None, None

    # 0) Dict con 'columns' + 'rows' (case-insensitive)
    if isinstance(payload, dict):
        _, rows_val = _ci_get(payload, "rows")
        _, cols_val = _ci_get(payload, "columns", "cols", "headers")

        if rows_val is not None:
            # rows = lista de dicts
            if isinstance(rows_val, list) and (not rows_val or isinstance(rows_val[0], dict)):
                flat = [_flatten_record(r) for r in rows_val]
                seen, cols = set(), []
                for r in flat:
                    for k in r.keys():
                        if k not in seen:
                            seen.add(k); cols.append(k)
                rows = [[_safe_cell(r.get(c)) for c in cols] for r in flat]
                return cols, rows

            # rows = lista de listas/tuplas
            if isinstance(rows_val, list) and rows_val and isinstance(rows_val[0], (list, tuple)):
                # columnas: usa 'columns' si llega; si son objetos, intenta name/label/header/title/field/key
                cols: list[str] = []
                if isinstance(cols_val, list) and cols_val:
                    if isinstance(cols_val[0], str):
                        cols = [str(c) for c in cols_val]
                    elif isinstance(cols_val[0], dict):
                        def pick_name(c: dict):
                            for key in ("name","label","header","title","text","field","key"):
                                if key in c: return str(c[key])
                            # si no encontramos nombre, devuelve el primer valor "representativo"
                            for v in c.values():
                                if isinstance(v, (str,int,float,bool)): return str(v)
                            return None
                        cols = [pick_name(c) or f"col_{i+1}" for i, c in enumerate(cols_val)]
                # si no hay columns válidas, genera col_1..N con el ancho de la primera fila
                if not cols:
                    n = len(rows_val[0])
                    cols = [f"col_{i+1}" for i in range(n)]
                # normaliza filas
                fixed_rows = []
                for r in rows_val:
                    r_list = list(r)
                    # ajusta longitud por si vienen de distinto largo
                    if len(r_list) < len(cols):
                        r_list += [None] * (len(cols) - len(r_list))
                    elif len(r_list) > len(cols):
                        r_list = r_list[:len(cols)]
                    fixed_rows.append([_safe_cell(x) for x in r_list])
                return cols, fixed_rows

            # rows = lista de escalares
            if isinstance(rows_val, list) and rows_val and not isinstance(rows_val[0], (list, tuple, dict)):
                col_name = "value"
                if isinstance(cols_val, list) and len(cols_val) == 1 and isinstance(cols_val[0], str):
                    col_name = cols_val[0]
                return [col_name], [[_safe_cell(v)] for v in rows_val]

            # rows = dict único -> 1 fila
            if isinstance(rows_val, dict):
                flat = _flatten_record(rows_val)
                cols = list(flat.keys())
                return cols, [[_safe_cell(flat.get(c)) for c in cols]]

        # 1) contenedores típicos (value/data/items/results/all/records/entries/elements/list)
        for key in ("value", "data", "items", "results", "all", "records", "entries", "elements", "list"):
            sub = payload.get(key)
            if isinstance(sub, (dict, list)):
                cols, rows = _table_from_json(sub)
                if cols or rows:
                    return cols, rows

        # 2) dict plano -> 1 fila
        if all(not isinstance(v, (list, dict)) for v in payload.values()):
            cols = list(payload.keys())
            return cols, [[_safe_cell(payload.get(c)) for c in cols]]

    # 3) lista de dicts
    if isinstance(payload, list) and (not payload or isinstance(payload[0], dict)):
        flat = [_flatten_record(r) for r in payload]
        seen, cols = set(), []
        for r in flat:
            for k in r.keys():
                if k not in seen:
                    seen.add(k); cols.append(k)
        rows = [[_safe_cell(r.get(c)) for c in cols] for r in flat]
        return cols, rows

    # 4) lista de listas
    if isinstance(payload, list) and payload and isinstance(payload[0], (list, tuple)):
        n = len(payload[0])
        cols = [f"col_{i+1}" for i in range(n)]
        rows = [[_safe_cell(v) for v in row] for row in payload]
        return cols, rows

    # 5) lista de escalares
    if isinstance(payload, list) and payload and not isinstance(payload[0], (list, tuple, dict)):
        return ["value"], [[_safe_cell(v)] for v in payload]

    # 6) dict anidado sin match claro -> intenta aplanar
    if isinstance(payload, dict):
        flat = _flatten_record(payload)
        if flat:
            cols = list(flat.keys())
            return cols, [[_safe_cell(flat.get(c)) for c in cols]]

    # vacío/ilegible
    return [], []



import time
from urllib.parse import unquote

SESSION_DIR = Path(os.getenv("SESSION_DIR", "./data"))
EXT_SESSION_PATH = SESSION_DIR / "ext_session.json"
ENA_SESSION_PATH = SESSION_DIR / "ena_session.json"

def _parse_set_cookie_expires(raw: str) -> float | None:
    # Busca Expires=... o Max-Age=...
    raw_l = raw.lower()
    # Max-Age tiene prioridad si existe
    m = re.search(r"max-age\s*=\s*(\d+)", raw_l)
    if m:
        try:
            sec = int(m.group(1))
            return time.time() + max(0, sec)
        except: 
            pass
    m = re.search(r"expires\s*=\s*([^;]+)", raw_l)
    if m:
        txt = m.group(1).strip()
        # formatos GMT típicos: Wdy, DD Mon YYYY HH:MM:SS GMT
        try:
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(txt)
            return dt.timestamp()
        except:
            return None
    return None

def _extract_xsrf_from_cookie(cookie_header: str) -> dict[str,str]:
    jar = _parse_cookie_header(cookie_header or "")
    # nombres habituales
    for key in ("XSRF-TOKEN", "CSRF-TOKEN", "RequestVerificationToken"):
        if key in jar:
            val = unquote(jar[key])
            return {
                "X-XSRF-TOKEN": val,
                "X-CSRF-TOKEN": val,  # manda ambos, inofensivo si sobra
                "RequestVerificationToken": val,   # ← añade esto
            }
    return {}

def _save_cookie_session(path: Path, cookie_header: str, expires_at: float | None):
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as fh:
            json.dump({"cookie": cookie_header, "expires_at": expires_at}, fh, ensure_ascii=False)
    except Exception as e:
        print("⚠️ No pude guardar sesión:", e)

def _load_cookie_session(path: Path) -> tuple[str, float | None]:
    try:
        if not path.exists(): return "", None
        obj = json.load(path.open("r", encoding="utf-8")) or {}
        return obj.get("cookie",""), obj.get("expires_at")
    except Exception:
        return "", None

def _merge_set_cookie_with_expiry(existing_cookie: str, set_cookie_headers: list[str]) -> tuple[str, float | None]:
    """
    Igual que _merge_set_cookie_into_header, pero devuelve también la expiración más cercana.
    """
    jar = _parse_cookie_header(existing_cookie) if existing_cookie else {}
    exp_candidates: list[float] = []

    for raw in (set_cookie_headers or []):
        if not raw: 
            continue
        first = raw.split(";", 1)[0]
        if "=" in first:
            k, v = first.split("=", 1)
            jar[k.strip()] = v.strip()
            exp = _parse_set_cookie_expires(raw)
            if exp: exp_candidates.append(exp)

    parts, seen = [], set()
    for part in (existing_cookie or "").split(";"):
        if "=" in part:
            name = part.split("=", 1)[0].strip()
            if name and name in jar and name not in seen:
                parts.append(f"{name}={jar[name]}")
                seen.add(name)
    for name, val in jar.items():
        if name not in seen:
            parts.append(f"{name}={val}")

    expires_at = min(exp_candidates) if exp_candidates else None
    return "; ".join(parts), expires_at


# --- reemplaza tu apply_external_table por esta versión ---
async def apply_external_table(payload: Any):
    cols, rows = _table_from_json(payload)

    # ⛔ No pises el snapshot si viene vacío/ilegible
    if not cols and not rows:
        print("ℹ️ External: payload vacío/ilegible; se conserva último estado.")
        # puedes avisar solo estado, sin cambiar version
        await manager.broadcast({
            "type": "external_status",
            "ok": True,
            "url": app.state._ext.current_url if hasattr(app.state, "_ext") else None,
            "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "note": "payload_vacio"
        })
        return

    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    latest_external_table["columns"]    = cols
    latest_external_table["rows"]       = rows
    latest_external_table["fetched_at"] = ts
    latest_external_table["version"]    = int(latest_external_table.get("version", 0)) + 1

    save_external_to_disk()

    print(f"📦 External table: {len(rows)} filas, {len(cols)} columnas. Ejemplo columnas: {cols[:8]}")
    await manager.broadcast({
        "type": "table_ping",
        "table": "external",
        "version": latest_external_table["version"],
        "rows": len(rows),
        "fetched_at": ts,
    })


async def apply_incidents_table(payload: Any):
    cols, rows = _table_from_json(payload)

    # ⛔ Si no pudimos parsear nada, no machacamos el último bueno
    if not cols and not rows:
        print("ℹ️ Incidentes: payload vacío/ilegible; se conserva último estado.")
        return

    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    latest_incidents_table["columns"]    = cols
    latest_incidents_table["rows"]       = rows
    latest_incidents_table["fetched_at"] = ts
    latest_incidents_table["version"]    = int(latest_incidents_table.get("version", 0)) + 1

    save_incidents_to_disk()  # ← persistir

    await manager.broadcast({
        "type": "table_ping",
        "table": "incidents",
        "version": latest_incidents_table["version"],
        "rows": len(rows),
        "fetched_at": ts,
    })


async def push_incidents_table_state(websocket: WebSocket):
    await manager.send_one(websocket, {
        "type": "table_state",
        "table": "incidents",
        "version": latest_incidents_table.get("version", 0),
        "rows": len(latest_incidents_table.get("rows", [])),
        "fetched_at": latest_incidents_table.get("fetched_at"),
    })

async def push_external_table_state(websocket: WebSocket):
    await manager.send_one(websocket, {
        "type": "table_state",
        "table": "external",
        "version": latest_external_table.get("version", 0),
        "rows": len(latest_external_table.get("rows", [])),
        "fetched_at": latest_external_table.get("fetched_at"),
    })

# 

# ---------- ENABLON PROBE (mejorado) ----------
# ---------- ENABLON PROBE (mínimo y exacto) ----------
# ---------- ENABLON PROBE (mínimo y exacto) ----------
async def enablon_probe():
    """
    GET a ENA_URL. Si es JSON:
      - loguea forma y claves,
      - si existe 'data' en raíz, vuelca payload['data']; si no, la raíz.
      - actualiza latest_external_table.
    Reenvía la cookie EXACTA como header para evitar 401 por parseos.
    """
    if not ENA_URL:
        print("⚠️ ENA_URL/ENABLON_URL no configurada.")
        return {"ok": False, "error": "ENA_URL not configured"}

    headers = {
        "User-Agent": ENA_USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "X-Requested-With": "XMLHttpRequest",
    }
    if ENA_REFERER:
        headers["Referer"] = ENA_REFERER
    if ENA_BEARER:
        headers["Authorization"] = f"Bearer {ENA_BEARER}"

    # ⬇️ CLAVE: no parsees la cookie; reenvíala tal cual
    cookie_header = (ENA_COOKIE or "").strip()
    if cookie_header:
        headers["Cookie"] = cookie_header

    try:
        async with httpx.AsyncClient(
            verify=_build_ssl_context_for(ENA_VERIFY_MODE, ENA_CAFILE),
            headers=headers,
            # NO pasamos cookies=..., para que no pise el header Cookie crudo
        ) as client:
            resp = await client.get(ENA_URL, timeout=30.0, follow_redirects=False)
            status = resp.status_code
            ctype  = (resp.headers.get("content-type") or "").lower()
            loc    = resp.headers.get("location")
            print(f"🔎 Enablon GET {ENA_URL} → {status} ({ctype})")
            print(f"   · headers: cookie={'sí' if 'Cookie' in headers else 'no'}, bearer={'sí' if ENA_BEARER else 'no'}, referer={'sí' if ENA_REFERER else 'no'}")
            if status in (301,302,303,307,308):
                print(f"   · redirección a: {loc}")

            info = {"ok": True, "status": status, "content_type": ctype}

            if status >= 300:
                head = (resp.text or "")[:500]
                print(f"⚠️ Respuesta HTTP {status}. Body (500 chars):\n{head}")
                info.update(ok=False, body_head=head, location=loc)
                # Pistas útiles:
                if status == 401:
                    print("🔐 PISTA: revisa que el header Cookie copiado del portal no haya caducado y que lo pegaste entero.")
                if status in (301,302,303,307,308) and loc and "login" in (loc.lower() if isinstance(loc,str) else ""):
                    print("🔐 PISTA: parece una redirección a login; falta cookie válida o bearer.")
                return info

            # Intentar JSON
            try:
                payload = resp.json()
                info["kind"] = type(payload).__name__
                if isinstance(payload, dict):
                    keys = list(payload.keys())
                    print(f"🧩 JSON = objeto con {len(keys)} claves. Primeras: {keys[:20]}")
                elif isinstance(payload, list):
                    print(f"🧩 JSON = lista con {len(payload)} elementos. Muestra 3:")
                    for i, row in enumerate(payload[:3]):
                        print(f"   • [{i}] {type(row).__name__}: {str(row)[:200]}")
                else:
                    print(f"🧩 JSON de tipo {type(payload).__name__}")

                # payload ya contiene el JSON de la respuesta
                base = payload
                json_path = os.getenv("ENA_JSON_PATH", "").strip()
                base = None
                if json_path:
                    picked = _pluck_by_path(payload, json_path)
                    if picked is not None:
                        base = picked

                # Si la ruta no existe o apunta a un contenedor, escogemos una lista preferida
                if base is None or (isinstance(base, dict) and not isinstance(base, list)):
                    base_list, best_path = _pick_preferred_list(base if base is not None else payload)
                    if base_list is not None:
                        print(f"🧭 ENA picker → {best_path} ({len(base_list)} filas)")
                        base = base_list

                # Fallback clásico a 'data' o raíz
                if base is None:
                    base = payload.get("data") if isinstance(payload, dict) and "data" in payload else payload

                await apply_incidents_table(base)

                
                rows = len(latest_incidents_table.get("rows", []))
                print(f"📤 Volcado a latest_incidents_table ({rows} filas).")
                info["pushed_to_table"] = True
                info["rows"] = rows
                return info

            except Exception:
                # No era JSON → mostrar texto o CSV
                body = resp.text or ""
                info["body_head"] = body[:500]
                if "text/csv" in ctype or ("," in body and "\n" in body[:200]):
                    print("🧾 CSV detectado. Primeras 5 líneas:")
                    for ln in body.splitlines()[:5]:
                        print("   ", ln[:200])
                else:
                    print("📄 Respuesta no-JSON. Muestra (500 chars):")
                    print(body[:500])
                return info

    except Exception as e:
        print("💥 Error en enablon_probe:", repr(e))
        return {"ok": False, "error": str(e)}







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
        # detección de “JSON de login/expulsado” aunque el HTTP sea 200
        def _looks_like_login(obj):
            if not isinstance(obj, dict):
                return False
            text = json.dumps({k: obj.get(k) for k in list(obj.keys())[:10]}).lower()
            bad = ("login", "signin", "unauthorized", "forbidden", "expired", "csrf", "xsrf")
            has_table = any(k in (key.lower() for key in obj.keys()) for k in ("data","value","items","results","rows","records","list"))
            return (any(b in text for b in bad) and not has_table)

        if _looks_like_login(data):
            raise httpx.HTTPStatusError("auth", request=resp.request, response=resp)

    except Exception:
        # Si no es JSON válido, no tocamos nada
        print("⚠️ La respuesta no es JSON; se ignora para la tabla externa.")
        return

    await apply_external_table(data)

# ============================
# ExternalConnector (robusto)
# ============================
import shlex
from dataclasses import dataclass, field

@dataclass
class ExternalSettings:
    urls: list[str] = field(default_factory=list)
    referer: str = ""
    user_agent: str = "Mozilla/5.0"
    cookie_header: str = ""          # "k1=v1; k2=v2"
    poll_seconds: int = 60
    verify_mode: str = "TRUSTSTORE"  # TRUSTSTORE|CERTIFI|CAFILE|FALSE
    cafile: str = ""
    auth_mode: str = "COOKIE"        # COOKIE|LOGIN_POST|SCRIPT

    # --- NUEVO: keepalive/track ---
    keepalive_url: str = ""          # GET ligero que renueva sesión
    track_url: str = ""              # POST telemetría/track que renueva sesión
    track_body_json: str = ""        # cuerpo JSON (plantilla con {now_ms},{now_iso},{url})
    keepalive_every: int = 0         # segundos; 0 = auto por caducidad cookie

    # LOGIN_POST
    login_url: str = ""
    login_payload_json: str = ""
    login_csrf_regex: str = ""
    username: str = ""
    password: str = ""

    # SCRIPT
    refresh_cmd: str = ""            # imprime JSON {cookie,url}

    # miscelánea
    http2: bool = True
    max_keepalive: int = 20
    max_connections: int = 40

    @classmethod
    def from_env(cls) -> "ExternalSettings":
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

            # NUEVO
            keepalive_url=os.getenv("EXT_KEEPALIVE_URL", "").strip(),
            track_url=os.getenv("EXT_TRACK_URL", "").strip(),
            track_body_json=os.getenv("EXT_TRACK_BODY_JSON", "").strip(),
            keepalive_every=int(os.getenv("EXT_KEEPALIVE_EVERY", "0") or "0"),

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


@dataclass
class EnablonSettings(ExternalSettings):
    @classmethod
    def from_env(cls) -> "EnablonSettings":
        # 1) primero intentamos ENA_URL1..ENA_URL5 (uno por línea en .env)
        urls: list[str] = []
        for i in range(1, 6):
            v = os.getenv(f"ENA_URL{i}", "").strip()
            if v:
                urls.append(v)

        # 2) si no hay, caemos a ENA_URLS (separado por salto de línea o por coma)
        if not urls:
            raw = os.getenv("ENA_URLS", "").strip()
            if "\n" in raw:
                urls = [u.strip() for u in raw.splitlines() if u.strip()]
            else:
                urls = [u.strip() for u in raw.split(",") if u.strip()]

        if not urls:
            u = _pick_env("ENA_URL", "ENABLON_URL")
            if u: urls = [u]

        return cls(
            urls=urls,
            referer=_pick_env("ENA_REFERER", "ENABLON_REFERER"),
            user_agent=_pick_env("ENA_USER_AGENT", "ENABLON_USER_AGENT", default="Mozilla/5.0"),
            cookie_header=_pick_env("ENA_COOKIE", "ENABLON_COOKIE"),
            poll_seconds=int(_pick_env("ENA_POLL_SECONDS", "ENABLON_POLL_SECONDS", default="60")),
            verify_mode=_pick_env("ENA_VERIFY_MODE", "EXT_VERIFY_MODE", default="TRUSTSTORE").upper(),
            cafile=_pick_env("ENA_CAFILE", "EXT_CAFILE", default=""),
            auth_mode=_pick_env("ENA_AUTH_MODE", "ENABLON_AUTH_MODE", default="COOKIE").upper(),
            login_url=_pick_env("ENA_LOGIN_URL", "ENABLON_LOGIN_URL"),
            login_payload_json=_pick_env("ENA_LOGIN_PAYLOAD_JSON", "ENABLON_LOGIN_PAYLOAD_JSON"),
            login_csrf_regex=_pick_env("ENA_LOGIN_CSRF_REGEX", "ENABLON_LOGIN_CSRF_REGEX"),
            username=_pick_env("ENA_USERNAME", "ENABLON_USERNAME"),
            password=_pick_env("ENA_PASSWORD", "ENABLON_PASSWORD"),
            refresh_cmd=_pick_env("ENA_REFRESH_CMD", "ENABLON_REFRESH_CMD"),
            keepalive_url=_pick_env("ENA_KEEPALIVE_URL","ENABLON_KEEPALIVE_URL"),
            http2=(_pick_env("ENA_HTTP2", "ENABLON_HTTP2", default="true").lower() == "true"),
            max_keepalive=int(_pick_env("ENA_MAX_KEEPALIVE", "ENABLON_MAX_KEEPALIVE", default="20")),
            max_connections=int(_pick_env("ENA_MAX_CONNECTIONS", "ENABLON_MAX_CONNECTIONS", default="40")),
        )


def _parse_endpoint_spec(spec: str):
    """
    ENA_URLS admite:
      - "GET https://host/api"
      - "POST https://host/api | { ...json... }"             (JSON)
      - "POST https://host/api | FORM a=1&b=2"               (x-www-form-urlencoded)
      - "POST https://host/api | RAW some=verbatim&payload"  (content/raw)
    """
    s = (spec or "").strip()
    method = "GET"
    body = None
    body_mode = "json"  # json|form|raw

    m = re.match(r"^(GET|POST)\s+(.+)$", s, flags=re.I)
    if m:
        method = m.group(1).upper()
        s = m.group(2).strip()

    url = s
    if "|" in s:
        url, body_str = s.split("|", 1)
        url, body_str = url.strip(), body_str.strip()
        # plantillas de tiempo
        body_str = (body_str
                    .replace("{now_ms}", str(int(time.time() * 1000)))
                    .replace("{now_iso}", datetime.utcnow().isoformat(timespec="seconds") + "Z"))
        # modos
        up = body_str.upper()
        if up.startswith("FORM "):
            body_mode = "form"
            body = body_str[5:].strip()  # tal cual: "a=1&b=2"
        elif up.startswith("RAW "):
            body_mode = "raw"
            body = body_str[4:].strip()  # payload sin tocar
        else:
            # por defecto: JSON
            try:
                body = json.loads(body_str)
            except Exception:
                body_mode = "raw"
                body = body_str
    return method, url, body, body_mode


import io

class EnablonConnector:
    def __init__(self):
        self.settings = EnablonSettings.from_env()
        self._env_path = Path(os.getenv("ENV_FILE", ".env"))
        self._env_mtime = self._env_path.stat().st_mtime if self._env_path.exists() else None
        self._client: httpx.AsyncClient | None = None
        self._url_idx = 0
        self._consec_fail = 0
        self._last_ok: str | None = None
        self._timeout_url = os.getenv("ENA_TIMEOUT_URL", "").strip()
        self._timeout_secs: int | None = None
        self._last_keepalive: float = 0.0
        self._keepalive_every: int = 300  # 5 min por defecto hasta medir
        self._status: dict[str, Any] = {
            "ok": False, "last_ok": None, "last_error": None, "fails": 0, "url": self.current_url
        }

    def _xsrf_headers(self) -> dict[str, str]:
        return _extract_xsrf_from_cookie(self.settings.cookie_header or "")
    
    @property
    def current_url(self) -> str:
        return self.settings.urls[self._url_idx] if self.settings.urls else ""

    def _ssl_verify(self):
        return _build_ssl_context_for(self.settings.verify_mode, self.settings.cafile)

    async def _ensure_client(self, recycle: bool = False):
        if self._client is not None and not recycle:
            return
        if self._client is not None and recycle:
            try: await self._client.aclose()
            except Exception: pass

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
            transport=transport,
            headers={
                "User-Agent": self.settings.user_agent,
                "Accept": "application/json, text/plain, */*",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": self.settings.referer or None,
            }
        )

    
    async def _keepalive(self) -> bool:
        """
        Mantiene viva la sesión de Enablon usando (por orden):
        ENA_KEEPALIVE_URL, ENA_TIMEOUT_URL o la URL actual.
        """
        if not self._client:
            return False
        url = (self.settings.keepalive_url or self._timeout_url or self.current_url)
        if not url:
            return False
        try:
            h = {}
            if self.settings.cookie_header:
                h["Cookie"] = self.settings.cookie_header
                h.update(self._xsrf_headers())
            r = await self._client.get(url, headers=h, timeout=20.0, follow_redirects=False)
            setc = r.headers.get_list("set-cookie")
            if setc:
                self.settings.cookie_header = _merge_set_cookie_into_header(self.settings.cookie_header, setc)
            ok = r.status_code < 400
            if ok and url == self._timeout_url:
                await self._fetch_timeout_secs()
            return ok
        except Exception:
            return False

    async def _reload_env_if_changed(self):
        try:
            if self._env_path.exists():
                m = self._env_path.stat().st_mtime
                if self._env_mtime != m:
                    self._env_mtime = m
                    load_dotenv(override=True)
                    new_settings = EnablonSettings.from_env()
                    if new_settings != self.settings:
                        self.settings = new_settings
                        await self._ensure_client(recycle=True)
                        self._url_idx = 0
                        print("🔄 .env Enablon recargado y cliente renovado.")
        except Exception as e:
            print("⚠️ Hot-reload .env (Enablon) error:", e)

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
                if m: csrf = m.group(1)
        except Exception as e:
            self._status.update(ok=False, last_error=f"login_get:{e}")
            return False

        payload = {}
        if self.settings.login_payload_json:
            try: payload = json.loads(self.settings.login_payload_json)
            except Exception: payload = {}
        if payload:
            payload = json.loads(json.dumps(payload)
                                 .replace("{username}", self.settings.username)
                                 .replace("{password}", self.settings.password)
                                 .replace("{csrf}", csrf or ""))        
        elif self.settings.username:
            payload = {"username": self.settings.username, "password": self.settings.password}
            if csrf is not None: payload["csrf"] = csrf

        try:
            r = await self._client.post(self.settings.login_url, data=payload, timeout=30.0)
            r.raise_for_status()
            return True
        except Exception as e:
            self._status.update(ok=False, last_error=f"login_post:{e}")
            return False

    async def _refresh_via_script(self) -> bool:
        if not self.settings.refresh_cmd:
            return False
        try:
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
            if cookie: self.settings.cookie_header = cookie
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
        if self.settings.auth_mode == "SCRIPT" and await self._refresh_via_script():
            return True
        if self.settings.auth_mode == "LOGIN_POST" and await self._login_if_needed():
            return True
        await self._reload_env_if_changed()
        await self._ensure_client(recycle=True)
        return True

    def _rotate_url(self):
        if len(self.settings.urls) > 1:
            self._url_idx = (self._url_idx + 1) % len(self.settings.urls)
            print(f"🔀 Enablon → {self.current_url}")

    async def _fetch_timeout_secs(self):
        if not self._timeout_url or not self._client:
            return
        h = {}
        if self.settings.cookie_header:
            h["Cookie"] = self.settings.cookie_header
            h.update(self._xsrf_headers())
        r = await self._client.get(self._timeout_url, headers=h, timeout=15.0, follow_redirects=False)
        setc = r.headers.get_list("set-cookie")
        if setc:
            self.settings.cookie_header = _merge_set_cookie_into_header(self.settings.cookie_header, setc)

        txt = (r.text or "").strip()
        m = re.search(r"\d{2,}", txt)  # caza 5399, 5389, etc.
        if m:
            t = int(m.group(0))
            self._timeout_secs = t
            # programa keepalive: mitad del timeout, con margen
            self._keepalive_every = max(120, min(t - 90, t // 2))

    
    async def _fetch_once(self):
        if not self.current_url:
            return
        await self._ensure_client()

        try:
            # --- Selección método/URL/body desde ENA_URLS ---
            method, url, body, body_mode = _parse_endpoint_spec(self.current_url)

            # --- Headers por petición ---
            headers = {}
            if self.settings.cookie_header:
                headers["Cookie"] = self.settings.cookie_header
                headers.update(self._xsrf_headers())  # XSRF/CSRF desde la cookie
            bearer = _pick_env("ENA_BEARER", "ENABLON_BEARER")
            if bearer:
                headers["Authorization"] = f"Bearer {bearer}"

            # --- Disparo (GET/POST) ---
            resp = None
            if method == "POST":
                post_headers = dict(headers)
                json_body = None
                content = None

                # Usa body_mode devuelto por _parse_endpoint_spec
                if body_mode == "form":
                    post_headers["Content-Type"] = "application/x-www-form-urlencoded"
                    content = body if isinstance(body, str) else ""
                elif body_mode == "raw":
                    content = body if isinstance(body, (str, bytes)) else ""
                else:
                    # json
                    if isinstance(body, (dict, list)):
                        json_body = body
                    else:
                        # por si te viene un JSON serializado como string
                        try:
                            json_body = json.loads(body)
                        except Exception:
                            content = str(body or "")

                resp = await self._client.post(
                    url,
                    headers=post_headers,
                    json=json_body if json_body is not None else None,
                    content=content if json_body is None else None,
                    timeout=30.0,
                    follow_redirects=False
                )
            else:
                # GET
                resp = await self._client.get(
                    url,
                    headers=headers,
                    timeout=30.0,
                    follow_redirects=False
                )

            # --- Cookies nuevas (si las hay) ---
            setc = resp.headers.get_list("set-cookie")
            if setc:
                self.settings.cookie_header = _merge_set_cookie_into_header(self.settings.cookie_header, setc)

            # --- Tipos de contenido (define ctype/disp ANTES de usarlos) ---
            ctype = (resp.headers.get("content-type") or "").lower()
            disp  = (resp.headers.get("content-disposition") or "").lower()



            # --- CSV ---
            if ("text/csv" in ctype) or ("application/csv" in ctype) or ("filename=" in disp and disp.endswith(".csv\"")):
                try:
                    text = resp.text  # httpx decodifica
                    import pandas as pd, io as _io
                    df = pd.read_csv(_io.StringIO(text))
                    base = df.to_dict(orient="records")
                    await apply_incidents_table(base)
                    # estado OK
                    self._consec_fail = 0
                    self._last_ok = datetime.utcnow().isoformat(timespec="seconds") + "Z"
                    self._status.update(ok=True, last_ok=self._last_ok, last_error=None, fails=0, url=url)
                    await manager.broadcast({"type":"enablon_status","ok":True,"url":url,"ts":self._last_ok})
                    return
                except Exception as e:
                    raise ValueError(f"CSV parse failed: {e}")

            # --- XLSX ---
            if ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" in ctype) or ("filename=" in disp and disp.endswith(".xlsx\"")):
                try:
                    import pandas as pd, io as _io
                    buf = _io.BytesIO(resp.content)
                    df = pd.read_excel(buf)
                    base = df.to_dict(orient="records")
                    await apply_incidents_table(base)
                    self._consec_fail = 0
                    self._last_ok = datetime.utcnow().isoformat(timespec="seconds") + "Z"
                    self._status.update(ok=True, last_ok=self._last_ok, last_error=None, fails=0, url=url)
                    await manager.broadcast({"type":"enablon_status","ok":True,"url":url,"ts":self._last_ok})
                    return
                except Exception as e:
                    raise ValueError(f"XLSX parse failed: {e}")

            # --- PDF (no tocar tabla; sirve como keepalive/side-effect) ---
            if "application/pdf" in ctype or ("filename=" in disp and disp.endswith(".pdf\"")):
                # Considera cambiar el body del POST para pedir CSV/XLSX en vez de PDF.
                self._consec_fail = 0
                self._last_ok = datetime.utcnow().isoformat(timespec="seconds") + "Z"
                self._status.update(ok=True, last_ok=self._last_ok, last_error=None, fails=0, url=url)
                await manager.broadcast({"type":"enablon_status","ok":True,"url":url,"ts":self._last_ok})
                return


            # --- Estados de auth/redirect: NO toques la tabla si ocurre ---
            if resp.status_code in (401, 403, 419, 440, 301, 302, 303, 307, 308):
                raise httpx.HTTPStatusError("auth", request=resp.request, response=resp)
            if resp.status_code >= 500:
                raise httpx.HTTPStatusError("5xx", request=resp.request, response=resp)

            # --- Cookies nuevas (si las hay) ---
            setc = resp.headers.get_list("set-cookie")
            if setc:
                self.settings.cookie_header = _merge_set_cookie_into_header(self.settings.cookie_header, setc)

            # --- Intentar JSON (acepta text/plain con cuerpo JSON válido) ---
            ctype = (resp.headers.get("content-type") or "").lower()
            payload = None
            if "application/json" in ctype:
                payload = resp.json()
            else:
                txt = (resp.text or "").lstrip()
                if txt.startswith("{") or txt.startswith("["):
                    try:
                        payload = json.loads(txt)
                    except Exception:
                        pass

            if payload is None and ("text/csv" in ctype or "application/csv" in ctype or ("," in (resp.text or "")[:200] and "\n" in (resp.text or "")[:200])):
                import csv, io
                buf = io.StringIO(resp.text or "")
                reader = csv.reader(buf)
                rows_csv = list(reader)
                if rows_csv:
                    cols = rows_csv[0]
                    data_rows = rows_csv[1:]
                    # vuelca a tabla incidentes
                    latest = {
                        "columns": cols,
                        "rows": data_rows,
                    }
                    await apply_incidents_table(latest)
                    self._consec_fail = 0
                    self._last_ok = datetime.utcnow().isoformat(timespec="seconds") + "Z"
                    self._status.update(ok=True, last_ok=self._last_ok, last_error=None, fails=0, url=url)
                    await manager.broadcast({"type":"enablon_status","ok":True,"url":url,"ts":self._last_ok})
                    return
            
            # Páginas “timeout” numéricas (text/plain “5399”, etc.) → no actualizar tabla, pero OK
            if payload is None:
                head = (resp.text or "")[:160]
                if "text/plain" in ctype and re.search(r"^\s*\d{2,}\s*$", head):
                    self._consec_fail = 0
                    self._last_ok = datetime.utcnow().isoformat(timespec="seconds") + "Z"
                    self._status.update(ok=True, last_ok=self._last_ok, last_error=None, fails=0, url=url)
                    await manager.broadcast({"type":"enablon_status","ok":True,"url":url,"ts":self._last_ok})
                    return
                raise ValueError(f"Respuesta Enablon no JSON (ctype={ctype})")

            # --- Filtra JSON de login/expulsión disfrazado ---
            if isinstance(payload, dict):
                peek = json.dumps({k: payload.get(k) for k in list(payload.keys())[:12]}).lower()
                looks_login = any(w in peek for w in ("login","signin","unauthorized","forbidden","expired","csrf","xsrf"))
                has_table  = any(k in (kk.lower() for kk in payload.keys())
                                for k in ("data","value","items","results","rows","records","list","columns"))
                if looks_login and not has_table:
                    raise httpx.HTTPStatusError("auth", request=resp.request, response=resp)

            # --- OK de verdad: volcamos a la tabla de incidentes ---
            # --- OK de verdad: seleccionar el array correcto con ENA_JSON_PATH ---
            json_path = os.getenv("ENA_JSON_PATH", "").strip()
            if json_path:
                picked = _pluck_by_path(payload, json_path)
                base = picked if picked is not None else (payload.get("data") if isinstance(payload, dict) and "data" in payload else payload)
            else:
                base = payload.get("data") if isinstance(payload, dict) and "data" in payload else payload

            await apply_incidents_table(base)


            # --- Estado OK ---
            self._consec_fail = 0
            self._last_ok = datetime.utcnow().isoformat(timespec="seconds") + "Z"
            self._status.update(ok=True, last_ok=self._last_ok, last_error=None, fails=0, url=url)
            await manager.broadcast({"type":"enablon_status","ok":True,"url":url,"ts":self._last_ok})

        except Exception as e:
            self._consec_fail += 1
            self._status.update(ok=False, last_error=str(e), fails=self._consec_fail, url=self.current_url)
            await manager.broadcast({"type":"enablon_status","ok":False,"url":self.current_url,"error":str(e),"fails":self._consec_fail})
            raise



    async def run(self):
        await self._ensure_client()
        if self.settings.auth_mode == "LOGIN_POST":
            await self._login_if_needed()

        if self._timeout_url:
            try:
                await self._fetch_timeout_secs()
            except Exception:
                pass

        while True:
            try:
                await self._reload_env_if_changed()

                # keepalive periódico
                now = time.time()
                if now - self._last_keepalive >= self._keepalive_every:
                    await self._keepalive()
                    if self._timeout_url and (self._timeout_secs is None):
                        await self._fetch_timeout_secs()
                    self._last_keepalive = now

                # 🔗 Ejecuta TODAS las URLs en orden (paso 1 → paso 2)
                for i in range(len(self.settings.urls)):
                    self._url_idx = i
                    await self._fetch_once()

                next_delay = self.settings.poll_seconds

            except Exception:
                step = min(self._consec_fail, 6)
                backoff = min(300, (2 ** step))
                jitter = int(0.2 * backoff * (1 + (os.getpid() % 5)))
                next_delay = max(10, backoff + (jitter % 7))
                if self._consec_fail % 3 == 0:
                    await self._ensure_client(recycle=True)
                # no rotamos: queremos mantener el orden; el siguiente ciclo volverá a intentar
                await self._reauth()

            await asyncio.sleep(next_delay)



    def status(self) -> dict[str, Any]:
        return dict(self._status)



# ---------------------------
# STARTUP: usa ExternalConnector
# ---------------------------





ROSTER_XLSX_PATH = os.getenv("ROSTER_XLSX_PATH", "C:/Users/iexposito/briefing/backend/data/Informe diario.xlsx")
ROSTER_TZ = os.getenv("ROSTER_TZ", "Europe/Madrid")
ROSTER_POLL_SECONDS = int(os.getenv("ROSTER_POLL_SECONDS", "60"))
ROSTER_NIGHT_PREV_DAY = os.getenv("ROSTER_NIGHT_PREV_DAY", "true").lower() == "true"




















    






from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Header, WebSocket, WebSocketDisconnect, Request
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
import tempfile
from pathlib import Path

TASKS_DB = os.getenv("TASKS_DB", "./data/tasks.json")

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


# --- arriba del archivo, cerca de INCIDENTS_DB ---
EXTERNAL_DB = os.getenv("EXTERNAL_DB", "./data/external_table.json")

def load_external_from_disk():
    global latest_external_table
    try:
        p = Path(EXTERNAL_DB)
        if not p.exists():
            return
        obj = json.load(p.open("r", encoding="utf-8")) or {}
        if isinstance(obj, dict) and "columns" in obj and "rows" in obj:
            latest_external_table.update(obj)
            if not latest_external_table.get("version"):
                latest_external_table["version"] = 1
            print(f"🗂️ Carga/Planificación cargada de disco: "
                  f"{len(latest_external_table.get('rows', []))} filas.")
    except Exception as e:
        print("⚠️ Error leyendo EXTERNAL_DB:", repr(e))

def save_external_to_disk():
    try:
        _atomic_write_json_any(EXTERNAL_DB, latest_external_table)
    except Exception as e:
        print("⚠️ Error guardando EXTERNAL_DB:", repr(e))

# -----------------------------------
# Utilidades
# -----------------------------------
# === Persistencia tabla Incidentes (Enablon) ===
INCIDENTS_DB = os.getenv("INCIDENTS_DB", "./data/incidents_table.json")

def load_incidents_from_disk():
    global latest_incidents_table
    try:
        p = Path(INCIDENTS_DB)
        if not p.exists():
            return
        obj = json.load(p.open("r", encoding="utf-8")) or {}
        if isinstance(obj, dict) and "columns" in obj and "rows" in obj:
            latest_incidents_table.update(obj)
            # asegúrate de que version > 0 para que el WS "table_state" dispare el primer render
            if not latest_incidents_table.get("version"):
                latest_incidents_table["version"] = 1
            print(f"🗂️ Incidentes cargados de disco: {len(latest_incidents_table.get('rows', []))} filas.")
    except Exception as e:
        print("⚠️ Error leyendo INCIDENTS_DB:", repr(e))

def save_incidents_to_disk():
    try:
        _atomic_write_json_any(INCIDENTS_DB, latest_incidents_table)
    except Exception as e:
        print("⚠️ Error guardando INCIDENTS_DB:", repr(e))






from datetime import date  # si no lo tienes ya

# ===== Asistencia / Presencia =====
ATTENDANCE_DB = os.getenv("ATTENDANCE_DB", "./data/attendance.json")

attendance_store: dict[str, dict[str, bool]] = {}  
# Estructura: { "YYYY-MM-DD|Mañana": { "Apellidos, Nombre": true/false, ... }, ... }

def _atomic_write_json_any(path: str, data: Any):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(p.parent), prefix=".attendance_", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False)
        os.replace(tmp, path)
    finally:
        try:
            if Path(tmp).exists(): os.remove(tmp)
        except Exception:
            pass

def load_attendance_from_disk():
    global attendance_store
    try:
        p = Path(ATTENDANCE_DB)
        if p.exists():
            attendance_store = json.load(p.open("r", encoding="utf-8")) or {}
        else:
            attendance_store = {}
        if not isinstance(attendance_store, dict):
            attendance_store = {}
        print(f"🗂️ Asistencia cargada ({len(attendance_store)} claves fecha|turno).")
    except Exception as e:
        print("⚠️ Error leyendo asistencia:", repr(e))
        attendance_store = {}

def save_attendance_to_disk():
    try:
        _atomic_write_json_any(ATTENDANCE_DB, attendance_store)
    except Exception as e:
        print("⚠️ Error guardando asistencia:", repr(e))

def _att_key(d: datetime.date, shift: str) -> str:
    return f"{d.isoformat()}|{shift}"



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



# ===== ENABLON efectivo (acepta ENA_* o ENABLON_*) =====
ENA_URL        = _pick_env("ENA_URL", "ENABLON_URL")
ENA_COOKIE     = _pick_env("ENA_COOKIE", "ENABLON_COOKIE")
ENA_REFERER    = _pick_env("ENA_REFERER", "ENABLON_REFERER")
ENA_USER_AGENT = _pick_env("ENA_USER_AGENT", "ENABLON_USER_AGENT", default="Mozilla/5.0")
# opcional: bearer/JWT si tu Enablon lo usa
ENA_BEARER     = _pick_env("ENA_BEARER", "ENABLON_BEARER")

# SSL específicos de ENA (si los tienes); si no, caerá a los de EXT más abajo
ENA_VERIFY_MODE  = _pick_env("ENA_VERIFY_MODE", "EXT_VERIFY_MODE", default="TRUSTSTORE").upper()
ENA_CAFILE       = _pick_env("ENA_CAFILE", "EXT_CAFILE", default="")

from pydantic import BaseModel
from typing import Optional, Dict, Any
from datetime import datetime
import tempfile

BRIEFING_DB = os.getenv("BRIEFING_DB", "./data/briefings.json")

def _atomic_append_json(path: str, item: dict):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    # lee existente
    arr = []
    if p.exists():
        try:
            with p.open("r", encoding="utf-8") as fh:
                arr = json.load(fh) or []
        except Exception:
            arr = []
    arr.append(item)
    # guarda atómico
    fd, tmp = tempfile.mkstemp(dir=str(p.parent), prefix=".brief_", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(arr, fh, ensure_ascii=False)
        os.replace(tmp, path)
    finally:
        try:
            if Path(tmp).exists():
                os.remove(tmp)
        except Exception:
            pass

class BriefingSection(BaseModel):
    status: Optional[str] = ""
    criteria: Dict[str, bool] = {}
    notes: Optional[str] = ""

class BriefingPayload(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    date_iso: str
    shift: str
    supervisor: Optional[str] = ""
    duration_sec: int = 0
    attendance_pct: Optional[float] = None
    sections: Dict[str, BriefingSection] = {}
    ok_blocks: Optional[int] = 0
    coverage_pct: Optional[float] = 0.0
    standard_met: Optional[bool] = False
    saved_at: Optional[str] = None



# -----------------------------------
# Modelos
# -----------------------------------


# -----------------------------------
# Estado & WS
# -----------------------------------

# ===== NUEVO: almacén de tabla externa (para Carga/Planificación) =====





def _parse_cookie_header(header: str) -> Dict[str, str]:
    """Convierte 'k1=v1; k2=v2' en dict {'k1':'v1','k2':'v2'}."""
    jar: Dict[str, str] = {}
    for part in header.split(";"):
        if "=" in part:
            k, v = part.split("=", 1)
            jar[k.strip()] = v.strip()
    return jar

def _merge_set_cookie_into_header(existing_cookie: str, set_cookie_headers: list[str]) -> str:
    """
    Toma el Cookie actual (crudo) y una lista de 'Set-Cookie' y devuelve
    un nuevo Cookie crudo, actualizando/añadiendo pares name=value sin tocar atributos.
    Preserva el orden original en lo posible.
    """
    jar = _parse_cookie_header(existing_cookie) if existing_cookie else {}
    for raw in (set_cookie_headers or []):
        if not raw:
            continue
        first = raw.split(";", 1)[0]
        if "=" in first:
            k, v = first.split("=", 1)
            jar[k.strip()] = v.strip()

    parts, seen = [], set()
    for part in (existing_cookie or "").split(";"):
        if "=" in part:
            name = part.split("=", 1)[0].strip()
            if name and name in jar and name not in seen:
                parts.append(f"{name}={jar[name]}")
                seen.add(name)
    for name, val in jar.items():
        if name not in seen:
            parts.append(f"{name}={val}")
    return "; ".join(parts)






# ---- httpx (cliente asíncrono) ----

def _pick_main_list(obj) -> tuple[list | None, str]:
    """
    Busca recursivamente la lista principal dentro de un payload típico:
    - Prioriza keys: rows, items, results, all, list
    - Devuelve (lista, ruta_en_texto) o (None, "")
    """
    PRIORITY = ("rows", "items", "results", "all", "list")
    # raíz ya es lista
    if isinstance(obj, list):
        return obj, "root"

    if isinstance(obj, dict):
        # primero: claves prioritarias con lista directa
        for k in PRIORITY:
            v = obj.get(k)
            if isinstance(v, list):
                return v, k
        # segundo: si hay dict anidados, baja un nivel y vuelve a intentar
        for k, v in obj.items():
            if isinstance(v, dict):
                lst, path = _pick_main_list(v)
                if lst is not None:
                    return lst, f"{k}.{path}"

    return None, ""









# ============================
# ExternalConnector (robusto)
# ============================
import shlex
from dataclasses import dataclass, field



        
# Reemplaza la clase ExternalConnector existente con esta versión mejorada

# Reemplaza SOLO el método __init__ de ExternalConnector con esta versión corregida

class ExternalConnector:
    def __init__(self):
    # PRIMERO: inicializar settings y propiedades básicas
        self.settings = ExternalSettings.from_env()
        self._url_idx = 0
        
        # Resto de inicialización
        self._last_keepalive: float = 0.0
        self._keepalive_every: int = getattr(self.settings, "keepalive_every", 0) or 120
        
        self._env_path = Path(os.getenv("ENV_FILE", ".env"))
        self._env_mtime = self._env_path.stat().st_mtime if self._env_path.exists() else None
        self._client: httpx.AsyncClient | None = None
        self._consec_fail = 0
        self._last_ok: str | None = None
        self._cookie_expires_at: float | None = None
        
        # Tracking de actividad de la sesión
        self._last_activity: float = 0.0
        self._session_health_score = 100
        
        # Cargar sesión persistida
        ck, exp = _load_cookie_session(EXT_SESSION_PATH)
        if ck:
            self.settings.cookie_header = ck
            self._cookie_expires_at = exp
    
        self._status: dict[str, Any] = {
            "ok": False, 
            "last_ok": None, 
            "last_error": None, 
            "fails": 0, 
            "url": self.current_url  # Ya funciona porque _url_idx está inicializado
        }

    async def _reload_env_if_changed(self):
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
                        print("🔄 .env External recargado y cliente renovado.")
        except Exception as e:
            print("⚠️ Hot-reload .env (External) error:", e)
    
    @property
    def current_url(self) -> str:
        return self.settings.urls[self._url_idx] if self.settings.urls else ""

    def _rotate_url(self):
        if len(self.settings.urls) > 1:
            self._url_idx = (self._url_idx + 1) % len(self.settings.urls)
            print(f"🔀 External → {self.current_url}")
    
    async def _reauth(self) -> bool:
        await self._reload_env_if_changed()
        await self._ensure_client(recycle=True)
        return True
    
    async def _post_track(self) -> bool:
        """POST de tracking/telemetría para mantener sesión"""
        if not (self._client and self.settings.track_url):
            return False
        try:
            headers = self._build_auth_headers()
            
            # Construir body si está configurado
            body = None
            if self.settings.track_body_json:
                body_str = self.settings.track_body_json.replace(
                    "{now_ms}", str(int(time.time() * 1000))
                ).replace(
                    "{now_iso}", datetime.utcnow().isoformat(timespec="seconds") + "Z"
                ).replace(
                    "{url}", self.current_url
                )
                try:
                    body = json.loads(body_str)
                except:
                    body = {"timestamp": int(time.time() * 1000)}
            
            r = await self._client.post(
                self.settings.track_url, 
                headers=headers, 
                json=body,
                timeout=15.0, 
                follow_redirects=False
            )
            
            self._update_cookie_from_response(r)
            return r.status_code < 400
            
        except Exception:
            return False

    async def _ensure_client(self, recycle: bool = False):
        if self._client is not None and not recycle:
            return
        if self._client is not None and recycle:
            try: await self._client.aclose()
            except Exception: pass

        # Configuración más robusta para conexiones persistentes
        transport = httpx.AsyncHTTPTransport(
            retries=0,
            http2=self.settings.http2,
            limits=httpx.Limits(
                max_connections=self.settings.max_connections,
                max_keepalive_connections=self.settings.max_keepalive,
                keepalive_expiry=180.0,  # Aumentado de 60 a 180 segundos
            ),
        )
        
        # Headers mejorados para mantener sesión
        default_headers = {
            "User-Agent": self.settings.user_agent,
            "Accept": "application/json, text/plain, */*",
            "X-Requested-With": "XMLHttpRequest",
            "Connection": "keep-alive",
            "Cache-Control": "no-cache",
        }
        
        if self.settings.referer:
            default_headers["Referer"] = self.settings.referer

        self._client = httpx.AsyncClient(
            verify=self._ssl_verify(),
            transport=transport,
            headers=default_headers,
            timeout=httpx.Timeout(30.0, connect=10.0, read=30.0, write=10.0)
        )

    async def _aggressive_keepalive(self) -> bool:
        """Keepalive más agresivo con múltiples estrategias"""
        if not self._client:
            return False
            
        success = False
        
        # Estrategia 1: POST track si está configurado
        if self.settings.track_url:
            success = await self._post_track()
            
        # Estrategia 2: GET keepalive si hay URL específica
        if not success and self.settings.keepalive_url:
            success = await self._simple_keepalive(self.settings.keepalive_url)
            
        # Estrategia 3: HEAD a la URL principal (menos intrusivo)
        if not success:
            success = await self._head_keepalive()
            
        # Estrategia 4: GET ligero a la URL principal
        if not success:
            success = await self._simple_keepalive(self.current_url)
            
        # Actualizar score de salud de sesión
        if success:
            self._session_health_score = min(100, self._session_health_score + 10)
            self._last_activity = time.time()
        else:
            self._session_health_score = max(0, self._session_health_score - 25)
            
        print(f"🔄 Keepalive: {'✓' if success else '✗'} (health: {self._session_health_score}%)")
        
        return success

    async def _head_keepalive(self) -> bool:
        """HEAD request ligero para mantener conexión"""
        if not (self._client and self.current_url):
            return False
        try:
            headers = self._build_auth_headers()
            r = await self._client.head(
                self.current_url, 
                headers=headers, 
                timeout=15.0, 
                follow_redirects=False
            )
            self._update_cookie_from_response(r)
            return r.status_code < 400
        except Exception:
            return False

    async def _simple_keepalive(self, url: str) -> bool:
        """GET request de keepalive"""
        if not (self._client and url):
            return False
        try:
            headers = self._build_auth_headers()
            r = await self._client.get(
                url, 
                headers=headers, 
                timeout=20.0, 
                follow_redirects=False
            )
            self._update_cookie_from_response(r)
            return r.status_code < 400
        except Exception:
            return False

    def _update_cookie_from_response(self, response):
        """Actualiza cookies desde Set-Cookie headers"""
        set_cookie_headers = response.headers.get_list("set-cookie")
        if set_cookie_headers:
            new_cookie, expires_at = _merge_set_cookie_with_expiry(
                self.settings.cookie_header, set_cookie_headers
            )
            self.settings.cookie_header = new_cookie
            if expires_at:
                self._cookie_expires_at = expires_at
                # Persistir sesión
                _save_cookie_session(EXT_SESSION_PATH, new_cookie, expires_at)
    
    def _build_auth_headers(self) -> dict:
        """Construye headers de autenticación consistentes"""
        headers = {}
        if self.settings.cookie_header:
            headers["Cookie"] = self.settings.cookie_header
            headers.update(_extract_xsrf_from_cookie(self.settings.cookie_header))
        if self.settings.referer:
            headers["Referer"] = self.settings.referer
        return headers

    def _should_preemptive_refresh(self) -> bool:
        """Determina si debe hacer refresh proactivo"""
        now = time.time()
        
        # Refresh si la cookie expira en menos de 5 minutos
        if self._cookie_expires_at and (self._cookie_expires_at - now) < 300:
            return True
            
        # Refresh si la salud de la sesión está baja
        if self._session_health_score < 30:
            return True
            
        # Refresh si ha pasado mucho tiempo sin actividad exitosa
        if self._last_activity and (now - self._last_activity) > 1800:  # 30 min
            return True
            
        return False

    async def _fetch_once(self):
        if not self.current_url:
            return
            
        await self._ensure_client()
        
        # Refresh proactivo si es necesario
        if self._should_preemptive_refresh():
            print(f"🔄 Refresh proactivo (health: {self._session_health_score}%)")
            await self._reauth()

        try:
            headers = self._build_auth_headers()
            
            resp = await self._client.get(
                self.current_url,
                headers=headers,
                timeout=30.0,
                follow_redirects=False
            )

            # Manejo mejorado de estados de auth
            if resp.status_code in (401, 403, 419, 440):
                print(f"🔐 Auth error {resp.status_code}, reautenticando...")
                await self._reauth()
                # Segundo intento después de reauth
                headers = self._build_auth_headers()
                resp = await self._client.get(
                    self.current_url,
                    headers=headers, 
                    timeout=30.0,
                    follow_redirects=False
                )

            # Manejo de redirects
            if resp.status_code in (301, 302, 303, 307, 308):
                location = resp.headers.get("location", "")
                if "login" in location.lower():
                    print("🔐 Redirect a login, reautenticando...")
                    await self._reauth()
                    headers = self._build_auth_headers()
                    resp = await self._client.get(
                        self.current_url,
                        headers=headers,
                        timeout=30.0,
                        follow_redirects=False
                    )

            if resp.status_code >= 500:
                raise httpx.HTTPStatusError("5xx", request=resp.request, response=resp)

            self._update_cookie_from_response(resp)
            
            # Validación de contenido JSON
            try:
                data = resp.json()
            except Exception:
                # Intento de parsing de texto que parece JSON
                text = resp.text or ""
                if text.lstrip().startswith(("{", "[")):
                    try:
                        data = json.loads(text)
                    except Exception:
                        raise ValueError(f"Contenido no es JSON válido: {resp.headers.get('content-type', 'unknown')}")
                else:
                    raise ValueError(f"Respuesta no es JSON: {resp.headers.get('content-type', 'unknown')}")

            # Detección de páginas de login disfrazadas
            if isinstance(data, dict):
                content_sample = json.dumps({k: data.get(k) for k in list(data.keys())[:10]}).lower()
                suspicious_terms = ("login", "signin", "unauthorized", "forbidden", "expired", "csrf", "xsrf")
                has_data_structure = any(k in (kk.lower() for kk in data.keys()) 
                                       for k in ("data", "value", "items", "results", "rows", "records", "list", "columns"))
                
                if any(term in content_sample for term in suspicious_terms) and not has_data_structure:
                    print("🔐 Contenido sospechoso de login, reautenticando...")
                    raise httpx.HTTPStatusError("auth", request=resp.request, response=resp)

            # Procesamiento exitoso
            base = data.get("data") if isinstance(data, dict) and "data" in data else data
            await apply_external_table(base)

            # Actualización de estado exitoso
            self._consec_fail = 0
            self._last_ok = datetime.utcnow().isoformat(timespec="seconds") + "Z"
            self._last_activity = time.time()
            self._session_health_score = min(100, self._session_health_score + 5)
            
            self._status.update(
                ok=True, 
                last_ok=self._last_ok, 
                last_error=None, 
                fails=0, 
                url=self.current_url
            )
            
            await manager.broadcast({
                "type": "external_status",
                "ok": True,
                "url": self.current_url,
                "ts": self._last_ok,
                "health": self._session_health_score
            })

        except Exception as e:
            self._consec_fail += 1
            self._session_health_score = max(0, self._session_health_score - 15)
            
            error_msg = str(e)
            print(f"❌ External fetch error: {error_msg}")
            
            self._status.update(
                ok=False, 
                last_error=error_msg, 
                fails=self._consec_fail, 
                url=self.current_url
            )
            
            await manager.broadcast({
                "type": "external_status",
                "ok": False,
                "url": self.current_url,
                "error": error_msg,
                "fails": self._consec_fail,
                "health": self._session_health_score
            })
            raise

    async def run(self):
        await self._ensure_client()
        if self.settings.auth_mode == "LOGIN_POST":
            await self._login_if_needed()

        next_delay = 0
        while True:
            try:
                await self._reload_env_if_changed()

                # Keepalive más frecuente y agresivo
                now = time.time()
                should_keepalive = (
                    now - self._last_keepalive >= self._keepalive_every or
                    self._session_health_score < 50 or
                    self._should_preemptive_refresh()
                )
                
                if should_keepalive:
                    await self._aggressive_keepalive()
                    self._last_keepalive = now

                await self._fetch_once()
                
                # Delay dinámico basado en salud de sesión
                base_delay = self.settings.poll_seconds
                if self._session_health_score > 80:
                    next_delay = base_delay
                elif self._session_health_score > 50:
                    next_delay = max(10, base_delay // 2)
                else:
                    next_delay = max(5, base_delay // 4)  # Más agresivo si hay problemas

            except Exception:
                # Backoff exponencial mejorado
                step = min(self._consec_fail, 6)
                backoff = min(300, (2 ** step))
                jitter = int(0.2 * backoff * (1 + (os.getpid() % 5)))
                next_delay = max(5, backoff + (jitter % 7))  # Mínimo reducido a 5s
                
                # Acciones de recuperación más agresivas
                if self._consec_fail % 2 == 0:
                    await self._ensure_client(recycle=True)
                if self._consec_fail % 3 == 0:
                    self._rotate_url()
                    await self._reauth()
                    
                print(f"🔄 Retry en {next_delay}s (intento {self._consec_fail})")

            await asyncio.sleep(next_delay)

    def _ssl_verify(self):
        return _build_ssl_context_for(self.settings.verify_mode, self.settings.cafile)

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
                if m: csrf = m.group(1)
        except Exception as e:
            print(f"❌ Login GET error: {e}")
            return False

        payload = {}
        if self.settings.login_payload_json:
            try: 
                payload = json.loads(self.settings.login_payload_json)
            except: 
                payload = {}
        
        if payload:
            payload = json.loads(json.dumps(payload)
                                 .replace("{username}", self.settings.username)
                                 .replace("{password}", self.settings.password)
                                 .replace("{csrf}", csrf or ""))        
        elif self.settings.username:
            payload = {"username": self.settings.username, "password": self.settings.password}
            if csrf: payload["csrf"] = csrf

        try:
            r = await self._client.post(self.settings.login_url, data=payload, timeout=30.0)
            r.raise_for_status()
            return True
        except Exception as e:
            print(f"❌ Login POST error: {e}")
            return False

    def status(self) -> dict[str, Any]:
        return dict(self._status)



# ---------------------------
# STARTUP: usa ExternalConnector
# ---------------------------



# ELIMINA estas líneas:
# @app.on_event("startup") 
# @app.on_event("shutdown")


# Al final del archivo, REEMPLAZA por:
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("🚀 Iniciando sistema...")
    load_tasks_from_disk()
    load_attendance_from_disk()
    load_incidents_from_disk()
    load_external_from_disk()
    
    app.state._roster = asyncio.create_task(_roster_watcher())
    app.state._ext = ExternalConnector()
    app.state._poller = asyncio.create_task(app.state._ext.run())
    app.state._ena = EnablonConnector()
    app.state._ena_task = asyncio.create_task(app.state._ena.run())
    
    print("🚀 Sistema iniciado correctamente")
    
    yield
    
    # Shutdown
    print("🛑 Deteniendo sistema...")
    for key in ("_poller", "_roster", "_ena_task"):
        task: asyncio.Task = getattr(app.state, key, None)
        if task and not task.done():
            task.cancel()
            try: 
                await task
            except asyncio.CancelledError: 
                pass
    
    print("🛑 Sistema detenido correctamente")

# ÚNICA instancia de la app
app = FastAPI(title="WFS1 MAD Dashboard", version="1.0.0", lifespan=lifespan)

# CORS
from fastapi.middleware.cors import CORSMiddleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



# Endpoint opcional de estado para debug/monitorización
@app.get("/api/external-status")
def api_external_status():
    ext = getattr(app.state, "_ext", None)
    return ext.status() if ext else {"ok": False, "last_error": "not_started"}

@app.get("/api/incidents-status")
def api_incidents_status():
    ena = getattr(app.state, "_ena", None)
    return ena.status() if ena else {"ok": False, "last_error": "not_started"}

@app.get("/api/enablon/probe")
async def api_enablon_probe():
    return await enablon_probe()
@app.get("/api/enablon/env-check")
def api_enablon_env_check():
    def mask(v, keep=6):
        if not v: return ""
        s = str(v)
        return s[:keep] + "…" if len(s) > keep else s
    return {
        "ENA_URL": bool(ENA_URL),
        "ENA_REFERER": bool(ENA_REFERER),
        "ENA_USER_AGENT": ENA_USER_AGENT,
        "ENA_BEARER_present": bool(ENA_BEARER),
        "ENA_COOKIE_present": bool(ENA_COOKIE),
        "ENA_COOKIE_head": mask(ENA_COOKIE, 12),
        "ENA_VERIFY_MODE": ENA_VERIFY_MODE,
        "ENA_CAFILE": bool(ENA_CAFILE),
    }


@app.get("/api/incidents-table")
async def get_incidents_table():
    return latest_incidents_table

def _compute_briefing_metrics(sections: Dict[str, Any], duration_sec: int) -> tuple[int, float, bool]:
    keys = ['s1','s2','s3','s4','s5','s6']
    ok_blocks = sum(1 for k in keys if (sections.get(k) or {}).get('status') == 'OK' or
                    (isinstance(sections.get(k), dict) and sections.get(k).get('status') == 'OK'))
    cov = round((ok_blocks/6)*100)
    std = (cov >= 95) and (int(duration_sec or 0) <= 600)
    return ok_blocks, cov, std

_last_briefing_cache: dict[str, Any] = {}

@app.get("/api/briefing")
def get_briefing():
    # devuelve el último guardado (si lo hay) o lo que haya en cache (por si aún no se ha guardado en disco)
    p = Path(BRIEFING_DB)
    if p.exists():
        try:
            arr = json.load(p.open("r", encoding="utf-8")) or []
            if arr:
                return arr[-1]
        except Exception:
            pass
    return _last_briefing_cache or {}

@app.get("/api/external/env-check")
def api_external_env_check():
    def yes(v): return bool(v and str(v).strip())
    return {
        "EXT_URLS": yes(os.getenv("EXT_URLS")) or yes(os.getenv("EXT_URL")),
        "EXT_REFERER": yes(os.getenv("EXT_REFERER")),
        "EXT_COOKIE_present": yes(os.getenv("EXT_COOKIE")),
        "EXT_KEEPALIVE_URL": yes(os.getenv("EXT_KEEPALIVE_URL")),
        "EXT_TRACK_URL": yes(os.getenv("EXT_TRACK_URL")),
        "EXT_TRACK_BODY_JSON": yes(os.getenv("EXT_TRACK_BODY_JSON")),
        "EXT_KEEPALIVE_EVERY": os.getenv("EXT_KEEPALIVE_EVERY", "auto"),
        "EXT_HTTP2": os.getenv("EXT_HTTP2", "true"),
        "EXT_VERIFY_MODE": os.getenv("EXT_VERIFY_MODE", "TRUSTSTORE"),
    }


@app.get("/api/external/probe")
async def api_external_probe():
    s = ExternalSettings.from_env()
    if not s.urls:
        return {"ok": False, "error": "EXT_URL(s) not configured"}
    headers = {
        "User-Agent": s.user_agent,
        "Accept": "application/json, text/plain, */*",
        "X-Requested-With": "XMLHttpRequest",
    }
    if s.referer: headers["Referer"] = s.referer
    if s.cookie_header: headers["Cookie"] = s.cookie_header
    try:
        async with httpx.AsyncClient(verify=_build_ssl_context_for(s.verify_mode, s.cafile)) as client:
            r = await client.get(s.urls[0], headers=headers, timeout=30.0, follow_redirects=False)
            info = {"ok": r.status_code < 400, "status": r.status_code,
                    "content_type": (r.headers.get("content-type") or "").lower(),
                    "location": r.headers.get("location")}
            if "application/json" in (info["content_type"] or ""):
                try:
                    pay = r.json()
                    info["json_type"] = type(pay).__name__
                    if isinstance(pay, dict): info["json_keys"] = list(pay.keys())[:20]
                    if isinstance(pay, list): info["json_len"] = len(pay)
                except Exception:
                    info["json_error"] = "json_parse_failed"
                    info["body_head"] = (r.text or "")[:500]
            else:
                info["body_head"] = (r.text or "")[:500]
            return info
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/briefing")
def post_briefing(payload: BriefingPayload):
    data = payload.dict()
    # recalcula en servidor
    okb, cov, std = _compute_briefing_metrics(data.get('sections') or {}, data.get('duration_sec') or 0)
    data['ok_blocks'] = okb
    data['coverage_pct'] = cov
    data['standard_met'] = std
    data['saved_at'] = datetime.utcnow().isoformat(timespec='seconds') + 'Z'
    # guarda
    try:
        _atomic_append_json(BRIEFING_DB, data)
        global _last_briefing_cache
        _last_briefing_cache = data
        # (opcional) notificar por WS
        asyncio.create_task(manager.broadcast({"type":"briefing_saved","ok_blocks":okb,"coverage":cov,"standard_met":std,"saved_at":data['saved_at']}))
        return {"status":"ok", **data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"briefing_save_failed: {e}")


# -----------------------------------
# Webhooks (EXISTENTE, SIN CAMBIOS)
# -----------------------------------
# Reemplaza el endpoint existente con esta versión mejorada

@app.post("/webhook/sharepoint-bulk-update")
async def sharepoint_bulk_update(payload: Dict[str, Any], x_api_key: Optional[str] = Header(None)):
    """Webhook mejorado con logging detallado y validación robusta"""
    
    # 1. Validación de API Key
    if x_api_key != API_KEY:
        print(f"⚠️ SharePoint webhook: API Key inválida. Recibida: {x_api_key[:10] if x_api_key else 'None'}...")
        raise HTTPException(status_code=401, detail="Invalid API Key")

    print(f"🟢 SharePoint webhook recibido: {datetime.utcnow().isoformat()}")
    
    # 2. Log del payload completo para debugging
    try:
        payload_str = json.dumps(payload, indent=2, ensure_ascii=False)[:1000]  # Limitar a 1000 chars
        print(f"📦 Payload recibido:\n{payload_str}")
        if len(json.dumps(payload)) > 1000:
            print(f"... (payload truncado, total: {len(json.dumps(payload))} chars)")
    except Exception as e:
        print(f"⚠️ No se pudo serializar el payload: {e}")

    # 3. Extracción de datos más robusta
    tasks_to_process = []
    
    # Intentar múltiples estructuras de payload
    if isinstance(payload, dict):
        # Estructura 1: {"body": [...]}
        if "body" in payload and isinstance(payload["body"], list):
            tasks_to_process = payload["body"]
            print(f"📋 Estructura 'body' detectada: {len(tasks_to_process)} elementos")
        
        # Estructura 2: {"tasks": [...]} o {"data": [...]}
        elif any(k in payload for k in ("tasks", "data", "items", "records")):
            for key in ("tasks", "data", "items", "records"):
                if key in payload and isinstance(payload[key], list):
                    tasks_to_process = payload[key]
                    print(f"📋 Estructura '{key}' detectada: {len(tasks_to_process)} elementos")
                    break
        
        # Estructura 3: Lista directa en el payload
        elif "id" in payload or "task_type" in payload or any(k in payload for k in ("station", "warehouse", "gw_date")):
            tasks_to_process = [payload]  # Un solo elemento
            print(f"📋 Elemento único detectado")
    
    # Estructura 4: Lista directa como payload
    elif isinstance(payload, list):
        tasks_to_process = payload
        print(f"📋 Lista directa: {len(tasks_to_process)} elementos")

    if not tasks_to_process:
        print(f"⚠️ No se encontraron tareas válidas en el payload")
        # Log las keys disponibles para debugging
        if isinstance(payload, dict):
            print(f"📋 Keys disponibles en payload: {list(payload.keys())}")
        return {"message": "No se encontraron tareas válidas", "processed": 0}

    print(f"🔄 Procesando {len(tasks_to_process)} elemento(s)...")

    # 4. Procesamiento con logging detallado
    processed = []
    errors = []
    
    for i, incoming in enumerate(tasks_to_process):
        try:
            print(f"🔍 Procesando elemento {i+1}/{len(tasks_to_process)}")
            
            # Validación básica
            if not isinstance(incoming, dict):
                print(f"⚠️ Elemento {i+1} no es un dict: {type(incoming)}")
                errors.append(f"Elemento {i+1}: no es un objeto válido")
                continue

            # Log del elemento individual
            if i < 3:  # Solo los primeros 3 para no spam
                try:
                    elem_str = json.dumps(incoming, indent=2, ensure_ascii=False)[:500]
                    print(f"📄 Elemento {i+1} contenido:\n{elem_str}")
                except:
                    print(f"📄 Elemento {i+1}: {list(incoming.keys()) if hasattr(incoming, 'keys') else 'No serializable'}")

            # 5. Inferencia de task_type más robusta
            original_task_type = incoming.get("task_type", "")
            
            if not original_task_type:
                # Heurística mejorada para detectar tipos
                if any(k in incoming for k in ("station", "warehouse", "gw_date", "shift_date")):
                    incoming["task_type"] = "gw_task"
                    print(f"  → Inferido como 'gw_task' por keys: station/warehouse/gw_date")
                elif any(k in incoming for k in ("category", "due_date", "assigned_to")):
                    incoming["task_type"] = "kanban_rapida"
                    print(f"  → Inferido como 'kanban_rapida' por keys: category/due_date/assigned_to")
                elif any(k in incoming for k in ("status", "title")):
                    incoming["task_type"] = "kaizen"
                    print(f"  → Inferido como 'kaizen' por keys: status/title")
                else:
                    incoming["task_type"] = "generic_task"
                    print(f"  → Asignado 'generic_task' por defecto")

            # 6. Sanitización con logging
            incoming = sanitize_task(incoming)
            tid = incoming.get("id")
            task_type = incoming.get("task_type")
            
            print(f"  → ID: {tid}, Tipo: {task_type}")
            
            if not tid:
                print(f"⚠️ Elemento {i+1} sin ID válido, generando UUID")
                incoming["id"] = str(uuid.uuid4())
                tid = incoming["id"]
            
            if not task_type:
                print(f"⚠️ Elemento {i+1} sin task_type, saltando")
                errors.append(f"Elemento {i+1}: sin task_type válido")
                continue

            # 7. MERGE que preserva estado del servidor
            existing = tasks_in_memory_store.get(tid)
            if existing:
                print(f"  → Actualizando tarea existente {tid}")
                print(f"    Estado actual: completed={existing.get('is_completed')}, note='{existing.get('note', '')[:50]}'")
            else:
                print(f"  → Nueva tarea {tid}")

            merged = merge_preserve_server(existing, incoming)
            
            # Log del resultado final
            print(f"  → Resultado: completed={merged.get('is_completed')}, status={merged.get('status')}")
            
            tasks_in_memory_store[tid] = merged
            processed.append(merged)

        except Exception as e:
            error_msg = f"Error procesando elemento {i+1}: {str(e)}"
            print(f"❌ {error_msg}")
            errors.append(error_msg)
            # Continuar con el siguiente elemento

    # 8. Persistencia con validación
    if processed:
        try:
            save_tasks_to_disk()
            print(f"💾 Guardadas {len(processed)} tareas en disco")
        except Exception as e:
            print(f"⚠️ Error guardando en disco: {e}")

    # 9. Broadcasting mejorado con retry
    broadcast_success = 0
    broadcast_errors = []
    
    for task in processed:
        try:
            print(f"📡 Broadcasting tarea {task['id']} tipo {task['task_type']}")
            await manager.broadcast(task)
            broadcast_success += 1
            
            # Pequeño delay para evitar spam
            await asyncio.sleep(0.1)
            
        except Exception as e:
            error_msg = f"Error broadcasting {task.get('id', 'unknown')}: {str(e)}"
            print(f"❌ {error_msg}")
            broadcast_errors.append(error_msg)

    # 10. Respuesta detallada
    response = {
        "message": f"{len(processed)} tarea(s) procesadas correctamente",
        "processed": len(processed),
        "broadcast_success": broadcast_success,
        "total_tasks_in_memory": len(tasks_in_memory_store),
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
    
    if errors:
        response["processing_errors"] = errors[:5]  # Solo primeros 5 errores
        response["total_errors"] = len(errors)
    
    if broadcast_errors:
        response["broadcast_errors"] = broadcast_errors[:3]  # Solo primeros 3
    
    # Log final
    print(f"✅ SharePoint webhook completado:")
    print(f"   - Recibidos: {len(tasks_to_process)}")
    print(f"   - Procesados: {len(processed)}")
    print(f"   - Broadcasted: {broadcast_success}")
    print(f"   - Errores: {len(errors)}")
    print(f"   - Total en memoria: {len(tasks_in_memory_store)}")

    return response


# NUEVO: Endpoint de diagnóstico para debugging
@app.get("/api/webhook-status")
async def webhook_status():
    """Endpoint de diagnóstico para verificar el estado del sistema de webhooks"""
    return {
        "tasks_in_memory": len(tasks_in_memory_store),
        "websocket_connections": len(manager.active_connections),
        "last_webhook_processed": "N/A",  # Podrías añadir timestamp si lo guardas
        "api_key_configured": bool(API_KEY),
        "sample_task_ids": list(tasks_in_memory_store.keys())[:5]
    }


# MEJORA: Broadcasting más robusto
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self._broadcast_stats = {"success": 0, "failed": 0, "last_error": None}

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        print(f"🔌 WS conectado. Activos: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            print(f"🔌 WS desconectado. Activos: {len(self.active_connections)}")

    async def broadcast(self, data: Dict[str, Any]):
        """Broadcasting mejorado con estadísticas y manejo de errores"""
        if not self.active_connections:
            print("📡 No hay conexiones WebSocket activas para broadcast")
            return

        success_count = 0
        failed_connections = []
        
        for connection in list(self.active_connections):  # Copia para modificar durante iteración
            try:
                await connection.send_json(data)
                success_count += 1
            except Exception as e:
                print(f"❌ Error enviando a WebSocket: {str(e)}")
                failed_connections.append(connection)
                self._broadcast_stats["failed"] += 1
                self._broadcast_stats["last_error"] = str(e)

        # Limpiar conexiones fallidas
        for failed_conn in failed_connections:
            self.disconnect(failed_conn)
        
        self._broadcast_stats["success"] += success_count
        
        if success_count > 0:
            print(f"📡 Broadcast exitoso a {success_count}/{len(self.active_connections) + len(failed_connections)} conexiones")
        
        return success_count

    async def send_one(self, websocket: WebSocket, data: Dict[str, Any]):
        try:
            await websocket.send_json(data)
        except Exception:
            self.disconnect(websocket)

    def get_stats(self):
        return {
            "active_connections": len(self.active_connections),
            "broadcast_stats": self._broadcast_stats
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


class PresenceUpdate(BaseModel):
    person: str
    present: bool
    date: Optional[str] = None    # ISO (YYYY-MM-DD), opcional
    shift: Optional[str] = None   # "Mañana"|"Tarde"|"Noche", opcional

@app.put("/api/roster/presence")
async def put_roster_presence(upd: PresenceUpdate):
    # Usa turno/fecha actuales si no vienen
    state = await _build_roster_state(force=False)
    sheet_date = state.get("sheet_date")
    shift = state.get("shift")
    if upd.date:
        try:
            sheet_date = datetime.fromisoformat(upd.date).date()
        except Exception:
            pass
    if upd.shift:
        shift = upd.shift

    if not sheet_date or not shift:
        raise HTTPException(status_code=400, detail="No hay turno/fecha activos")

    key = _att_key(sheet_date, shift)
    attendance_store.setdefault(key, {})
    if not upd.person:
        raise HTTPException(status_code=400, detail="Campo 'person' requerido")

    attendance_store[key][upd.person] = bool(upd.present)
    save_attendance_to_disk()

    # WS puntual para actualizar el cliente
    payload = {
        "type": "presence_update",
        "sheet_date": sheet_date.isoformat(),
        "shift": shift,
        "person": upd.person,
        "present": bool(upd.present),
    }
    await manager.broadcast(payload)
    return payload


from fastapi import Query

@app.get("/api/tasks")
def list_tasks(task_type: str | None = Query(None), station: str | None = Query(None)):
    """
    Devuelve las tareas en memoria (opcionalmente filtradas por tipo y estación).
    """
    out = list(tasks_in_memory_store.values())
    if task_type:
        out = [t for t in out if t.get("task_type") == task_type]
    if station:
        st = station.upper()
        out = [t for t in out if not t.get("station") or str(t.get("station")).upper() == st]
    # Asegura el schema esperado por el front
    out = [sanitize_task(t) for t in out]
    return out


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
# Lifespan: lanzar/limpiar poller (NUEVO)
# -----------------------------------

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



def _current_shift_info(now):
    hhmm = now.strftime("%H:%M")
    if "06:00" <= hhmm < "14:00":
        return "Mañana", now.date(), "06:00", "14:00"
    if "14:00" <= hhmm < "22:00":
        return "Tarde", now.date(), "14:00", "22:00"
    # Noche → si son de 00:00 a 05:59, usamos la hoja de AYER
    sheet_date = now.date() - timedelta(days=1) if hhmm < "06:00" and ROSTER_NIGHT_PREV_DAY else now.date()
    return "Noche", sheet_date, "22:00", "06:00"


# -----------------------------------
# Frontend estático
# -----------------------------------








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
        # Estado inicial Carga/Planificación e Incidentes (aunque version=0)
        await push_external_table_state(websocket)
        await push_incidents_table_state(websocket)

        # Mantén el socket vivo
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)


# -----------------------------------
# Lifespan: lanzar/limpiar poller (NUEVO)
# -----------------------------------

# ====== ROSTER: construir estado desde Excel y publicar por WS ======


def _now_local():
    return datetime.now(ZoneInfo(ROSTER_TZ))



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
        "attendance": state.get("attendance", {}),
    }
@app.get("/api/roster/sheets")
def api_roster_sheets():
    return {
        "path": ROSTER_XLSX_PATH,
        "sheets": _list_sheet_names(ROSTER_XLSX_PATH),
    }

@app.get("/api/enablon/candidates")
async def api_enablon_candidates():
    # usa la URL actual del conector (ENA_URL1/2/3…)
    ena: EnablonConnector = getattr(app.state, "_ena", None)
    if not ena or not ena.current_url:
        raise HTTPException(status_code=400, detail="Enablon no configurado (ENA_URL1.. o ENA_URLS).")

    # disparo igual que el poller: respeta método/POST/FORM
    method, url, body, body_mode = _parse_endpoint_spec(ena.current_url)

    headers = {
        "User-Agent": ENA_USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "X-Requested-With": "XMLHttpRequest",
    }
    if ENA_REFERER: headers["Referer"] = ENA_REFERER
    if ENA_BEARER:  headers["Authorization"] = f"Bearer {ENA_BEARER}"
    if ENA_COOKIE:  headers["Cookie"] = ENA_COOKIE

    async with httpx.AsyncClient(verify=_build_ssl_context_for(ENA_VERIFY_MODE, ENA_CAFILE), headers=headers) as client:
        if method == "POST":
            post_headers = dict(headers)
            json_body = None
            content = None
            if body_mode == "form":
                post_headers["Content-Type"] = "application/x-www-form-urlencoded"
                content = body if isinstance(body, str) else ""
            elif body_mode == "raw":
                content = body if isinstance(body, (str, bytes)) else ""
            else:
                json_body = body if isinstance(body, (dict, list)) else None
                if json_body is None and isinstance(body, str):
                    try: json_body = json.loads(body)
                    except: content = body

            r = await client.post(url, headers=post_headers, json=json_body, content=content, timeout=30.0, follow_redirects=False)
        else:
            r = await client.get(url, timeout=30.0, follow_redirects=False)

        # si no es JSON, error legible
        try:
            pay = r.json()
        except Exception:
            raise HTTPException(status_code=502, detail=f"Respuesta no JSON ({r.headers.get('content-type')})")

        # listar listas candidatas evitando 'guides'
        out = []
        for path, lst in _walk_lists(pay):
            # filtro opcional de keys de UI
            last = path.split(".")[-1].lower()
            if last in {"guides","guidewidget","features","segmentflags"}:
                continue
            keys = list((lst[0] or {}).keys())[:15] if lst and isinstance(lst[0], dict) else []
            out.append({"path": path, "rows": len(lst), "sample_keys": keys})

        prefer = [s.strip().lower() for s in os.getenv("ENA_PREFER_LIST_KEYS", "incidents,incident,events,event,records,rows,items,results,issues,alerts,tickets").split(",") if s.strip()]
        out.sort(key=lambda c: (prefer.index(c["path"].split(".")[-1].lower()) if c["path"].split(".")[-1].lower() in prefer else 999, -c["rows"], c["path"]))
        return {"candidates": out}


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



app.mount("/", StaticFiles(directory="../frontend", html=True), name="static")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)











