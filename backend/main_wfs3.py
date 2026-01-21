import uvicorn
import os
import uuid
import json
import re
import asyncio
import ssl
from datetime import datetime, timedelta, date
from typing import Optional, List, Dict, Any
import unicodedata
import base64
import httpx
from zoneinfo import ZoneInfo
from pathlib import Path
import pandas as pd
from dataclasses import dataclass, field
from urllib.parse import parse_qs
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Header, WebSocket, WebSocketDisconnect, Request, Depends
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
import tempfile
# --- NUEVO IMPORT PARA SQL ---
from database import init_db, SessionLocal, TaskDB, IncidentDB, AttendanceDB, BriefingDB
# -----------------------------------
# Configuración General WFS3
# -----------------------------------
load_dotenv()
API_KEY = os.getenv("API_KEY")
# --- AÑADE ESTO EN LA SECCIÓN DE MODELOS DE main_wfs3.py ---
class PresenceUpdate(BaseModel):
    person: str
    present: bool
    date: Optional[str] = None    # ISO (YYYY-MM-DD), opcional
    shift: Optional[str] = None   # "Mañana"|"Tarde"|"Noche", opcional
# === RUTAS Y VARIABLES WFS3 (Aislamiento de datos) ===
STATION_NAME = "WFS3"
ROSTER_TZ = os.getenv("ROSTER_TZ", "Europe/Madrid")
ROSTER_POLL_SECONDS = int(os.getenv("ROSTER_POLL_SECONDS", "60"))
ROSTER_NIGHT_PREV_DAY = os.getenv("ROSTER_NIGHT_PREV_DAY", "true").lower() == "true"

# Rutas de datos específicas para WFS3

# El Excel puede ser compartido o único, se configura en .env
ROSTER_XLSX_PATH = os.getenv("ROSTER_XLSX_PATH", "./data/Informe diario.xlsx")

# Configuración GitHub
STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "file").lower()
USE_GITHUB = (STORAGE_BACKEND == "github")

# Configuración Fuente Externa (Intranet/Planificación)
EXT_URL = os.getenv("EXT_URL", "").strip()
EXT_POLL_SECONDS = int(os.getenv("EXT_POLL_SECONDS", "60"))
EXT_VERIFY_MODE = os.getenv("EXT_VERIFY_MODE", "TRUSTSTORE").upper()
EXT_CAFILE = os.getenv("EXT_CAFILE", "").strip()
EXT_USER_AGENT = os.getenv("EXT_USER_AGENT", "Mozilla/5.0")
EXT_REFERER = os.getenv("EXT_REFERER", "").strip()
EXT_COOKIE = os.getenv("EXT_COOKIE", "").strip()

app = FastAPI(title=f"Dashboard {STATION_NAME}")

# -----------------------------------
# Helpers Lógica de Turnos y Excel (Heredado de BCN)
# -----------------------------------
SPANISH_DAY = ["Lunes","Martes","Miércoles","Jueves","Viernes","Sábado","Domingo"]

roster_cache: dict[str, Any] = {
    "file_mtime": None,
    "sheet_date": None,
    "shift": None,
    "people": [],
    "updated_at": None,
}

# --- 1. CONFIGURACIÓN API MADRID - NAVE 3 ---
ROSTER_API_URL = os.getenv("ROSTER_API_URL")
ROSTER_API_KEY = os.getenv("ROSTER_API_KEY")
STATION_CODE_API = "MAD"  # Para la API, Madrid siempre es MAD
TARGET_NAVE = "N3"        # Identificador para Nave 3

def _att_key(d, s):
    if hasattr(d, 'isoformat'): return f"{d.isoformat()}|{s}"
    return f"{d}|{s}"

# --- 2. FUNCIÓN DE LLAMADA A LA API (LA QUE FALTA) ---
async def fetch_roster_api_data(escala: str, fecha: str):
    """Realiza la llamada POST a la API externa para obtener el personal"""
    if not ROSTER_API_URL or not ROSTER_API_KEY:
        print("⚠️ Error: ROSTER_API_URL o ROSTER_API_KEY no detectadas")
        return None
    
    headers = {"api-key": ROSTER_API_KEY, "Accept": "application/json"}
    payload = {"escala": escala, "fecha": fecha} # fecha debe ser DD/MM/YYYY

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            # Enviamos como POST tal como requiere la documentación
            response = await client.post(ROSTER_API_URL, headers=headers, data=payload)
            if response.status_code == 200:
                data = response.json()
                print(f"✅ API MAD: Datos recibidos para Nave 3.")
                return data
            else:
                print(f"❌ Error API MAD: {response.status_code}")
                return None
    except Exception as e:
        print(f"💥 Fallo de conexión API: {e}")
        return None

# --- 3. TRIPLE FILTRO NAVE 3 (Asegúrate de tenerlo también) ---
def filter_mad_people_by_shift_and_nave(api_data: Any, current_shift: str, target_nave: str):
    normalized = []
    target = target_nave.upper() # "N4"

    # 1. Localizar la lista de trabajadores
    workers_list = []
    if isinstance(api_data, list): workers_list = api_data
    elif isinstance(api_data, dict):
        for key in ("value", "data", "items", "workers", "body"):
            if isinstance(api_data.get(key), list):
                workers_list = api_data[key]
                break
    if not workers_list: return []

    for p in workers_list:
        if not isinstance(p, dict): continue
        try:
            nomina = p.get("nomina", {}) if isinstance(p.get("nomina"), dict) else {}
            
            # --- NORMALIZACIÓN ---
            def clean(t): return str(t or "").upper().strip()

            cod_destino  = clean(p.get("codDestino") or nomina.get("codDestino"))
            desc_destino = clean(p.get("descDestino") or nomina.get("descDestino"))
            grupo_raw    = clean(p.get("nombreGrupoTrabajo") or nomina.get("nombreGrupoTrabajo"))
            
            # --- FILTRO 1: DESTINO FÍSICO (DEBE SER NAVE 4) ---
            # Si no pone N4 o NAVE 4 en el destino, queda fuera
            es_nave_3 = (target in cod_destino or "NAVE 3" in desc_destino)
            if not es_nave_3:
                continue

            # --- FILTRO 2: DEPARTAMENTO (SOLO OPS, BLOQUEAR ALM) ---
            # Bloqueamos explícitamente a cualquiera de Almacén aunque esté en N4
            if any(x in grupo_raw for x in ("ALM", "ALMACEN", "ALMACENEROS")):
                continue
            
            # Solo permitimos grupos que contengan estas palabras clave de Operaciones
            es_ops = any(x in grupo_raw for x in ( "OPERARIOS-OPS", "DGR", "SUPERVISOR", "LEAD", "APOYO", "EPA", "MOSTRADOR", "UX"))
            if not es_ops:
                continue

            # --- FILTRO 3: TURNO (HORAS) ---
            raw_inicio = p.get("horaInicio") or nomina.get("horaInicio") or ""
            if " " not in raw_inicio: continue
            
            h_inicio = int(raw_inicio.split(" ")[1].split(":")[0])
            
            # Horquillas (Mañana: 4-14, Tarde: 14-22, Noche: 22-4)
            match = False
            if current_shift == "Mañana" and (4 <= h_inicio < 14): match = True
            elif current_shift == "Tarde" and (14 <= h_inicio < 22): match = True
            elif current_shift == "Noche" and (h_inicio >= 22 or h_inicio < 4): match = True

            if match:
                raw_fin = p.get("horaFin") or nomina.get("horaFin") or ""
                h_fin = raw_fin.split(" ")[1] if " " in raw_fin else ""

                normalized.append({
                    "nombre_completo": p.get("nombreApellidos") or nomina.get("nombreApellidos") or "Sin Nombre",
                    "horario": f"{raw_inicio.split(' ')[1]} - {h_fin}",
                    "grupo": grupo_raw,
                    "cod_destino": cod_destino,
                    "desc_destino": desc_destino,
                    "is_incidencia": p.get("IsIncidencias", False)
                })
        except: continue
            
    return normalized



async def _build_roster_state(force=False) -> dict:
    now = _now_local()
    shift, sdate, start, end = _current_shift_info(now)
    api_date_str = sdate.strftime("%d/%m/%Y")
    
    # Esta es la llamada que daba el NameError:
    raw_api_data = await fetch_roster_api_data("MAD", api_date_str)
    
    people = []
    if raw_api_data and isinstance(raw_api_data, list):
        # Usamos el filtro de N3 que acabamos de definir arriba
        people = filter_mad_people_by_shift_and_nave(raw_api_data, shift, "N3")
        source = "api"
    else:
        # Fallback Excel si falla la API
        sheet_real, _ = _find_sheet_for_date(ROSTER_XLSX_PATH, sdate)
        people = _read_sheet_people(ROSTER_XLSX_PATH, sheet_real, shift) if sheet_real else []
        source = "excel"

    # Actualizar caché y notificar por WebSocket
    roster_cache.update({
        "sheet_date": sdate, "shift": shift, "people": people,
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "window": {"from": start, "to": end}, "source": source
    })
    await manager.broadcast({"type": "roster_update", **roster_cache, "sheet_date": sdate.isoformat()})
    return roster_cache

# --- 4. ENDPOINT PRESENCIA (SIN SQL) ---
@app.put("/api/roster/presence")
async def put_roster_presence(upd: PresenceUpdate):
    state = await _build_roster_state(force=False)
    d, s = state.get("sheet_date"), state.get("shift")
    if not d or not s: raise HTTPException(status_code=400)
    key = _att_key(d, s)
    if key not in attendance_store: attendance_store[key] = {}
    attendance_store[key][upd.person] = upd.present
    await manager.broadcast({"type":"presence_update","sheet_date":d.isoformat(),"shift":s,"person":upd.person,"present":upd.present})
    return {"status": "ok"}

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
    "Noche":  (22*60, 6*60),
}

def _match_shift(horario: str, shift: str) -> bool:
    rng = _parse_range_to_tuple(horario)
    if not rng: return False
    s,e = rng
    S,E = SHIFT_RANGES[shift]
    if shift != "Noche":
        return (s, e) == (S, E)
    return (s == S and e == E) or (s == S and e in (0,)) or (s == 22*60 and e == 6*60)

def _normalize_cols(cols):
    norm = {}
    for c in cols:
        k = c.strip().lower()
        norm[k] = c
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

DATE_SHEET_RE = re.compile(r'(\d{1,2})\D+(\d{1,2})\D+(\d{2,4})')

def _parse_sheet_date(name: str) -> date | None:
    if not isinstance(name, str): return None
    m = DATE_SHEET_RE.search(name.strip())
    if not m: return None
    d, mth, y = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
    if y < 100: y += 2000
    try: return date(y, mth, d)
    except ValueError: return None

def _list_sheet_names(xlsx_path: str) -> list[str]:
    if not Path(xlsx_path).exists(): return []
    try:
        xl = pd.ExcelFile(xlsx_path)
        return list(xl.sheet_names or [])
    except Exception as e:
        print("⚠️ No se pudieron listar hojas:", repr(e))
        return []

def _find_sheet_for_date(xlsx_path: str, desired: date) -> tuple[str | None, list[str]]:
    names = _list_sheet_names(xlsx_path)
    if not names: return None, []
    parsed = [(n, _parse_sheet_date(n)) for n in names]
    for n, dte in parsed:
        if dte == desired: return n, names
    futures = sorted([(dte, n) for n, dte in parsed if dte and dte >= desired], key=lambda x: x[0])
    pasts   = sorted([(dte, n) for n, dte in parsed if dte and dte <  desired], key=lambda x: x[0], reverse=True)
    if futures: return futures[0][1], names
    if pasts: return pasts[0][1], names
    return None, names

def _read_sheet_people(xlsx_path: str, sheet_name: str, shift: str) -> list[dict]:
    if not Path(xlsx_path).exists(): return []
    try:
        df = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    except Exception as e:
        print("⚠️ No se pudo leer hoja:", sheet_name, repr(e))
        return []
    cols = _normalize_cols(list(df.columns))
    req = ["apellidos","nombre","horario"]
    if any(cols[k] is None for k in req):
        return []
    ap_col = cols["apellidos"]; no_col = cols["nombre"]; ho_col = cols["horario"]
    ob_col = cols["observaciones"] or cols["horario"]
    people = []
    for _, row in df.iterrows():
        ap = (row.get(ap_col) or "").strip()
        no = (row.get(no_col) or "").strip()
        ho = (row.get(ho_col) or "").strip()
        ob = (row.get(ob_col) or "").strip() if cols["observaciones"] else ""
        if not ap and not no: continue
        if not _match_shift(ho, shift): continue
        full = f"{ap}, {no}" if ap and no else (ap or no)
        people.append({
            "apellidos": ap, "nombre": no, "nombre_completo": full,
            "horario": ho, "observaciones": ob
        })
    return people

# -----------------------------------
# Persistencia Tareas y Personas
# -----------------------------------
tasks_in_memory_store: Dict[str, Any] = {}
manual_persons_store: Dict[str, Any] = {}

def sanitize_task(raw: dict) -> dict:
    t = dict(raw or {})
    t["id"] = str(t.get("id") or t.get("ID") or t.get("Id") or "")
    t.setdefault("is_completed", False)
    return t

SERVER_FIELDS = ("is_completed", "note")
def merge_preserve_server(existing: dict | None, incoming: dict | None) -> dict:
    base = dict(existing or {})
    base.update(incoming or {})
    if existing:
        for f in SERVER_FIELDS:
            if f in existing and f not in (incoming or {}):
                base[f] = existing[f]
    return sanitize_task(base)

def _atomic_write_json(path: str, data: Any):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(p.parent), prefix=".tmp_wfs3_", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False)
        os.replace(tmp, path)
    finally:
        try:
            if Path(tmp).exists(): os.remove(tmp)
        except Exception:
            pass
# ==========================================
# 🧱 NUEVA CAPA DE DATOS SQL (COPIAR Y PEGAR)
# ==========================================

# 1. TAREAS (Tasks)
def load_tasks_from_disk():
    global tasks_in_memory_store
    db = SessionLocal()
    try:
        tasks_db = db.query(TaskDB).all()
        tasks_in_memory_store.clear()
        for t in tasks_db:
            # Mezclamos datos planos con extra_data para el frontend
            task_dict = t.extra_data.copy() if t.extra_data else {}
            task_dict.update({
                "id": t.id,
                "title": t.title,
                "status": t.status,
                "task_type": t.task_type,
                "is_completed": t.is_completed,
                "created_at": t.created_at
            })
            tasks_in_memory_store[t.id] = task_dict
        print(f"🗂️ [SQL] Cargadas {len(tasks_in_memory_store)} tareas.")
    except Exception as e:
        print(f"⚠️ Error SQL Tasks Load: {e}")
    finally:
        db.close()

def save_tasks_to_disk():
    db = SessionLocal()
    try:
        # Estrategia simple: Upsert manual
        for t_id, t_data in tasks_in_memory_store.items():
            extra = t_data.copy()
            # Quitamos las columnas fijas para dejar solo lo 'extra'
            for k in ["id", "title", "status", "task_type", "is_completed", "created_at"]:
                extra.pop(k, None)
            
            existing = db.query(TaskDB).filter(TaskDB.id == t_id).first()
            if existing:
                existing.title = t_data.get("title")
                existing.status = t_data.get("status")
                existing.task_type = t_data.get("task_type")
                existing.is_completed = t_data.get("is_completed", False)
                existing.extra_data = extra
            else:
                new_task = TaskDB(
                    id=t_id,
                    title=t_data.get("title"),
                    status=t_data.get("status"),
                    task_type=t_data.get("task_type"),
                    is_completed=t_data.get("is_completed", False),
                    created_at=t_data.get("created_at"),
                    extra_data=extra
                )
                db.add(new_task)
        db.commit()
    except Exception as e:
        print(f"⚠️ Error SQL Tasks Save: {e}")
        db.rollback()
    finally:
        db.close()

# 2. INCIDENTES (Incidents)
def load_incidents_from_disk():
    global latest_incidents_table
    db = SessionLocal()
    try:
        last = db.query(IncidentDB).order_by(IncidentDB.id.desc()).first()
        if last and last.data:
            latest_incidents_table.update(last.data)
            if not latest_incidents_table.get("version"):
                 latest_incidents_table["version"] = last.version
            print(f"🗂️ [SQL] Incidentes cargados (v{last.version}).")
    except Exception as e:
        print(f"⚠️ Error SQL Incidents Load: {e}")
    finally:
        db.close()

def save_incidents_to_disk():
    db = SessionLocal()
    try:
        new_entry = IncidentDB(
            data=latest_incidents_table,
            version=int(latest_incidents_table.get("version", 1))
        )
        db.add(new_entry)
        db.commit()
    except Exception as e:
        print(f"⚠️ Error SQL Incidents Save: {e}")
    finally:
        db.close()

# 3. ASISTENCIA (Attendance)
def load_attendance_from_disk():
    global attendance_store
    db = SessionLocal()
    try:
        records = db.query(AttendanceDB).all()
        attendance_store.clear()
        for r in records:
            attendance_store[r.shift_key] = r.data
        print(f"🗂️ [SQL] Asistencia cargada: {len(attendance_store)} registros.")
    except Exception as e:
        print(f"⚠️ Error SQL Attendance Load: {e}")
    finally:
        db.close()

def save_attendance_to_disk():
    db = SessionLocal()
    try:
        for key, data in attendance_store.items():
            existing = db.query(AttendanceDB).filter(AttendanceDB.shift_key == key).first()
            if existing:
                existing.data = data
            else:
                db.add(AttendanceDB(shift_key=key, data=data))
        db.commit()
    except Exception as e:
        print(f"⚠️ Error SQL Attendance Save: {e}")
    finally:
        db.close()

# 4. BRIEFING (Briefings)
def _append_briefing(data: dict):
    db = SessionLocal()
    try:
        new_br = BriefingDB(
            id=data.get("id", str(uuid.uuid4())),
            date=str(data.get("date_iso", "")),
            shift=data.get("shift", ""),
            full_snapshot=data
        )
        db.add(new_br)
        db.commit()
        print(f"✅ [SQL] Briefing guardado.")
    except Exception as e:
        print(f"⚠️ Error SQL Briefing Save: {e}")
    finally:
        db.close()

def _load_last_briefing() -> dict:
    db = SessionLocal()
    try:
        last = db.query(BriefingDB).order_by(BriefingDB.created_at.desc()).first()
        if last and last.full_snapshot:
            return last.full_snapshot
    except Exception:
        pass
    finally:
        db.close()
    return _last_briefing_cache or {}



def save_persons_to_disk():
    try:
        payload = list(manual_persons_store.values())
        _atomic_write_json(PERSONS_DB, payload)
    except Exception as e:
        print("⚠️ Error guardando personas WFS3:", repr(e))

def load_persons_from_disk():
    try:
        p = Path(PERSONS_DB)
        if not p.exists(): return
        with p.open("r", encoding="utf-8") as fh:
            arr = json.load(fh) or []
        manual_persons_store.clear()
        for raw in arr:
            manual_persons_store[raw["id"]] = raw
        print(f"🗂️ Cargadas {len(manual_persons_store)} personas manuales WFS3")
    except Exception as e:
        print("⚠️ Error leyendo personas WFS3:", repr(e))

# -----------------------------------
# Modelos Pydantic
# -----------------------------------
class GitHubStore:
    def __init__(self):
        self.api = os.getenv("GH_API_URL", "https://api.github.com").rstrip("/")
        self.repo = os.getenv("GH_REPO", "").strip()
        self.branch = os.getenv("GH_BRANCH", "main")
        self.dir = (os.getenv("GH_DIR", "data").strip("/"))
        self.token = os.getenv("GH_TOKEN", "")
        self.commit_name = os.getenv("GH_COMMIT_NAME", "CI-WFS3") # WFS3
        self.commit_email = os.getenv("GH_COMMIT_EMAIL", "ci-wfs3@example.com")
        self._sha_cache = {}

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    def _gh_path(self, local_path: str) -> str:
        p = local_path.replace("\\", "/").lstrip("./")
        if self.dir and not p.startswith(self.dir + "/"):
            p = f"{self.dir}/{p.split('/')[-1] if p.startswith('data/') else p}"
        return p

    def _url(self, gh_path: str) -> str:
        return f"{self.api}/repos/{self.repo}/contents/{gh_path}"

    def write_text(self, local_path: str, text: str, message: str):
        if not self.token: return
        gh_path = self._gh_path(local_path)
        payload = {
            "message": message,
            "content": base64.b64encode(text.encode("utf-8")).decode("ascii"),
            "branch": self.branch,
            "committer": {"name": self.commit_name, "email": self.commit_email}
        }
        if gh_path in self._sha_cache:
            payload["sha"] = self._sha_cache[gh_path]
        try:
            with httpx.Client(timeout=15.0) as c:
                if "sha" not in payload:
                    r_get = c.get(self._url(gh_path), headers=self._headers(), params={"ref": self.branch})
                    if r_get.status_code == 200:
                        payload["sha"] = r_get.json()["sha"]
                r = c.put(self._url(gh_path), headers=self._headers(), json=payload)
                if r.status_code in (200, 201):
                    self._sha_cache[gh_path] = r.json()["content"]["sha"]
        except Exception as e:
            print(f"⚠️ GitHub Write Error: {e}")

    def write_json(self, local_path: str, data: Any, message: str):
        self.write_text(local_path, json.dumps(data, ensure_ascii=False, indent=2), message)

gh_store = GitHubStore() if USE_GITHUB else None

class Task(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    task_type: str
    status: Optional[str] = None
    title: str
    assigned_to: Optional[str] = None
    due_date: Optional[str] = None
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat(timespec='seconds') + 'Z')
    action: Optional[str] = None
    is_completed: Optional[bool] = False
    category: Optional[str] = None
    note: Optional[str] = None
    station: Optional[str] = None
    warehouse: Optional[str] = None
    gw_date: Optional[str] = None

class TaskUpdate(BaseModel):
    status: str

class TaskCompletionUpdate(BaseModel):
    is_completed: bool

class TaskNoteUpdate(BaseModel):
    note: str | None = None

class BriefingSnapshot(BaseModel):
    station: Optional[str] = STATION_NAME  # Usa la variable dinámica
    date: str
    shift: str
    timer: str
    supervisor: str = "No especificado"
    checklist: Dict[str, str] = {}
    kpis: Dict[str, Any] = {}
    roster_details: str = ""
    prev_shift_note: str = ""
    present_names: List[str] = []
    ops_updates: List[Dict[str, Any]] = []
    briefing_time: Optional[str] = None 
    # --- AÑADIR ESTO ---
    safety_incidents: List[Dict[str, Any]] = [] 
    # -------------------
    
    kanban_counts: Dict[str, int] = {}
    kanban_details: str = ""
    roster_stats: str = ""

    class Config: extra = "allow"

# -----------------------------------
# Estado & WS
# -----------------------------------
latest_external_table: Dict[str, Any] = {
    "columns": [], "rows": [], "fetched_at": None, "version": 0,
}


class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, data: Dict[str, Any]):
        for connection in list(self.active_connections):
            try: await connection.send_json(data)
            except Exception: self.disconnect(connection)
    
    async def send_one(self, websocket: WebSocket, data: Dict[str, Any]):
        try: await websocket.send_json(data)
        except Exception: self.disconnect(websocket)

manager = ConnectionManager()

# -----------------------------------
# Lógica Externa (Tabla Planificación)
# -----------------------------------
# ... (Manteniendo helpers de ExternalConnector simplificados para no alargar infinito, 
#      pero funcionales para leer EXT_URL si existe)
def _build_ssl_context() -> ssl.SSLContext | bool:
    mode = (EXT_VERIFY_MODE or "").upper()
    if mode == "FALSE": return False
    if mode == "TRUSTSTORE":
        try: import truststore; truststore.inject_into_ssl(); return ssl.create_default_context()
        except: return ssl.create_default_context()
    if mode == "CAFILE" and EXT_CAFILE: return ssl.create_default_context(cafile=EXT_CAFILE)
    return ssl.create_default_context()

def _table_from_json(payload: Any) -> tuple[list[str], list[list[Any]]]:
    # (Lógica simplificada de BCN para extraer filas y columnas)
    if isinstance(payload, dict) and "rows" in payload:
        return payload.get("columns") or [], payload.get("rows") or []
    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        cols = list(payload[0].keys())
        rows = [[str(r.get(c)) for c in cols] for r in payload]
        return cols, rows
    return [], []

async def apply_external_table(payload: Any):
    cols, rows = _table_from_json(payload)
    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    latest_external_table["columns"] = cols
    latest_external_table["rows"] = rows
    latest_external_table["fetched_at"] = ts
    latest_external_table["version"] = int(latest_external_table.get("version", 0)) + 1
    await manager.broadcast({
        "type": "table_ping", "table": "external",
        "version": latest_external_table["version"], "rows": len(rows), "fetched_at": ts,
    })

async def push_external_table_state(websocket: WebSocket):
    await manager.send_one(websocket, {
        "type": "table_state", "table": "external",
        "version": latest_external_table.get("version", 0),
        "rows": len(latest_external_table.get("rows", [])),
        "fetched_at": latest_external_table.get("fetched_at"),
    })

# Connector básico (versión reducida de BCN para no saturar)
@dataclass
class ExternalSettings:
    urls: list[str] = field(default_factory=list)
    poll_seconds: int = 60

    @classmethod
    def from_env(cls):
        urls = [u.strip() for u in os.getenv("EXT_URLS", "").split(",") if u.strip()]
        if not urls and EXT_URL: urls = [EXT_URL]
        return cls(urls=urls, poll_seconds=EXT_POLL_SECONDS)

class ExternalConnector:
    def __init__(self):
        self.settings = ExternalSettings.from_env()
        self._client = None

    async def run(self):
        if not self.settings.urls: return
        # Bucle simple de polling
        while True:
            try:
                async with httpx.AsyncClient(verify=_build_ssl_context(), timeout=30.0) as client:
                    resp = await client.get(self.settings.urls[0])
                    if resp.status_code == 200:
                        await apply_external_table(resp.json())
            except Exception as e:
                print(f"External poll error: {e}")
            await asyncio.sleep(self.settings.poll_seconds)
    
    def status(self): return {"ok": True} # Placeholder

# -----------------------------------
# Endpoints y Lógica de Negocio
# -----------------------------------

async def send_to_excel_online(data: BriefingSnapshot):
    url = os.getenv("EXCEL_WEBHOOK_URL_WFS3")
    if not url: return

    # 1. Formatear Actualizaciones Operativas
    # Inicializamos siempre la variable antes del IF
    ops_text = "Sin actualizaciones"
    if data.ops_updates:
        ops_lines = [f"[{op.get('impact','-')}] {op.get('title','-')}" for op in data.ops_updates]
        ops_text = " | ".join(ops_lines)

    # 2. Formatear Incidentes de Seguridad (NUEVO)
    # CORRECCIÓN: Inicializamos la variable aquí, fuera de cualquier IF, para evitar el NameError
    safety_text = "Sin incidentes manuales"
    
    if data.safety_incidents:
        safe_lines = []
        for inc in data.safety_incidents:
            # En MAD usas title y desc
            titulo = str(inc.get('title', 'Sin título'))
            desc = str(inc.get('desc', ''))
            safe_lines.append(f"[{titulo}] {desc}")
        # Si hay líneas, actualizamos la variable
        if safe_lines:
            safety_text = " | ".join(safe_lines)

    # 3. Payload
    # Ahora safety_text siempre existe, tenga incidentes o no
    payload = {
        "fecha": str(data.date),
        "turno": str(data.shift),
        "timer": str(data.timer),
        "supervisor": str(data.supervisor),
        "equipo": str(data.roster_details if data.roster_details else "Sin datos"),
        "kpi_uph": str(data.kpis.get("UPH", "-")),
        "kpi_costes": str(data.kpis.get("Costes", "-")),
        "notas_turno_ant": str(data.prev_shift_note),
        "actualizaciones_ops": str(ops_text),
        "feedback_kanban": str(data.kanban_details or "Sin feedback"),
        "hora_briefing": str(data.briefing_time or datetime.now().strftime("%H:%M")),
        "incidentes_seguridad": str(safety_text) 
    }

    print(f"📤 Payload Excel: {json.dumps(payload)}")
    
    try:
        async with httpx.AsyncClient() as client:
            await client.post(url, json=payload, timeout=15.0)
    except Exception as e:
        print(f"Error Excel: {e}")

# --- Webhooks para PowerBI / Sharepoint ---
def _extract_tasks_flex(data: Any) -> list[dict]:
    # Helper para parsear tareas entrantes
    if isinstance(data, list): return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for k in ['body', 'value', 'items']:
            if k in data and isinstance(data[k], list): return data[k]
    return []

async def _best_effort_get_payload(request: Request) -> Any:
    try: return await request.json()
    except: return {}

@app.post("/webhook/sharepoint-bulk-update")
async def sharepoint_bulk_update(request: Request, x_api_key: Optional[str] = Header(None)):
    if x_api_key != API_KEY: raise HTTPException(status_code=401, detail="Invalid API Key")
    payload = await _best_effort_get_payload(request)
    tasks_raw = _extract_tasks_flex(payload)
    
    upserts = []
    for it in tasks_raw:
        # Convertir a modelo gw_task
        _id = str(it.get("id") or it.get("ID") or uuid.uuid4())
        t = sanitize_task({
            "id": _id, "task_type": "gw_task",
            "title": str(it.get("title") or "Tarea"),
            "station": it.get("station"),
            "is_completed": str(it.get("completed") or "").lower() in ["true", "yes", "1"]
        })
        prev = tasks_in_memory_store.get(t["id"])
        merged = merge_preserve_server(prev, t)
        tasks_in_memory_store[merged["id"]] = merged
        upserts.append(merged)

    save_tasks_to_disk()
    for t in upserts: await manager.broadcast(t)
    await apply_external_table(tasks_raw) # Refrescar tabla planning también
    return {"ok": True, "upserts": len(upserts)}

@app.post("/webhook/powerbi-total-cost")
async def powerbi_total_cost(request: Request, x_api_key: Optional[str] = Header(None)):
    if x_api_key != API_KEY: raise HTTPException(status_code=401, detail="Invalid API Key")
    data = await _best_effort_get_payload(request)
    
    payload = {
        "type": "kpi_update",
        "tab": str(data.get("tab","")),
        "metric": str(data.get("metric","")),
        "value": data.get("value"),
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
    await manager.broadcast(payload)
    return {"status": "ok"}

URL_TEAMS_SOPORTE = os.getenv("URL_TEAMS_SOPORTE")

class DashboardIssue(BaseModel):
    estacion: str
    supervisor: str
    tipo_fallo: str
    detalles: str
    
@app.post("/api/report-dashboard-issue")
async def report_dashboard_issue(data: DashboardIssue):
    # La URL en Render debe ir entre comillas: "https://..."
    url = os.getenv("URL_TEAMS_SOPORTE")
    
    if not url:
        print(f"⚠️ AVISO SOPORTE (Local): {data.estacion} - {data.detalles}")
        return {"status": "success"}

    payload = {
        "estacion": data.estacion,
        "supervisor": data.supervisor,
        "tipo_fallo": data.tipo_fallo,
        "detalles": data.detalles
    }

    async with httpx.AsyncClient() as client:
        # Enviamos la alerta a Power Automate para que llegue a Teams
        resp = await client.post(url, json=payload, timeout=10.0)
        print(f"📡 Reporte enviado desde {data.estacion}. Microsoft Status: {resp.status_code}")
        return {"status": "success"}

@app.post("/api/briefing/summary")
async def save_briefing_summary(data: BriefingSnapshot):
    def clean_str(text: str) -> str:
        s = unicodedata.normalize('NFD', text)
        return ''.join(c for c in s if unicodedata.category(c) != 'Mn')

    lines = []
    lines.append(f"# 📝 Resumen WFS3 - {data.station}")
    lines.append(f"**Fecha:** {data.date} | **Turno:** {data.shift}")
    lines.append(f"**⏱️ Cronómetro:** {data.timer}")
    lines.append(f"**👮 Supervisor:** {data.supervisor}")
    lines.append(f"\n**👥 Equipo:**\n{data.roster_details}")
    lines.append("\n### ↩️ Turno Anterior")
    lines.append(f"{data.prev_shift_note or 'Sin novedades.'}")
    lines.append("\n### 📊 KPIs")
    for k, v in data.kpis.items(): lines.append(f"- **{k}:** {v}")
    lines.append("\n### ✅ Checklist")
    for k, v in data.checklist.items():
        icon = "🟢" if v == "OK" else "🔴"
        lines.append(f"- {icon} {k}: {v}")
    if data.ops_updates:
        lines.append("\n### 🚧 Actualizaciones Ops")
        for op in data.ops_updates: lines.append(f"- [{op.get('impact')}] {op.get('title')}")

    final_markdown = "\n".join(lines)
    
    # Guardar con sufijo WFS3
    safe_date = data.date.replace("/", "-")
    safe_shift = clean_str(data.shift)
    timestamp = datetime.now().strftime("%H-%M-%S")
    filename = f"{safe_date}_{safe_shift}_{timestamp}_Briefing_WFS3.md"
    store_path = f"summaries_wfs3/{filename}"

    try:
        if USE_GITHUB and gh_store:
            gh_store.write_text(store_path, final_markdown, message=f"Briefing WFS3 {data.date}")
            json_path = store_path.replace(".md", ".json")
            gh_store.write_json(json_path, data.dict(), message="Briefing Data WFS3")
        else:
            p = Path("./data") / store_path
            p.parent.mkdir(parents=True, exist_ok=True)
            with open(p, "w", encoding="utf-8") as f: f.write(final_markdown)
        
        asyncio.create_task(send_to_excel_online(data))

    except Exception as e:
        print(f"❌ Error summary: {e}")
        return {"saved": False, "error": str(e)}

    return {"summary": final_markdown, "saved": True}

# --- CRUD Tareas ---
@app.get("/api/tasks")
async def get_all_tasks():
    return [sanitize_task(t) for t in tasks_in_memory_store.values()]

@app.post("/api/tasks", response_model=Task, status_code=201)
async def create_task(task: Task):
    t_dict = task.dict()
    sanitized = sanitize_task(t_dict)
    tasks_in_memory_store[sanitized["id"]] = sanitized
    save_tasks_to_disk()
    await manager.broadcast(sanitized)
    return Task(**sanitized)

@app.put("/api/tasks/{task_id}", response_model=Task)
async def update_task_status(task_id: str, task_update: TaskUpdate):
    if task_id not in tasks_in_memory_store: raise HTTPException(status_code=404)
    tasks_in_memory_store[task_id]["status"] = task_update.status
    save_tasks_to_disk()
    await manager.broadcast(tasks_in_memory_store[task_id])
    return Task(**tasks_in_memory_store[task_id])

@app.put("/api/tasks/{task_id}/complete")
async def update_task_completion(task_id: str, upd: TaskCompletionUpdate):
    if task_id not in tasks_in_memory_store: raise HTTPException(status_code=404)
    tasks_in_memory_store[task_id]["is_completed"] = upd.is_completed
    save_tasks_to_disk()
    await manager.broadcast(tasks_in_memory_store[task_id])
    return tasks_in_memory_store[task_id]

@app.put("/api/tasks/{task_id}/note")
async def update_task_note(task_id: str, upd: TaskNoteUpdate):
    if task_id not in tasks_in_memory_store: raise HTTPException(status_code=404)
    tasks_in_memory_store[task_id]["note"] = upd.note or ""
    save_tasks_to_disk()
    await manager.broadcast(tasks_in_memory_store[task_id])
    return tasks_in_memory_store[task_id]

@app.delete("/api/tasks/{task_id}", status_code=204)
async def delete_task(task_id: str):
    if task_id in tasks_in_memory_store:
        t = tasks_in_memory_store.pop(task_id)
        save_tasks_to_disk()
        await manager.broadcast({"id": task_id, "action": "delete", "task_type": t.get("task_type")})
    return {}

@app.get("/api/external-table")
async def get_external_table():
    return latest_external_table

# --- Roster Personas Manuales ---
def _require_api_key(x_api_key: Optional[str] = Header(None)):
    if x_api_key != API_KEY: raise HTTPException(status_code=401)

@app.post("/api/roster/persons")
async def add_persons(payload: Any):
    items = payload if isinstance(payload, list) else [payload]
    upserts = []
    for it in items:
        d = _person_defaults(it)
        manual_persons_store[d["id"]] = d
        upserts.append(d)
    save_persons_to_disk()
    await _build_roster_state(force=True)
    for d in upserts: await manager.broadcast({"type":"roster_manual_upsert","id":d["id"]})
    return {"ok": True}

@app.delete("/api/roster/persons/{pid}")
async def delete_person(pid: str):
    if pid in manual_persons_store:
        manual_persons_store.pop(pid)
        save_persons_to_disk()
        await _build_roster_state(force=True)
        await manager.broadcast({"type":"roster_manual_delete","id":pid})
    return {"ok": True}

@app.get("/api/roster/persons")
def list_persons(date: Optional[str] = None, shift: Optional[str] = None):
    vals = list(manual_persons_store.values())
    if date: vals = [p for p in vals if p.get("fecha") == date]
    if shift: vals = [p for p in vals if p.get("turno") == shift]
    return vals

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

async def _roster_watcher():
    try: await _build_roster_state(force=True)
    except Exception as e: print(f"Roster init error: {e}")
    while True:
        await asyncio.sleep(max(15, ROSTER_POLL_SECONDS))
        try: await _build_roster_state(force=False)
        except: pass

# -----------------------------------
# Websocket Endpoint
# -----------------------------------
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        if latest_external_table.get("version", 0) > 0:
            await push_external_table_state(websocket)
        while True: await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# -----------------------------------
# Startup / Shutdown
# -----------------------------------
@app.on_event("startup")
async def startup():
    init_db()
    load_tasks_from_disk()
    load_persons_from_disk()
    app.state._roster = asyncio.create_task(_roster_watcher())
    app.state._ext = ExternalConnector()
    app.state._poller = asyncio.create_task(app.state._ext.run())
    print(f"🚀 WFS3 MAD Backend Iniciado en puerto 8002")

@app.on_event("shutdown")
async def shutdown():
    for key in ("_poller","_roster"):
        task: asyncio.Task = getattr(app.state, key, None)
        if task: task.cancel()

# -----------------------------------
# Montaje Frontend
# -----------------------------------
# CAMBIO AQUÍ: Añadimos .parent.parent para salir de 'backend' y buscar en la raíz
FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend_wfs3"

if not FRONTEND_DIR.exists():
    print(f"⚠️ FRONTEND_DIR no existe: {FRONTEND_DIR} (Revisa la estructura de carpetas)")

app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="static")

if __name__ == "__main__":
    uvicorn.run("main_wfs3:app", host="0.0.0.0", port=8002, reload=True)
