from __future__ import annotations
import asyncio
import io
import pdfplumber
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
from contextlib import asynccontextmanager
import unicodedata
import base64
import shlex
from dataclasses import dataclass, field

import pandas as pd
import httpx
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Header, WebSocket, WebSocketDisconnect, Request, Query, Body, UploadFile, File
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
# --- NUEVO IMPORT PARA SQL ---
from database import init_db, SessionLocal, TaskDB, IncidentDB, AttendanceDB, BriefingDB
import hmac
import hashlib

# --- CONFIGURACIÓN FIIX WFS 2B ---
FIIX_SITE_ID = 29449435
# Buscamos activos que tengan "WFS2B" en el nombre o código
TAG_NAVE = "WFS2B" 
ID_PREVENTIVO = 531546
ID_URGENTE = 278571
FIIX_CACHE_FILE = "./data/fiix_cache_wfs2b.json"

# --- PERSISTENCIA (Para que no salga 0 al reiniciar) ---
def save_fiix_cache_to_disk(data):
    try:
        os.makedirs(os.path.dirname(FIIX_CACHE_FILE), exist_ok=True)
        with open(FIIX_CACHE_FILE, "w") as f:
            json.dump(data, f)
    except Exception as e:
        print(f"⚠️ Error guardando caché Fiix: {e}")

def load_fiix_cache_from_disk():
    try:
        if os.path.exists(FIIX_CACHE_FILE):
            with open(FIIX_CACHE_FILE, "r") as f:
                data = json.load(f)
                return data
    except Exception: pass
    return {} # Vacío para forzar carga si no hay fichero

# Inicializar variable global
fiix_memory_cache = load_fiix_cache_from_disk()

# --- 1. DEFINIR EL ALMACÉN DE ASISTENCIA (Esto es lo que falta) ---
attendance_store: dict[str, dict[str, bool]] = {}

# --- 2. FUNCIÓN HELPER PARA LA CLAVE DE ASISTENCIA ---
def _att_key(d, s):
    """Genera una clave única como '2026-01-08|Mañana'"""
    if hasattr(d, 'isoformat'):
        return f"{d.isoformat()}|{s}"
    return f"{d}|{s}"

# --- 3. ASEGÚRATE DE TENER EL MODELO PresenceUpdate ---
class PresenceUpdate(BaseModel):
    person: str
    present: bool
    date: Optional[str] = None
    shift: Optional[str] = None

# ==========================================
# CONFIGURACIÓN WFS1 (AISLAMIENTO)
# ==========================================
load_dotenv()
API_KEY = os.getenv("API_KEY")
# Añadir al bloque de configuración inicial

STATION_NAME = "WFS2"
ROSTER_TZ = os.getenv("ROSTER_TZ", "Europe/Madrid")
ROSTER_POLL_SECONDS = int(os.getenv("ROSTER_POLL_SECONDS", "60"))
ROSTER_NIGHT_PREV_DAY = os.getenv("ROSTER_NIGHT_PREV_DAY", "true").lower() == "true"
# Excel de turnos (puede ser compartido o específico)
ROSTER_XLSX_PATH = os.getenv("ROSTER_XLSX_PATH", "./data/Informe diario.xlsx")

# === RUTAS DE DATOS ESPECÍFICAS WFS1 ===
STATION_CODE_API = "MAD"

# Configuración GitHub / Almacenamiento
STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "file").lower()
USE_DISK = (STORAGE_BACKEND == "file")
USE_GITHUB = (STORAGE_BACKEND == "github")

INCIDENTS_VISIBLE_LIMIT = int(os.getenv("INCIDENTS_VISIBLE_LIMIT", "3"))
SPANISH_DAY = ["Lunes","Martes","Miércoles","Jueves","Viernes","Sábado","Domingo"]

# SharePoint Cache TTL
SP_TTL_SECONDS = int(os.getenv("SP_TTL_SECONDS", str(8*3600)))
sp_last_update_ts: float | None = None

# Configuración Fuente Externa (Intranet/Planificación)
EXT_URL = os.getenv("EXT_URL", "").strip()
EXT_VERIFY_MODE = os.getenv("EXT_VERIFY_MODE", "TRUSTSTORE").upper()
EXT_CAFILE = os.getenv("EXT_CAFILE", "").strip()
EXT_USER_AGENT = os.getenv("EXT_USER_AGENT", "Mozilla/5.0")
EXT_REFERER = os.getenv("EXT_REFERER", "").strip()
ROSTER_DB = os.getenv("ROSTER_DB", "./data/roster.json")
# Configuración Enablon
ENA_URL = os.getenv("ENA_URL") or os.getenv("ENABLON_URL")
ENA_COOKIE = os.getenv("ENA_COOKIE") or os.getenv("ENABLON_COOKIE")
ENA_REFERER = os.getenv("ENA_REFERER") or os.getenv("ENABLON_REFERER")
ENA_USER_AGENT = os.getenv("ENA_USER_AGENT") or os.getenv("ENABLON_USER_AGENT") or "Mozilla/5.0"
ENA_BEARER = os.getenv("ENA_BEARER") or os.getenv("ENABLON_BEARER")
ENA_VERIFY_MODE = (os.getenv("ENA_VERIFY_MODE") or os.getenv("EXT_VERIFY_MODE") or "TRUSTSTORE").upper()
ENA_CAFILE = os.getenv("ENA_CAFILE") or os.getenv("EXT_CAFILE") or ""
ROSTER_API_URL = os.getenv("ROSTER_API_URL")
ROSTER_API_KEY = os.getenv("ROSTER_API_KEY")
SCALA_API = "MAD"   # Opciones: "MAD", "BCN", "VLC", "ALM"
NAVE_TARGET = "N2"  # Opciones: "N1", "N2", "N3", "N4" o "TODO" (para BCN/VLC/ALM)
# --- 1. LLAMADA API MADRID (Form-Data) ---
async def fetch_roster_api_data(escala: str, fecha: str):
    url = os.getenv("ROSTER_API_URL")
    key = os.getenv("ROSTER_API_KEY")
    
    if not url or not key:
        print(f"⚠️ [API {escala}] URL o KEY no configuradas en el .env")
        return None
    
    headers = {"api-key": key, "Accept": "application/json"}
    payload = {"escala": escala, "fecha": fecha}

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            # Enviamos como POST con data=payload (form-data)
            response = await client.post(url, headers=headers, data=payload)
            if response.status_code == 200:
                data = response.json()
                print(f"✅ [API {escala}] Recibidos {len(data)} registros totales.")
                return data
            else:
                print(f"❌ [API {escala}] Error HTTP {response.status_code}: {response.text[:100]}")
                return None
    except Exception as e:
        print(f"💥 [API {escala}] Fallo de conexión: {str(e)}")
        return None
        
async def fetch_roster_api_mad(fecha: str):
    """Llamada única a la API de personal de Madrid"""
    if not ROSTER_API_URL or not ROSTER_API_KEY:
        print("⚠️ API Roster no configurada en .env")
        return None
    
    payload = {"escala": "MAD", "fecha": fecha}
    headers = {"api-key": ROSTER_API_KEY, "Accept": "application/json"}

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            # Enviamos como data= para simular un formulario (form-data)
            response = await client.post(ROSTER_API_URL, headers=headers, data=payload)
            if response.status_code == 200:
                return response.json()
            print(f"❌ Error API Roster Madrid: {response.status_code}")
    except Exception as e:
        print(f"💥 Error conexión API Madrid: {e}")
    return None

# --- 2. TRIPLE FILTRO MADRID (Identificación Nave 2) ---
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
            es_nave_2 = (target in cod_destino or "NAVE 2" in desc_destino)
            if not es_nave_2:
                continue

            # --- FILTRO 2: DEPARTAMENTO (SOLO OPS, BLOQUEAR ALM) ---
            # Bloqueamos explícitamente a cualquiera de Almacén aunque esté en N4
            if any(x in grupo_raw for x in ("ALM", "ALMACEN", "ALMACENEROS")):
                continue
            
            # Solo permitimos grupos que contengan estas palabras clave de Operaciones
            es_ops = any(x in grupo_raw for x in ("CAPATACES E-COMMERCE", "OPERARIOS E-COMMERCE"))
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

# --- 3. CONSTRUCTOR DE ESTADO (Sync Roster) ---
async def _build_roster_state(force=False) -> dict:
    now = _now_local()
    shift, sdate, start, end = _current_shift_info(now)
    api_date_str = sdate.strftime("%d/%m/%Y")
    
    # Esta es la llamada que daba el NameError:
    raw_api_data = await fetch_roster_api_data("MAD", api_date_str)
    
    people = []
    if raw_api_data and isinstance(raw_api_data, list):
        # Usamos el filtro de N3 que acabamos de definir arriba
        people = filter_mad_people_by_shift_and_nave(raw_api_data, shift, "N2")
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
# -----------------------------------
# Modelos de Datos
# -----------------------------------
class BriefingSnapshot(BaseModel):
    station: Optional[str] = STATION_NAME  # Usa la variable dinámica
    date: str
    shift: str
    timer: str
    supervisor: str = "No especificado"
    checklist: Dict[str, str] = {}
    kpis: Dict[str, Any] = {}
    roster_details: str = ""
    briefing_time: Optional[str] = None 
    prev_shift_note: str = ""
    present_names: List[str] = []
    ops_updates: List[Dict[str, Any]] = []
    
    # --- AÑADIR ESTO ---
    safety_incidents: List[Dict[str, Any]] = [] 
    # -------------------
    
    kanban_counts: Dict[str, int] = {}
    kanban_details: str = ""
    roster_stats: str = ""

    class Config: extra = "allow"

class Task(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    task_type: str
    status: Optional[str] = None
    title: str
    assigned_to: Optional[str] = None
    due_date: Optional[str] = None
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat(timespec='seconds') + 'Z')
    action: Optional[str] = None
    is_completed: bool = False
    category: Optional[str] = None
    note: Optional[str] = None
    station: Optional[str] = None

class TaskUpdate(BaseModel):
    status: str

class TaskCompletionUpdate(BaseModel):
    is_completed: bool

class TaskNoteUpdate(BaseModel):
    note: str | None = None

class TaskPatch(BaseModel):
    task_type: Optional[str] = None
    status: Optional[str] = None
    title: Optional[str] = None
    assigned_to: Optional[str] = None
    due_date: Optional[str] = None
    category: Optional[str] = None
    is_completed: Optional[bool] = None
    note: Optional[str] = None

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

class PresenceUpdate(BaseModel):
    person: str
    present: bool
    date: Optional[str] = None
    shift: Optional[str] = None

# -----------------------------------
# Almacenes en Memoria
# -----------------------------------
tasks_in_memory_store: Dict[str, Any] = {}
latest_incidents_table: Dict[str, Any] = {"columns": [], "rows": [], "fetched_at": None, "version": 0}
attendance_store: dict[str, dict[str, bool]] = {}
roster_store: dict[str, dict] = {}
_last_briefing_cache: dict[str, Any] = {}

# Roster Cache (volátil)
roster_cache: dict[str, Any] = {
    "file_mtime": None, "sheet_date": None, "shift": None, "people": [], "updated_at": None, "window": None, "sheet": None,
}

# -----------------------------------
# GitHub Store
# -----------------------------------
class GitHubStore:
    def __init__(self):
        self.api = os.getenv("GH_API_URL", "https://api.github.com").rstrip("/")
        self.repo = os.getenv("GH_REPO", "").strip()
        self.branch = os.getenv("GH_BRANCH", "main")
        self.dir = (os.getenv("GH_DIR", "data").strip("/"))
        self.token = os.getenv("GH_TOKEN", "")
        self.commit_name = os.getenv("GH_COMMIT_NAME", "CI-WFS2") # <--- COMMIT WFS1
        self.commit_email = os.getenv("GH_COMMIT_EMAIL", "ci-wfs1@example.com")
        self._sha_cache: dict[str,str] = {}

    def _headers(self):
        return {"Authorization": f"Bearer {self.token}", "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}

    def _gh_path(self, local_path: str) -> str:
        p = local_path.replace("\\", "/").lstrip("./")
        if self.dir and not p.startswith(self.dir + "/"):
            p = f"{self.dir}/{p.split('/')[-1] if p.startswith('data/') else p}"
        return p

    def _url(self, gh_path: str) -> str:
        return f"{self.api}/repos/{self.repo}/contents/{gh_path}"

    def read_json(self, local_path: str):
        gh_path = self._gh_path(local_path)
        try:
            with httpx.Client(timeout=15.0) as c:
                r = c.get(self._url(gh_path), headers=self._headers(), params={"ref": self.branch})
                if r.status_code == 404: return None
                r.raise_for_status()
                obj = r.json()
                self._sha_cache[gh_path] = obj.get("sha", "")
                if obj.get("content"):
                    return json.loads(base64.b64decode(obj["content"]).decode("utf-8"))
        except Exception: pass
        return None

    def write_text(self, local_path: str, text: str, message: str):
        gh_path = self._gh_path(local_path)
        payload = {"message": message, "content": base64.b64encode(text.encode("utf-8")).decode("ascii"), "branch": self.branch, "committer": {"name": self.commit_name, "email": self.commit_email}}
        if gh_path in self._sha_cache: payload["sha"] = self._sha_cache[gh_path]
        try:
            with httpx.Client(timeout=30.0) as c:
                if "sha" not in payload:
                    r_get = c.get(self._url(gh_path), headers=self._headers(), params={"ref": self.branch})
                    if r_get.status_code == 200: payload["sha"] = r_get.json()["sha"]
                r = c.put(self._url(gh_path), headers=self._headers(), json=payload)
                if r.status_code in (200, 201): self._sha_cache[gh_path] = r.json()["content"]["sha"]
        except Exception as e: print(f"⚠️ GitHub Write Error: {e}")

    def write_json(self, local_path: str, data: Any, message: str):
        self.write_text(local_path, json.dumps(data, ensure_ascii=False, indent=2), message)

gh_store = GitHubStore() if USE_GITHUB else None

# -----------------------------------
# Persistencia Genérica
# -----------------------------------
def store_read_json(path: str, default: Any):
    if USE_GITHUB and gh_store: return gh_store.read_json(path) or default
    p = Path(path)
    if p.exists():
        try: return json.load(p.open("r", encoding="utf-8"))
        except: pass
    return default

def store_write_json(path: str, data: Any, message: str = None):
    if USE_GITHUB and gh_store:
        gh_store.write_json(path, data, message or f"Update {Path(path).name}")
        return
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as fh: json.dump(data, fh, ensure_ascii=False)

def store_append_json(path: str, item: dict, message: str = None):
    arr = store_read_json(path, [])
    if not isinstance(arr, list): arr = []
    arr.append(item)
    store_write_json(path, arr, message or f"Append {Path(path).name}")

def sanitize_task(raw: dict) -> dict:
    t = dict(raw or {})
    t["id"] = str(t.get("id") or t.get("ID") or "")
    t.setdefault("is_completed", False)
    return t

SERVER_FIELDS = ("is_completed", "note")
def merge_preserve_server(existing: dict | None, incoming: dict | None) -> dict:
    base = dict(existing or {})
    base.update(incoming or {})
    if existing:
        for f in SERVER_FIELDS:
            if f in existing and f not in (incoming or {}): base[f] = existing[f]
    return sanitize_task(base)

# Cargas de disco

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

def load_roster_from_disk():
    global roster_store
    roster_store.update(store_read_json(ROSTER_DB, {}) or {})

def save_roster_to_disk():
    store_write_json(ROSTER_DB, roster_store, "Update roster WFS2")



def _append_summary(data: dict):
    store_append_json(SUMMARIES_DB, data, "Append summary WFS2")

def _list_summaries() -> list:
    return store_read_json(SUMMARIES_DB, []) or []

# -----------------------------------
# Websockets
# -----------------------------------
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
            except: self.disconnect(connection)
    
    async def send_one(self, websocket: WebSocket, data: Dict[str, Any]):
        try: await websocket.send_json(data)
        except: self.disconnect(websocket)

manager = ConnectionManager()

# -----------------------------------
# Lógica Roster (Excel)
# -----------------------------------
DATE_SHEET_RE = re.compile(r'(\d{1,2})\D+(\d{1,2})\D+(\d{2,4})')
def _parse_sheet_date(name: str) -> date | None:
    m = DATE_SHEET_RE.search(name.strip())
    if not m: return None
    d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if y < 100: y += 2000
    try: return date(y, mo, d)
    except: return None

def _list_sheet_names(path):
    if not Path(path).exists(): return []
    try: return pd.ExcelFile(path).sheet_names
    except: return []

def _find_sheet_for_date(path, desired: date):
    names = _list_sheet_names(path)
    parsed = [(n, _parse_sheet_date(n)) for n in names]
    for n, d in parsed:
        if d == desired: return n, names
    # Búsqueda aproximada (futuro o pasado más cercano)
    futures = sorted([(d, n) for n, d in parsed if d and d >= desired], key=lambda x: x[0])
    if futures: return futures[0][1], names
    pasts = sorted([(d, n) for n, d in parsed if d and d < desired], key=lambda x: x[0], reverse=True)
    if pasts: return pasts[0][1], names
    return None, names

def _normalize_cols(cols):
    norm = {c.strip().lower().replace(' ',''): c for c in cols}
    def pick(*cands):
        for c in cands:
            k = c.strip().lower().replace(' ','')
            if k in norm: return norm[k]
        return None
    return {
        "apellidos": pick("apellidos","apellido"),
        "nombre": pick("nombre","nombres"),
        "apellidos,nombre": pick("apellidos,nombre","apellidosnombre","nombrecompleto"),
        "horario": pick("horario","turno","franja"),
        "observaciones": pick("observaciones","obs","funciondiaria"),
        "funcion_diaria": pick("funciondiaria","funciondel dia")
    }

def _match_shift(horario: str, shift: str) -> bool:
    s = str(horario or "").lower().replace("ñ","n")
    if shift == "Mañana": return "manana" in s or "06" in s or "14" in s
    if shift == "Tarde": return "tarde" in s or "14" in s or "22" in s
    if shift == "Noche": return "noche" in s or "22" in s or "06" in s
    return True

def _read_sheet_people(path, sheet, shift):
    if not Path(path).exists(): return []
    try:
        df = pd.read_excel(path, sheet_name=sheet, dtype=str)
    except: return []
    
    cols = _normalize_cols(list(df.columns))
    people = []
    
    full_col = cols.get("apellidos,nombre")
    ap_col = cols.get("apellidos")
    no_col = cols.get("nombre")
    ho_col = cols.get("horario")
    ob_col = cols.get("observaciones") or ho_col
    fu_col = cols.get("funcion_diaria")

    for _, row in df.iterrows():
        ho = str(row.get(ho_col,"")).strip()
        if not _match_shift(ho, shift): continue
        
        full = ""
        if full_col: full = str(row.get(full_col,"")).strip()
        elif ap_col and no_col: full = f"{row.get(ap_col,'')}, {row.get(no_col,'')}".strip(", ")
        
        if not full or full.lower() == "nan": continue
        
        obs = str(row.get(fu_col if fu_col else ob_col,"")).strip()
        people.append({"nombre_completo": full, "horario": ho, "observaciones": obs})
        
    return people

def _now_local(): return datetime.now(ZoneInfo(ROSTER_TZ))

def _current_shift_info(now):
    hhmm = now.strftime("%H:%M")
    if "06:00" <= hhmm < "14:00": return "Mañana", now.date(), "06:00", "14:00"
    if "14:00" <= hhmm < "22:00": return "Tarde", now.date(), "14:00", "22:00"
    sheet_date = now.date() - timedelta(days=1) if ROSTER_NIGHT_PREV_DAY and hhmm < "06:00" else now.date()
    return "Noche", sheet_date, "22:00", "06:00"



def _att_key(d, s): return f"{d.isoformat()}|{s}"

# -----------------------------------
# Enablon & External (Simplificado)
# -----------------------------------
def _build_ssl_context_for(mode, cafile):
    if mode == "FALSE": return False
    if mode == "TRUSTSTORE":
        try: import truststore; truststore.inject_into_ssl()
        except: pass
        return ssl.create_default_context()
    if mode == "CAFILE" and cafile: return ssl.create_default_context(cafile=cafile)
    return ssl.create_default_context()

class EnablonConnector:
    # (Versión mínima funcional para mantener vivo el WS y la tabla de incidentes)
    def __init__(self):
        self.status_data = {"ok": False}
    async def run(self):
        while True:
            await asyncio.sleep(60)
    def status(self): return self.status_data

# -----------------------------------
# PDF Parsing (Incidentes)
# -----------------------------------
def extract_incidents_from_pdf(raw_pdf: bytes, target_station="Madrid Cargo WFS4") -> dict:
    # Reutiliza la lógica del original si es necesario, o un placeholder
    return {"ok": True, "matches": []}

# -----------------------------------
# Lógica de Negocio Principal
# -----------------------------------
async def send_to_excel_online(data: BriefingSnapshot):
    url = os.getenv("EXCEL_WEBHOOK_URL_WFS2ALM")
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




def _compute_briefing_metrics(sections, duration):
    ok = sum(1 for s in sections.values() if s.get('status') == 'OK')
    cov = round(ok / 6 * 100) if sections else 0
    std = (cov >= 95 and duration <= 600)
    return ok, cov, std

class FiixConnector:
    def __init__(self):
        self.host = os.getenv("FIIX_HOST", "wfs.macmms.com").strip()
        self.app_key = os.getenv("FIIX_APP_KEY", "").strip()
        self.access_key = os.getenv("FIIX_ACCESS_KEY", "").strip()
        self.secret_key = os.getenv("FIIX_SECRET_KEY", "").strip()
        self.client = httpx.AsyncClient(timeout=30.0)
        self.base_url = f"https://{self.host}/api/"

    def _build_auth(self) -> tuple[dict, dict]:
        ts_ms = int(time.time() * 1000)
        auth_params = {
            "accessKey": self.access_key, "appKey": self.app_key,
            "signatureMethod": "HmacSHA256", "signatureVersion": "1",
            "timestamp": str(ts_ms),
        }
        sorted_keys = sorted(auth_params.keys())
        query_string = "&".join([f"{k}={auth_params[k]}" for k in sorted_keys])
        signature_base = f"{self.host}/api/?{query_string}"
        signature = hmac.new(self.secret_key.encode("utf-8"), signature_base.encode("utf-8"), hashlib.sha256).hexdigest().lower()
        return auth_params, {"Content-Type": "application/json", "Authorization": signature}

    async def _fiix_rpc(self, body: dict) -> list:
        if not self.app_key: return []
        auth_params, headers = self._build_auth()
        body["clientVersion"] = {"major": 2, "minor": 8, "patch": 1}
        try:
            resp = await self.client.post(self.base_url, params=auth_params, json=body, headers=headers)
            return resp.json().get("objects") or []
        except Exception as e:
            print(f"❌ [FIIX Error]: {e}")
            return []

    async def fetch_metrics(self):
        global fiix_memory_cache
        yesterday_str = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
        
        # Filtro: Buscar WFS2B, N2B, o NAVE 2B
        search_tags = ["WFS2B", "N2B", "NAVE 2B"] 
        
        print(f"🔄 [FIIX WFS2B] Buscando datos...")

        try:
            # 1. Traer activos (Máquinas)
            body_assets = {
                "_maCn": "FindRequest", "className": "Asset",
                "fields": "id, bolIsOnline, strCode, strName",
                "filters": [{"ql": "intSiteID = ? AND intKind = 2", "parameters": [FIIX_SITE_ID]}],
                "maxObjects": 1000
            }
            all_assets = await self._fiix_rpc(body_assets)
            
            # 2. Filtrar en memoria (Nombre O Código)
            assets = []
            used_tag = TAG_NAVE
            for tag in search_tags:
                candidates = [a for a in all_assets if tag in str(a.get("strCode", "")).upper() or tag in str(a.get("strName", "")).upper()]
                if candidates:
                    assets = candidates
                    used_tag = tag
                    break

            # 3. Traer Órdenes
            body_wo = {
                "_maCn": "FindRequest", "className": "WorkOrder",
                "fields": "id, intMaintenanceTypeID, intPriorityID, dtmDateCreated, dtmDateCompleted, strAssets",
                "filters": [{"ql": "intSiteID = ? AND dtmDateCreated >= ?", 
                             "parameters": [FIIX_SITE_ID, yesterday_str]}]
            }
            all_wos = await self._fiix_rpc(body_wo)
            
            # Filtrar WOs por el tag encontrado
            wos = [w for w in all_wos if used_tag in str(w.get("strAssets", "")).upper()]

            # 4. Cálculos
            total_assets = len(assets)
            broken = sum(1 for a in assets if a.get("bolIsOnline") == 0)
            avail = round(((total_assets - broken) / total_assets) * 100) if total_assets > 0 else 100
            
            cost = 0.0
            total_dt = 0
            for wo in wos:
                if wo.get("intPriorityID") == ID_URGENTE: cost += 450.0
                elif wo.get("intMaintenanceTypeID") != ID_PREVENTIVO: cost += 120.0
                else: cost += 35.0
                if wo.get("dtmDateCreated") and wo.get("dtmDateCompleted"):
                    start = wo["dtmDateCreated"]
                    end = wo["dtmDateCompleted"]
                    if isinstance(start, (int,float)) and isinstance(end, (int,float)):
                         total_dt += (end - start) / (1000 * 60)

            mttr = round((total_dt / len(wos)) / 60, 1) if wos else 0

            # 5. Guardar (Claves WFS2B)
            new_data = {
                "fiix_wfs2b_availability": avail,
                "fiix_wfs2b_damage_cost": round(cost, 2),
                "fiix_wfs2b_mttr": mttr,
                "fiix_wfs2b_broken_count": broken,
                "last_update": datetime.utcnow().isoformat() + "Z"
            }
            
            fiix_memory_cache = new_data
            save_fiix_cache_to_disk(new_data)

            print(f"🚀 [FIIX WFS2B] Activos: {total_assets} | Disp: {avail}% | Coste: {cost}€")
            
            await manager.broadcast({
                "type": "kpi_update",
                "station": "WFS2B",
                **fiix_memory_cache
            })

        except Exception as e:
            print(f"❌ [FIIX Error]: {e}")

async def fiix_auto_worker():
    connector = FiixConnector()
    await asyncio.sleep(10) 
    while True:
        try: await connector.fetch_metrics()
        except: pass
        await asyncio.sleep(600)

# -----------------------------------
# LIFESPAN & APP
# -----------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Iniciando WFS2B Dashboard...")
    init_db()
    load_tasks_from_disk()
    load_incidents_from_disk()
    load_attendance_from_disk()
    load_roster_from_disk()
    
    app.state._roster_task = asyncio.create_task(_roster_watcher())
    
    # Heartbeat
    async def _hb():
        while True:
            await asyncio.sleep(30)
            try: await manager.broadcast({"type":"server_heartbeat","ts":datetime.utcnow().isoformat()+"Z"})
            except: pass
    app.state._hb = asyncio.create_task(_hb())
    app.state._fiix_task = asyncio.create_task(fiix_auto_worker())
    
    yield
    print("🛑 Deteniendo WFS2...")
    app.state._hb.cancel()
    app.state._roster_task.cancel()
    app.state._fiix_task.cancel() # <--- Y ESTA PARA CERRAR

async def _roster_watcher():
    try: await _build_roster_state(force=True)
    except Exception as e: print(f"Roster init error: {e}")
    while True:
        await asyncio.sleep(max(15, ROSTER_POLL_SECONDS))
        try: await _build_roster_state(force=False)
        except: pass

app = FastAPI(title="WFS2 MAD Dashboard", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------
# Endpoints API
# -----------------------------------

@app.get("/api/fiix/current")
async def get_fiix_current():
    global fiix_memory_cache
    # Carga bajo demanda si está vacío
    if not fiix_memory_cache or not fiix_memory_cache.get("last_update"):
        print("⏳ [API] Cache vacía, forzando carga inmediata WFS2B...")
        conn = FiixConnector()
        await conn.fetch_metrics()
    return fiix_memory_cache

# --- 4. ENDPOINTS DE ROSTER ---
@app.get("/api/roster/current")
async def get_roster_current():
    state = await _build_roster_state(force=False)
    # Generar la llave de asistencia basada en el estado actual
    key = _att_key(state.get("sheet_date"), state.get("shift"))
    att_map = attendance_store.get(key, {})
    
    return {
        "shift": state.get("shift"),
        "sheet_date": state.get("sheet_date").isoformat(),
        "people": state.get("people", []),
        "attendance": att_map,
        "source": state.get("source")
    }

@app.put("/api/roster/presence")
async def put_roster_presence(upd: PresenceUpdate):
    state = await _build_roster_state(force=False)
    # Llave dinámica: "2026-01-08|Mañana"
    key = _att_key(state.get("sheet_date"), state.get("shift"))
    
    if key not in attendance_store:
        attendance_store[key] = {}
    
    attendance_store[key][upd.person] = upd.present
    
    await manager.broadcast({
        "type": "presence_update", 
        "person": upd.person, 
        "present": upd.present, 
        "shift": state.get("shift"),
        "sheet_date": state.get("sheet_date").isoformat()
    })
    return {"status": "ok"}
@app.get("/api/tasks")
async def list_tasks(task_type: Optional[str] = None, station: Optional[str] = None):
    items = list(tasks_in_memory_store.values())
    if task_type: items = [t for t in items if t.get("task_type") == task_type]
    if station: items = [t for t in items if (t.get("station") or "").upper() == station.upper()]
    return items

@app.post("/api/tasks", status_code=201)
async def create_task(task: Task):
    t = sanitize_task(task.dict())
    tasks_in_memory_store[t["id"]] = t
    save_tasks_to_disk()
    await manager.broadcast(t)
    return t

@app.put("/api/tasks/{task_id}")
async def update_task(task_id: str, task: Task):
    if task_id not in tasks_in_memory_store: raise HTTPException(404)
    t = sanitize_task(task.dict())
    tasks_in_memory_store[task_id] = t
    save_tasks_to_disk()
    await manager.broadcast(t)
    return t

@app.patch("/api/tasks/{task_id}")
async def patch_task(task_id: str, patch: TaskPatch):
    if task_id not in tasks_in_memory_store: raise HTTPException(404)
    existing = tasks_in_memory_store[task_id]
    incoming = {k: v for k, v in patch.dict().items() if v is not None}
    merged = merge_preserve_server(existing, incoming)
    tasks_in_memory_store[task_id] = merged
    save_tasks_to_disk()
    await manager.broadcast(merged)
    return merged

@app.delete("/api/tasks/{task_id}", status_code=204)
async def delete_task(task_id: str):
    if task_id in tasks_in_memory_store:
        t = tasks_in_memory_store.pop(task_id)
        save_tasks_to_disk()
        await manager.broadcast({"id": task_id, "action": "delete", "task_type": t.get("task_type")})
    return {}

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
    # 1. Generar Markdown
    def clean_str(text: str) -> str:
        s = unicodedata.normalize('NFD', text)
        return ''.join(c for c in s if unicodedata.category(c) != 'Mn')

    lines = []
    lines.append(f"# 📝 Resumen WFS2 - {data.station}")
    lines.append(f"**Fecha:** {data.date} | **Turno:** {data.shift}")
    lines.append(f"**Supervisor:** {data.supervisor}")
    lines.append(f"\n**👥 Equipo:**\n{data.roster_details}")
    lines.append(f"\n**↩️ Turno Anterior:**\n{data.prev_shift_note}")
    lines.append(f"\n**📊 KPIs:** UPH: {data.kpis.get('UPH')} | Costes: {data.kpis.get('Costes')}")
    lines.append(f"\n**🗣️ Feedback:**\n{data.kanban_details}")
    
    final_markdown = "\n".join(lines)

    # 2. Guardar en GitHub / Disco
    safe_date = data.date.replace("/", "-")
    safe_shift = clean_str(data.shift)
    timestamp = datetime.now().strftime("%H-%M-%S")
    filename = f"{safe_date}_{safe_shift}_{timestamp}_Briefing_WFS2.md"
    store_path = f"summaries_wfs2/{filename}" # Carpeta separada

    try:
        if USE_GITHUB and gh_store:
            gh_store.write_text(store_path, final_markdown, message=f"Briefing WFS2 {data.date}")
            gh_store.write_json(store_path.replace(".md",".json"), data.dict(), message="Data WFS2")
        else:
            p = Path("./data") / store_path
            p.parent.mkdir(parents=True, exist_ok=True)
            with open(p, "w", encoding="utf-8") as f: f.write(final_markdown)
        
        # 3. Excel
        asyncio.create_task(send_to_excel_online(data))
        
    except Exception as e:
        print(f"❌ Error guardando WFS2: {e}")
        return {"saved": False, "error": str(e)}

    return {"summary": final_markdown, "saved": True}

# Webhook SharePoint (Genera tareas)
@app.post("/webhook/sharepoint-bulk-update")
async def sharepoint_bulk_update(payload: Dict[str, Any], x_api_key: Optional[str] = Header(None)):
    if API_KEY and x_api_key != API_KEY: raise HTTPException(401)
    
    items = payload.get("body") or payload.get("value") or []
    if isinstance(payload, list): items = payload
    
    upserts = []
    for it in items:
        if not isinstance(it, dict): continue
        # Solo procesar si es para WFS1 o genérico (puedes ajustar esta lógica)
        if it.get("station") and it["station"] != "WFS2": continue
        
        tid = str(it.get("id") or uuid.uuid4())
        t = sanitize_task({
            "id": tid,
            "task_type": it.get("task_type", "gw_task"),
            "title": it.get("title", "Tarea"),
            "station": "WFS2",
            "is_completed": bool(it.get("completed"))
        })
        tasks_in_memory_store[tid] = t
        upserts.append(t)
        
    save_tasks_to_disk()
    for t in upserts: await manager.broadcast(t)
    return {"ok": True, "processed": len(upserts)}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True: await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# -----------------------------------
# Montaje Frontend (CON DIAGNÓSTICO)
# -----------------------------------
# Busca la carpeta 'frontend_wfs1' EN LA RAÍZ (subiendo 2 niveles desde backend/main_wfs1.py)
FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend_wfs2alm"

print(f"🔍 DEBUG PATH: Buscando frontend en: {FRONTEND_DIR}")

if not FRONTEND_DIR.exists():
    print(f"❌ ERROR CRÍTICO: La carpeta {FRONTEND_DIR} NO EXISTE en el servidor.")
else:
    print("✅ La carpeta existe. Listando contenido:")
    files = list(FRONTEND_DIR.iterdir())
    if not files:
        print("⚠️ LA CARPETA ESTÁ VACÍA (Git no sube carpetas vacías o hubo error al subir)")
    for f in files:
        print(f"   📄 Encontrado archivo: '{f.name}'")

# Montar estáticos
app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="static")

if __name__ == "__main__":
    # Puerto diferente para no chocar con WFS4/BCN
    uvicorn.run("main_wfs2:app", host="0.0.0.0", port=10030, reload=True)
