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
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Header, WebSocket, WebSocketDisconnect, Request, Depends
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
import tempfile
from urllib.parse import parse_qs
# --- NUEVO IMPORT PARA SQL ---
from database import init_db, SessionLocal, TaskDB, IncidentDB, AttendanceDB, BriefingDB

# -----------------------------------
# Configuración General WFS3
# -----------------------------------
load_dotenv()
API_KEY = os.getenv("API_KEY")

# Configuración específica WFS3
STATION_NAME = "WFS3 MAD ALM"
ROSTER_TZ = os.getenv("ROSTER_TZ", "Europe/Madrid")
ROSTER_DB = os.getenv("ROSTER_DB", "./data/roster.json")
ROSTER_XLSX_PATH = os.getenv("ROSTER_XLSX_PATH", "./data/Informe diario.xlsx") # Puede ser compartido o único

# Configuración GitHub (Reutilizable o específica según variable de entorno)
STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "file").lower()
USE_GITHUB = (STORAGE_BACKEND == "github")

# Configuración Externa (Intranet / Planificación)
EXT_URL = os.getenv("EXT_URL", "").strip()
EXT_POLL_SECONDS = int(os.getenv("EXT_POLL_SECONDS", "60"))
EXT_VERIFY_MODE = os.getenv("EXT_VERIFY_MODE", "TRUSTSTORE").upper()
EXT_CAFILE = os.getenv("EXT_CAFILE", "").strip()

app = FastAPI(title=f"Dashboard {STATION_NAME}")

class PresenceUpdate(BaseModel):
    person: str
    present: bool
    date: Optional[str] = None
    shift: Optional[str] = None

# -----------------------------------
# Clases y Utilidades (Heredadas de BCN)
# -----------------------------------
# --- 1. CONFIGURACIÓN API MADRID - NAVE 3 ---
ROSTER_API_URL = os.getenv("ROSTER_API_URL")
ROSTER_API_KEY = os.getenv("ROSTER_API_KEY")
STATION_CODE_API = "MAD"  # Para la API, Madrid siempre es MAD
TARGET_NAVE = "N3"        # Identificador para Nave 3

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
def filter_mad_people_by_shift_and_nave(api_data: list, current_shift: str, target: str):
    normalized = []
    target_up = target.upper()
    for p in api_data:
        try:
            cod = str(p.get("codDestino", "")).upper()
            desc = str(p.get("descDestino", "")).upper()
            grupo = str(p.get("nombreGrupoTrabajo", "")).upper()
            
            # Buscamos N3 en cualquiera de los tres campos
            if target_up not in cod and "NAVE 3" not in desc and target_up not in grupo:
                continue

            raw_inicio = p.get("horaInicio", "")
            if not raw_inicio or " " not in raw_inicio: continue
            
            hora_completa = raw_inicio.split(" ")[1]
            h_inicio = int(hora_completa.split(":")[0])
            
            # Horquillas Turno (6-14, 14-22, 22-6)
            is_mañana = (4 <= h_inicio < 14)
            is_tarde  = (14 <= h_inicio < 22)
            is_noche  = (h_inicio >= 22 or h_inicio < 4)

            match = False
            if current_shift == "Mañana" and is_mañana: match = True
            elif current_shift == "Tarde" and is_tarde: match = True
            elif current_shift == "Noche" and is_noche: match = True

            if match:
                raw_fin = p.get("horaFin", "")
                h_fin_limpia = raw_fin.split(" ")[1] if (raw_fin and " " in raw_fin) else raw_fin
                normalized.append({
                    "nombre_completo": p.get("nombreApellidos", "Sin Nombre"),
                    "nomina": p.get("nomina"),
                    "horario": f"{hora_completa} - {h_fin_limpia}",
                    "observaciones": p.get("nombreGrupoTrabajo", ""),
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


@app.get("/api/roster/current")
async def get_roster_current():
    # Esta es la ruta que te daba 404
    state = await _build_roster_state(force=False)
    # Lógica de asistencia en memoria
    d_iso = state.get("sheet_date").isoformat() if state.get("sheet_date") else None
    shift = state.get("shift")
    key = _att_key(state.get("sheet_date"), shift) if d_iso else None
    att_map = attendance_store.get(key, {})
    
    return {
        "shift": shift,
        "sheet": state.get("sheet"),
        "sheet_date": d_iso,
        "window": state.get("window"),
        "people": state.get("people", []),
        "attendance": att_map,
        "updated_at": state.get("updated_at")
    }

@app.put("/api/roster/presence")
async def put_roster_presence(upd: PresenceUpdate):
    state = await _build_roster_state(force=False)
    key = _att_key(state.get("sheet_date"), state.get("shift"))
    if key not in attendance_store: attendance_store[key] = {}
    attendance_store[key][upd.person] = upd.present
    # Sincronizar vía WS
    await manager.broadcast({
        "type": "presence_update", 
        "person": upd.person, 
        "present": upd.present, 
        "sheet_date": state.get("sheet_date").isoformat(),
        "shift": state.get("shift")
    })
    return {"status": "ok"}
# ... (El bloque de GitHubStore se mantiene igual, solo cambiaremos cómo se usa si es necesario) ...
class GitHubStore:
    def __init__(self):
        self.api = os.getenv("GH_API_URL", "https://api.github.com").rstrip("/")
        self.repo = os.getenv("GH_REPO", "").strip()
        self.branch = os.getenv("GH_BRANCH", "main")
        self.dir = (os.getenv("GH_DIR", "data").strip("/"))
        self.token = os.getenv("GH_TOKEN", "")
        self.commit_name = os.getenv("GH_COMMIT_NAME", "CI-WFS3") # Ajustado
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

# Modelos de Datos
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
    # 👇 ESTA LÍNEA ES OBLIGATORIA:
    briefing_time: Optional[str] = None 
    # --- AÑADIR ESTO ---
    safety_incidents: List[Dict[str, Any]] = [] 
    # -------------------
    
    kanban_counts: Dict[str, int] = {}
    kanban_details: str = ""
    roster_stats: str = ""

    class Config: extra = "allow"

# Persistencia
tasks_in_memory_store: Dict[str, Any] = {}
manual_persons_store: Dict[str, Any] = {}

def _atomic_write_json(path: str, data: Any):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(p.parent), prefix=".tmp_wfs3alm_", suffix=".json")
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
        print("⚠️ Error guardando personas WFS3ALM:", repr(e))

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

# WebSockets Manager
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
            try:
                await connection.send_json(data)
            except Exception:
                self.disconnect(connection)

manager = ConnectionManager()

# -----------------------------------
# Lógica de Negocio WFS3
# -----------------------------------

async def send_to_excel_online(data: BriefingSnapshot):
    # BUSCA LA VARIABLE ESPECÍFICA DE WFS3
    url = os.getenv("EXCEL_WEBHOOK_URL_WFS3ALM") 
    
    if not url:
        print("⚠️ EXCEL_WEBHOOK_URL_WFS3ALM no definida.")
        return

    # 1. Procesar Actualizaciones Operativas (Ya lo tenías)
    ops_text = "Sin actualizaciones"
    if data.ops_updates:
        ops_lines = [f"[{op.get('impact','-')}] {op.get('title','-')}" for op in data.ops_updates]
        ops_text = " | ".join(ops_lines)

    # 2. Procesar Incidentes de Seguridad (ESTO ES LO QUE FALTABA)
    safety_text = "Sin incidentes reportados"
    if data.safety_incidents:
        # Asumimos que el frontend envía dicts con 'title' y 'desc'
        safety_lines = [f"{inc.get('title','Aviso')}: {inc.get('desc','')}" for inc in data.safety_incidents]
        safety_text = " | ".join(safety_lines)

    # 3. Construir el Payload
    payload = {
        "fecha": str(data.date),
        "turno": str(data.shift),
        "timer": str(data.timer),
        "supervisor": str(data.supervisor),
        "equipo": str(data.roster_details or "Sin datos"),
        "kpi_uph": str(data.kpis.get("UPH", "-")),
        "kpi_costes": str(data.kpis.get("Costes", "-")),
        "notas_turno_ant": str(data.prev_shift_note),
        "actualizaciones_ops": str(ops_text),
        "feedback_kanban": str(data.kanban_details or "Sin feedback"),
        "hora_briefing": str(data.briefing_time or datetime.now().strftime("%H:%M")),
        "incidentes_seguridad": str(safety_text) # <--- Ahora esta variable YA EXISTE
    }
    
    print(f"📤 Enviando a Excel WFS3ALM: {json.dumps(payload)}")
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, json=payload, timeout=20.0)
            if resp.status_code < 300:
                print("✅ Excel WFS3ALM actualizado.")
            else:
                print(f"❌ Error Excel WFS3: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"❌ Excepción Excel WFS3: {e}")

# -----------------------------------
# Endpoints API
# -----------------------------------

@app.post("/api/briefing/summary")
async def save_briefing_summary(data: BriefingSnapshot):
    # 1. Generar Markdown
    lines = []
    lines.append(f"# 📝 Resumen {data.station} - WFS3")
    lines.append(f"**Fecha:** {data.date} | **Turno:** {data.shift}")
    lines.append(f"**⏱️ Cronómetro:** {data.timer}")
    lines.append(f"**👮 Supervisor:** {data.supervisor}")
    lines.append(f"\n**👥 Equipo:**\n{data.roster_details}")
    
    # ... (Lógica de checklist y KPIs igual que BCN) ...
    lines.append("\n### ✅ Checklist")
    for k, v in data.checklist.items():
        icon = "🟢" if v == "OK" else "🔴"
        lines.append(f"- {icon} {k}: {v}")

    final_markdown = "\n".join(lines)

    # 2. Guardar con Timestamp y Sufijo WFS3
    safe_date = data.date.replace("/", "-")
    timestamp = datetime.now().strftime("%H-%M-%S")
    filename = f"{safe_date}_{data.shift}_{timestamp}_Briefing_WFS3alm.md"
    store_path = f"summaries_wfs3/{filename}" # Carpeta separada opcional, o prefijo en nombre

    try:
        if USE_GITHUB and gh_store:
            gh_store.write_text(store_path, final_markdown, message=f"Briefing WFS3 {data.date}")
            json_path = store_path.replace(".md", ".json")
            gh_store.write_json(json_path, data.dict(), message="Briefing Data WFS3")
        else:
            p = Path("./data") / store_path
            p.parent.mkdir(parents=True, exist_ok=True)
            with open(p, "w", encoding="utf-8") as f:
                f.write(final_markdown)

        # 3. Enviar a Excel WFS3
        asyncio.create_task(send_to_excel_online(data))

    except Exception as e:
        print(f"❌ Error summary WFS3: {e}")
        return {"saved": False, "error": str(e)}

    return {"summary": final_markdown, "saved": True}

@app.get("/api/tasks")
async def get_all_tasks():
    return list(tasks_in_memory_store.values())

@app.post("/api/tasks", response_model=Task, status_code=201)
async def create_task(task: Task):
    t_dict = task.dict()
    tasks_in_memory_store[t_dict["id"]] = t_dict
    save_tasks_to_disk()
    await manager.broadcast(t_dict)
    return Task(**t_dict)

@app.put("/api/tasks/{task_id}/complete")
async def update_task_completion(task_id: str, update_data: TaskCompletionUpdate):
    if task_id in tasks_in_memory_store:
        tasks_in_memory_store[task_id]["is_completed"] = update_data.is_completed
        save_tasks_to_disk()
        await manager.broadcast(tasks_in_memory_store[task_id])
    return tasks_in_memory_store.get(task_id)

@app.delete("/api/tasks/{task_id}", status_code=204)
async def delete_task(task_id: str):
    if task_id in tasks_in_memory_store:
        t = tasks_in_memory_store.pop(task_id)
        save_tasks_to_disk()
        await manager.broadcast({"id": task_id, "action": "delete", "task_type": t.get("task_type")})
    return {}

# Endpoint Personas Manuales
@app.post("/api/roster/persons")
async def add_persons(payload: Any):
    items = payload if isinstance(payload, list) else [payload]
    upserts = []
    for it in items:
        # Generar ID si no existe
        if not it.get("id"): it["id"] = str(uuid.uuid4())
        it["source"] = "manual"
        manual_persons_store[it["id"]] = it
        upserts.append(it)
    save_persons_to_disk()
    for d in upserts:
        await manager.broadcast({"type":"roster_manual_upsert","id":d["id"], "data": d})
    return {"ok": True}

@app.get("/api/roster/persons")
def list_persons(date: Optional[str] = None, shift: Optional[str] = None):
    vals = list(manual_persons_store.values())
    if date: vals = [p for p in vals if p.get("fecha") == date]
    if shift: vals = [p for p in vals if p.get("turno") == shift]
    return vals

@app.delete("/api/roster/persons/{pid}")
async def delete_person(pid: str):
    if pid in manual_persons_store:
        manual_persons_store.pop(pid)
        save_persons_to_disk()
        await manager.broadcast({"type":"roster_manual_delete","id":pid})
    return {"ok": True}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# Eventos de Ciclo de Vida
@app.on_event("startup")
async def startup():
    init_db()
    load_tasks_from_disk()
    load_persons_from_disk()
    print("🚀 WFS3 ALM MAD Backend Iniciado")

# Servir Frontend WFS3
# Asumimos que index_wfs3.html estará en una carpeta llamada 'frontend_wfs3alm'
FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend_wfs3alm"
if not FRONTEND_DIR.exists():
    FRONTEND_DIR.mkdir(exist_ok=True)

app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="static")

if __name__ == "__main__":
    # PUERTO LOCAL 8002 COMO SOLICITADO
    uvicorn.run("main_wfs3alm:app", host="0.0.0.0", port=10090, reload=True)
