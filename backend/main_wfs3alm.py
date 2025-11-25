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

# -----------------------------------
# Configuración General WFS3
# -----------------------------------
load_dotenv()
API_KEY = os.getenv("API_KEY")

# Configuración específica WFS3
STATION_NAME = "WFS3 MAD"
ROSTER_TZ = os.getenv("ROSTER_TZ", "Europe/Madrid")
TASKS_DB = os.getenv("TASKS_DB_WFS3", "./data/tasks_wfs3.json")
PERSONS_DB = os.getenv("PERSONS_DB_WFS3", "./data/roster_persons_wfs3.json")
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

# -----------------------------------
# Clases y Utilidades (Heredadas de BCN)
# -----------------------------------

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
    station: Optional[str] = "WFS3"  # Ajustado para WFS3
    date: str
    shift: str
    timer: str        # Requerido
    supervisor: str   # Requerido
    checklist: Dict[str, str] = {}
    kpis: Dict[str, Any] = {}
    roster_details: str = ""
    prev_shift_note: str = ""
    present_names: List[str] = []
    ops_updates: List[Dict[str, Any]] = []
    kanban_counts: Dict[str, int] = {}
    roster_stats: str = ""

    class Config:
        extra = "allow"

# Persistencia
tasks_in_memory_store: Dict[str, Any] = {}
manual_persons_store: Dict[str, Any] = {}

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

def save_tasks_to_disk():
    try:
        payload = [t for t in tasks_in_memory_store.values()]
        _atomic_write_json(TASKS_DB, payload)
    except Exception as e:
        print("⚠️ Error guardando tareas WFS3:", repr(e))

def load_tasks_from_disk():
    try:
        p = Path(TASKS_DB)
        if not p.exists(): return
        with p.open("r", encoding="utf-8") as fh:
            arr = json.load(fh)
        tasks_in_memory_store.clear()
        for t in arr or []:
            if t.get("id"): tasks_in_memory_store[t["id"]] = t
        print(f"🗂️ Cargadas {len(tasks_in_memory_store)} tareas WFS3")
    except Exception as e:
        print("⚠️ Error leyendo tareas WFS3:", repr(e))

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
        print("⚠️ EXCEL_WEBHOOK_URL_WFS3 no definida.")
        return

    ops_text = "Sin actualizaciones"
    if data.ops_updates:
        ops_lines = [f"[{op.get('impact','-')}] {op.get('title','-')}" for op in data.ops_updates]
        ops_text = " | ".join(ops_lines)

    payload = {
        "fecha": str(data.date),
        "turno": str(data.shift),
        "timer": str(data.timer),          # CAMPO CRÍTICO
        "supervisor": str(data.supervisor), # CAMPO CRÍTICO
        "equipo": str(data.roster_details if data.roster_details else "Sin datos"),
        "kpi_uph": str(data.kpis.get("UPH", "-")),
        "kpi_costes": str(data.kpis.get("Costes", "-")),
        "notas_turno_ant": str(data.prev_shift_note),
        "actualizaciones_ops": str(ops_text),
        "origen": "WFS3"
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
    filename = f"{safe_date}_{data.shift}_{timestamp}_Briefing_WFS3.md"
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
    load_tasks_from_disk()
    load_persons_from_disk()
    print("🚀 WFS3 MAD Backend Iniciado")

# Servir Frontend WFS3
# Asumimos que index_wfs3.html estará en una carpeta llamada 'frontend_wfs3'
FRONTEND_DIR = Path(__file__).resolve().parent / "frontend_wfs3"
if not FRONTEND_DIR.exists():
    FRONTEND_DIR.mkdir(exist_ok=True)

app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="static")

if __name__ == "__main__":
    # PUERTO LOCAL 8002 COMO SOLICITADO
    uvicorn.run("main_wfs3:app", host="0.0.0.0", port=10010, reload=True)
