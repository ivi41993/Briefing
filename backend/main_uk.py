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
import hmac
import hashlib
import base64
from datetime import timezone
from urllib.parse import unquote
from contextlib import asynccontextmanager

import pandas as pd
import httpx
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Header, WebSocket, WebSocketDisconnect, Request
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

import sys

# Truco: Añadimos la carpeta donde está este archivo (backend) al sistema de búsqueda de Python
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from database import init_db, SessionLocal, TaskDB, IncidentDB, AttendanceDB, BriefingDB
except ImportError:
    from backend.database import init_db, SessionLocal, TaskDB, IncidentDB, AttendanceDB, BriefingDB

# Cargar variables de entorno INMEDIATAMENTE
load_dotenv()

# === Storage backend: file | memory | github ===
STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "file").lower()
USE_DISK    = (STORAGE_BACKEND == "file")
USE_GITHUB  = (STORAGE_BACKEND == "github")
ROSTER_API_URL = os.getenv("ROSTER_API_URL")
ROSTER_API_KEY = os.getenv("ROSTER_API_KEY")
INCIDENTS_VISIBLE_LIMIT = int(os.getenv("INCIDENTS_VISIBLE_LIMIT", "3"))
ROSTER_DB = os.getenv("ROSTER_DB", "./data/roster_uk.json")
ROSTER_XLSX_PATH = os.getenv("ROSTER_XLSX_PATH_UK", "./data/Informe_diario_UK.xlsx")

# 🇬🇧 ZONA HORARIA DE UK
ROSTER_TZ = os.getenv("ROSTER_TZ", "Europe/London")

ROSTER_POLL_SECONDS = int(os.getenv("ROSTER_POLL_SECONDS", "60"))
ROSTER_NIGHT_PREV_DAY = os.getenv("ROSTER_NIGHT_PREV_DAY", "true").lower() == "true"

SPANISH_DAY = ["Lunes","Martes","Miércoles","Jueves","Viernes","Sábado","Domingo"]
SP_TTL_SECONDS = int(os.getenv("SP_TTL_SECONDS", str(8*3600)))  
sp_last_update_ts: float | None = None  

# ===== MODELOS Y VARIABLES GLOBALES =====
class BriefingSnapshot(BaseModel):
    station: Optional[str] = "UK"
    date: str
    shift: str
    timer: str
    supervisor: str = "No especificado"
    briefing_time: Optional[str] = None 
    checklist: Dict[str, str] = {}
    kpis: Dict[str, Any] = {}
    roster_details: str = ""
    prev_shift_note: str = ""
    safety_incidents: List[Dict[str, Any]] = [] 
    present_names: List[str] = []
    ops_updates: List[Dict[str, Any]] = []
    kanban_counts: Dict[str, int] = {}
    kanban_details: str = "" 
    roster_stats: str = ""

    class Config:
        extra = "allow"

tasks_in_memory_store: Dict[str, Any] = {}
_last_briefing_cache: dict[str, Any] = {}
latest_incidents_table: Dict[str, Any] = {"columns": [], "rows": [], "fetched_at": None, "version": 0}
attendance_store: dict[str, dict[str, bool]] = {}  
roster_store: dict[str, dict] = {}
roster_cache: dict[str, Any] = {"file_mtime": None, "sheet_date": None, "shift": None, "people": [], "updated_at": None, "window": None, "sheet": None}

# ===== ENVÍO A POWER AUTOMATE (EXCEL) =====
async def send_to_excel_online(data: BriefingSnapshot):
    url = os.getenv("MAIN_WEBHOOK")
    if not url: return

    ops_text = "Sin actualizaciones"
    if data.ops_updates:
        ops_lines = [f"[{op.get('impact','-')}] {op.get('title','-')}" for op in data.ops_updates]
        ops_text = " | ".join(ops_lines)

    safety_text = "Sin incidentes manuales"
    if data.safety_incidents:
        safe_lines = [f"[{inc.get('title', 'Sin título')}] {inc.get('desc', '')}" for inc in data.safety_incidents]
        safety_text = " | ".join(safe_lines)

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

    try:
        async with httpx.AsyncClient() as client:
            await client.post(url, json=payload, timeout=15.0)
    except Exception as e:
        print(f"Error Excel: {e}")

# ===== CONECTOR FIIX (ADAPTADO A UK) =====
fiix_memory_cache_uk = {}

class FiixConnector:
    def __init__(self):
        self.host = os.getenv("FIIX_HOST", "").strip()
        self.app_key = os.getenv("FIIX_APP_KEY", "").strip()
        self.access_key = os.getenv("FIIX_ACCESS_KEY", "").strip()
        self.secret_key = os.getenv("FIIX_SECRET_KEY", "").strip()
        self.client = httpx.AsyncClient(timeout=30.0)
        self.base_url = f"https://{self.host}/api/"

    def _build_auth(self) -> tuple[dict, dict]:
        ts_ms = int(time.time() * 1000)
        auth_params = {
            "accessKey": self.access_key, "appKey": self.app_key,
            "signatureMethod": "HmacSHA256", "signatureVersion": "1", "timestamp": str(ts_ms),
        }
        sorted_keys = sorted(auth_params.keys())
        query_string = "&".join([f"{k}={auth_params[k]}" for k in sorted_keys])
        signature_base = f"{self.host}/api/?{query_string}"
        signature = hmac.new(self.secret_key.encode("utf-8"), signature_base.encode("utf-8"), hashlib.sha256).hexdigest().lower()
        return auth_params, {"Content-Type": "application/json", "Authorization": signature}

    async def _fiix_rpc(self, body: dict) -> list:
        auth_params, headers = self._build_auth()
        body["clientVersion"] = {"major": 2, "minor": 8, "patch": 1}
        try:
            resp = await self.client.post(self.base_url, params=auth_params, json=body, headers=headers)
            if resp.status_code != 200: return []
            res_json = resp.json()
            if "error" in res_json: return []
            return res_json.get("objects") or res_json.get("object") or []
        except Exception: return []

    async def fetch_metrics_uk(self):
        """Métricas en tiempo real para UK"""
        global fiix_memory_cache_uk
        # ⚠️ IMPORTANTE: Pide al gestor de flota el Site ID correcto para UK y cámbialo aquí
        SITE_ID_UK = 99999999 
        PREFIX_UK = "UK-LHR-"
        ID_PREVENTIVO = 531546
        
        KEYWORDS_FLOTA = ["CTS", "VEH", "GT", "LIFT", "TRACTOR"]
        KEYWORDS_EXCLUIR = ["RENTAL", "FUEL", "INVOICE", "MONTHLY", "CLEANING"]
        
        yesterday = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")

        try:
            body_assets = {
                "_maCn": "FindRequest", "className": "Asset",
                "fields": "id, bolIsOnline, strCode, strName",
                "filters": [{"ql": "intSiteID = ? AND strCode LIKE ?", "parameters": [SITE_ID_UK, f"%{PREFIX_UK}%"]}]
            }
            res_assets = await self._fiix_rpc(body_assets)
            
            total_c = len(res_assets)
            broken_assets = [a.get("strName") for a in res_assets if a.get("bolIsOnline") == 0]
            avail = round(((total_c - len(broken_assets)) / total_c) * 100) if total_c > 0 else 100

            body_wo = {
                "_maCn": "FindRequest", "className": "WorkOrder",
                "fields": "id, intMaintenanceTypeID, strDescription, strAssets",
                "filters": [{"ql": "intSiteID = ? AND dtmDateCreated >= ?", "parameters": [SITE_ID_UK, yesterday]}]
            }
            res_wos = await self._fiix_rpc(body_wo)
            
            real_damages = []
            for w in res_wos:
                desc = str(w.get("strDescription", "")).upper()
                if not any(k in desc for k in KEYWORDS_EXCLUIR) and w.get("intMaintenanceTypeID") != ID_PREVENTIVO:
                    real_damages.append(w)

            fiix_memory_cache_uk = {
                "fiix_uk_availability": avail,
                "fiix_uk_broken_text": f"⚠️ {', '.join(broken_assets)}" if broken_assets else "Flota UK Operativa",
                "fiix_uk_damage_count_24h": len(real_damages),
                "last_update": datetime.utcnow().isoformat() + "Z"
            }
            
            await manager.broadcast({"type": "kpi_update", "station": "UK", **fiix_memory_cache_uk})
            return fiix_memory_cache_uk
        except Exception as e:
            print(f"❌ Error UK Fiix: {e}")
            return {}

# ===== API ROSTER ADAPTADA A UK =====
async def fetch_uk_roster_from_api():
    """Llama a la API central solicitando la escala de UK"""
    url = str(os.getenv("ROSTER_API_URL", "")).strip()
    key = str(os.getenv("ROSTER_API_KEY", "")).strip()
    
    if not url or not key: return None

    ahora = datetime.now(ZoneInfo(ROSTER_TZ))
    fecha_slash = ahora.strftime("%d/%m/%Y")
    
    # 🇬🇧 SOLICITAMOS UK O LHR (Depende de cómo se llame en vuestra BBDD)
    payload = {"escala": "UK", "fecha": fecha_slash} 
    
    headers = {
        "api-key": key, "Accept": "application/json",
        "User-Agent": "Mozilla/5.0", "Connection": "close"
    }

    try:
        async with httpx.AsyncClient(timeout=30.0, verify=False, http1=True, http2=False, trust_env=False) as client:
            response = await client.post(url, headers=headers, data=payload)
            if response.status_code == 200:
                data = response.json()
                return data if isinstance(data, list) else None
            return None
    except Exception as e:
        print(f"💥 UK Roster API Error: {repr(e)}")
        return None

def filter_uk_people_by_shift(api_data: Any, current_shift: str):
    normalized = []
    workers_list = []
    if isinstance(api_data, list): workers_list = api_data
    elif isinstance(api_data, dict):
        for key in ("value", "data", "items", "workers", "body"):
            if isinstance(api_data.get(key), list):
                workers_list = api_data[key]
                break

    for p in workers_list:
        if not isinstance(p, dict): continue
        try:
            nomina = p.get("nomina", {}) if isinstance(p.get("nomina"), dict) else {}
            
            def clean(t): return str(t or "").upper().strip()
            cod_destino  = clean(p.get("codDestino") or nomina.get("codDestino"))
            grupo_raw    = clean(p.get("nombreGrupoTrabajo") or nomina.get("nombreGrupoTrabajo"))
            
            # 🇬🇧 Filtro genérico para operaciones en UK
            es_ops = any(x in grupo_raw for x in ("OPS", "CARGO", "RAMP", "LEAD", "SUPERVISOR", "WAREHOUSE"))
            if not es_ops: continue

            raw_inicio = p.get("horaInicio") or nomina.get("horaInicio") or ""
            if " " not in raw_inicio: continue
            h_inicio = int(raw_inicio.split(" ")[1].split(":")[0])
            
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
                    "is_incidencia": p.get("IsIncidencias", False)
                })
        except: continue
    return normalized

def _current_shift_info(now):
    hhmm = now.strftime("%H:%M")
    if "06:00" <= hhmm < "14:00": return "Mañana", now.date(), "06:00", "14:00"
    if "14:00" <= hhmm < "22:00": return "Tarde", now.date(), "14:00", "22:00"
    sheet_date = now.date() - timedelta(days=1) if hhmm < "06:00" and ROSTER_NIGHT_PREV_DAY else now.date()
    return "Noche", sheet_date, "22:00", "06:00"

async def _build_roster_state(force=False) -> dict:
    now = datetime.now(ZoneInfo(ROSTER_TZ))
    shift, sdate, start, end = _current_shift_info(now)
    
    raw_api_data = await fetch_uk_roster_from_api()
    people = []

    if raw_api_data and isinstance(raw_api_data, list):
        people = filter_uk_people_by_shift(raw_api_data, shift)
        source = "api"
    else:
        source = "offline"

    roster_cache.update({
        "sheet_date": sdate, "shift": shift, "people": people,
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "window": {"from": start, "to": end}, "source": source
    })
    
    await manager.broadcast({"type": "roster_update", **roster_cache, "sheet_date": sdate.isoformat()})
    return roster_cache


# ===== WEBSOCKETS MANAGER =====
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
        failed = []
        for connection in self.active_connections:
            try: await connection.send_json(data)
            except: failed.append(connection)
        for f in failed: self.disconnect(f)

manager = ConnectionManager()


# ===== TAREAS EN SEGUNDO PLANO =====
async def _ws_heartbeat(interval_sec: int = 30):
    while True:
        try: await manager.broadcast({"type": "server_heartbeat", "ts": datetime.utcnow().isoformat() + "Z"})
        except: pass
        await asyncio.sleep(interval_sec)

async def fiix_auto_worker():
    connector = FiixConnector()
    while True:
        try: await connector.fetch_metrics_uk()
        except: pass
        await asyncio.sleep(600)

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🇬🇧 Iniciando sistema UK...")
    init_db()
    app.state._hb = asyncio.create_task(_ws_heartbeat(30))
    app.state._fiix_task = asyncio.create_task(fiix_auto_worker())
    yield
    print("🛑 Deteniendo sistema UK...")
    app.state._hb.cancel()
    app.state._fiix_task.cancel()


# ===== INICIALIZAR FASTAPI UK =====
app = FastAPI(title="UK Dashboard", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)


# ===== ENDPOINTS =====
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await asyncio.wait_for(websocket.receive_text(), timeout=120.0)
    except Exception:
        manager.disconnect(websocket)

@app.get("/api/roster/current")
async def get_roster_current():
    state = await _build_roster_state(force=False)
    return state

@app.get("/api/fiix/current")
async def get_fiix_current():
    connector = FiixConnector()
    return await connector.fetch_metrics_uk()

@app.post("/api/briefing/summary")
async def save_briefing_summary(data: BriefingSnapshot):
    # Guardado local/GitHub y envío a Excel
    print(f"✅ Resumen UK recibido por el supervisor: {data.supervisor}")
    asyncio.create_task(send_to_excel_online(data))
    return {"summary": "OK", "saved": True}

# --- MONTAJE ARCHIVOS ESTÁTICOS ---
# ⚠️ CUIDADO CON LA RUTA: Apunta a "frontend/uk"
# Subimos un nivel desde 'backend' (parent) y buscamos 'frontend_uk'
FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend_uk"

if not FRONTEND_DIR.exists():
    print(f"⚠️ ¡OJO! La carpeta frontend_uk no existe en la ruta: {FRONTEND_DIR}")

app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="static")

if __name__ == "__main__":
    uvicorn.run("main_uk:app", host="0.0.0.0", port=8000, reload=True)
