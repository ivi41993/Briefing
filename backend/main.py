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
# Asegúrate de tener estos también si no estaban
from datetime import timezone
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

async def send_to_excel_online(data: BriefingSnapshot):
    url = os.getenv("EXCEL_WEBHOOK_URL")
    if not url: return

    # 1. Formatear Actualizaciones Operativas
    ops_text = "Sin actualizaciones"
    if data.ops_updates:
        ops_lines = [f"[{op.get('impact','-')}] {op.get('title','-')}" for op in data.ops_updates]
        ops_text = " | ".join(ops_lines)

    # 2. Formatear Incidentes de Seguridad (NUEVO)
    safety_text = "Sin incidentes manuales"
    if data.safety_incidents:
        safe_lines = []
        for inc in data.safety_incidents:
            # En MAD usas title y desc (owner)
            titulo = str(inc.get('title', 'Sin título'))
            desc = str(inc.get('desc', ''))
            safe_lines.append(f"[{titulo}] {desc}")
        safety_text = " | ".join(safe_lines)

    # 3. Payload
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
        
        # --- NUEVO CAMPO PARA POWER AUTOMATE ---
        "incidentes_seguridad": str(safety_text)
    }

    print(f"📤 Payload Excel: {json.dumps(payload)}")
    
    try:
        async with httpx.AsyncClient() as client:
            await client.post(url, json=payload, timeout=15.0)
    except Exception as e:
        print(f"Error Excel: {e}")

def generate_html_report(data: BriefingSnapshot) -> str:
    """Genera un HTML bonito y autocontenido con los datos del briefing."""
    
    # Preparamos listas HTML
    ops_html = ""
    for op in data.ops_updates:
        ops_html += f"<li class='op-item'><span class='tag {op.get('impact','Medio')}'>{op.get('impact','Medio')}</span> <strong>{op.get('title','')}</strong> ({op.get('scope','')})</li>"
    
    checklist_html = ""
    for k, v in data.checklist.items():
        color = "green" if v == "OK" else "red"
        checklist_html += f"<div class='check-item'><span class='dot {color}'></span> {k}: <strong>{v}</strong></div>"

    html = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <style>
            body {{ font-family: sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; color: #333; }}
            .header {{ background: #b91c1c; color: white; padding: 20px; border-radius: 8px; }}
            .meta {{ display: flex; justify-content: space-between; margin-top: 10px; background: #f3f4f6; padding: 10px; border-radius: 8px; }}
            h2 {{ border-bottom: 2px solid #b91c1c; padding-bottom: 5px; color: #b91c1c; }}
            .kpi-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin: 20px 0; }}
            .kpi-card {{ background: #fff; border: 1px solid #ddd; padding: 15px; text-align: center; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            .kpi-val {{ font-size: 1.5em; font-weight: bold; color: #b91c1c; }}
            .tag {{ padding: 2px 6px; border-radius: 4px; font-size: 0.8em; color: white; }}
            .tag.Alto {{ background: #dc2626; }} .tag.Medio {{ background: #f59e0b; }} .tag.Bajo {{ background: #10b981; }}
            .dot {{ height: 10px; width: 10px; border-radius: 50%; display: inline-block; margin-right: 5px; }}
            .dot.green {{ background: #10b981; }} .dot.red {{ background: #dc2626; }}
            .check-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1 style="margin:0">Resumen de Turno - {data.station}</h1>
            <div style="margin-top:5px; opacity:0.9">Generado automáticamente</div>
        </div>
        
        <div class="meta">
            <div><strong>Fecha:</strong> {data.date}</div>
            <div><strong>Turno:</strong> {data.shift}</div>
            <div><strong>Tiempo Briefing:</strong> {data.timer}</div>
        </div>

        <div class="kpi-grid">
            <div class="kpi-card"><div>UPH</div><div class="kpi-val">{data.kpis.get('UPH','-')}</div></div>
            <div class="kpi-card"><div>OTIF</div><div class="kpi-val">{data.kpis.get('OTIF','-')}</div></div>
            <div class="kpi-card"><div>Backlog</div><div class="kpi-val">{data.kpis.get('Backlog','-')}</div></div>
            <div class="kpi-card"><div>Costes</div><div class="kpi-val">{data.kpis.get('Costes','-')}</div></div>
        </div>

        <h2>👥 Equipo ({data.roster_stats})</h2>
        <p>{', '.join(data.present_names) if data.present_names else 'Sin registro de nombres.'}</p>

        <h2>✅ Checklist</h2>
        <div class="check-grid">{checklist_html}</div>

        <h2>↩️ Turno Anterior</h2>
        <div style="background:#fff3cd; padding:15px; border-left:5px solid #f59e0b;">
            {data.prev_shift_note or "Sin novedades."}
        </div>

        <h2>🚧 Actualizaciones Operativas</h2>
        <ul>{ops_html}</ul>
    </body>
    </html>
    """
    return html

# Cargar variables de entorno INMEDIATAMENTE
load_dotenv()

# === Storage backend: file | memory | github ===
STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "file").lower()
USE_DISK    = (STORAGE_BACKEND == "file")
USE_GITHUB  = (STORAGE_BACKEND == "github")

INCIDENTS_VISIBLE_LIMIT = int(os.getenv("INCIDENTS_VISIBLE_LIMIT", "3"))

ROSTER_XLSX_PATH = os.getenv("ROSTER_XLSX_PATH", "C:/Users/iexposito/briefing/backend/data/Informe diario.xlsx")
ROSTER_TZ = os.getenv("ROSTER_TZ", "Europe/Madrid")
ROSTER_POLL_SECONDS = int(os.getenv("ROSTER_POLL_SECONDS", "60"))
ROSTER_NIGHT_PREV_DAY = os.getenv("ROSTER_NIGHT_PREV_DAY", "true").lower() == "true"

SPANISH_DAY = ["Lunes","Martes","Miércoles","Jueves","Viernes","Sábado","Domingo"]




# === SharePoint memory cache / TTL ===
SP_TTL_SECONDS = int(os.getenv("SP_TTL_SECONDS", str(8*3600)))  # 8 horas por defecto
sp_last_update_ts: float | None = None  # timestamp de última actualización válida






def _sheet_name_for_date(d):
    return SPANISH_DAY[d.weekday()]  # Lunes..Domingo












import unicodedata

def _norm_key(txt: str) -> str:
    s = (txt or "").strip().lower()
    # quita acentos/diacríticos
    s = ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))
    # homogeneiza separadores
    s = s.replace(',', '').replace(';','').replace(':','')
    s = s.replace('\u00a0', ' ')  # NBSP → espacio normal
    s = s.replace('  ', ' ').replace(' ', '')
    return s

def _normalize_cols(cols):
    norm = {_norm_key(c): c for c in cols}

    def pick(*cands):
        for cand in cands:
            real = norm.get(_norm_key(cand))
            if real:
                return real
        return None

    return {
        "apellidos": pick("apellidos","apellido","apellidos/s"),
        "nombre": pick("nombre","nombres"),
        "apellidos, nombre": pick("apellidos, nombre","apellidos nombre","nombre completo"),
        "horario": pick("horario","turno","franja"),
        "observaciones": pick("observaciones","observacion","obs"),

        # 🔽 NUEVO: “Función diaria”
        # acepta variantes con/ sin tilde y con/ sin espacios
        "funcion_diaria": pick(
            "funcion diaria", "función diaria", "funcion_diaria", "funciondiaria",
            "funcion del dia", "función del día", "funciondía", "funcion dia", "función dia"
        ),

        # (ya estaban, las dejamos)
        "nomina": pick("nomina","nómina"),
        "contrato": pick("contrato"),
        "formacion": pick("formacion","formación"),
        "limitaciones": pick("limitaciones","limitacion"),
    }


def _match_shift(horario: str, shift: str) -> bool:
    s = (horario or "").strip().lower()
    s = s.replace('mañana','manana')
    # códigos sueltos muy típicos
    if s in ('m','t','n'):
        return (shift == 'Mañana' and s=='m') or (shift=='Tarde' and s=='t') or (shift=='Noche' and s=='n')

    def starts_any(x: str, *pref): return any(x.startswith(p) for p in pref)

    if shift == "Mañana":
        return ('manana' in s) or starts_any(s, "06:", "07:", "08:", "09:", "10:", "11:", "12:", "13:") or "06" in s and "14" in s
    if shift == "Tarde":
        return ('tarde' in s) or starts_any(s, "14:", "15:", "16:", "17:", "18:", "19:", "20:", "21:") or "14" in s and "22" in s
    if shift == "Noche":
        return ('noche' in s) or starts_any(s, "22:", "23:", "00:", "01:", "02:", "03:", "04:", "05:", "06:") or "22" in s and "06" in s
    return True



def _read_sheet_people(xlsx_path: str, sheet_name: str, shift: str) -> list[dict]:
    if not Path(xlsx_path).exists():
        return []
    df = None
    cols = None

    # Prioriza header=2 (fila 3 de Excel), pero prueba 1 y 0 por si acaso
    for hdr in (2, 1, 0):
        try:
            df_try = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str, header=hdr)
            cols_try = _normalize_cols(list(df_try.columns))
            if cols_try.get("horario") and (cols_try.get("apellidos, nombre") or (cols_try.get("apellidos") and cols_try.get("nombre"))):
                df, cols = df_try, cols_try
                break
        except Exception as e:
            print(f"⚠️ No se pudo leer hoja={sheet_name} header={hdr}: {e}")

    if df is None:
        print("⚠️ Hoja sin columnas esperadas:", sheet_name)
        return []

    full_name_col = cols.get("apellidos, nombre")
    ap_col        = cols.get("apellidos")
    no_col        = cols.get("nombre")
    ho_col        = cols.get("horario")
    ob_col        = cols.get("observaciones") or ho_col  # si no hay observaciones, no rompe

    # 🔽 NUEVO: columna “Función diaria” (si existe)
    fu_col        = cols.get("funcion_diaria")

    people = []
    for _, row in df.iterrows():
        # nombre completo
        if full_name_col:
            full_name = str(row.get(full_name_col) or "").strip()
        else:
            ap = str(row.get(ap_col) or "").strip()
            no = str(row.get(no_col) or "").strip()
            full_name = f"{ap}, {no}".strip(", ").strip()

        ho = str(row.get(ho_col) or "").strip() if ho_col else ""
        ob = str(row.get(ob_col) or "").strip()

        # 🔽 NUEVO: función diaria (si la hoja la tiene)
        fu = str(row.get(fu_col) or "").strip() if fu_col else ""

        # limpia "nan"
        if full_name.lower() == "nan": full_name = ""
        if ho.lower() == "nan": ho = ""
        if ob.lower() == "nan": ob = ""
        if fu.lower() == "nan": fu = ""

        if not full_name:
            continue
        if not _match_shift(ho, shift):
            continue

        # 🔽 NUEVO: si existe la columna “Función diaria”, filtra filas sin valor
        if fu_col and not fu:
            continue

        # 🔽 NUEVO: si hay “Función diaria”, se mostrará en tarjeta en lugar de observaciones
        ob_display = fu if fu_col and fu else ob

        ap_out, no_out = "", ""
        if "," in full_name:
            ap_out, no_out = [p.strip() for p in full_name.split(",", 1)]
        else:
            ap_out = full_name

        people.append({
            "apellidos": ap_out,
            "nombre": no_out,
            "nombre_completo": full_name,
            "horario": ho,
            "observaciones": ob_display,   # 👈 mostrará la función diaria si aplica
            "funcion_diaria": fu,          # 👈 guardamos el valor explícitamente también
        })

    print(f"👥 Roster: {len(people)} personas tras filtrar por turno='{shift}' en hoja='{sheet_name}'"
          f"{' (con filtro de Función diaria)' if fu_col else ''}")
    return people



from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Header, WebSocket, WebSocketDisconnect, Request
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
import tempfile
from pathlib import Path

# Donde defines rutas
TASKS_DB = os.getenv("GH_FILE_TASKS") or os.getenv("TASKS_DB", "./data/tasks.json")
INCIDENTS_DB = os.getenv("INCIDENTS_DB", "./data/incidents_table.json")
ATTENDANCE_DB = os.getenv("ATTENDANCE_DB", "./data/attendance.json")
ROSTER_DB = os.getenv("ROSTER_DB", "./data/roster_store.json")
BRIEFING_DB = os.getenv("BRIEFING_DB", "./data/briefings.json")
SUMMARIES_DIR   = os.getenv("SUMMARIES_DIR", "./data/summaries")
SUMMARIES_INDEX = os.getenv("SUMMARIES_INDEX", "./data/summaries_index.json")
# === Resúmenes operativos ===
SUMMARIES_DB = os.getenv("SUMMARIES_DB", "./data/summaries.json")

def _append_summary(item: dict):
    """Append seguro de un resumen al archivo/repo configurado."""
    store_append_json(SUMMARIES_DB, item, message="Append summaries.json")

def _list_summaries() -> list[dict]:
    arr = store_read_json(SUMMARIES_DB, []) or []
    return arr if isinstance(arr, list) else []

def _get_summary(summary_id: str) -> dict | None:
    for it in reversed(_list_summaries()):
        if it.get("id") == summary_id:
            return it
    return None

def save_tasks_to_disk():
    try:
        payload = [sanitize_task(t) for t in tasks_in_memory_store.values()]
        store_write_json(TASKS_DB, payload, message="Update tasks.json")
    except Exception as e:
        print("⚠️ Error guardando tareas:", repr(e))

def save_incidents_to_disk():
    try:
        store_write_json(INCIDENTS_DB, latest_incidents_table, message="Update incidents_table.json")
    except Exception as e:
        print("⚠️ Error guardando incidents_table:", repr(e))

def save_attendance_to_disk():
    try:
        store_write_json(ATTENDANCE_DB, attendance_store, message="Update attendance.json")
    except Exception as e:
        print("⚠️ Error guardando asistencia:", repr(e))

def save_roster_to_disk():
    try:
        store_write_json(ROSTER_DB, roster_store, message="Update roster_store.json")
    except Exception as e:
        print("⚠️ Error guardando roster_store:", repr(e))

# En el POST /api/briefing usa append unificado:
def _append_briefing(data: dict):
    store_append_json(BRIEFING_DB, data, message="Append briefings.json")


def _atomic_write_json(path: str, data: list[dict]):
    if USE_GITHUB and gh_store:
        gh_store.write_json(path, data, message=f"Update {Path(path).name}")
        return
    if not USE_DISK:
        return
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(p.parent), prefix=".tmp_", suffix=".json")
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

DATE_RE = re.compile(
    r'(?P<d>\d{1,2})[\/\-\.\s](?P<m>\d{1,2})[\/\-\.\s](?P<y>\d{2,4})'
)

def _parse_date_es(txt: str) -> str | None:
    """
    Intenta dd/mm/yyyy, d-m-yy, etc. Devuelve ISO YYYY-MM-DD o None.
    """
    if not txt:
        return None
    s = str(txt).strip()
    m = DATE_RE.search(s)
    if not m: 
        return None
    d, mo, y = int(m.group('d')), int(m.group('m')), int(m.group('y'))
    if y < 100:
        y += 2000 if y < 70 else 1900
    try:
        return date(y, mo, d).isoformat()
    except ValueError:
        return None
def _likely_header_row(rows: list[list[str]]) -> int | None:
    """
    Devuelve el índice de la fila que parece cabecera (contiene 'Station' o afines).
    """
    for i, r in enumerate(rows):
        joined = " ".join([str(c or "") for c in r]).lower()
        if "station" in joined:
            return i
    return None

def _normalize_headers(cells: list[str]) -> list[str]:
    mapping = {
        "station": "station",
        "eventtype": "event_type",
        "event type": "event_type",
        "tipoevento": "event_type",

        # 👇 NUEVO: título del accidente
        "titulodelaccidente": "titulo_accidente",
        "titulo del accidente": "titulo_accidente",
        "título del accidente": "titulo_accidente",
        "accidenttitle": "titulo_accidente",
        "title": "titulo_accidente",
        "asunto": "titulo_accidente",
        "resumen": "titulo_accidente",
        "descripcion": "titulo_accidente",
        "descripción": "titulo_accidente",

        "fechaaccidente": "fecha_accidente",
        "fechadelaccidente": "fecha_accidente",
        "fecha accidente": "fecha_accidente",
        "accidentdate": "fecha_accidente",
        "dateofaccident": "fecha_accidente",
        "fecha": "fecha_accidente",
                # variantes sin "del"
        "titulodeaccidente": "titulo_accidente",
        "titulo de accidente": "titulo_accidente",
        "título de accidente": "titulo_accidente",

    }
    out = []
    for c in cells:
        k = _norm_key(str(c or ""))
        out.append(mapping.get(k, k))
    return out

def _iter_pdf_tables(page):
    """
    Devuelve distintas interpretaciones de una misma página:
    1) lattice (con líneas), 2) stream (por texto), 3) find_tables() (auto).
    """
    # 1) líneas
    yield page.extract_table(table_settings={
        "vertical_strategy": "lines",
        "horizontal_strategy": "lines",
        "intersection_tolerance": 5,
    })
    # 2) stream
    yield page.extract_table(table_settings={
        "vertical_strategy": "text",
        "horizontal_strategy": "text",
        "snap_tolerance": 6,
        "join_tolerance": 6,
        "edge_min_length": 3,
        "text_x_tolerance": 2,
        "text_y_tolerance": 2,
    })
    # 3) find_tables() (más tolerante)
    try:
        for t in page.find_tables(table_settings={
            "intersection_tolerance": 5,
            "snap_tolerance": 6,
            "join_tolerance": 6,
            "edge_min_length": 3,
        }):
            yield t.extract()
    except Exception:
        pass

def _station_matches(val: str, target="Madrid Cargo WFS4") -> bool:
    # Si target viene vacío o "*", no filtra por estación
    if not target or target == "*":
        return True
    a = _norm_key(val or "")
    b = _norm_key(target or "")
    # Igual, sin espacios, o contiene/está contenido (para variantes tipo "WFS 4", "WFS4", etc.)
    return (a == b) or (a.replace(" ", "") == b.replace(" ", "")) or (a in b) or (b in a)


def extract_incidents_from_pdf(raw_pdf: bytes, target_station="Madrid Cargo WFS4") -> dict:
    matches = []
    pages_scanned = 0
    with pdfplumber.open(io.BytesIO(raw_pdf)) as pdf:
        for p in pdf.pages:
            pages_scanned += 1
            for table in _iter_pdf_tables(p):
                if not table:
                    continue
                rows = [[(c or "").strip() for c in row] for row in table if row]
                if not rows:
                    continue

                hdr_idx = _likely_header_row(rows)
                if hdr_idx is None:
                    hdr_idx = 0

                headers_raw = rows[hdr_idx]
                headers = _normalize_headers(headers_raw)

                try:
                    idx_station = headers.index("station")
                except ValueError:
                    idx_station = None

                idx_title = headers.index("titulo_accidente") if "titulo_accidente" in headers else None
                idx_ev    = headers.index("event_type") if "event_type" in headers else None
                idx_dt    = headers.index("fecha_accidente") if "fecha_accidente" in headers else None

                for r in rows[hdr_idx + 1:]:
                    if any(_norm_key(x) == "station" for x in r):
                        continue

                    st_val = r[idx_station] if (idx_station is not None and idx_station < len(r)) else ""
                    if idx_station is not None and not _station_matches(st_val, target_station):
                        continue

                    # 🟣 Título del accidente con fallback a event_type
                    title_val = r[idx_title] if (idx_title is not None and idx_title < len(r)) else ""
                    ev_val    = r[idx_ev] if (idx_ev is not None and idx_ev < len(r)) else ""
                    titulo    = (title_val or ev_val or "").strip()

                    dt_val = r[idx_dt] if (idx_dt is not None and idx_dt < len(r)) else ""
                    dt_iso = _parse_date_es(dt_val) or _parse_date_es(ev_val) or _parse_date_es(title_val)

                    if any((titulo, dt_val, dt_iso, st_val)):
                        matches.append({
                            "titulo_accidente": titulo,
                            "fecha_accidente": (dt_iso or (dt_val or "").strip()),
                            "station": st_val,
                            "source_page": p.page_number,
                        })

    return {
        "ok": True,
        "target_station": target_station,
        "pages_scanned": pages_scanned,
        "found_rows": len(matches),
        "matches": matches
    }





# ==== Helpers de persistencia unificados (DISK o GITHUB) ====

def store_read_json(path: str, default: Any):
    if USE_GITHUB and gh_store:
        obj = gh_store.read_json(path)
        return default if obj is None else obj
    # disco
    p = Path(path)
    if p.exists():
        try:
            return json.load(p.open("r", encoding="utf-8"))
        except Exception:
            return default
    return default

def store_write_json(path: str, data: Any, message: str | None = None):
    if USE_GITHUB and gh_store:
        gh_store.write_json(path, data, message=message or f"Update {Path(path).name}")
        return
    # disco
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(p.parent), prefix=".tmp_", suffix=".json")
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

def store_append_json(path: str, item: dict, message: str | None = None):
    """Append seguro para arrays JSON (briefings, etc.). En GitHub: lee → añade → write."""
    arr = []
    try:
        arr = store_read_json(path, []) or []
        if not isinstance(arr, list):
            arr = []
    except Exception:
        arr = []
    arr.append(item)
    store_write_json(path, arr, message=message or f"Append {Path(path).name}")


def _load_last_briefing() -> dict:
    """
    Devuelve el último briefing persistido (si existe) o el cache en memoria.
    """
    p = Path(BRIEFING_DB)
    if p.exists():
        try:
            arr = json.load(p.open("r", encoding="utf-8")) or []
            if isinstance(arr, list) and arr:
                return arr[-1]
        except Exception:
            pass
    return _last_briefing_cache or {}

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
class TaskPatch(BaseModel):
    task_type: Optional[str] = None
    status: Optional[str] = None
    title: Optional[str] = None
    assigned_to: Optional[str] = None
    due_date: Optional[str] = None   # acepta dd/mm/yyyy, d-m-yy o ISO
    category: Optional[str] = None
    is_completed: Optional[bool] = None
    note: Optional[str] = None

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
# === Cache del último briefing (faltaba una definición por defecto) ===
_last_briefing_cache: dict[str, Any] = {}






# ===== NUEVO: almacén de INCIDENTES (Enablon) =====
latest_incidents_table: Dict[str, Any] = {
    "columns": [],
    "rows": [],
    "fetched_at": None,
    "version": 0,
}


# Asegúrate de que solo hay UNA instancia de ConnectionManager


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

def _pick_idx(cols: list[str], *cands: str) -> int | None:
    norm = {_norm_key(c): i for i, c in enumerate(cols)}
    for cand in cands:
        i = norm.get(_norm_key(cand))
        if i is not None:
            return i
    return None
def _fmt_pct(x):
    try:
        return f"{float(x):.1f}%"
    except:
        return "-"
def _summarize_briefing_last() -> tuple[str, dict]:
    """
    Convierte el último briefing (checklist) en Markdown compacto.
    """
    br = _load_last_briefing()
    if not br:
        return "### Checklist / Briefing\n- No hay briefing guardado aún.\n", {"has_briefing": False}

    date_iso = br.get("date_iso") or "-"
    shift    = br.get("shift") or "-"
    supervisor = br.get("supervisor") or ""
    duration = int(br.get("duration_sec") or 0)
    attendance_pct = br.get("attendance_pct")
    ok_blocks = br.get("ok_blocks")
    coverage  = br.get("coverage_pct")
    standard  = br.get("standard_met")

    head = "### Checklist / Briefing\n"
    lines = []
    meta  = {"has_briefing": True, "date": date_iso, "shift": shift}

    hdr = f"- Fecha: **{date_iso}** — Turno: **{shift}**"
    if supervisor: hdr += f" — Supervisor: **{supervisor}**"
    lines.append(hdr)

    if duration:        lines.append(f"- Duración: {duration}s")
    if attendance_pct is not None: lines.append(f"- Asistencia: {attendance_pct:.0f}%")
    if coverage  is not None:      lines.append(f"- Cobertura: {coverage:.0f}%")
    if ok_blocks is not None:      lines.append(f"- Bloques OK: {ok_blocks}")
    if standard is not None:       lines.append(f"- Estándar cumplido: {'Sí' if standard else 'No'}")

    sections = br.get("sections") or {}
    if sections:
        lines.append("\n- **Secciones**:")
        for sec_name, sec in sections.items():
            status  = (sec or {}).get("status") or ""
            criteria = (sec or {}).get("criteria") or {}
            notes   = (sec or {}).get("notes") or ""

            # Ticks marcados
            ticks = [k for k, v in criteria.items() if v]
            ticks_s = ", ".join(ticks) if ticks else "—"

            label = f"  - {sec_name}:"
            if status: label += f" _{status}_"
            lines.append(label)
            lines.append(f"    - ✓ Criterios: {ticks_s}")
            if notes:
                lines.append(f"    - Notas: {notes}")

    text = head + "\n".join(lines) + "\n"
    # Metadatos útiles para el front o export
    meta.update({
        "duration_sec": duration,
        "attendance_pct": attendance_pct,
        "coverage_pct": coverage,
        "ok_blocks": ok_blocks,
        "standard_met": standard,
        "sections_count": len(sections)
    })
    return text, meta

def _summarize_incidents(limit=10) -> tuple[str, dict]:
    tbl = latest_incidents_table or {}
    cols = [c.lower() for c in (tbl.get("columns") or [])]
    rows = tbl.get("rows") or []
    # índices tolerantes
    def _idx(name, *alts):
        norm = {_norm_key(c): i for i, c in enumerate(cols)}
        for cand in (name,) + alts:
            k = _norm_key(cand)
            if k in norm:
                return norm[k]
        return None
    i_title = _idx("Título del accidente", "titulo_accidente", "title", "asunto", "resumen", "event_type")
    i_date  = _idx("Fecha del accidente", "fecha_accidente", "fecha", "date")
    i_page  = _idx("Página", "page", "source_page")

    head = f"### Incidentes ({len(rows)} total)\n"
    if not rows:
        return head + "- No hay registros.\n", {"count": 0}

    lines = []
    for r in rows[:limit]:
        title = (r[i_title] if (i_title is not None and i_title < len(r)) else "") or "Sin título"
        fdate = (r[i_date]  if (i_date  is not None and i_date  < len(r)) else "") or ""
        page  = (r[i_page]  if (i_page  is not None and i_page  < len(r)) else "")
        suffix = f" (pág. {page})" if str(page).strip() else ""
        lines.append(f"- {fdate} — {title}{suffix}")
    more = "" if len(rows) <= limit else f"\n> …y {len(rows)-limit} más."
    return head + "\n".join(lines) + more + "\n", {"count": len(rows)}

def _summarize_roster_current() -> tuple[str, dict]:
    # Intento directo (reutiliza tu lógica actual)
    try:
        # Construye el estado sin forzar para no leer Excel si ya está en cache
        # (reutilizando tu helper)
        import asyncio as _asyncio
        state = _asyncio.get_event_loop().run_until_complete(_build_roster_state(force=False))
    except RuntimeError:
        # si ya estamos en loop, usa el cache directo
        state = roster_cache

    shift = state.get("shift") or "-"
    sheet_date = state.get("sheet_date")
    sheet_iso = sheet_date.isoformat() if sheet_date else "-"
    people = state.get("people") or []
    head = f"### Roster actual — {shift} ({sheet_iso})\n"
    if not people:
        return head + "- Sin personal cargado o no disponible para este turno.\n", {"shift": shift, "date": sheet_iso, "count": 0}

    # Muestra hasta 12 nombres
    lines = []
    for p in people[:12]:
        nm = p.get("nombre_completo") or f"{p.get('apellidos','')}, {p.get('nombre','')}".strip(", ")
        fn = p.get("funcion_diaria") or p.get("observaciones") or ""
        fn = f" — {fn}" if fn else ""
        lines.append(f"- {nm}{fn}")
    more = "" if len(people) <= 12 else f"\n> …y {len(people)-12} más."
    return head + "\n".join(lines) + more + "\n", {"shift": shift, "date": sheet_iso, "count": len(people)}

def _summarize_tasks() -> tuple[str, dict]:
    items = list(tasks_in_memory_store.values())
    total = len(items)
    done = sum(1 for t in items if t.get("is_completed"))
    pending = total - done
    by_type = {}
    for t in items:
        by_type[t.get("task_type","?")] = by_type.get(t.get("task_type","?"), 0) + 1
    lines = [f"### Tareas ({total} total)"]
    lines.append(f"- Completadas: {done}")
    lines.append(f"- Pendientes: {pending}")
    if by_type:
        lines.append("- Por tipo:")
        for k, v in sorted(by_type.items(), key=lambda x: (-x[1], x[0])):
            lines.append(f"  - {k}: {v}")
    return "\n".join(lines) + "\n", {"total": total, "done": done, "by_type": by_type}

def _build_operational_summary(scope: dict | None = None) -> dict:
    """
    Construye un resumen operativo en Markdown con metadatos.
    scope: {"incidents":bool, "roster":bool, "tasks":bool, "briefing":bool}
    """
    scope = scope or {}
    use_inc = scope.get("incidents", True)
    use_ros = scope.get("roster", True)
    use_tsk = scope.get("tasks", True)
    use_brf = scope.get("briefing", True)

    parts = []
    meta: dict[str, Any] = {"sections": {}}

    # Fecha/hora cabecera (zona local)
    nowz = datetime.now(ZoneInfo(ROSTER_TZ))
    # Además, intenta capturar la “fecha activa de dashboard” (roster)
    try:
        state = asyncio.get_event_loop().run_until_complete(_build_roster_state(force=False))
    except RuntimeError:
        state = roster_cache
    active_date = state.get("sheet_date")
    active_shift = state.get("shift")
    active_date_iso = active_date.isoformat() if active_date else "-"

    hdr = (
        f"# Resumen operativo — {nowz.strftime('%Y-%m-%d %H:%M')} ({ROSTER_TZ})\n\n"
        f"- **Fecha activa de dashboard**: {active_date_iso} — **Turno**: {active_shift or '-'}\n"
    )
    parts.append(hdr)

    if use_brf:
        s, m = _summarize_briefing_last()
        parts.append(s + "\n")
        meta["sections"]["briefing"] = m

    if use_inc:
        s, m = _summarize_incidents()
        parts.append(s + "\n")
        meta["sections"]["incidents"] = m

    if use_ros:
        s, m = _summarize_roster_current()
        parts.append(s + "\n")
        meta["sections"]["roster"] = m

    if use_tsk:
        s, m = _summarize_tasks()
        parts.append(s + "\n")
        meta["sections"]["tasks"] = m

    text = "\n".join(parts).strip() + "\n"
    payload = {
        "id": str(uuid.uuid4()),
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "tz": ROSTER_TZ,
        "markdown": text,
        **meta
    }
    return payload


def _harmonize_incidents_columns(cols: list[str], rows: list[list[Any]]) -> tuple[list[str], list[list[Any]]]:
    """
    Fuerza la tabla de incidentes a columnas en español, priorizando 'Título del accidente'.
    Busca múltiples alias (incl. 'event_type') y reordena/renombra sin perder datos.
    """
    # Índices de columnas relevantes
    i_title = _pick_idx(
        cols,
        "titulo_accidente", "título del accidente", "titulo del accidente",
        "título de accidente", "titulo de accidente",
        "accidenttitle", "title", "asunto", "resumen", "descripcion", "descripción",
        "event_type", "event type", "tipoevento"
    )
    i_date = _pick_idx(
        cols,
        "fecha_accidente", "fecha del accidente", "fecha accidente",
        "accidentdate", "dateofaccident", "fecha"
    )
    i_station = _pick_idx(cols, "station", "estacion", "estación", "site", "ubicacion", "ubicación")
    i_page = _pick_idx(cols, "source_page", "pagina", "página", "page")

    # Construimos nuevas columnas y un "picker" de índices en ese orden
    new_cols: list[str] = []
    pickers: list[int | None] = []

    if i_title is not None:
        new_cols.append("Título del accidente"); pickers.append(i_title)
    if i_date is not None:
        new_cols.append("Fecha del accidente"); pickers.append(i_date)
    if i_station is not None:
        new_cols.append("Estación"); pickers.append(i_station)
    if i_page is not None:
        new_cols.append("Página"); pickers.append(i_page)

    # Si no encontramos ninguna de las columnas clave, devolvemos la tabla original
    if not new_cols:
        return cols, rows

    # Reordenamos/filtramos filas según los índices seleccionados
    new_rows: list[list[Any]] = []
    for r in rows:
        rr = []
        for idx in pickers:
            rr.append(r[idx] if idx is not None and idx < len(r) else None)
        new_rows.append(rr)

    return new_cols, new_rows



async def apply_incidents_table(payload: Any):
    cols, rows = _table_from_json(payload)

    if not cols and not rows:
        print("ℹ️ Incidentes: payload vacío/ilegible; se conserva último estado.")
        return

    # 🔽 Normaliza SIEMPRE a español y prioriza 'Título del accidente'
    cols, rows = _harmonize_incidents_columns(cols, rows)

    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    latest_incidents_table["columns"]    = cols
    latest_incidents_table["rows"]       = rows
    latest_incidents_table["fetched_at"] = ts
    latest_incidents_table["version"]    = int(latest_incidents_table.get("version", 0)) + 1

    save_incidents_to_disk()

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

    await apply_incidents_table(data)


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


def _read_json_any(path: str, default: Any):
    # (opcional) puedes eliminar esta función y usar store_read_json directamente
    return store_read_json(path, default)

def load_tasks_from_disk():
    global tasks_in_memory_store, sp_last_update_ts
    arr = store_read_json(TASKS_DB, [])
    tasks_in_memory_store.clear()
    for t in arr or []:
        t = sanitize_task(t)
        if t.get("id") and t.get("task_type"):
            tasks_in_memory_store[t["id"]] = t
    print(f"🗂️ Cargadas {len(tasks_in_memory_store)} tareas (backend={STORAGE_BACKEND}).")
    if tasks_in_memory_store and not sp_last_update_ts:
        sp_last_update_ts = time.time()

def load_incidents_from_disk():
    global latest_incidents_table
    obj = store_read_json(INCIDENTS_DB, {}) or {}
    if isinstance(obj, dict) and "columns" in obj and "rows" in obj:
        latest_incidents_table.update(obj)
        if not latest_incidents_table.get("version"):
            latest_incidents_table["version"] = 1
        print(f"🗂️ Incidentes: {len(latest_incidents_table.get('rows', []))} filas (backend={STORAGE_BACKEND}).")

def load_attendance_from_disk():
    global attendance_store
    attendance_store = store_read_json(ATTENDANCE_DB, {}) or {}
    if not isinstance(attendance_store, dict):
        attendance_store = {}
    print(f"🗂️ Asistencia cargada ({len(attendance_store)} claves) (backend={STORAGE_BACKEND}).")

def load_roster_from_disk():
    global roster_store
    roster_store = store_read_json(ROSTER_DB, {}) or {}
    if not isinstance(roster_store, dict):
        roster_store = {}
    print(f"🗂️ Roster persistente cargado ({len(roster_store)} fechas).")





















    













# -----------------------------------
# Utilidades
# -----------------------------------
# === Persistencia tabla Incidentes (Enablon) ===







from datetime import date  # si no lo tienes ya

# ===== Asistencia / Presencia =====


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



# ===== ROSTER persistente =====


# Estructura:
# {
#   "YYYY-MM-DD": {
#       "raw": [ {apellidos, nombre, nombre_completo, horario, observaciones}, ... ],
#       "by_shift": {
#           "Mañana": [...],
#           "Tarde":  [...],
#           "Noche":  [...]
#       },
#       "sheet": "01-09-2025",
#       "saved_at": "2025-09-01T07:00:00Z"
#   },
#   ...
# }
roster_store: dict[str, dict] = {}




from pydantic import BaseModel
from typing import Optional, Dict, Any
from datetime import datetime
import tempfile





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




import base64

class GitHubStore:
    # ==== Helpers de escritura de texto/bytes (DISK o GITHUB) ====

    def store_write_text(path: str, text: str, message: str | None = None):
        if USE_GITHUB and gh_store:
            gh_store.write_text(path, text, message=message or f"Update {Path(path).name}")
            return
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkdtemp(), None
        try:
            tmp_path = p.parent / f".tmp_{uuid.uuid4().hex}.md"
            tmp_path.write_text(text, encoding="utf-8")
            os.replace(tmp_path, p)
        finally:
            pass
    
    def store_exists(path: str) -> bool:
        if USE_GITHUB and gh_store:
            try:
                return gh_store.exists(path)
            except Exception:
                return False
        return Path(path).exists()

    
    def __init__(self):
        self.api    = os.getenv("GH_API_URL", "https://api.github.com").rstrip("/")
        owner       = os.getenv("GH_OWNER", "").strip()
        repo_name   = os.getenv("GH_REPO", "").strip()
        repo_full   = os.getenv("GH_REPO_FULL", "").strip()

        if "/" in repo_name:
            self.repo = repo_name
        elif owner and repo_name:
            self.repo = f"{owner}/{repo_name}"
        elif repo_full:
            self.repo = repo_full
        else:
            self.repo = ""  # se detectará más abajo

        self.branch = os.getenv("GH_BRANCH", "main")
        self.dir    = (os.getenv("GH_DIR", "data").strip("/"))
        self.token  = os.getenv("GH_TOKEN", "")
        self.commit_name  = os.getenv("GH_COMMIT_NAME", "CI")
        self.commit_email = os.getenv("GH_COMMIT_EMAIL", "ci@example.com")
        self._sha_cache: dict[str,str] = {}

        print(f"📦 GitHubStore → repo='{self.repo}' branch='{self.branch}' dir='{self.dir or '(root)'}'")

    def _headers(self):
        if not self.token:
            raise RuntimeError("GH_TOKEN no configurado")
        if not self.repo:
            raise RuntimeError("GH_OWNER/GH_REPO no configurados")
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }



    def _gh_path(self, local_path: str) -> str:
        # mapea "./data/tasks.json" → "data/tasks.json" dentro del repo
        p = local_path.replace("\\", "/")
        p = p.lstrip("./")
        # si el path ya trae otra carpeta, lo respetamos; si empieza por data/, ok
        if self.dir and not p.startswith(self.dir + "/"):
            p = f"{self.dir}/{p.split('/')[-1] if p.startswith('data/') else p}"
        return p

    def _url(self, gh_path: str) -> str:
        return f"{self.api}/repos/{self.repo}/contents/{gh_path}"

    def read_json(self, local_path: str):
        gh_path = self._gh_path(local_path)
        url = self._url(gh_path)
        with httpx.Client(timeout=30.0) as c:
            r = c.get(url, headers=self._headers(), params={"ref": self.branch})
            if r.status_code == 404:
                return None
            r.raise_for_status()
            obj = r.json()
            self._sha_cache[gh_path] = obj.get("sha", "")
            content_b64 = obj.get("content", "")
            if content_b64:
                raw = base64.b64decode(content_b64)
                try:
                    return json.loads(raw.decode("utf-8"))
                except Exception:
                    return None
            return None

    def exists(self, local_path: str) -> bool:
        gh_path = self._gh_path(local_path)
        url = self._url(gh_path)
        with httpx.Client(timeout=30.0) as c:
            r = c.get(url, headers=self._headers(), params={"ref": self.branch})
            return r.status_code == 200

    def write_text(self, local_path: str, text: str, message: str | None = None):
        gh_path = self._gh_path(local_path)
        url = self._url(gh_path)
        payload = text.encode("utf-8")
        body = {
            "message": message or f"Update {gh_path}",
            "content": base64.b64encode(payload).decode("ascii"),
            "branch": self.branch,
            "committer": {"name": self.commit_name, "email": self.commit_email},
        }
        sha = self._sha_cache.get(gh_path)
        if sha:
            body["sha"] = sha
        with httpx.Client(timeout=30.0) as c:
            r = c.put(url, headers=self._headers(), json=body)
            r.raise_for_status()
            res = r.json()
            new_sha = (res.get("content") or {}).get("sha")
            if new_sha:
                self._sha_cache[gh_path] = new_sha
    
    def write_json(self, local_path: str, data: Any, message: str | None = None):
        gh_path = self._gh_path(local_path)
        url = self._url(gh_path)
        payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
        body = {
            "message": message or f"Update {gh_path}",
            "content": base64.b64encode(payload).decode("ascii"),
            "branch": self.branch,
            "committer": {"name": self.commit_name, "email": self.commit_email},
        }
        sha = self._sha_cache.get(gh_path)
        if sha:
            body["sha"] = sha  # necesario para actualizar
        with httpx.Client(timeout=30.0) as c:
            r = c.put(url, headers=self._headers(), json=body)
            r.raise_for_status()
            res = r.json()
            new_sha = (res.get("content") or {}).get("sha")
            if new_sha:
                self._sha_cache[gh_path] = new_sha

# instancia global si toca
gh_store = GitHubStore() if USE_GITHUB else None


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



# ---------------------------
# STARTUP: usa ExternalConnector
# ---------------------------



 
# @app.on_event("shutdown")
async def _ws_heartbeat(interval_sec: int = 30):
    """Emite un latido cada `interval_sec` para mantener el WS vivo y dar señal de salud."""
    while True:
        try:
            await manager.broadcast({
                "type": "server_heartbeat",
                "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                
            })
        except Exception as e:
            print("⚠️ Heartbeat WS error:", repr(e))
        await asyncio.sleep(interval_sec)


# ==========================================
# CONECTOR FIIX (MANTENIMIENTO)
# ==========================================
class FiixConnector:
    def __init__(self):
        self.host = os.getenv("FIIX_HOST", "").strip()
        self.access_key = os.getenv("FIIX_ACCESS_KEY", "").strip()
        self.secret_key = os.getenv("FIIX_SECRET_KEY", "").strip()
        self.site_id = os.getenv("FIIX_SITE_ID", "").strip()
        
        # Cliente HTTP
        self.client = httpx.AsyncClient(timeout=60.0, follow_redirects=True)

        print(f"🔍 FIIX CONFIG: Host={self.host}")

    def _generate_signature(self, body_bytes: bytes) -> str:
        """Genera la firma HMAC-SHA256 sobre los BYTES exactos del cuerpo."""
        if not self.secret_key: return ""
        key = self.secret_key.encode('utf-8')
        # body_bytes ya son bytes, no volvemos a codificar
        signature = hmac.new(key, body_bytes, hashlib.sha256).digest()
        return base64.b64encode(signature).decode('utf-8')

    async def fetch_metrics(self):
        if not self.host or not self.access_key or not self.secret_key:
            print("⚠️ FIIX: Faltan credenciales.")
            return

        url = f"https://{self.host}/api/"
        
        # 1. Preparar parámetros de fecha y Site ID
        today = datetime.now()
        first_day = datetime(today.year, today.month, 1)
        ts_start = int(first_day.timestamp() * 1000)
        
        # Convertir Site ID a entero si existe (importante para el array de parámetros)
        site_id_int = None
        if self.site_id and self.site_id.strip().isdigit():
            site_id_int = int(self.site_id)

        # 2. Construir Filtro 1: Backlog (Abiertas)
        # Equivalente a: intCompleted = 0 AND intSiteID = ?
        ql_open = "intCompleted = ?"
        params_open = [0] # 0 = False
        
        if site_id_int:
            ql_open += " AND intSiteID = ?"
            params_open.append(site_id_int)

        # 3. Construir Filtro 2: Costes (Cerradas este mes)
        # Equivalente a: intCompleted = 1 AND dtmDateCompleted >= ? AND intSiteID = ?
        ql_cost = "intCompleted = ? AND dtmDateCompleted >= ?"
        params_cost = [1, ts_start] # 1 = True
        
        if site_id_int:
            ql_cost += " AND intSiteID = ?"
            params_cost.append(site_id_int)

        # --- PAYLOAD CORREGIDO ---
        # "filters" ahora es una lista de objetos, no un string.
        payload = {
            "msg_id": str(uuid.uuid4()),
            "requests": [
                {
                    "action": "find",
                    "className": "WorkOrder",
                    "filters": [
                        {
                            "ql": ql_open,
                            "parameters": params_open
                        }
                    ],
                    "fields": "id, intPriorityID, dtmSuggestedCompletionDate",
                    "max": 1000
                },
                {
                    "action": "find",
                    "className": "WorkOrder",
                    "filters": [
                        {
                            "ql": ql_cost,
                            "parameters": params_cost
                        }
                    ],
                    "fields": "id, dblTotalCost",
                    "max": 1000
                }
            ]
        }

        try:
            # Mantener la codificación binaria estricta que hicimos antes
            body_str = json.dumps(payload, separators=(',', ':'))
            body_bytes = body_str.encode('utf-8')

            secret = self.secret_key.encode('utf-8')
            signature = base64.b64encode(hmac.new(secret, body_bytes, hashlib.sha256).digest()).decode('utf-8')

            headers = {
                "Content-Type": "application/json; charset=utf-8",
                "Access-Key": self.access_key,
                "Signature": signature,
                "Content-Length": str(len(body_bytes))
            }

            resp = await self.client.post(url, content=body_bytes, headers=headers)
            
            if resp.status_code != 200:
                print(f"❌ FIIX HTTP {resp.status_code}: {resp.text}")
                return

            data = resp.json()

            if "error" in data:
                print(f"❌ FIIX API ERROR: {data}")
                return
                
            # Procesamiento de respuesta
            resp_open = data["responses"][0]
            resp_cost = data["responses"][1]

            if resp_open.get("error"):
                print(f"❌ Error en query Open: {resp_open['error']}")
                return

            open_wos = resp_open.get("value", [])
            closed_wos = resp_cost.get("value", [])

            count_backlog = len(open_wos)
            count_urgent = sum(1 for w in open_wos if w.get("intPriorityID") == 0) # 0 suele ser High/Critical
            total_cost = sum(float(w.get("dblTotalCost") or 0) for w in closed_wos)

            print(f"✅ FIIX OK: Backlog={count_backlog}, Urgentes={count_urgent}, Coste={total_cost:.2f}")

            # Enviar al frontend
            ts = datetime.utcnow().isoformat() + "Z"
            await manager.broadcast({"type":"kpi_update", "metric":"fiix_backlog", "value": count_backlog, "timestamp": ts})
            await manager.broadcast({"type":"kpi_update", "metric":"fiix_urgent",  "value": count_urgent,  "timestamp": ts})
            await manager.broadcast({"type":"kpi_update", "metric":"fiix_cost",    "value": total_cost,    "timestamp": ts})

        except Exception as e:
            print(f"❌ FIIX EXCEPCIÓN: {e}")
# Al final del archivo, REEMPLAZA por:
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Iniciando sistema...")
    load_tasks_from_disk()
    load_attendance_from_disk()
    load_incidents_from_disk()
    load_roster_from_disk()
    

    # app.state._roster = asyncio.create_task(_roster_watcher())
   
    app.state._ena = EnablonConnector()
    app.state._ena_task = asyncio.create_task(app.state._ena.run())

    # 👇 NUEVO: heartbeat WS
    app.state._hb = asyncio.create_task(_ws_heartbeat(30))

    # ... (código existente) ...
    
    # Iniciar el conector de Fiix (WFS1)
   # --- FIIX LOOP DE DIAGNÓSTICO ---
    fiix = FiixConnector()
    async def _fiix_loop():
        print("⏳ FIIX: Esperando 5s para primera conexión...")
        await asyncio.sleep(5) # Espera corta inicial
        while True:
            await fiix.fetch_metrics()
            await asyncio.sleep(300) # Luego cada 5 mins
            
    app.state._fiix_task = asyncio.create_task(_fiix_loop())
    # --------------------------------

    # Heartbeat
    async def _hb():
        while True:
            await asyncio.sleep(30)
            try: await manager.broadcast({"type":"server_heartbeat","ts":datetime.utcnow().isoformat()+"Z"})
            except: pass
    app.state._hb = asyncio.create_task(_hb())
    
    yield
    print("🛑 Deteniendo...")
    app.state._hb.cancel()
    app.state._roster_task.cancel()
    if hasattr(app.state, '_fiix_task'): app.state._fiix_task.cancel()
   

    print("🚀 Sistema iniciado correctamente")
    yield
    print("🛑 Deteniendo sistema...")

    for key in ("_hb", "_ena_task"):
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

@app.get("/api/incidents/protocol")
def get_protocol_dummy():
    # Endpoint vacío para evitar error 404 en el frontend
    return {}

@app.post("/api/incidents/protocol")
def post_protocol_dummy(payload: dict):
    # Endpoint dummy para guardar protocolo
    return {"ok": True}

@app.get("/api/backup")
def api_backup():
    """
    Devuelve un snapshot de todo lo 'persistente' en memoria.
    Lo puedes descargar desde el front y guardarlo donde quieras (gratis).
    """
    return {
        "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "tasks": list(tasks_in_memory_store.values()),
        "attendance": attendance_store,
        "incidents_table": latest_incidents_table,
        "last_briefing": _last_briefing_cache,
        # añade aquí lo que quieras incluir
    }

from fastapi import UploadFile, File

@app.post("/api/roster/upload")
async def upload_roster(file: UploadFile = File(...)):
    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Solo se admite .xlsx/.xls")

    target = Path(ROSTER_XLSX_PATH)
    target.parent.mkdir(parents=True, exist_ok=True)

    # Backup del fichero anterior (si existía)
    if target.exists():
        backup = target.with_name(
            target.stem + f".{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}" + target.suffix + ".bak"
        )
        try:
            target.replace(backup)
        except Exception:
            pass

    raw = await file.read()
    with target.open("wb") as fh:
        fh.write(raw)

    # === Parsear todo el libro y persistir por fecha ===
    try:
        xl = pd.ExcelFile(target)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"No pude abrir el Excel: {e}")

    new_dates, total_rows = 0, 0
    for sheet_name in xl.sheet_names:
        dte = _parse_sheet_date(sheet_name)   # ya la tienes definida
        if not dte:
            continue

        # Unimos lo encontrado por cada turno para obtener el RAW completo de la hoja
        seen = set()
        raw_rows: list[dict] = []
        for shift_tag in ("Mañana", "Tarde", "Noche"):
            ppl = _read_sheet_people(ROSTER_XLSX_PATH, sheet_name, shift_tag)
            for r in ppl:
                key = (r.get("nombre_completo","").strip(), r.get("horario","").strip(), r.get("observaciones","").strip())
                if key not in seen:
                    seen.add(key)
                    raw_rows.append(r)

        # Reparto por turnos con tu _match_shift
        by_shift = {"Mañana": [], "Tarde": [], "Noche": []}
        for r in raw_rows:
            for sh in ("Mañana","Tarde","Noche"):
                if _match_shift(r.get("horario",""), sh):
                    by_shift[sh].append(r)

        iso = dte.isoformat()
        roster_store[iso] = {
            "raw": raw_rows,
            "by_shift": {
                "Mañana": by_shift["Mañana"],
                "Tarde":  by_shift["Tarde"],
                "Noche":  by_shift["Noche"],
            },
            "sheet": sheet_name,
            "saved_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        }
        new_dates += 1
        total_rows += len(raw_rows)

    save_roster_to_disk()                 # ← persistimos TODO
    roster_cache["file_mtime"] = None
    await _build_roster_state(force=True) # refresca cache

    return {
        "ok": True,
        "path": str(target),
        "dates_parsed": new_dates,
        "rows_total": total_rows,
        "hint": "Indexado y persistido por fecha; ya se sirve desde el almacén.",
    }

from fastapi import UploadFile, File

@app.post("/api/incidents/scan-pdf")
async def incidents_scan_pdf(file: UploadFile = File(...), station: str = "Madrid Cargo WFS4"):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Sube un PDF")
    raw = await file.read()
    try:
        result = extract_incidents_from_pdf(raw, target_station=station)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"parse_failed: {e}")
        
from fastapi import Query

@app.get("/api/roster/by")
def get_roster_by(
    date_iso: str = Query(..., description="Fecha ISO YYYY-MM-DD"),
    shift: str = Query(..., pattern="^(Mañana|Tarde|Noche)$")
):
    # 1) Persistencia primero
    rec = roster_store.get(date_iso)
    if rec:
        lst = rec.get("by_shift", {}).get(shift, [])
        return {
            "sheet": rec.get("sheet"),
            "sheet_date": date_iso,
            "shift": shift,
            "people": lst,
            "count": len(lst),
            "source": "store",
        }

    # 2) Fallback: Excel directo (compatibilidad)
    try:
        d = datetime.fromisoformat(date_iso).date()
    except Exception:
        raise HTTPException(status_code=400, detail="date_iso inválida, usa YYYY-MM-DD")

    sheet_real, _ = _find_sheet_for_date(ROSTER_XLSX_PATH, d)
    if not sheet_real:
        return {"sheet": None, "people": [], "sheet_date": d.isoformat(), "shift": shift, "source": "fallback"}

    people = _read_sheet_people(ROSTER_XLSX_PATH, sheet_real, shift)
    return {
        "sheet": sheet_real,
        "sheet_date": d.isoformat(),
        "shift": shift,
        "people": people,
        "count": len(people),
        "source": "excel",
    }


@app.get("/api/roster/needs-update")
def roster_needs_update():
    names = _list_sheet_names(ROSTER_XLSX_PATH)
    if not names:
        return {"has_file": False, "message": "No se encontró el Excel", "days_cover_remaining": None}

    # Parsear fechas de nombres de hoja
    ds = [ _parse_sheet_date(n) for n in names ]
    ds = [ d for d in ds if d is not None ]
    if not ds:
        return {"has_file": True, "message": "No hay hojas con nombre de fecha DD-MM-AAAA", "days_cover_remaining": None}

    last = max(ds)
    today = date.today()
    days_left = (last - today).days

    return {
        "has_file": True,
        "last_sheet": last.isoformat(),
        "days_cover_remaining": days_left,
        "needs_new_upload": (days_left < 14),
        "hint": "Sube un Excel con al menos 14 días por delante si 'needs_new_upload' es true."
    }

@app.post("/api/restore")
async def api_restore(payload: Dict[str, Any], x_api_key: Optional[str] = Header(None)):
    """
    Restaura desde un snapshot exportado por /api/backup.
    Protegido opcionalmente por tu API_KEY si quieres.
    """
    # (opcional) seguridad
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")

    # Tareas
    tasks = payload.get("tasks") or []
    if isinstance(tasks, list):
        tasks_in_memory_store.clear()
        for t in tasks:
            t = sanitize_task(t)
            if t.get("id"):
                tasks_in_memory_store[t["id"]] = t
        # broadcast opcional
        for t in tasks_in_memory_store.values():
            await manager.broadcast(t)

    # Asistencia
    global attendance_store
    att = payload.get("attendance")
    if isinstance(att, dict):
        attendance_store = att

    # Tablas
    

    inc = payload.get("incidents_table")
    if isinstance(inc, dict):
        latest_incidents_table.update(inc)
        latest_incidents_table["version"] = int(latest_incidents_table.get("version") or 0) + 1
        await manager.broadcast({"type":"table_ping","table":"incidents","version":latest_incidents_table["version"],
                                 "rows": len(latest_incidents_table.get("rows", [])),
                                 "fetched_at": latest_incidents_table.get("fetched_at")})

    # Briefing
    global _last_briefing_cache
    lb = payload.get("last_briefing")
    if isinstance(lb, dict):
        _last_briefing_cache = lb

    return {"status": "ok", "restored": True}

# Endpoint opcional de estado para debug/monitorización


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

from fastapi import Body


@app.post("/api/summary/generate")
async def api_summary_generate(
    scope: Dict[str, bool] = Body(default=None, description="{'incidents':bool,'roster':bool,'tasks':bool,'briefing':bool}")
):
    scope = scope or {}
    scope.setdefault("briefing", True)  # fuerza checklist ON por defecto
    payload = _build_operational_summary(scope)

    try:
        _append_summary(payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"summary_save_failed: {e}")

    try:
        _write_summary_markdown_file(payload)
    except Exception as e:
        print("⚠️ No se pudo escribir copia .md:", e)

    try:
        await manager.broadcast({
            "type": "summary_generated",
            "id": payload["id"],
            "generated_at": payload["generated_at"],
            "sections": list((payload.get("sections") or {}).keys()),
        })
    except Exception:
        pass

    return {"ok": True, **payload}

@app.get("/api/summary/latest")
def api_summary_latest():
    arr = _list_summaries()
    return arr[-1] if arr else {}

@app.get("/api/summary/{summary_id}")
def api_summary_get(summary_id: str):
    it = _get_summary(summary_id)
    if not it:
        raise HTTPException(status_code=404, detail="Resumen no encontrado")
    return it

@app.get("/api/summaries")
def api_summaries_list(limit: int = Query(20, ge=1, le=200)):
    arr = _list_summaries()
    return list(reversed(arr))[:limit]

# FastAPI (ejemplo)
from fastapi import APIRouter
from typing import List, Dict, Any
from datetime import datetime

router = APIRouter()
def _write_summary_markdown_file(item: dict):
    """
    Escribe un .md legible junto al JSON. Si usas GitHub, sube al repo.
    Nombre: summaries/YYYYMMDD_HHMM_<id>.md
    """
    name = f"summaries/{datetime.utcnow().strftime('%Y%m%d_%H%M')}_{item.get('id','')[:8]}.md"
    content = item.get("markdown", "")
    # Reutiliza store_write_json con un truco simple: el contenido MD lo empaquetamos en JSON {"markdown": "..."}
    # o, si prefieres, añade una variante store_write_text.
    if USE_GITHUB and gh_store:
        gh_store.write_json(name, {"markdown": content}, message=f"Add {name}")
    elif USE_DISK:
        p = Path("./data") / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")

def to_table_payload(data: List[Dict[str, Any]] | List[List[Any]]):
    # Si ya viene como arrays + columns, respeta.
    if isinstance(data, dict) and 'columns' in data and 'rows' in data:
        return data

    # Si viene lista de objetos: unifica columnas en el orden estable
    if data and isinstance(data[0], dict):
        # columnas = unión ordenada por primera aparición
        seen, columns = set(), []
        for row in data:
            for k in row.keys():
                if k not in seen:
                    seen.add(k); columns.append(k)
        rows = [[row.get(c, None) for c in columns] for row in data]
        return {"columns": columns, "rows": rows, "fetched_at": datetime.utcnow().isoformat()}

    # Si ya es lista de listas pero sin columns, crea columnas genéricas
    if data and isinstance(data[0], (list, tuple)):
        maxlen = max(len(r) for r in data)
        columns = [f"Col{idx+1}" for idx in range(maxlen)]
        rows = [list(r) + [None]*(maxlen-len(r)) for r in data]
        return {"columns": columns, "rows": rows, "fetched_at": datetime.utcnow().isoformat()}

    # Vacío
    return {"columns": [], "rows": [], "fetched_at": datetime.utcnow().isoformat()}

from fastapi import UploadFile, File

def _best_table_from_pdf(raw_pdf: bytes) -> dict:
    import io, pdfplumber
    best = None
    with pdfplumber.open(io.BytesIO(raw_pdf)) as pdf:
        for page in pdf.pages:
            for table in (
                page.extract_table(table_settings={"vertical_strategy": "lines",
                                                   "horizontal_strategy": "lines"}),
                page.extract_table(table_settings={"vertical_strategy": "text",
                                                   "horizontal_strategy": "text"}),
            ):
                if not table:
                    continue
                rows = [[(c or "").strip() for c in r] for r in table if r]
                if not rows:
                    continue
                header = rows[0]
                data_rows = rows[1:] if len(rows) > 1 else []
                if not any(header):
                    n = max(len(r) for r in rows)
                    header = [f"Col{i+1}" for i in range(n)]
                    fixed = []
                    for r in rows:
                        rr = list(r) + [""] * (n - len(r))
                        fixed.append(rr)
                    data_rows = fixed
                n = len(header)
                fixed_rows = [ (r + [""] * n)[:n] for r in data_rows ]
                score = len(fixed_rows) * max(1, n)
                if best is None or score > best[0]:
                    best = (score, header, fixed_rows)
    if not best:
        return {"columns": [], "rows": []}
    _, columns, rows = best
    return {"columns": columns, "rows": rows}

from fastapi import UploadFile, File, HTTPException

from fastapi import Query

from fastapi import Query, UploadFile, File, HTTPException

@app.post("/api/incidents-table")
async def upload_incidents_table(
    file: UploadFile = File(...),
    station: str = Query("*", description="Nombre exacto o * para no filtrar")
):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Sube un archivo .pdf")

    raw = await file.read()
    print(f"📥 PDF recibido: {file.filename} ({len(raw)} bytes)  station={station!r}")

    try:
        # Pasa el filtro de estación (o * para no filtrar)
        found = extract_incidents_from_pdf(raw, target_station=station or "*")

        # 🟣 Cambiamos a Título del accidente
        cols = ["Título del accidente", "Fecha del accidente", "Página"]
        rows = [[
            m.get("titulo_accidente",""),
            m.get("fecha_accidente",""),
            m.get("source_page","")
        ] for m in (found.get("matches") or [])]

        ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        latest_incidents_table["columns"] = cols
        latest_incidents_table["rows"] = rows
        latest_incidents_table["fetched_at"] = ts
        latest_incidents_table["version"] = int(latest_incidents_table.get("version", 0)) + 1
        save_incidents_to_disk()

        await manager.broadcast({
            "type": "table_ping",
            "table": "incidents",
            "version": latest_incidents_table["version"],
            "rows": len(rows),
            "fetched_at": ts,
        })

        return {
            "ok": True,
            "columns": cols,
            "rows": rows,
            "version": latest_incidents_table["version"],
            "fetched_at": ts,
            "debug_found": found,  # opcional
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"parse_failed: {e}")



from fastapi import Query

@app.get("/api/incidents-table")
def get_incidents_table(limit: int = Query(None, ge=1, le=1000)):
    """
    Devuelve la tabla de incidentes, pero solo las primeras `limit` filas.
    Por defecto usa INCIDENTS_VISIBLE_LIMIT (3). Se mantiene `total_rows`
    con el total real para depurar/mostrar un 'ver más' en el front.
    """
    lim = limit if limit is not None else INCIDENTS_VISIBLE_LIMIT

    table = latest_incidents_table or {}
    rows_all = table.get("rows", []) or []
    limited_rows = rows_all[:lim]

    # no tocamos el objeto original en memoria; devolvemos una copia limitada
    return {
        "columns": table.get("columns", []),
        "rows": limited_rows,
        "fetched_at": table.get("fetched_at"),
        "version": table.get("version", 0),
        "total_rows": len(rows_all),
        "limit": lim,
        "limited": True,
    }

@app.get("/api/incidents")
def get_incidents_alias():
    """
    Redirige /api/incidents a la tabla real.
    Arregla el error 404 en el frontend/logs.
    """
    return get_incidents_table(limit=50)

import pdfplumber, io
from PIL import Image
import pytesseract

import io, pdfplumber
from PIL import Image  # Pillow sí puede estar importado a nivel de módulo

def _ocr_available() -> tuple[bool, str]:
    """
    Devuelve (disponible, detalle). Comprueba módulo pytesseract y binario tesseract.
    """
    try:
        import pytesseract  # noqa
        try:
            # Comprueba binario (no lanza si existe)
            _ = pytesseract.get_tesseract_version()
            return True, "pytesseract + binario tesseract OK"
        except Exception as e:
            return False, f"pytesseract está pero falta binario tesseract: {e}"
    except Exception as e:
        return False, f"pytesseract no instalado: {e}"

def extract_text_robust(pdf_bytes: bytes, ocr_langs: str | None = None) -> str:
    """
    1) Lee texto embebido del PDF (pdfplumber).
    2) Si es escaso, intenta OCR por página si hay pytesseract + tesseract instalados.
       Si no hay OCR disponible, devuelve lo que haya sin romper el servidor.
    """
    ocr_langs = ocr_langs or os.getenv("OCR_LANGS", "spa+eng")

    # 1) Texto embebido
    text = ""
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text += (page.extract_text() or "") + "\n"

    if len(text.strip()) >= 50:
        return text

    # 2) OCR (solo si está disponible)
    ok, reason = _ocr_available()
    if not ok:
        print(f"ℹ️ OCR desactivado: {reason}. Devuelvo texto embebido (posiblemente vacío).")
        return text  # puede ser vacío; arriba ya lo intentamos

    try:
        import pytesseract  # import local, ya verificado
        ocr_text = []
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                # rasteriza a 200 dpi (suficiente para OCR)
                pil = page.to_image(resolution=200).original
                ocr_text.append(pytesseract.image_to_string(pil, lang=ocr_langs))
        joined = "\n".join(ocr_text)
        # si OCR tampoco saca gran cosa, al menos devolvemos lo embebido
        return joined if len(joined.strip()) >= len(text.strip()) else text
    except Exception as e:
        print(f"⚠️ OCR falló: {e}. Devuelvo texto embebido.")
        return text
PATTERNS = {
    "id": re.compile(r'\b(incident\s*id|case\s*(?:no|num)|n[ºo]\s*expediente)\b[:\-]?\s*([A-Z0-9\-\/]+)', re.I),
    "status": re.compile(r'\b(status|estado)\b[:\-]?\s*([A-Z][a-zA-Z ]+)', re.I),
    "title": re.compile(r'\b(title|asunto|resumen|descripcion)\b[:\-]?\s*(.+)', re.I),
    # añade los que necesites
}

def parse_incidents_from_text(txt: str) -> list[dict]:
    items = []
    # ejemplo básico: parte por saltos grandes/delimitadores
    chunks = re.split(r'(?:\n\s*\n|^-{3,}$)', txt, flags=re.M)
    for ch in chunks:
        d = {}
        for k, rgx in PATTERNS.items():
            m = rgx.search(ch)
            if m:
                d[k] = m.group(m.lastindex or 2).strip()
        if d:
            items.append(d)
    return items


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
        _append_briefing(data)
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
            global sp_last_update_ts
            sp_last_update_ts = time.time()


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


@app.get("/api/sharepoint/state")
def api_sp_state():
    age = None
    if sp_last_update_ts:
        age = int(time.time() - sp_last_update_ts)
    return {
        "in_memory": len(tasks_in_memory_store),
        "last_update_age_sec": age,
        "ttl_sec": SP_TTL_SECONDS,
        "is_fresh": (age is not None and age <= SP_TTL_SECONDS)
    }

@app.delete("/api/sharepoint/clear", status_code=204)
def api_sp_clear(x_api_key: Optional[str] = Header(None)):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")
    tasks_in_memory_store.clear()
    save_tasks_to_disk()  # no-op en memory
    global sp_last_update_ts
    sp_last_update_ts = None
    return {}


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
        if not self.active_connections:
            print("📡 No hay conexiones WebSocket activas para broadcast")
            return
        success_count = 0
        failed_connections = []
        for connection in list(self.active_connections):
            try:
                await connection.send_json(data)
                success_count += 1
            except Exception as e:
                print(f"❌ Error enviando a WebSocket: {str(e)}")
                failed_connections.append(connection)
                self._broadcast_stats["failed"] += 1
                self._broadcast_stats["last_error"] = str(e)
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

# ✅ Instancia global ÚNICA (fuera de la clase)
manager = ConnectionManager()


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
async def list_tasks(
    task_type: Optional[str] = None,
    station: Optional[str] = None,
    fresh_only: bool = Query(False, description="Oculta datos si superan el TTL")
):
    # Si fresh_only y no hay actualización fresca, responde vacío (o lo que prefieras)
    if fresh_only:
        if not sp_last_update_ts:
            return []
        if (time.time() - sp_last_update_ts) > SP_TTL_SECONDS:
            return []

    items = list(tasks_in_memory_store.values())
    if task_type:
        items = [t for t in items if t.get("task_type") == task_type]
    if station:
        items = [t for t in items if (t.get("station") or "").upper() == station.upper()]
    items.sort(key=lambda t: t.get("created_at",""), reverse=True)
    return items



@app.post("/api/tasks", response_model=Task, status_code=201)
async def create_task(task: Task):
    task_dict = task.dict()
    task_dict.pop("action", None)

    # Normaliza due_date si llega en formato ES (dd/mm/yyyy, d-m-yy, etc.)
    if task_dict.get("due_date"):
        norm = _parse_date_es(task_dict["due_date"]) or task_dict["due_date"]
        task_dict["due_date"] = norm

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

@app.patch("/api/tasks/{task_id}", response_model=Task)
async def patch_task(task_id: str, patch: TaskPatch):
    if task_id not in tasks_in_memory_store:
        raise HTTPException(status_code=404, detail="Tarea no encontrada")

    existing = tasks_in_memory_store[task_id]
    incoming = {k: v for k, v in patch.dict().items() if v is not None}

    # Normaliza due_date si viene
    if "due_date" in incoming and incoming["due_date"]:
        incoming["due_date"] = _parse_date_es(incoming["due_date"]) or incoming["due_date"]

    # Aplica merge preservando campos del servidor (ticks y nota si no vienen en el PATCH)
    merged = merge_preserve_server(existing, incoming)

    # Saneamos por si acaso
    merged = sanitize_task(merged)
    tasks_in_memory_store[task_id] = merged
    save_tasks_to_disk()

    # WS para sincronizar front
    await manager.broadcast(merged)
    return Task(**merged)



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


# -----------------------------------
# WebSocket (EXISTENTE, SIN CAMBIOS)
# -----------------------------------
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        
        await push_incidents_table_state(websocket)

        # Bucle pasivo: no exige tráfico del cliente
        while True:
            try:
                # Espera “algo” del cliente con timeout suave; si no llega, seguimos vivos
                msg = await asyncio.wait_for(websocket.receive_text(), timeout=120.0)
                # si quieres, puedes ignorar o inspeccionar `msg`
            except asyncio.TimeoutError:
                # silencio del cliente → seguimos; el heartbeat server-side mantiene el canal
                continue
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

    # Si hay persistencia para esa fecha/turno, sirve desde ahí
    d_iso = state.get("sheet_date").isoformat() if state.get("sheet_date") else None
    sh = state.get("shift")
    if d_iso and sh and d_iso in roster_store:
        rec = roster_store[d_iso]
        people = rec.get("by_shift", {}).get(sh, [])
        return {
            "shift": sh,
            "sheet": rec.get("sheet"),
            "sheet_date": d_iso,
            "window": state.get("window"),
            "people": people,
            "updated_at": rec.get("saved_at") or state.get("updated_at"),
            "attendance": state.get("attendance", {}),
            "source": "store"
        }

    # Fallback al Excel/cálculo en caliente
    return {
        "shift": state.get("shift"),
        "sheet": state.get("sheet"),
        "sheet_date": state.get("sheet_date").isoformat() if state.get("sheet_date") else None,
        "window": state.get("window"),
        "people": state.get("people", []),
        "updated_at": state.get("updated_at"),
        "attendance": state.get("attendance", {}),
        "source": "excel"
    }
@app.get("/api/roster/persisted")
def api_roster_persisted():
    return {
        "dates": sorted(roster_store.keys()),
        "count_dates": len(roster_store),
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



from pathlib import Path

# Ruta absoluta al directorio del frontend de Madrid
FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"

if not FRONTEND_DIR.exists():
    # Log visible en Render si algo va mal con la ruta
    print(f"⚠️ FRONTEND_DIR no existe: {FRONTEND_DIR}")
# ==========================================
# NUEVO: Generación y Guardado de Resumen
# ==========================================

# ==========================================
# NUEVO: Generación y Guardado de Resumen (MEJORADO)
# ==========================================

# ==========================================
# FINAL: Generación de Resumen y Subida a GITHUB
# ==========================================

# === MODELO ACTUALIZADO ===
class BriefingSnapshot(BaseModel):
    station: Optional[str] = "MAD"
    date: str
    shift: str
    timer: str
    supervisor: str = "No especificado"  # <--- NUEVO CAMPO
    briefing_time: Optional[str] = None 
    checklist: Dict[str, str] = {}
    kpis: Dict[str, Any] = {}
    roster_details: str = ""
    prev_shift_note: str = ""
    safety_incidents: List[Dict[str, Any]] = [] 
    
    # Campos opcionales para evitar errores
    present_names: List[str] = []
    ops_updates: List[Dict[str, Any]] = []
    kanban_counts: Dict[str, int] = {}
    kanban_details: str = "" 
    roster_stats: str = ""

    class Config:
        extra = "allow"
@app.post("/api/briefing/summary")
async def save_briefing_summary(data: BriefingSnapshot):
    def clean_str(text: str) -> str:
        s = unicodedata.normalize('NFD', text)
        return ''.join(c for c in s if unicodedata.category(c) != 'Mn')

    # 1. Generar Markdown visual
    lines = []
    lines.append(f"# 📝 Resumen - {data.station}")
    lines.append(f"**Fecha:** {data.date} | **Turno:** {data.shift}")
    lines.append(f"**⏱️ Cronómetro:** {data.timer}")
    lines.append(f"**👮 Supervisor:** {data.supervisor}")
    
    lines.append(f"\n**👥 Equipo:**\n{data.roster_details}")

    lines.append("\n### ↩️ Turno Anterior")
    lines.append(f"{data.prev_shift_note or 'Sin novedades.'}")

    # --- NUEVA SECCIÓN DE SEGURIDAD EN EL MARKDOWN ---
    if data.safety_incidents:
        lines.append("\n### 🛡️ Seguridad y Protección (Manual)")
        for inc in data.safety_incidents:
            lines.append(f"- **{inc.get('title')}**: {inc.get('desc')}")
    # -------------------------------------------------

    lines.append("\n### 📊 KPIs")
    for k, v in data.kpis.items():
        lines.append(f"- **{k}:** {v}")
        
    if data.ops_updates:
        lines.append("\n### 🚧 Actualizaciones Ops")
        for op in data.ops_updates:
            lines.append(f"- [{op.get('impact')}] {op.get('title')}")

    final_markdown = "\n".join(lines)

    # 2. Configurar nombre de archivo ÚNICO (Timestamp)
    safe_date = data.date.replace("/", "-")
    safe_shift = clean_str(data.shift)
    
    # ¡ESTA ES LA CLAVE DEL ARREGLO! Añadimos la hora exacta
    timestamp = datetime.now().strftime("%H-%M-%S") 
    
    filename = f"{safe_date}_{safe_shift}_{timestamp}_Briefing.md"
    
    # Ruta relativa ("summaries/archivo.md")
    store_path = f"summaries/{filename}"

    log_msg = ""
    
    # 3. Guardar en GitHub
    try:
        if USE_GITHUB and gh_store:
            print(f"☁️ Subiendo a GitHub: {store_path}...")
            
            # Escribir Markdown
            gh_store.write_text(store_path, final_markdown, message=f"Briefing {data.date} {data.shift}")
            
            # Escribir JSON (opcional, pero útil)
            json_path = store_path.replace(".md", ".json")
            gh_store.write_json(json_path, data.dict(), message="Data")

            log_msg = f"✅ Guardado en GitHub: {store_path}"
        else:
            # Fallback local
            p = Path("./data") / store_path
            p.parent.mkdir(parents=True, exist_ok=True)
            with open(p, "w", encoding="utf-8") as f:
                f.write(final_markdown)
            log_msg = f"⚠️ Guardado LOCAL: {store_path}"

        # 4. Enviar a Excel (Lanzar y olvidar)
        asyncio.create_task(send_to_excel_online(data))

    except Exception as e:
        print(f"❌ Error guardando: {e}")
        log_msg = f"Error: {str(e)}"

    return {"summary": final_markdown, "saved": True, "log": log_msg}


app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="static")


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)


















