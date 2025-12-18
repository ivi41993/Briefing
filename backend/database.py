import os
from datetime import datetime
# CAMBIO 1: Importamos JSON de sqlalchemy genérico, no de mysql
from sqlalchemy import create_engine, Column, Integer, String, Boolean, Text, DateTime, JSON
from sqlalchemy.orm import sessionmaker, declarative_base
import uuid

# --- LÓGICA HÍBRIDA (DOCKER vs RENDER) ---
DB_HOST = os.getenv("DB_HOST") # En Render esto estará vacío
DB_USER = os.getenv("DB_USER", "app_user")
DB_PASS = os.getenv("DB_PASS", "password_app")
DB_NAME = os.getenv("DB_NAME", "db_main")

if DB_HOST:
    # CASO 1: ESTAMOS EN DOCKER (Usamos MariaDB)
    print(f"🚀 Modo DOCKER detectado. Conectando a MariaDB: {DB_NAME}")
    DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
    # Configuración para MySQL (Pool de conexiones para rendimiento)
    engine_args = {"pool_pre_ping": True, "pool_recycle": 3600}
else:
    # CASO 2: ESTAMOS EN RENDER (Modo Prueba/Fallback)
    # Usamos SQLite (un archivo local que actúa como base de datos)
    print("⚠️ Modo RENDER/LOCAL detectado. Usando SQLite temporal.")
    # Creamos un archivo db local. En Render se borrará al reiniciar, 
    # pero te permite probar que la APP arranca y funciona.
    DATABASE_URL = "sqlite:///./test_fallback.db"
    # Configuración para SQLite (necesaria para hilos en Python)
    engine_args = {"connect_args": {"check_same_thread": False}}

# Crear motor con los argumentos seleccionados
engine = create_engine(DATABASE_URL, **engine_args)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- MODELOS (Igual que antes) ---
class TaskDB(Base):
    __tablename__ = "tasks"
    id = Column(String(50), primary_key=True, index=True)
    title = Column(Text, nullable=True)
    status = Column(String(50), nullable=True)
    task_type = Column(String(50), nullable=True)
    is_completed = Column(Boolean, default=False)
    created_at = Column(String(50), nullable=True)
    extra_data = Column(JSON, nullable=True)

class IncidentDB(Base):
    __tablename__ = "incidents"
    id = Column(Integer, primary_key=True, index=True)
    data = Column(JSON, nullable=False)
    version = Column(Integer, default=1)
    updated_at = Column(DateTime, default=datetime.utcnow)

class AttendanceDB(Base):
    __tablename__ = "attendance"
    id = Column(Integer, primary_key=True, index=True)
    shift_key = Column(String(100), unique=True, index=True)
    data = Column(JSON, nullable=False)

class BriefingDB(Base):
    __tablename__ = "briefings"
    id = Column(String(50), primary_key=True)
    date = Column(String(20), index=True)
    shift = Column(String(20))
    full_snapshot = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

def init_db():
    try:
        Base.metadata.create_all(bind=engine)
        print(f"✅ Tablas inicializadas correctamente.")
    except Exception as e:
        print(f"❌ Error inicializando DB: {e}")
