import os
import json
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Boolean, Text, DateTime, Date
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.mysql import JSON  # Usaremos columnas JSON para flexibilidad

# 1. Configuración de Conexión Dinámica
# Cada contenedor Docker tendrá un DB_NAME distinto en su .env interno
DB_USER = os.getenv("DB_USER", "app_user")
DB_PASS = os.getenv("DB_PASS", "password_app_segura")
DB_HOST = os.getenv("DB_HOST", "db")
DB_NAME = os.getenv("DB_NAME", "db_main") # Default por si acaso

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"

engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=3600)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# 2. Definición de Tablas (Modelos)

class TaskDB(Base):
    __tablename__ = "tasks"
    id = Column(String(50), primary_key=True, index=True)
    title = Column(Text, nullable=True)
    status = Column(String(50), nullable=True)
    task_type = Column(String(50), nullable=True)
    is_completed = Column(Boolean, default=False)
    created_at = Column(String(50), nullable=True)
    # Guardamos el resto de campos (due_date, assigned_to, note) en un JSON
    # para no tener que migrar la tabla cada vez que cambies el frontend.
    extra_data = Column(JSON, nullable=True) 

class IncidentDB(Base):
    __tablename__ = "incidents"
    id = Column(Integer, primary_key=True, index=True)
    # Guardamos la estructura completa de la tabla de incidentes aquí
    # ya que tu código guarda una "tabla completa" (columnas + filas)
    data = Column(JSON, nullable=False)
    version = Column(Integer, default=1)
    updated_at = Column(DateTime, default=datetime.utcnow)

class AttendanceDB(Base):
    __tablename__ = "attendance"
    id = Column(Integer, primary_key=True, index=True)
    # Clave compuesta: "2023-10-01|Mañana"
    shift_key = Column(String(100), unique=True, index=True) 
    # Guardamos el dict de asistencia {"Pepe": true, "Juan": false}
    data = Column(JSON, nullable=False)

class BriefingDB(Base):
    __tablename__ = "briefings"
    id = Column(String(50), primary_key=True)
    date = Column(String(20), index=True) # YYYY-MM-DD
    shift = Column(String(20))
    # Aquí guardamos todo el objeto BriefingSnapshot
    full_snapshot = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

# 3. Función para inicializar tablas
def init_db():
    try:
        Base.metadata.create_all(bind=engine)
        print(f"✅ Tablas SQL inicializadas en {DB_NAME}")
    except Exception as e:
        print(f"❌ Error conectando a DB {DB_NAME}: {e}")

# 4. Dependencia para FastAPI
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
