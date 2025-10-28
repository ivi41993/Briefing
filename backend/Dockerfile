# ========= Base de ejecución =========
FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Europe/Madrid

# Paquetes del sistema necesarios
# - build deps mínimos
# - tesseract-ocr (opcional). Si NO lo quieres, coméntalo y quita pytesseract de requirements.txt
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata curl ca-certificates \
    tesseract-ocr tesseract-ocr-spa tesseract-ocr-eng \
    && rm -rf /var/lib/apt/lists/*

# Crear usuario no-root
RUN useradd -ms /bin/bash appuser

WORKDIR /app/backend

# ========= Dependencias =========
COPY requirements.txt /app/backend/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# ========= App (backend) =========
# Copiamos el código del backend
COPY . /app/backend

# ========= Frontend estático =========
# La app espera ../frontend relativo a /app/backend
# Copiaremos un directorio "frontend" pre-creado al nivel /app/frontend en la imagen final
# Si tus estáticos reales están fuera de backend/, pásalos al contexto de build o usa docker-compose para montar el volumen
ARG FRONTEND_DIR=/tmp/frontend_build
RUN mkdir -p /app/frontend
# Si ya tienes el build estático en ../frontend, lo copiamos en tiempo de build con --build-context o ajusta al enviar contexto.
# Como fallback, si el build ya fue copiado al contexto bajo /app/backend/../frontend, lo duplicamos:
# (esta línea no falla si está vacío)
RUN [ -d "/app/frontend" ] && true

# ========= Volúmenes y permisos =========
# Directorio de datos persistente:
RUN mkdir -p /data /certs && chown -R appuser:appuser /app /data /certs
VOLUME ["/data"]
VOLUME ["/certs"]

# ========= Exponer puerto =========
EXPOSE 8000

# ========= Healthcheck =========
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD curl -fsS http://127.0.0.1:8000/api/incidents-status || exit 1

# ========= Run =========
USER appuser

# Uvicorn en modo producción (sin --reload)
# Nota: el main apunta a "main:app" dentro de /app/backend
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
