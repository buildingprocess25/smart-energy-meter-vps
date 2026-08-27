# ── Stage: runtime ──────────────────────────────────────────────────────────
FROM python:3.11-slim

# Jangan buffer stdout/stderr (agar log Gunicorn langsung tampil)
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Copy requirements dulu (memanfaatkan layer cache Docker)
COPY backend/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy seluruh project
# Flask serve frontend dari ../frontend relatif terhadap backend/app.py
COPY backend/ ./backend/
COPY frontend/ ./frontend/

# Port yang di-expose (Flask/Gunicorn)
EXPOSE 5000

# Jalankan dengan 1 worker agar _mqtt_live_data tidak terpecah antar proses
CMD ["gunicorn", \
     "--workers", "1", \
     "--threads", "12", \
     "--timeout", "120", \
     "--worker-class", "gthread", \
     "--bind", "0.0.0.0:5000", \
     "--chdir", "backend", \
     "app:app"]
