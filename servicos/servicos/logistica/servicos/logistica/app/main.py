import os
import threading
from fastapi import FastAPI
import psycopg2

from consumer import iniciar_consumer

app = FastAPI(title="Serviço de Logística")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_DB = os.getenv("POSTGRES_DB", "shopflow")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

def get_db_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

@app.on_event("startup")
def startup_event():
    t = threading.Thread(target=iniciar_consumer, daemon=True)
    t.start()
    print("[logistica] Thread do consumer iniciada.")

@app.get("/health")
def health():
    return {"status": "ok", "servico": "logistica"}

@app.get("/metrics")
def metrics():
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM eventos_processados_logistica;")
            total_processados = cur.fetchone()[0]
        conn.close()
        return {
            "status": "ok",
            "servico": "logistica",
            "total_eventos_processados": total_processados
        }
    except Exception as e:
        print(f"[logistica] Erro no /metrics: {e}")
        return {
            "status": "ok",
            "servico": "logistica",
            "total_eventos_processados": 0
        }