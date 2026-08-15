import os
from fastapi import FastAPI, HTTPException
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI(title="Serviço de Pagamento")

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

@app.get("/health")
def health():
    return {"status": "ok", "servico": "pagamento"}

@app.get("/metrics")
def metrics():
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Conta o total de eventos processados no banco de dados do pagamento
            cur.execute("SELECT COUNT(*) FROM eventos_processados_pagamento;")
            total_processados = cur.fetchone()[0]

        conn.close()
        return {
            "status": "ok",
            "servico": "pagamento",
            "total_eventos_processados": total_processados
        }
    except Exception as e:
        print(f"[pagamento] Erro no /metrics: {e}")
        return {
            "status": "ok",
            "servico": "pagamento",
            "total_eventos_processados": 0
        }