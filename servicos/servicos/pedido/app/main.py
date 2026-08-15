import os
from fastapi import FastAPI, HTTPException
from pymongo import MongoClient

app = FastAPI(title="Serviço de Pedidos")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:rootpassword@localhost:27017/shopflow?authSource=admin")

cliente_mongo = MongoClient(MONGO_URI)
db = cliente_mongo["shopflow"]
colecao_pedidos = db["pedidos"]

@app.get("/health")
def health():
    return {"status": "ok", "servico": "pedido"}

@app.get("/pedidos/{pedido_id}")
def obter_pedido(pedido_id: str):
    pedido = colecao_pedidos.find_one({"pedido_id": pedido_id}, {"_id": 0})
    if not pedido:
        raise HTTPException(status_code=404, detail="Pedido não encontrado")
    return pedido

@app.get("/metrics")
def metrics():
    try:
        total_criados = colecao_pedidos.count_documents({"status": "criado"})
        total_confirmados = colecao_pedidos.count_documents({"status": "confirmado"})
        total_cancelados = colecao_pedidos.count_documents({"status": "cancelado"})
        total_entregues = colecao_pedidos.count_documents({"status": "entregue"})

        pedidos_cursor = colecao_pedidos.find({}, {"_id": 0}).sort("criado_em", -1).limit(50)
        pedidos_recentes = list(pedidos_cursor)

        return {
            "total_criados": total_criados,
            "total_confirmados": total_confirmados,
            "total_cancelados": total_cancelados,
            "total_entregues": total_entregues,
            "pedidos_recentes": pedidos_recentes
        }
    except Exception as e:
        print(f"[pedido] Erro ao gerar métricas: {e}")
        raise HTTPException(status_code=500, detail=str(e))