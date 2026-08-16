import os
import json
import uuid
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
import pika

app = FastAPI(title="Serviço de Pedidos")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:rootpassword@localhost:27017/shopflow?authSource=admin")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")

cliente_mongo = MongoClient(MONGO_URI)
db = cliente_mongo["shopflow"]
colecao_pedidos = db["pedidos"]

def publicar_evento(exchange: str, routing_key: str, mensagem: dict):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        channel.exchange_declare(exchange=exchange, exchange_type='topic', durable=True)
        channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=json.dumps(mensagem),
            properties=pika.BasicProperties(content_type='application/json', delivery_mode=2)
        )
        connection.close()
    except Exception as e:
        print(f"[pedido] Erro ao publicar evento no RabbitMQ: {e}")

@app.get("/health")
def health():
    return {"status": "ok", "servico": "pedido"}

@app.post("/pedidos", status_code=201)
@app.post("/", status_code=201)
def criar_pedido(dados: dict):
    try:
        pedido_id = f"PED-{uuid.uuid4().hex[:8].upper()}"
        correlation_id = str(uuid.uuid4())
        agora = datetime.now(timezone.utc).isoformat()

        pedido = {
            "pedido_id": pedido_id,
            "correlation_id": correlation_id,
            "cliente_id": dados.get("cliente_id"),
            "itens": dados.get("itens", []),
            "valor_total": dados.get("valor_total", 0.0),
            "forma_pagamento": dados.get("forma_pagamento"),
            "status": "criado",
            "criado_em": agora
        }

        colecao_pedidos.insert_one(pedido)
        
        # Remove o _id gerado pelo MongoDB antes de enviar
        pedido.pop("_id", None)

        publicar_evento(
            exchange="shopflow_events",
            routing_key="pedido.criado",
            mensagem=pedido
        )

        return pedido
    except Exception as e:
        print(f"[pedido] Erro ao criar pedido: {e}")
        raise HTTPException(status_code=500, detail=str(e))

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