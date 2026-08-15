import json
import time
import threading
import pika
from pymongo import MongoClient
import os

# Configurações de conexão via variáveis de ambiente
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")

mongo_client = MongoClient(MONGO_URI)
db = mongo_client["shopflow"]
colecao_pedidos = db["pedidos"]
colecao_eventos = db["eventos_processados"]

def publicar_evento(ch, routing_key, evento):
    ch.basic_publish(
        exchange='amq.topic',
        routing_key=routing_key,
        body=json.dumps(evento)
    )
    print(f"[pedido] Evento publicado: {routing_key} | correlation_id={evento.get('correlation_id')}")

def checar_e_atualizar_saga(ch, correlation_id):
    pedido = colecao_pedidos.find_one({"correlation_id": correlation_id})
    if not pedido:
        return

    # Se já foi finalizado, ignora
    if pedido.get("status") in ["confirmado", "cancelado"]:
        return

    pagamento_ok = pedido.get("pagamento_ok")
    fraude_ok = pedido.get("fraude_ok")

    # Regra da Saga: Se ambos responderam
    if pagamento_ok is not None and fraude_ok is not None:
        if pagamento_ok and fraude_ok:
            novo_status = "confirmado"
            evento_tipo = "pedido.confirmado"
        else:
            novo_status = "cancelado"
            evento_tipo = "pedido.cancelado"

        colecao_pedidos.update_one(
            {"correlation_id": correlation_id},
            {"$set": {"status": novo_status}}
        )

        evento = {
            "evento_id": f"evt-{int(time.time()*1000)}",
            "evento_tipo": evento_tipo,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "correlation_id": correlation_id,
            "versao_schema": "1.0",
            "payload": {
                "pedido_id": pedido["pedido_id"],
                "status": novo_status
            }
        }
        publicar_evento(ch, evento_tipo, evento)

def processar_mensagem(ch, method, properties, body):
    try:
        data = json.loads(body)
        evento_id = data.get("evento_id")
        correlation_id = data.get("correlation_id")
        evento_tipo = data.get("evento_tipo")

        # Idempotência: Checa se evento já foi processado
        if colecao_eventos.find_one({"evento_id": evento_id}):
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        print(f"[pedido] Recebido evento: {evento_tipo} | correlation_id={correlation_id}")

        if evento_tipo == "pagamento.aprovado":
            colecao_pedidos.update_one({"correlation_id": correlation_id}, {"$set": {"pagamento_ok": True}})
        elif evento_tipo == "pagamento.recusado":
            colecao_pedidos.update_one({"correlation_id": correlation_id}, {"$set": {"pagamento_ok": False}})
        elif evento_tipo == "pedido.aprovado_fraude":
            colecao_pedidos.update_one({"correlation_id": correlation_id}, {"$set": {"fraude_ok": True}})
        elif evento_tipo == "pedido.bloqueado_fraude":
            colecao_pedidos.update_one({"correlation_id": correlation_id}, {"$set": {"fraude_ok": False}})

        # Registra evento processado
        colecao_eventos.insert_one({"evento_id": evento_id, "processado_em": time.time()})
        
        # Avalia estado da Saga
        checar_e_atualizar_saga(ch, correlation_id)

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"[pedido] Erro ao processar mensagem: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def iniciar_consumer():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()

            # Fila para escutar Pagamento e Antifraude
            channel.queue_declare(queue='pedido.saga.queue', durable=True)
            channel.queue_bind(exchange='amq.topic', queue='pedido.saga.queue', routing_key='pagamento.*')
            channel.queue_bind(exchange='amq.topic', queue='pedido.saga.queue', routing_key='pedido.*_fraude')

            channel.basic_consume(queue='pedido.saga.queue', on_message_callback=processar_mensagem)
            print("[pedido] Consumer rodando e aguardando eventos...")
            channel.start_consuming()
        except Exception as e:
            print(f"[pedido] Erro de conexão com RabbitMQ. Tentando novamente em 5s... ({e})")
            time.sleep(5)