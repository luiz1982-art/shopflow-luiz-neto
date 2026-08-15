import os
import time
import json
import threading
import pika
from fastapi import FastAPI

app = FastAPI(title="Mock Antifraude")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")

# --- Lógica do Consumer RabbitMQ ---
def publicar_evento(ch, routing_key, evento):
    ch.basic_publish(
        exchange='amq.topic',
        routing_key=routing_key,
        body=json.dumps(evento)
    )
    print(f"[antifraude] Evento publicado: {routing_key} | correlation_id={evento.get('correlation_id')}")

def processar_mensagem(ch, method, properties, body):
    try:
        data = json.loads(body)
        correlation_id = data.get("correlation_id")
        payload = data.get("payload", {})
        pedido_id = payload.get("pedido_id")

        print(f"[antifraude] Analisando risco do pedido={pedido_id}...")

        # Simula aprovação de fraude
        evento_tipo = "pedido.aprovado_fraude"

        evento_resposta = {
            "evento_id": f"evt-ant-{int(time.time()*1000)}",
            "evento_tipo": evento_tipo,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "correlation_id": correlation_id,
            "versao_schema": "1.0",
            "payload": {
                "pedido_id": pedido_id,
                "score_risco": 0.05,
                "aprovado": True
            }
        }

        publicar_evento(ch, evento_tipo, evento_resposta)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"[antifraude] Erro ao processar mensagem: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def iniciar_consumer():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()

            channel.queue_declare(queue='antifraude.pedidos.queue', durable=True)
            channel.queue_bind(exchange='amq.topic', queue='antifraude.pedidos.queue', routing_key='pedido.criado')

            channel.basic_consume(queue='antifraude.pedidos.queue', on_message_callback=processar_mensagem)
            print("[antifraude] Consumer rodando e aguardando eventos pedido.criado...")
            channel.start_consuming()
        except Exception as e:
            print(f"[antifraude] Erro de conexão com RabbitMQ. Tentando novamente em 5s... ({e})")
            time.sleep(5)

# --- Endpoints da API FastAPI ---
@app.on_event("startup")
def startup_event():
    t = threading.Thread(target=iniciar_consumer, daemon=True)
    t.start()
    print("[antifraude] Thread do consumer iniciada.")

@app.get("/health")
def health():
    return {"status": "ok", "servico": "antifraude"}