import json
import time
import random
import threading
import os
import pika
import psycopg2

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_DB = os.getenv("POSTGRES_DB", "shopflow")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

def conectar_postgres():
    while True:
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS eventos_processados_logistica (
                        evento_id VARCHAR(255) PRIMARY KEY,
                        processado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                conn.commit()
            return conn
        except Exception as e:
            print(f"[logistica] Aguardando PostgreSQL... ({e})")
            time.sleep(3)

def publicar_evento(routing_key, evento):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        channel.basic_publish(
            exchange='amq.topic',
            routing_key=routing_key,
            body=json.dumps(evento)
        )
        connection.close()
        print(f"[logistica] Evento publicado: {routing_key} | correlation_id={evento.get('correlation_id')}")
    except Exception as e:
        print(f"[logistica] Erro ao publicar evento {routing_key}: {e}")

def simular_entrega(correlation_id, pedido_id):
    # Simula o tempo de envio em transporte (5 a 15 segundos)
    tempo_entrega = random.randint(5, 15)
    print(f"[logistica] Pedido {pedido_id} em transporte. Previsão de entrega em {tempo_entrega}s...")
    time.sleep(tempo_entrega)

    evento_entregue = {
        "evento_id": f"evt-log-ent-{int(time.time()*1000)}",
        "evento_tipo": "pedido.entregue",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "correlation_id": correlation_id,
        "versao_schema": "1.0",
        "payload": {
            "pedido_id": pedido_id,
            "status": "entregue"
        }
    }
    publicar_evento("pedido.entregue", evento_entregue)

def processar_mensagem(ch, method, properties, body):
    conn = conectar_postgres()
    try:
        data = json.loads(body)
        evento_id = data.get("evento_id")
        correlation_id = data.get("correlation_id")
        payload = data.get("payload", {})
        pedido_id = payload.get("pedido_id")

        # Checa idempotência
        with conn.cursor() as cur:
            cur.execute("SELECT evento_id FROM eventos_processados_logistica WHERE evento_id = %s", (evento_id,))
            if cur.fetchone():
                ch.basic_ack(delivery_tag=method.delivery_tag)
                conn.close()
                return

        print(f"[logistica] Processando expedição do pedido={pedido_id}")

        # Grava na tabela de idempotência
        with conn.cursor() as cur:
            cur.execute("INSERT INTO eventos_processados_logistica (evento_id) VALUES (%s)", (evento_id,))
            conn.commit()

        # 1. Publica pedido.despachado imediatamente
        evento_despachado = {
            "evento_id": f"evt-log-desp-{int(time.time()*1000)}",
            "evento_tipo": "pedido.despachado",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "correlation_id": correlation_id,
            "versao_schema": "1.0",
            "payload": {
                "pedido_id": pedido_id,
                "status": "despachado"
            }
        }
        
        # Publica o evento de despachado no broker
        ch.basic_publish(
            exchange='amq.topic',
            routing_key='pedido.despachado',
            body=json.dumps(evento_despachado)
        )
        print(f"[logistica] Evento publicado: pedido.despachado | correlation_id={correlation_id}")

        # 2. Inicia uma thread em background para simular a entrega após alguns segundos
        threading.Thread(target=simular_entrega, args=(correlation_id, pedido_id), daemon=True).start()

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"[logistica] Erro ao processar mensagem: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    finally:
        conn.close()

def iniciar_consumer():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()

            channel.queue_declare(queue='logistica.pedidos.queue', durable=True)
            channel.queue_bind(exchange='amq.topic', queue='logistica.pedidos.queue', routing_key='pedido.confirmado')

            channel.basic_consume(queue='logistica.pedidos.queue', on_message_callback=processar_mensagem)
            print("[logistica] Consumer rodando e aguardando eventos pedido.confirmado...")
            channel.start_consuming()
        except Exception as e:
            print(f"[logistica] Erro de conexão com RabbitMQ. Tentando novamente em 5s... ({e})")
            time.sleep(5)