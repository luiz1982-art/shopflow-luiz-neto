import json
import random
import time
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
            # Criar tabela de idempotência se não existir
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS eventos_processados_pagamento (
                        evento_id VARCHAR(255) PRIMARY KEY,
                        processado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                conn.commit()
            return conn
        except Exception as e:
            print(f"[pagamento] Aguardando PostgreSQL... ({e})")
            time.sleep(3)

def publicar_evento(ch, routing_key, evento):
    ch.basic_publish(
        exchange='amq.topic',
        routing_key=routing_key,
        body=json.dumps(evento)
    )
    print(f"[pagamento] Evento publicado: {routing_key} | correlation_id={evento.get('correlation_id')}")

def processar_mensagem(ch, method, properties, body):
    conn = conectar_postgres()
    try:
        data = json.loads(body)
        evento_id = data.get("evento_id")
        correlation_id = data.get("correlation_id")
        payload = data.get("payload", {})
        pedido_id = payload.get("pedido_id")
        valor_total = payload.get("valor_total", 0.0)

        # Checa idempotência no Postgres
        with conn.cursor() as cur:
            cur.execute("SELECT evento_id FROM eventos_processados_pagamento WHERE evento_id = %s", (evento_id,))
            if cur.fetchone():
                ch.basic_ack(delivery_tag=method.delivery_tag)
                conn.close()
                return

        print(f"[pagamento] Processando pagamento para pedido={pedido_id} | R$ {valor_total}")

        # Lógica de Negócio:
        # 1. Recusa automática se valor > R$ 1500
        # 2. Nos demais casos: 80% aprova, 20% recusa
        if valor_total > 1500.00:
            aprovado = False
            motivo = "Valor acima do limite permitido de R$ 1.500,00"
        else:
            aprovado = random.random() < 0.8
            motivo = "Aprovado com sucesso" if aprovado else "Recusado pelo banco emissor"

        evento_tipo = "pagamento.aprovado" if aprovado else "pagamento.recusado"

        evento_resposta = {
            "evento_id": f"evt-pag-{int(time.time()*1000)}",
            "evento_tipo": evento_tipo,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "correlation_id": correlation_id,
            "versao_schema": "1.0",
            "payload": {
                "pedido_id": pedido_id,
                "aprovado": aprovado,
                "motivo": motivo
            }
        }

        # Grava na tabela de idempotência
        with conn.cursor() as cur:
            cur.execute("INSERT INTO eventos_processados_pagamento (evento_id) VALUES (%s)", (evento_id,))
            conn.commit()

        # Publica a resposta
        publicar_evento(ch, evento_tipo, evento_resposta)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"[pagamento] Erro ao processar mensagem: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    finally:
        conn.close()

def iniciar_consumer():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()

            channel.queue_declare(queue='pagamento.pedidos.queue', durable=True)
            channel.queue_bind(exchange='amq.topic', queue='pagamento.pedidos.queue', routing_key='pedido.criado')

            channel.basic_consume(queue='pagamento.pedidos.queue', on_message_callback=processar_mensagem)
            print("[pagamento] Consumer rodando e aguardando eventos pedido.criado...")
            channel.start_consuming()
        except Exception as e:
            print(f"[pagamento] Erro de conexão com RabbitMQ. Tentando novamente em 5s... ({e})")
            time.sleep(5)