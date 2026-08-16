import argparse
import time
import random
import requests

def gerar_pedidos(total: int, taxa: float, url: str):
    print(f"Iniciando gerador de carga: {total} pedidos com taxa de {taxa} req/s...")
    
    produtos = ["PROD-001", "PROD-002", "PROD-003", "PROD-004", "PROD-005"]
    formas_pagamento = ["cartao_credito", "pix", "boleto"]

    for i in range(1, total + 1):
        payload = {
            "cliente_id": f"cli-{random.randint(100, 999)}",
            "itens": [
                {
                    "produto_id": random.choice(produtos),
                    "quantidade": random.randint(1, 3),
                    "preco_unitario": round(random.uniform(20.0, 500.0), 2)
                }
            ],
            "valor_total": round(random.uniform(50.0, 1800.0), 2),
            "forma_pagamento": random.choice(formas_pagamento)
        }

        try:
            # Tenta primeiramente a rota raiz '/'
            endpoint = f"{url.rstrip('/')}/"
            response = requests.post(endpoint, json=payload)
            
            # Se a rota raiz retornar 404, tenta a rota alternativa '/pedidos'
            if response.status_code == 404:
                endpoint = f"{url.rstrip('/')}/pedidos"
                response = requests.post(endpoint, json=payload)

            if response.status_code in [200, 201]:
                data = response.json()
                pedido_id = data.get('pedido_id') or data.get('id') or 'N/A'
                correlation_id = data.get('correlation_id') or 'N/A'
                print(f"[{i}/{total}] Pedido Criado: ID={pedido_id} | Correlation={correlation_id}")
            else:
                print(f"[{i}/{total}] Falha ao criar pedido: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"[{i}/{total}] Erro de conexao com API de Pedidos: {e}")

        time.sleep(1.0 / taxa)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gerador de Carga de Pedidos")
    parser.add_argument("--total", type=int, default=10, help="Total de pedidos a criar")
    parser.add_argument("--taxa", type=float, default=1.0, help="Pedidos por segundo")
    parser.add_argument("--url", type=str, default="http://localhost:8000", help="URL do servico de pedido")

    args = parser.parse_args()
    gerar_pedidos(args.total, args.taxa, args.url)