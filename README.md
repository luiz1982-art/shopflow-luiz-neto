# ShopFlow - Arquitetura Orientada a Eventos

Este repositório contém a implementação do ecossistema de e-commerce **ShopFlow**, focado em comunicação assíncrona baseada em eventos entre microsserviços.

---

## 👥 Identificação do Grupo

* **Integrante:** José Luiz Neto
* **Repositório:** [luiz1982-arte/shopflow-luiz-neto](https://github.com/luiz1982-art/shopflow-luiz-neto)
* **Entrega:** Módulo 5 - Consolidação e Demonstração Final

---

## 🛠️ Tecnologias Utilizadas

* **Linguagem:** Python 3.11+
* **Framework Web:** FastAPI
* **Mensageria / Broker:** RabbitMQ
* **Bancos de Dados:** PostgreSQL e MongoDB
* **Orquestração e Contêineres:** Docker & Docker Compose
* **Visualização:** Streamlit (Dashboard interativo)

---

## 🚀 Como Executar o Projeto

### 1. Pré-requisitos
* Docker Desktop instalado e em execução.
* Python 3.10+ instalado na máquina.

### 2. Subir o Ambiente Docker
Para inicializar os microsserviços, bancos de dados, broker de mensageria e o dashboard, execute no terminal:

```bash
docker-compose up -d --build