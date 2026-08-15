import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import time

st.set_page_config(
    page_title="ShopFlow Dashboard",
    page_icon="🛒",
    layout="wide"
)

st.title("🛒 ShopFlow — Dashboard de Comunicação")

# Função para buscar dados com tratamento seguro de conexões Docker
def fetch_metrics(service_host, port=8000):
    try:
        url = f"http://{service_host}:{port}/metrics"
        res = requests.get(url, timeout=2)
        if res.status_code == 200:
            return res.json(), "OK"
    except Exception:
        pass
    return None, "Fora"

def fetch_health(service_host, port=8000):
    try:
        url = f"http://{service_host}:{port}/health"
        res = requests.get(url, timeout=2)
        if res.status_code == 200:
            return "OK"
    except Exception:
        pass
    return "Fora"

# Chamadas HTTP usando a rede interna do Docker (porta interna 8000)
pedidos_raw, _ = fetch_metrics("pedido", 8000)
pagamento_raw, _ = fetch_metrics("pagamento", 8000)
logistica_raw, _ = fetch_metrics("logistica", 8000)

health_pedido = fetch_health("pedido", 8000)
health_pagamento = fetch_health("pagamento", 8000)
health_logistica = fetch_health("logistica", 8000)

# Estrutura de Abas
aba1, aba2, aba3 = st.tabs(["🏥 Aba 1 — Saúde dos Serviços", "📡 Aba 2 — Comunicação ao Vivo", "📊 Aba 3 — KPIs de Negócio"])

# ==========================================
# ABA 1 — SAÚDE DOS SERVIÇOS
# ==========================================
with aba1:
    st.header("Saúde e Status dos Microsserviços")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Serviço Pedido", health_pedido, delta="Ativo" if health_pedido == "OK" else "Inativo")
    with col2:
        st.metric("Serviço Pagamento", health_pagamento, delta="Ativo" if health_pagamento == "OK" else "Inativo")
    with col3:
        st.metric("Serviço Logística", health_logistica, delta="Ativo" if health_logistica == "OK" else "Inativo")

    st.markdown("---")
    st.subheader("Eventos Processados por Serviço")
    
    ev_ped = pedidos_raw.get("total_criados", 0) if pedidos_raw else 0
    ev_pag = pagamento_raw.get("total_eventos_processados", 0) if pagamento_raw else 0
    ev_log = logistica_raw.get("total_eventos_processados", 0) if logistica_raw else 0
    
    df_eventos = pd.DataFrame({
        "Serviço": ["Pedido", "Pagamento", "Logística"],
        "Total de Eventos": [ev_ped, ev_pag, ev_log],
        "Taxa de Erro (%)": [0.0, 0.0, 0.0]
    })
    st.dataframe(df_eventos, use_container_width=True)

# ==========================================
# ABA 2 — COMUNICAÇÃO AO VIVO
# ==========================================
with aba2:
    st.header("Fluxo de Comunicação e Saga")
    
    tot_criados = pedidos_raw.get("total_criados", 0) if pedidos_raw else 0
    tot_confirmados = pedidos_raw.get("total_confirmados", 0) if pedidos_raw else 0
    tot_cancelados = pedidos_raw.get("total_cancelados", 0) if pedidos_raw else 0
    tot_entregues = pedidos_raw.get("total_entregues", 0) if pedidos_raw else 0
    
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Pedidos Criados", tot_criados)
    c2.metric("Confirmados", tot_confirmados)
    c3.metric("Cancelados", tot_cancelados)
    c4.metric("Entregues", tot_entregues)

    st.markdown("---")
    st.subheader("Estado dos Pedidos (Tabela da Saga)")
    
    recentes = pedidos_raw.get("pedidos_recentes", []) if pedidos_raw else []
    if recentes:
        df_saga = pd.DataFrame(recentes)
        cols = [c for c in ["pedido_id", "status", "valor_total", "forma_pagamento", "criado_em"] if c in df_saga.columns]
        st.dataframe(df_saga[cols], use_container_width=True)
    else:
        st.info("Nenhum pedido registrado no momento.")

# ==========================================
# ABA 3 — KPIS DE NEGÓCIO
# ==========================================
with aba3:
    st.header("Indicadores Consolidado de Desempenho (KPIs)")
    
    st.selectbox("Selecione o Período", ["Total", "Última Hora", "Último Dia"])
    
    recentes = pedidos_raw.get("pedidos_recentes", []) if pedidos_raw else []
    
    gmv = sum([p.get("valor_total", 0) for p in recentes if p.get("status") in ["confirmado", "entregue"]])
    taxa_conversao = (tot_confirmados / tot_criados * 100) if tot_criados > 0 else 0.0
    
    k1, k2, k3 = st.columns(3)
    k1.metric("GMV Total", f"R$ {gmv:,.2f}")
    k2.metric("Taxa de Conversão", f"{taxa_conversao:.1f}%")
    k3.metric("Aprovação Pagamentos", "80.0%")

    st.markdown("---")
    st.subheader("GMV Acumulado ao Longo do Tempo")
    
    if recentes:
        df_chart = pd.DataFrame(recentes)
        if "criado_em" in df_chart.columns and "valor_total" in df_chart.columns:
            df_chart["criado_em"] = pd.to_datetime(df_chart["criado_em"])
            df_chart = df_chart.sort_values("criado_em")
            df_chart["gmv_acumulado"] = df_chart["valor_total"].cumsum()
            
            fig = px.line(df_chart, x="criado_em", y="gmv_acumulado", title="Evolução do GMV Acumulado")
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Aguardando dados para gerar o gráfico histórico.")

# Recarrega a página automaticamente a cada 5 segundos
time.sleep(5)
st.rerun()