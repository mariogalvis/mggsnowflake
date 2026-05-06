from app_pages.page_template import render_page

config = {
    "key": "wal",
    "table": "MGG_FINTECH.PAGOS_Y_ECOSISTEMA.WALLETS_DIGITALES",
    "icon": ":material/account_balance_wallet:",
    "title": "Wallets Digitales",
    "subtitle": "Monitoreo de wallets digitales con transaccionalidad, limites y deteccion de fraude.",
    "cards": ["Gestion de billeteras digitales: saldo, recargas, retiros y transferencias P2P", "Registra movimientos, fondeo, limites regulatorios y estado de cada billetera en tiempo real", "Experiencia de manejo de dinero sin cuenta bancaria tradicional, inclusion financiera directa"],
    "date_col": "FECHA_HORA",
    "filter_cols": ["TIPO_MOVIMIENTO", "FUENTE_FONDEO", "CIUDAD"],
    "kpi_query": """SELECT ROUND(AVG(MONTO_COP)/1e3,1), ROUND(AVG(SALDO_POSTERIOR_COP)/1e3,1), ROUND(SUM(CASE WHEN SOSPECHA_FRAUDE THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(AVG(COMISION_COP),0), ROUND(AVG(TRANSACCIONES_MES_USUARIO),1), ROUND(AVG(VOLUMEN_MES_USUARIO_COP)/1e3,1), ROUND(AVG(USO_LIMITE_DIARIO_PCT)*100,1), COUNT(*) FROM {table} WHERE {where}""",
    "kpi_labels": ["Monto K", "Saldo K", "Fraude %", "Comision", "Txns/Mes", "Vol Mes K", "Uso Limite %", "Total"],
    "kpi_formats": ["${:.1f}K", "${:.1f}K", "{:.1f}%", "${:.0f}", "{:.1f}", "${:.1f}K", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_HORA) AS MES, ROUND(AVG(MONTO_COP)/1e3,1) AS MONTO_K, ROUND(AVG(USO_LIMITE_DIARIO_PCT)*100,1) AS USO_LIMITE FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["MONTO_K", "USO_LIMITE"],
    "treemap_query": """SELECT TIPO_MOVIMIENTO AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Tipo Movimiento"},
    "geo_query": """SELECT CIUDAD, AVG(VOLUMEN_MES_USUARIO_COP) AS VALOR FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Volumen por Ciudad", "color_col": "VALOR"},
    "diagnostics": None,
    "simulator": None,
}

render_page(config)

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from app_pages.conn_helper import run_query

TABLE = config["table"]
CHART_LAYOUT = dict(paper_bgcolor="#FAFBFC", plot_bgcolor="#FAFBFC", font=dict(family="Inter, sans-serif", color="#334155"))
SEMAFORO = ["#2D9B2D", "#6ABF4B", "#F5D63D", "#F5A623", "#E63946", "#1B8C1B", "#8BC34A", "#D32F2F"]

st.divider()
st.subheader(":material/bar_chart: Analisis Avanzado de Wallets")

df_heat = run_query(f"SELECT TIPO_MOVIMIENTO, FUENTE_FONDEO, ROUND(AVG(MONTO_COP)/1e3,1) AS MONTO_K FROM {TABLE} GROUP BY 1,2")
pivot = df_heat.pivot(index="TIPO_MOVIMIENTO", columns="FUENTE_FONDEO", values="MONTO_K").fillna(0)
fig_heat = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0,"#E63946"],[0.25,"#F5A623"],[0.5,"#F5D63D"],[0.75,"#6ABF4B"],[1,"#2D9B2D"]], texttemplate="%{z:.1f}K"))
fig_heat.update_layout(**CHART_LAYOUT, title="Monto Promedio (K): Tipo Movimiento × Fuente Fondeo")
st.plotly_chart(fig_heat, use_container_width=True)

col1, col2 = st.columns(2)
with col1:
    df_sc = run_query(f"SELECT TRANSACCIONES_MES_USUARIO AS TRANSACCIONES_MES, VOLUMEN_MES_USUARIO_COP/1e3 AS VOLUMEN_K, TIPO_WALLET FROM {TABLE} LIMIT 2000")
    fig_sc = px.scatter(df_sc, x="TRANSACCIONES_MES", y="VOLUMEN_K", color="TIPO_WALLET", color_discrete_sequence=SEMAFORO)
    fig_sc.update_layout(**CHART_LAYOUT, title="Transacciones vs Volumen por Tipo Wallet")
    st.plotly_chart(fig_sc, use_container_width=True)
with col2:
    df_donut = run_query(f"SELECT PLATAFORMA, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
    fig_donut = go.Figure(go.Pie(labels=df_donut["PLATAFORMA"], values=df_donut["N"], hole=0.5, marker=dict(colors=SEMAFORO)))
    fig_donut.update_layout(**CHART_LAYOUT, title="Distribucion por Plataforma")
    st.plotly_chart(fig_donut, use_container_width=True)

col3, col4 = st.columns(2)
with col3:
    df_fun = run_query(f"SELECT COUNT(*) AS TOTAL, SUM(CASE WHEN ESTADO='Exitoso' THEN 1 ELSE 0 END) AS EXITOSO, SUM(CASE WHEN ESTADO='Exitoso' AND NOT SOSPECHA_FRAUDE THEN 1 ELSE 0 END) AS SIN_FRAUDE FROM {TABLE}")
    fig_fun = go.Figure(go.Funnel(y=["Total","Exitoso","Sin Sospecha Fraude"], x=[df_fun["TOTAL"].iloc[0], df_fun["EXITOSO"].iloc[0], df_fun["SIN_FRAUDE"].iloc[0]], marker=dict(color=SEMAFORO[:3])))
    fig_fun.update_layout(**CHART_LAYOUT, title="Funnel de Transacciones")
    st.plotly_chart(fig_fun, use_container_width=True)
with col4:
    df_box = run_query(f"SELECT TIPO_MOVIMIENTO, MONTO_COP/1e3 AS MONTO_K FROM {TABLE} LIMIT 3000")
    fig_box = px.box(df_box, x="TIPO_MOVIMIENTO", y="MONTO_K", color_discrete_sequence=SEMAFORO)
    fig_box.update_layout(**CHART_LAYOUT, title="Monto (K) por Tipo Movimiento")
    st.plotly_chart(fig_box, use_container_width=True)
