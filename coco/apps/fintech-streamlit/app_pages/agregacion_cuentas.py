from app_pages.page_template import render_page

config = {
    "key": "agc",
    "table": "MGG_FINTECH.OPEN_BANKING.AGREGACION_CUENTAS",
    "icon": ":material/link:",
    "title": "Agregacion Cuentas",
    "subtitle": "Conexion y sincronizacion de cuentas bancarias externas via Open Banking para vision 360.",
    "cards": ["Conecta cuentas bancarias externas del usuario para ver todo su dinero en un solo lugar", "Sincroniza saldos y movimientos de multiples bancos en tiempo real con Open Banking", "Vista 360 de finanzas del usuario que habilita insights cross-banco y mejor decision crediticia"],
    "date_col": "FECHA_PRIMERA_CONEXION",
    "filter_cols": ["BANCO_EXTERNO", "TIPO_CUENTA", "ESTADO_CONEXION"],
    "kpi_query": """SELECT ROUND(AVG(TASA_EXITO_SYNC)*100,1), ROUND(AVG(TOTAL_CUENTAS_CONECTADAS),1), ROUND(AVG(BANCOS_CONECTADOS),1), ROUND(AVG(PATRIMONIO_AGREGADO_COP)/1e6,2), ROUND(AVG(ERRORES_SYNC_30D),1), ROUND(AVG(ENGAGEMENT_AGREGACION)*100,1), ROUND(SUM(CASE WHEN CONSENTIMIENTO_ACTIVO THEN 1 ELSE 0 END)*100.0/COUNT(*),1), COUNT(*) FROM {table} WHERE {where}""",
    "kpi_labels": ["Exito Sync %", "Cuentas Conect", "Bancos Conect", "Patrimonio M", "Errores 30D", "Engagement %", "Consentimiento %", "Total"],
    "kpi_formats": ["{:.1f}%", "{:.1f}", "{:.1f}", "${:.2f}M", "{:.1f}", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_PRIMERA_CONEXION) AS MES, ROUND(AVG(TASA_EXITO_SYNC)*100,1) AS EXITO_SYNC, ROUND(AVG(ENGAGEMENT_AGREGACION)*100,1) AS ENGAGEMENT FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["EXITO_SYNC", "ENGAGEMENT"],
    "treemap_query": """SELECT BANCO_EXTERNO AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Banco Externo"},
    "geo_query": """SELECT CIUDAD, AVG(TASA_EXITO_SYNC) AS VALOR FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Tasa Exito Sync por Ciudad", "color_col": "VALOR"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Agregacion")

df_heat = run_query(f"SELECT BANCO_EXTERNO, TIPO_CUENTA, ROUND(AVG(TASA_EXITO_SYNC)*100,1) AS EXITO FROM {TABLE} GROUP BY 1,2")
pivot = df_heat.pivot(index="BANCO_EXTERNO", columns="TIPO_CUENTA", values="EXITO").fillna(0)
fig_heat = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0,"#E63946"],[0.25,"#F5A623"],[0.5,"#F5D63D"],[0.75,"#6ABF4B"],[1,"#2D9B2D"]], texttemplate="%{z:.1f}%"))
fig_heat.update_layout(**CHART_LAYOUT, title="Tasa Exito Sync: Banco × Tipo Cuenta")
st.plotly_chart(fig_heat, use_container_width=True)

col1, col2 = st.columns(2)
with col1:
    df_estado = run_query(f"SELECT ESTADO_CONEXION, COUNT(*) AS N FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC")
    color_map = {"Activa": "#2D9B2D", "Suspendida": "#F5D63D", "Requiere reautenticacion": "#F5A623", "Desconectada": "#E63946", "Error sincronizacion": "#D32F2F"}
    fig_estado = px.bar(df_estado, x="ESTADO_CONEXION", y="N", color="ESTADO_CONEXION", color_discrete_map=color_map)
    fig_estado.update_layout(**CHART_LAYOUT, title="Estado de Conexiones", showlegend=False)
    st.plotly_chart(fig_estado, use_container_width=True)
with col2:
    df_donut = run_query(f"SELECT TIPO_CUENTA, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
    fig_donut = go.Figure(go.Pie(labels=df_donut["TIPO_CUENTA"], values=df_donut["N"], hole=0.5, marker=dict(colors=SEMAFORO)))
    fig_donut.update_layout(**CHART_LAYOUT, title="Distribucion por Tipo de Cuenta")
    st.plotly_chart(fig_donut, use_container_width=True)

col3, col4 = st.columns(2)
with col3:
    df_bar = run_query(f"SELECT BANCO_EXTERNO, ROUND(AVG(BANCOS_CONECTADOS),2) AS AVG_BANCOS FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC LIMIT 8")
    fig_bar = px.bar(df_bar, x="BANCO_EXTERNO", y="AVG_BANCOS", color_discrete_sequence=SEMAFORO)
    fig_bar.update_layout(**CHART_LAYOUT, title="Bancos Conectados Promedio (Top 8)")
    st.plotly_chart(fig_bar, use_container_width=True)
with col4:
    df_box = run_query(f"SELECT BANCO_EXTERNO, ERRORES_SYNC_30D FROM {TABLE} LIMIT 3000")
    fig_box = px.box(df_box, x="BANCO_EXTERNO", y="ERRORES_SYNC_30D", color_discrete_sequence=SEMAFORO)
    fig_box.update_layout(**CHART_LAYOUT, title="Errores Sync 30D por Banco")
    st.plotly_chart(fig_box, use_container_width=True)
