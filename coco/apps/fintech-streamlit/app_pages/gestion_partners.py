from app_pages.page_template import render_page

config = {
    "key": "gpt",
    "table": "MGG_FINTECH.PARTNERS_BAAS.GESTION_PARTNERS",
    "icon": ":material/handshake:",
    "title": "Gestion Partners",
    "subtitle": "Gestion integral de partners BaaS con SLA, revenue, churn y crecimiento.",
    "cards": ["Monitoreo integral de todos los partners del ecosistema fintech con KPIs en tiempo real", "Registra volumen transaccional, revenue compartido, nivel de riesgo, SLA y satisfaccion", "Gestiona el ecosistema de aliados, detecta partners de alto riesgo y optimiza relacion comercial"],
    "date_col": "FECHA_CORTE",
    "filter_cols": ["TIPO_PARTNER", "NIVEL_PARTNER", "ESTADO"],
    "kpi_query": """SELECT ROUND(AVG(REVENUE_MENSUAL_COP)/1e6,1), ROUND(AVG(TAKE_RATE_PCT),1), ROUND(AVG(SLA_CUMPLIMIENTO_REAL)*100,1), ROUND(AVG(NPS_PARTNER),1), ROUND(AVG(PROB_CHURN_PARTNER)*100,1), ROUND(AVG(INCIDENTES_MES),1), ROUND(AVG(CRECIMIENTO_MOM_PCT),1), COUNT(*) FROM {table} WHERE {where}""",
    "kpi_labels": ["Revenue (M)", "Take rate %", "SLA real %", "NPS", "Churn prob %", "Incidentes", "Crecimiento MoM %", "Partners"],
    "kpi_formats": ["${:.1f}M", "{:.1f}%", "{:.1f}%", "{:.1f}", "{:.1f}%", "{:.1f}", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_CORTE) AS MES, ROUND(AVG(REVENUE_MENSUAL_COP)/1e6,1) AS REVENUE, ROUND(AVG(CRECIMIENTO_MOM_PCT),1) AS CRECIMIENTO_MOM_PCT FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["REVENUE", "CRECIMIENTO_MOM_PCT"],
    "treemap_query": """SELECT TIPO_PARTNER AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Tipo Partner"},
    "geo_query": """SELECT CIUDAD_HQ AS CIUDAD, AVG(REVENUE_MENSUAL_COP) AS VALOR FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Revenue por Ciudad HQ", "color_col": "VALOR"},
    "diagnostics": None,
    "simulator": {
        "title": "Simulador de Churn de Partner",
        "desc": "Estima la probabilidad de perder un partner del ecosistema segun sus indicadores de relacion.",
        "features": [
            {"name": "Volumen Mensual (K txns)", "min": 0, "max": 100, "default": 20, "weight": -0.25, "step": 5},
            {"name": "Revenue Compartido %", "min": 0, "max": 50, "default": 15, "weight": -0.2, "step": 1},
            {"name": "SLA Cumplimiento %", "min": 80, "max": 100, "default": 95, "weight": -0.25, "step": 1},
            {"name": "Satisfaccion (1-10)", "min": 1, "max": 10, "default": 7, "weight": -0.2, "step": 1},
            {"name": "Meses sin Renegociar", "min": 0, "max": 24, "default": 6, "weight": 0.1, "step": 1},
        ],
        "thresholds": [0.33, 0.66],
        "labels": ["Retencion Alta", "Riesgo Moderado", "Riesgo de Fuga"],
    },
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
st.subheader(":material/bar_chart: Analisis Avanzado de Partners")

df_scatter = run_query(f"SELECT REVENUE_MENSUAL_COP/1e6 AS REVENUE_M, CRECIMIENTO_MOM_PCT, TIPO_PARTNER, USUARIOS_ACTIVOS FROM {TABLE} SAMPLE (2000 ROWS)")
fig_sc = px.scatter(df_scatter, x="REVENUE_M", y="CRECIMIENTO_MOM_PCT", color="TIPO_PARTNER", size="USUARIOS_ACTIVOS", color_discrete_sequence=SEMAFORO, opacity=0.7)
fig_sc.update_layout(**CHART_LAYOUT, title="Revenue (M) vs Crecimiento MoM por Tipo Partner")
st.plotly_chart(fig_sc, use_container_width=True)

df_heatmap = run_query(f"SELECT TIPO_PARTNER, NIVEL_PARTNER, ROUND(AVG(NPS_PARTNER),1) AS NPS FROM {TABLE} GROUP BY 1,2")
pivot = df_heatmap.pivot_table(index="TIPO_PARTNER", columns="NIVEL_PARTNER", values="NPS", aggfunc="mean").fillna(0)
fig_hm = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0, SEMAFORO[3]], [1, SEMAFORO[0]]], texttemplate="%{z:.1f}"))
fig_hm.update_layout(**CHART_LAYOUT, title="NPS por Tipo Partner × Nivel Partner", height=420)
st.plotly_chart(fig_hm, use_container_width=True)

col1, col2 = st.columns(2)
df_donut = run_query(f"SELECT ESTADO, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
fig_dn = px.pie(df_donut, names="ESTADO", values="N", hole=0.5, color_discrete_sequence=SEMAFORO)
fig_dn.update_layout(**CHART_LAYOUT, title="Distribucion por Estado")
col1.plotly_chart(fig_dn, use_container_width=True)

df_bar = run_query(f"SELECT NOMBRE_PARTNER, SUM(REVENUE_MENSUAL_COP)/1e6 AS REVENUE_M FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC LIMIT 8")
fig_bar = px.bar(df_bar, x="NOMBRE_PARTNER", y="REVENUE_M", color_discrete_sequence=[SEMAFORO[3]])
fig_bar.update_layout(**CHART_LAYOUT, title="Revenue (M) por Partner Top 8")
col2.plotly_chart(fig_bar, use_container_width=True)

df_box = run_query(f"SELECT TIPO_PARTNER, TAKE_RATE_PCT FROM {TABLE}")
fig_bx = px.box(df_box, x="TIPO_PARTNER", y="TAKE_RATE_PCT", color_discrete_sequence=[SEMAFORO[2]])
fig_bx.update_layout(**CHART_LAYOUT, title="Take Rate (%) por Tipo Partner")
st.plotly_chart(fig_bx, use_container_width=True)
