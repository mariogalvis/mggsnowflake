from app_pages.page_template import render_page

config = {
    "key": "sus",
    "table": "MGG_FINTECH.MONETIZACION.SUSCRIPCIONES_FINANCIERAS",
    "icon": ":material/card_membership:",
    "title": "Suscripciones",
    "subtitle": "Monitoreo de planes de suscripcion con uso de beneficios, churn y NPS.",
    "cards": ["Monetizacion por suscripciones premium tipo Nubank Ultravioleta o Rappi Prime", "Registra plan, precio, beneficios activos, uso real, renovacion y churn de cada suscriptor", "Mide valor percibido por el usuario, optimiza pricing y reduce cancelaciones del plan premium"],
    "date_col": "FECHA_INICIO",
    "filter_cols": ["PLAN_ACTUAL", "ESTADO", "CIUDAD"],
    "kpi_query": """SELECT ROUND(AVG(REVENUE_MENSUAL_USUARIO_COP)/1e3,1), ROUND(AVG(ROI_SUSCRIPCION),2), ROUND(AVG(TASA_USO_BENEFICIOS)*100,1), ROUND(AVG(PROB_CHURN_SUSCRIPCION)*100,1), ROUND(AVG(NPS_PLAN),1), ROUND(AVG(MESES_ACTIVO),1), ROUND(AVG(PRECIO_MENSUAL_COP)/1e3,1), COUNT(*) FROM {table} WHERE {where}""",
    "kpi_labels": ["Revenue K", "ROI", "Uso Benef %", "Churn %", "NPS", "Meses Activo", "Precio K", "Total"],
    "kpi_formats": ["${:.1f}K", "{:.2f}", "{:.1f}%", "{:.1f}%", "{:.1f}", "{:.1f}", "${:.1f}K", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_INICIO) AS MES, ROUND(AVG(ROI_SUSCRIPCION),2) AS ROI, ROUND(AVG(PROB_CHURN_SUSCRIPCION)*100,1) AS CHURN FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["ROI", "CHURN"],
    "treemap_query": """SELECT PLAN_ACTUAL AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Plan"},
    "geo_query": """SELECT CIUDAD, AVG(NPS_PLAN) AS VALOR FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "NPS por Ciudad", "color_col": "VALOR"},
    "diagnostics": None,
    "simulator": {
        "title": "Simulador de Churn de Suscripcion",
        "desc": "Estima la probabilidad de que un usuario cancele su plan premium ajustando variables clave.",
        "features": [
            {"name": "Tasa Uso Beneficios %", "min": 0, "max": 100, "default": 60, "weight": -0.35, "step": 5},
            {"name": "Antiguedad (meses)", "min": 1, "max": 36, "default": 6, "weight": -0.2, "step": 1},
            {"name": "Precio Plan (K COP)", "min": 10, "max": 100, "default": 30, "weight": 0.15, "step": 5},
            {"name": "ROI Suscripcion", "min": 0, "max": 5, "default": 1.5, "weight": -0.2, "step": 0.1},
            {"name": "Downgrades Intentados", "min": 0, "max": 5, "default": 0, "weight": 0.1, "step": 1},
        ],
        "thresholds": [0.33, 0.66],
        "labels": ["Churn Bajo", "Churn Medio", "Churn Alto"],
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
st.subheader(":material/bar_chart: Analisis Avanzado de Suscripciones")

df_heat = run_query(f"SELECT PLAN_ACTUAL, ESTADO, ROUND(AVG(ROI_SUSCRIPCION),2) AS ROI FROM {TABLE} GROUP BY 1,2")
pivot = df_heat.pivot(index="PLAN_ACTUAL", columns="ESTADO", values="ROI").fillna(0)
fig_heat = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0,"#E63946"],[0.25,"#F5A623"],[0.5,"#F5D63D"],[0.75,"#6ABF4B"],[1,"#2D9B2D"]], texttemplate="%{z:.2f}"))
fig_heat.update_layout(**CHART_LAYOUT, title="ROI: Plan × Estado")
st.plotly_chart(fig_heat, use_container_width=True)

col1, col2 = st.columns(2)
with col1:
    df_sc = run_query(f"SELECT TASA_USO_BENEFICIOS, PROB_CHURN_SUSCRIPCION AS PROB_CHURN, PLAN_ACTUAL FROM {TABLE} LIMIT 2000")
    fig_sc = px.scatter(df_sc, x="TASA_USO_BENEFICIOS", y="PROB_CHURN", color="PLAN_ACTUAL", color_discrete_sequence=SEMAFORO)
    fig_sc.update_layout(**CHART_LAYOUT, title="Uso Beneficios vs Prob Churn")
    st.plotly_chart(fig_sc, use_container_width=True)
with col2:
    df_fun = run_query(f"SELECT COUNT(*) AS TOTAL, SUM(CASE WHEN ESTADO='Activo' THEN 1 ELSE 0 END) AS ACTIVO, SUM(CASE WHEN ESTADO='Activo' AND TASA_USO_BENEFICIOS>0.7 THEN 1 ELSE 0 END) AS ALTA_USO, SUM(CASE WHEN ESTADO='Activo' AND NPS_PLAN>8 THEN 1 ELSE 0 END) AS NPS_ALTO FROM {TABLE}")
    fig_fun = go.Figure(go.Funnel(y=["Total","Activo","Alta Uso","NPS>8"], x=[df_fun["TOTAL"].iloc[0], df_fun["ACTIVO"].iloc[0], df_fun["ALTA_USO"].iloc[0], df_fun["NPS_ALTO"].iloc[0]], marker=dict(color=SEMAFORO[:4])))
    fig_fun.update_layout(**CHART_LAYOUT, title="Funnel de Suscriptores")
    st.plotly_chart(fig_fun, use_container_width=True)

col3, col4 = st.columns(2)
with col3:
    df_donut = run_query(f"SELECT PLAN_ACTUAL, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
    fig_donut = go.Figure(go.Pie(labels=df_donut["PLAN_ACTUAL"], values=df_donut["N"], hole=0.5, marker=dict(colors=SEMAFORO)))
    fig_donut.update_layout(**CHART_LAYOUT, title="Distribucion por Plan")
    st.plotly_chart(fig_donut, use_container_width=True)
with col4:
    df_box = run_query(f"SELECT PLAN_ACTUAL, REVENUE_MENSUAL_USUARIO_COP/1e3 AS REVENUE_K FROM {TABLE} LIMIT 3000")
    fig_box = px.box(df_box, x="PLAN_ACTUAL", y="REVENUE_K", color_discrete_sequence=SEMAFORO)
    fig_box.update_layout(**CHART_LAYOUT, title="Revenue Mensual (K) por Plan")
    st.plotly_chart(fig_box, use_container_width=True)
