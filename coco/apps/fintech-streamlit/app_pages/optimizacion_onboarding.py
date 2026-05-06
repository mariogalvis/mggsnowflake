from app_pages.page_template import render_page

config = {
    "key": "onb",
    "table": "MGG_FINTECH.PRODUCTO_Y_CRECIMIENTO.OPTIMIZACION_ONBOARDING",
    "icon": ":material/person_add:",
    "title": "Optimizacion Onboarding",
    "subtitle": "Analisis de conversion del onboarding digital con tracking por paso y activacion temprana.",
    "cards": ["Analisis del funnel de apertura de cuenta digital paso a paso para detectar abandono", "Identifica donde y por que abandonan los usuarios, y evalua variantes que mejoran conversion", "Reduce tasa de abandono en onboarding y baja el costo de adquisicion de clientes (CAC)"],
    "date_col": "FECHA_HORA_INICIO",
    "filter_cols": ["FUENTE_ADQUISICION", "PLATAFORMA", "VARIANTE_EXPERIMENTO"],
    "kpi_query": """SELECT ROUND(SUM(CASE WHEN ONBOARDING_COMPLETADO THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(AVG(TIEMPO_TOTAL_SEGUNDOS),1), ROUND(AVG(CAC_ESTIMADO_COP)/1e3,1), ROUND(SUM(CASE WHEN PRIMERA_RECARGA_24H THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(SUM(CASE WHEN ACTIVO_DIA_30 THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(AVG(SCORE_BIOMETRIA)*100,1), ROUND(AVG(PROB_ACTIVACION)*100,1), COUNT(*) FROM {table} WHERE {where}""",
    "kpi_labels": ["Completado %", "Tiempo Seg", "CAC K", "Recarga 24H %", "Activo D30 %", "Biometria %", "Prob Activ %", "Total"],
    "kpi_formats": ["{:.1f}%", "{:.1f}", "${:.1f}K", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_HORA_INICIO) AS MES, ROUND(SUM(CASE WHEN ONBOARDING_COMPLETADO THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS COMPLETADO_PCT, ROUND(AVG(PROB_ACTIVACION)*100,1) AS PROB_ACTIVACION FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["COMPLETADO_PCT", "PROB_ACTIVACION"],
    "treemap_query": """SELECT FUENTE_ADQUISICION AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Fuente Adquisicion"},
    "geo_query": """SELECT CIUDAD, SUM(CASE WHEN ONBOARDING_COMPLETADO THEN 1 ELSE 0 END)*100.0/COUNT(*) AS VALOR FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Completado % por Ciudad", "color_col": "VALOR"},
    "diagnostics": None,
    "simulator": {
        "title": "Simulador de Activacion de Usuario",
        "desc": "Estima la probabilidad de que un usuario complete el onboarding segun las variables del proceso.",
        "features": [
            {"name": "Score Biometria", "min": 0, "max": 100, "default": 75, "weight": 0.3, "step": 5},
            {"name": "Pasos Completados", "min": 1, "max": 8, "default": 4, "weight": 0.25, "step": 1},
            {"name": "Tiempo en Paso (seg)", "min": 5, "max": 300, "default": 45, "weight": -0.2, "step": 10},
            {"name": "Edad Usuario", "min": 18, "max": 70, "default": 30, "weight": -0.1, "step": 1},
            {"name": "Intentos Documento", "min": 1, "max": 5, "default": 1, "weight": -0.15, "step": 1},
        ],
        "thresholds": [0.33, 0.66],
        "labels": ["Activacion Improbable", "Activacion Posible", "Activacion Probable"],
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
st.subheader(":material/bar_chart: Analisis Avanzado de Onboarding")

df_fun = run_query(f"SELECT COUNT(*) AS TOTAL, SUM(CASE WHEN PASO_ALCANZADO>=3 THEN 1 ELSE 0 END) AS PASO3, SUM(CASE WHEN ONBOARDING_COMPLETADO THEN 1 ELSE 0 END) AS COMPLETADO, SUM(CASE WHEN PRIMERA_RECARGA_24H THEN 1 ELSE 0 END) AS PRIMERA_RECARGA, SUM(CASE WHEN ACTIVO_DIA_30 THEN 1 ELSE 0 END) AS ACTIVO_D30 FROM {TABLE}")
fig_fun = go.Figure(go.Funnel(y=["Total","Paso 3","Completado","Primera Recarga","Activo D30"], x=[df_fun["TOTAL"].iloc[0], df_fun["PASO3"].iloc[0], df_fun["COMPLETADO"].iloc[0], df_fun["PRIMERA_RECARGA"].iloc[0], df_fun["ACTIVO_D30"].iloc[0]], marker=dict(color=SEMAFORO[:5])))
fig_fun.update_layout(**CHART_LAYOUT, title="Funnel de Onboarding")
st.plotly_chart(fig_fun, use_container_width=True)

df_heat = run_query(f"SELECT FUENTE_ADQUISICION, PLATAFORMA, ROUND(SUM(CASE WHEN ONBOARDING_COMPLETADO THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS COMPLETADO_PCT FROM {TABLE} GROUP BY 1,2")
pivot = df_heat.pivot(index="FUENTE_ADQUISICION", columns="PLATAFORMA", values="COMPLETADO_PCT").fillna(0)
fig_heat = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0,"#E63946"],[0.25,"#F5A623"],[0.5,"#F5D63D"],[0.75,"#6ABF4B"],[1,"#2D9B2D"]], texttemplate="%{z:.1f}%"))
fig_heat.update_layout(**CHART_LAYOUT, title="% Completado: Fuente × Plataforma")
st.plotly_chart(fig_heat, use_container_width=True)

col1, col2 = st.columns(2)
with col1:
    df_sc = run_query(f"SELECT CAC_ESTIMADO_COP/1e3 AS CAC_K, PROB_ACTIVACION, FUENTE_ADQUISICION AS FUENTE FROM {TABLE} LIMIT 2000")
    fig_sc = px.scatter(df_sc, x="CAC_K", y="PROB_ACTIVACION", color="FUENTE", color_discrete_sequence=SEMAFORO)
    fig_sc.update_layout(**CHART_LAYOUT, title="CAC vs Prob Activacion")
    st.plotly_chart(fig_sc, use_container_width=True)
with col2:
    df_donut = run_query(f"SELECT MOTIVO_ABANDONO, COUNT(*) AS N FROM {TABLE} WHERE NOT ONBOARDING_COMPLETADO GROUP BY 1")
    fig_donut = go.Figure(go.Pie(labels=df_donut["MOTIVO_ABANDONO"], values=df_donut["N"], hole=0.5, marker=dict(colors=SEMAFORO)))
    fig_donut.update_layout(**CHART_LAYOUT, title="Motivos de Abandono")
    st.plotly_chart(fig_donut, use_container_width=True)

df_box = run_query(f"SELECT PLATAFORMA, TIEMPO_TOTAL_SEGUNDOS/60.0 AS TIEMPO_MIN FROM {TABLE} LIMIT 3000")
fig_box = px.box(df_box, x="PLATAFORMA", y="TIEMPO_MIN", color_discrete_sequence=SEMAFORO)
fig_box.update_layout(**CHART_LAYOUT, title="Tiempo Total (min) por Plataforma")
st.plotly_chart(fig_box, use_container_width=True)
