from app_pages.page_template import render_page

config = {
    "key": "ews",
    "table": "MGG_BANCA.RIESGO_Y_CREDITO.EARLY_WARNING",
    "icon": ":material/warning:",
    "title": "Early Warning System",
    "subtitle": "Sistema de alertas tempranas que detecta deterioro crediticio antes de que el cliente entre en mora.",
    "cards": [
        "Detecta senales de deterioro 60-90 dias antes del incumplimiento, permitiendo accion preventiva.",
        "Monitorea variacion de score, transaccionalidad, sobregiros y uso de cupo para generar alertas.",
        "Reduce migracion a cartera vencida en 25-40% mediante intervencion oportuna.",
    ],
    "date_col": "FECHA_EVALUACION",
    "filter_cols": ["TIPO_ALERTA", "NIVEL_RIESGO", "ESTADO_GESTION", "CIUDAD"],
    "kpi_query": """SELECT
        COUNT(*) AS TOTAL_ALERTAS,
        ROUND(AVG(PROB_DETERIORO_90D) * 100, 2) AS PROB_DETERIORO_PROM,
        ROUND(AVG(DIAS_MORA_ACTUAL), 1) AS MORA_ACTUAL_PROM,
        ROUND(AVG(VARIACION_SCORE), 1) AS VAR_SCORE_PROM,
        ROUND(AVG(USO_CUPO_TARJETA_PCT), 1) AS USO_CUPO_PROM,
        ROUND(AVG(SOBREGIROS_ULTIMOS_3M), 2) AS SOBREGIROS_PROM,
        ROUND(SUM(CASE WHEN NIVEL_RIESGO = 'CRITICO' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS PCT_CRITICOS,
        ROUND(AVG(VARIACION_INGRESO_PCT), 1) AS VAR_INGRESO_PROM
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Total Alertas", "Prob Deterioro %", "Mora Actual (dias)", "Var Score Prom", "Uso Cupo %", "Sobregiros Prom", "% Criticos", "Var Ingreso %"],
    "kpi_formats": ["{:,.0f}", "{:.2f}%", "{:.1f}", "{:.1f}", "{:.1f}%", "{:.2f}", "{:.1f}%", "{:.1f}%"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_EVALUACION) AS MES,
        COUNT(*) AS ALERTAS,
        ROUND(AVG(PROB_DETERIORO_90D) * 100, 2) AS PROB_DETERIORO
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Alertas", "Prob Deterioro %"],
    "treemap_query": """SELECT TIPO_ALERTA, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Alertas por Tipo"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(PROB_DETERIORO_90D) * 100, 2) AS PROB_DET FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Alertas Tempranas por Ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "PROB_DET", "caption": "Tamano: volumen alertas | Color: probabilidad deterioro"},
    "diagnostics": None,
    "simulator": {
        "title": "Simulador de Deterioro Crediticio",
        "desc": "Ajuste indicadores del cliente para estimar probabilidad de deterioro a 90 dias.",
        "features": [
            {"name": "Variacion Score", "min": -200, "max": 50, "default": -20, "step": 5, "weight": 0.25, "desc": "Cambio en score crediticio ultimos 6 meses"},
            {"name": "Uso Cupo Tarjeta %", "min": 0, "max": 100, "default": 50, "weight": 0.2, "desc": "Porcentaje de utilizacion del cupo"},
            {"name": "Sobregiros Ultimos 3M", "min": 0, "max": 15, "default": 1, "step": 1, "weight": 0.15, "desc": "Numero de sobregiros en 3 meses"},
            {"name": "Dias Mora Actual", "min": 0, "max": 90, "default": 0, "step": 5, "weight": 0.2, "desc": "Dias en mora actualmente"},
            {"name": "Variacion Ingreso %", "min": -80, "max": 50, "default": 0, "step": 5, "weight": 0.1, "desc": "Cambio porcentual en ingresos estimados"},
            {"name": "Variacion Transaccionalidad %", "min": -80, "max": 50, "default": 0, "step": 5, "weight": 0.1, "desc": "Cambio en actividad transaccional"},
        ],
        "thresholds": [0.33, 0.66],
        "labels": ["Bajo Riesgo", "Riesgo Medio", "Alto Riesgo"],
    },
}

render_page(config)

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from app_pages.conn_helper import run_query

TABLE = config["table"]
CHART_LAYOUT = dict(paper_bgcolor="#FAFBFC", plot_bgcolor="#FAFBFC", font=dict(family="Inter, sans-serif", color="#334155"))
NAVY = ["#0F2B46", "#1B3A5C", "#2E7D8C", "#29B5E8", "#0F4C75", "#3282B8", "#11567F", "#1A5276"]

st.divider()
st.subheader(":material/bar_chart: Analisis Avanzado de Early Warning")

# HEATMAP - FULL WIDTH
st.markdown("**Probabilidad de Deterioro por Tipo de Alerta y Nivel de Riesgo**")
df_heat = run_query(f"SELECT TIPO_ALERTA, NIVEL_RIESGO, ROUND(AVG(PROB_DETERIORO_90D) * 100, 2) AS PROB_DET FROM {TABLE} GROUP BY 1, 2")
pivot = df_heat.pivot_table(index="TIPO_ALERTA", columns="NIVEL_RIESGO", values="PROB_DET", aggfunc="mean")
fig = go.Figure(data=go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0, NAVY[3]], [1, NAVY[0]]], texttemplate="%{z:.2f}%"))
fig.update_layout(**CHART_LAYOUT)
st.plotly_chart(fig, use_container_width=True)

# SCATTER + DONUT - SIDE BY SIDE
col_a, col_b = st.columns(2)
with col_a:
    st.markdown("**Score Actual vs Probabilidad de Deterioro**")
    df_sc = run_query(f"SELECT SCORE_ACTUAL, PROB_DETERIORO_90D, NIVEL_RIESGO FROM {TABLE} WHERE SCORE_ACTUAL IS NOT NULL")
    fig = px.scatter(df_sc, x="SCORE_ACTUAL", y="PROB_DETERIORO_90D", color="NIVEL_RIESGO", color_discrete_sequence=NAVY, opacity=0.7)
    fig.update_layout(**CHART_LAYOUT)
    st.plotly_chart(fig, use_container_width=True)
with col_b:
    st.markdown("**Distribucion por Estado de Gestion**")
    df_donut = run_query(f"SELECT ESTADO_GESTION, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
    fig = px.pie(df_donut, names="ESTADO_GESTION", values="N", hole=0.5, color_discrete_sequence=NAVY)
    fig.update_layout(**CHART_LAYOUT)
    st.plotly_chart(fig, use_container_width=True)

# FUNNEL - FULL WIDTH
st.markdown("**Alertas por Nivel de Riesgo**")
df_funnel = run_query(f"SELECT NIVEL_RIESGO, COUNT(*) AS N FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC")
fig = go.Figure(go.Funnel(y=df_funnel["NIVEL_RIESGO"].tolist(), x=df_funnel["N"].tolist(), marker=dict(color=NAVY[:len(df_funnel)])))
fig.update_layout(**CHART_LAYOUT)
st.plotly_chart(fig, use_container_width=True)

# BOX PLOT - FULL WIDTH
st.markdown("**Uso de Cupo de Tarjeta por Nivel de Riesgo**")
df_box = run_query(f"SELECT NIVEL_RIESGO, USO_CUPO_TARJETA_PCT FROM {TABLE}")
fig = px.box(df_box, x="NIVEL_RIESGO", y="USO_CUPO_TARJETA_PCT", color="NIVEL_RIESGO", color_discrete_sequence=NAVY)
fig.update_layout(**CHART_LAYOUT, showlegend=False)
st.plotly_chart(fig, use_container_width=True)

# WATERFALL - FULL WIDTH
st.markdown("**Factores de Deterioro Crediticio**")
df_wf = run_query(f"""SELECT
    ROUND(AVG(ABS(VARIACION_SCORE)) / 2, 1) AS VAR_SCORE,
    ROUND(AVG(USO_CUPO_TARJETA_PCT) / 5, 1) AS USO_CUPO,
    ROUND(AVG(SOBREGIROS_ULTIMOS_3M) * 3, 1) AS SOBREGIROS,
    ROUND(AVG(ABS(VARIACION_INGRESO_PCT)) / 3, 1) AS VAR_INGRESO,
    ROUND(AVG(ABS(VARIACION_SALDO_PCT)) / 3, 1) AS VAR_SALDO
FROM {TABLE}""")
factors = ["Var. Score", "Uso Cupo", "Sobregiros", "Var. Ingreso", "Var. Saldo"]
values = [df_wf["VAR_SCORE"].iloc[0], df_wf["USO_CUPO"].iloc[0], df_wf["SOBREGIROS"].iloc[0], df_wf["VAR_INGRESO"].iloc[0], df_wf["VAR_SALDO"].iloc[0]]
fig = go.Figure(go.Waterfall(
    x=factors,
    y=values,
    measure=["absolute", "relative", "relative", "relative", "relative"],
    connector=dict(line=dict(color=NAVY[1])),
    increasing=dict(marker=dict(color=NAVY[0])),
    decreasing=dict(marker=dict(color=NAVY[3])),
    totals=dict(marker=dict(color=NAVY[4]))
))
fig.update_layout(**CHART_LAYOUT, yaxis_title="Contribucion al Deterioro")
st.plotly_chart(fig, use_container_width=True)
