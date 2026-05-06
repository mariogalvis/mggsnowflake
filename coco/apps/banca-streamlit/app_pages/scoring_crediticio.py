from app_pages.page_template import render_page

config = {
    "key": "scr",
    "table": "MGG_BANCA.RIESGO_Y_CREDITO.SCORING_CREDITICIO",
    "icon": ":material/credit_score:",
    "title": "Scoring Crediticio",
    "subtitle": "Modelo de scoring PD integrando bureau, ingresos, endeudamiento e historial crediticio.",
    "cards": [
        "Identifica riesgo de incumplimiento antes de la originacion, reduciendo cartera vencida.",
        "Integra score bureau + ingresos + endeudamiento + historial crediticio en un modelo unificado de PD.",
        "Reduce la probabilidad de default en 20-30% y optimiza la tasa de aprobacion sin sacrificar calidad.",
    ],
    "date_col": None,
    "filter_cols": ["CIUDAD", "NIVEL_EDUCATIVO", "OCUPACION", "RESULTADO_SCORING"],
    "kpi_query": """SELECT
        ROUND(AVG(SCORE_BUREAU), 1) AS SCORE_PROM,
        ROUND(AVG(PROBABILIDAD_INCUMPLIMIENTO) * 100, 2) AS PD_PROM,
        ROUND(AVG(RATIO_ENDEUDAMIENTO) * 100, 1) AS RATIO_END_PROM,
        COUNT(*) AS TOTAL_CLIENTES,
        ROUND(SUM(CASE WHEN RESULTADO_SCORING = 'APROBADO' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS TASA_APROBACION,
        ROUND(AVG(INGRESO_MENSUAL_COP) / 1e6, 2) AS INGRESO_PROM_M,
        ROUND(AVG(MESES_HISTORIAL_CREDITICIO), 0) AS HIST_PROM_MESES,
        ROUND(AVG(NUM_CREDITOS_CASTIGADOS), 2) AS CASTIGADOS_PROM
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Score Bureau Prom", "PD Promedio %", "Ratio Endeudamiento %", "Total Clientes", "Tasa Aprobacion %", "Ingreso Prom (M)", "Historial Prom (meses)", "Castigados Prom"],
    "kpi_formats": ["{:.1f}", "{:.2f}%", "{:.1f}%", "{:,.0f}", "{:.1f}%", "${:.2f}M", "{:.0f}", "{:.2f}"],
    "trend_query": """SELECT RESULTADO_SCORING AS CATEGORIA, COUNT(*) AS N, ROUND(AVG(PROBABILIDAD_INCUMPLIMIENTO) * 100, 2) AS PD_PROM
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "trend_cols": ["Clientes", "PD Promedio"],
    "treemap_query": """SELECT NIVEL_EDUCATIVO, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Nivel Educativo"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(PROBABILIDAD_INCUMPLIMIENTO) * 100, 2) AS PD_PROM FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Mapa de Riesgo Crediticio por Ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "PD_PROM", "caption": "Tamano: volumen clientes | Color: PD promedio"},
    "diagnostics": None,
    "simulator": {
        "title": "Simulador de Scoring Crediticio",
        "desc": "Ajuste las variables del perfil para estimar la probabilidad de incumplimiento.",
        "features": [
            {"name": "Score Bureau", "min": 150, "max": 950, "default": 650, "weight": -0.25, "desc": "Puntaje de bureau crediticio"},
            {"name": "Ingreso Mensual (M COP)", "min": 1, "max": 30, "default": 5, "step": 0.5, "weight": -0.15, "desc": "Ingreso mensual en millones"},
            {"name": "Ratio Endeudamiento %", "min": 0, "max": 100, "default": 40, "weight": 0.2, "desc": "Deuda total / Ingresos"},
            {"name": "Meses Historial", "min": 0, "max": 360, "default": 60, "weight": -0.15, "desc": "Antiguedad historial crediticio"},
            {"name": "Creditos Castigados", "min": 0, "max": 10, "default": 0, "step": 1, "weight": 0.15, "desc": "Numero de creditos castigados"},
            {"name": "Max Dias Mora Historico", "min": 0, "max": 360, "default": 30, "step": 5, "weight": 0.10, "desc": "Maximo dias en mora historico"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Scoring Crediticio")

# HISTOGRAM - FULL WIDTH
st.markdown("**Distribucion de Score Bureau**")
df_hist = run_query(f"SELECT SCORE_BUREAU FROM {TABLE} WHERE SCORE_BUREAU IS NOT NULL")
fig = px.histogram(df_hist, x="SCORE_BUREAU", nbins=40, color_discrete_sequence=[NAVY[3]])
fig.update_layout(**CHART_LAYOUT, xaxis_title="Score Bureau", yaxis_title="Frecuencia")
st.plotly_chart(fig, use_container_width=True)

# SCATTER + DONUT - SIDE BY SIDE
col_a, col_b = st.columns(2)
with col_a:
    st.markdown("**Score Bureau vs Probabilidad de Incumplimiento**")
    df_sc = run_query(f"SELECT SCORE_BUREAU, PROBABILIDAD_INCUMPLIMIENTO, RESULTADO_SCORING, INGRESO_MENSUAL_COP FROM {TABLE} WHERE SCORE_BUREAU IS NOT NULL")
    fig = px.scatter(df_sc, x="SCORE_BUREAU", y="PROBABILIDAD_INCUMPLIMIENTO", color="RESULTADO_SCORING", size="INGRESO_MENSUAL_COP", color_discrete_sequence=NAVY, opacity=0.7)
    fig.update_layout(**CHART_LAYOUT)
    st.plotly_chart(fig, use_container_width=True)
with col_b:
    st.markdown("**Distribucion por Resultado de Scoring**")
    df_donut = run_query(f"SELECT RESULTADO_SCORING, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
    fig = px.pie(df_donut, names="RESULTADO_SCORING", values="N", hole=0.5, color_discrete_sequence=NAVY)
    fig.update_layout(**CHART_LAYOUT)
    st.plotly_chart(fig, use_container_width=True)

# RADAR - FULL WIDTH
st.markdown("**Factores de Riesgo por Resultado de Scoring**")
df_radar = run_query(f"""SELECT RESULTADO_SCORING,
    ROUND(AVG(RATIO_ENDEUDAMIENTO) * 100, 1) AS ENDEUDAMIENTO,
    ROUND(AVG(MAX_DIAS_MORA_HISTORICO) / 3.6, 1) AS MORA_HISTORICA,
    ROUND(AVG(NUM_CREDITOS_ACTIVOS) * 10, 1) AS CREDITOS_ACTIVOS,
    ROUND(AVG(NUM_CREDITOS_CASTIGADOS) * 20, 1) AS CASTIGADOS,
    ROUND(AVG(MESES_HISTORIAL_CREDITICIO) / 3.6, 1) AS HISTORIAL
FROM {TABLE} GROUP BY 1""")
categories = ["ENDEUDAMIENTO", "MORA_HISTORICA", "CREDITOS_ACTIVOS", "CASTIGADOS", "HISTORIAL"]
fig = go.Figure()
for i, row in df_radar.iterrows():
    fig.add_trace(go.Scatterpolar(r=[row[c] for c in categories], theta=categories, fill="toself", name=row["RESULTADO_SCORING"], line=dict(color=NAVY[i % len(NAVY)])))
fig.update_layout(**CHART_LAYOUT, polar=dict(bgcolor="#FAFBFC"))
st.plotly_chart(fig, use_container_width=True)

# BOX PLOT - FULL WIDTH
st.markdown("**Ingreso Mensual por Resultado de Scoring**")
df_box = run_query(f"SELECT RESULTADO_SCORING, INGRESO_MENSUAL_COP FROM {TABLE}")
fig = px.box(df_box, x="RESULTADO_SCORING", y="INGRESO_MENSUAL_COP", color="RESULTADO_SCORING", color_discrete_sequence=NAVY)
fig.update_layout(**CHART_LAYOUT, showlegend=False)
st.plotly_chart(fig, use_container_width=True)

# HEATMAP - FULL WIDTH
st.markdown("**PD Promedio por Nivel Educativo y Ocupacion**")
df_heat = run_query(f"SELECT NIVEL_EDUCATIVO, OCUPACION, ROUND(AVG(PROBABILIDAD_INCUMPLIMIENTO) * 100, 2) AS PD_PROM FROM {TABLE} GROUP BY 1, 2")
pivot = df_heat.pivot_table(index="NIVEL_EDUCATIVO", columns="OCUPACION", values="PD_PROM", aggfunc="mean")
fig = go.Figure(data=go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0, NAVY[3]], [1, NAVY[0]]], texttemplate="%{z:.2f}%"))
fig.update_layout(**CHART_LAYOUT)
st.plotly_chart(fig, use_container_width=True)
