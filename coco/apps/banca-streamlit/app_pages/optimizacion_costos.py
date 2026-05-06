from app_pages.page_template import render_page

config = {
    "key": "opc",
    "table": "MGG_BANCA.OPERACIONES_Y_EFICIENCIA.OPTIMIZACION_COSTOS",
    "icon": ":material/savings:",
    "title": "Optimizacion Costos",
    "subtitle": "Identificacion y seguimiento de iniciativas de optimizacion de costos operativos.",
    "cards": [
        "Identifica iniciativas de ahorro midiendo porcentaje de reduccion y ahorro mensual estimado.",
        "Evalua ROI por iniciativa considerando inversion requerida, complejidad y prioridad estrategica.",
        "Monitorea reduccion de FTEs y porcentaje automatizable para optimizar capacidad operativa.",
    ],
    "date_col": "FECHA_IDENTIFICACION",
    "filter_cols": ["AREA", "ESTADO_INICIATIVA", "CIUDAD_OPERACION"],
    "kpi_query": """SELECT
        ROUND(AVG(PORCENTAJE_AHORRO), 2) AS PCT_AHORRO_PROM,
        ROUND(SUM(AHORRO_MENSUAL_ESTIMADO_COP) / 1e6, 1) AS AHORRO_MENSUAL_M,
        ROUND(AVG(ROI_MESES), 1) AS ROI_MESES,
        ROUND(AVG(FTES_ACTUALES - FTES_POST_OPTIMIZACION), 1) AS FTES_REDUCIDOS_PROM,
        ROUND(AVG(PORCENTAJE_AUTOMATIZABLE), 2) AS PCT_AUTOMATIZABLE,
        ROUND(AVG(TASA_ERROR_ACTUAL), 3) AS TASA_ERROR_ACTUAL,
        ROUND(SUM(INVERSION_REQUERIDA_COP) / 1e6, 1) AS INVERSION_M,
        COUNT(*) AS TOTAL_INICIATIVAS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["% Ahorro Prom", "Ahorro Mensual (M)", "ROI (meses)", "FTEs Reducidos Prom", "% Automatizable", "Tasa Error Actual", "Inversion (M)", "Total Iniciativas"],
    "kpi_formats": ["{:.2f}%", "${:.1f}M", "{:.1f} meses", "{:.1f}", "{:.2f}%", "{:.3f}", "${:.1f}M", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_IDENTIFICACION) AS MES,
        ROUND(AVG(PORCENTAJE_AHORRO), 2) AS PCT_AHORRO,
        ROUND(SUM(AHORRO_MENSUAL_ESTIMADO_COP) / 1e6, 1) AS AHORRO_M
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["% Ahorro", "Ahorro (M)"],
    "treemap_query": """SELECT AREA, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Area"},
    "geo_query": """SELECT CIUDAD_OPERACION AS CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(PORCENTAJE_AHORRO), 2) AS PCT_AHORRO FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Mapa de Ahorro por Ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "PCT_AHORRO", "caption": "Tamano: volumen iniciativas | Color: porcentaje ahorro promedio"},
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
NAVY = ["#0F2B46", "#1B3A5C", "#2E7D8C", "#29B5E8", "#0F4C75", "#3282B8", "#11567F", "#1A5276"]

st.divider()
st.subheader(":material/bar_chart: Analisis Avanzado de Optimizacion de Costos")

df_scatter = run_query(f"SELECT ROI_MESES, PORCENTAJE_AHORRO, ESTADO_INICIATIVA, AHORRO_MENSUAL_ESTIMADO_COP FROM {TABLE}")
fig_scatter = px.scatter(df_scatter, x="ROI_MESES", y="PORCENTAJE_AHORRO", color="ESTADO_INICIATIVA", size="AHORRO_MENSUAL_ESTIMADO_COP", color_discrete_sequence=NAVY)
fig_scatter.update_layout(title="ROI vs % Ahorro por Estado Iniciativa", **CHART_LAYOUT)
st.plotly_chart(fig_scatter, use_container_width=True)

col1, col2 = st.columns(2)

df_heat = run_query(f"SELECT AREA, ESTADO_INICIATIVA, ROUND(AVG(PORCENTAJE_AHORRO), 2) AS PCT_AHORRO FROM {TABLE} GROUP BY 1, 2")
pivot_heat = df_heat.pivot_table(index="AREA", columns="ESTADO_INICIATIVA", values="PCT_AHORRO", aggfunc="mean")
fig_heat = go.Figure(go.Heatmap(z=pivot_heat.values, x=pivot_heat.columns.tolist(), y=pivot_heat.index.tolist(), colorscale=[[0, NAVY[0]], [0.5, NAVY[3]], [1, "#A5F3FC"]], texttemplate="%{z:.2f}", textfont=dict(size=10)))
fig_heat.update_layout(title="% Ahorro por Area × Estado", **CHART_LAYOUT)
col1.plotly_chart(fig_heat, use_container_width=True)

df_wf = run_query(f"SELECT AREA, ROUND(SUM(AHORRO_MENSUAL_ESTIMADO_COP) / 1e6, 1) AS AHORRO_M FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC")
fig_wf = go.Figure(go.Waterfall(x=df_wf["AREA"], y=df_wf["AHORRO_M"], connector=dict(line=dict(color=NAVY[2])), increasing=dict(marker=dict(color=NAVY[3])), decreasing=dict(marker=dict(color=NAVY[0]))))
fig_wf.update_layout(title="Ahorro Mensual Total por Area (M COP)", **CHART_LAYOUT)
col2.plotly_chart(fig_wf, use_container_width=True)

col3, col4 = st.columns(2)

df_funnel = run_query(f"SELECT COUNT(*) AS TOTAL, SUM(CASE WHEN ESTADO_INICIATIVA = 'En implementacion' THEN 1 ELSE 0 END) AS EN_IMPL, SUM(CASE WHEN ESTADO_INICIATIVA = 'Completada' THEN 1 ELSE 0 END) AS COMPLETADAS FROM {TABLE}")
fig_funnel = go.Figure(go.Funnel(y=["Total Iniciativas", "En Implementacion", "Completadas"], x=[df_funnel["TOTAL"].iloc[0], df_funnel["EN_IMPL"].iloc[0], df_funnel["COMPLETADAS"].iloc[0]], marker=dict(color=NAVY[:3])))
fig_funnel.update_layout(title="Funnel de Iniciativas", **CHART_LAYOUT)
col3.plotly_chart(fig_funnel, use_container_width=True)

df_err = run_query(f"SELECT AREA, ROUND(AVG(TASA_ERROR_ACTUAL), 3) AS ERROR_ACTUAL, ROUND(AVG(TASA_ERROR_OBJETIVO), 3) AS ERROR_OBJETIVO FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC LIMIT 6")
fig_err = go.Figure()
fig_err.add_trace(go.Bar(x=df_err["AREA"], y=df_err["ERROR_ACTUAL"], name="Error Actual", marker_color=NAVY[0]))
fig_err.add_trace(go.Bar(x=df_err["AREA"], y=df_err["ERROR_OBJETIVO"], name="Error Objetivo", marker_color=NAVY[3]))
fig_err.update_layout(title="Error Actual vs Objetivo por Area (Top 6)", barmode="group", **CHART_LAYOUT)
col4.plotly_chart(fig_err, use_container_width=True)

df_box = run_query(f"SELECT AREA, PORCENTAJE_AHORRO FROM {TABLE}")
fig_box = px.box(df_box, x="AREA", y="PORCENTAJE_AHORRO", color="AREA", color_discrete_sequence=NAVY)
fig_box.update_layout(title="Distribucion % Ahorro por Area", showlegend=False, **CHART_LAYOUT)
st.plotly_chart(fig_box, use_container_width=True)
