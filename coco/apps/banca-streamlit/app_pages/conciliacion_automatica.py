from app_pages.page_template import render_page

config = {
    "key": "con",
    "table": "MGG_BANCA.OPERACIONES_Y_EFICIENCIA.CONCILIACION_AUTOMATICA",
    "icon": ":material/fact_check:",
    "title": "Conciliacion Automatica",
    "subtitle": "Conciliacion automatizada de transacciones entre fuentes con matching inteligente.",
    "cards": [
        "Concilia transacciones entre fuentes A y B usando score de matching inteligente multi-criterio.",
        "Detecta diferencias en montos y fechas generando alertas automaticas para investigacion.",
        "Mide tasa de conciliacion por lote y tiempo de resolucion para optimizar eficiencia operativa.",
    ],
    "date_col": "FECHA_CONCILIACION",
    "filter_cols": ["TIPO_TRANSACCION", "ESTADO_CONCILIACION", "METODO_CONCILIACION"],
    "kpi_query": """SELECT
        ROUND(AVG(SCORE_MATCHING), 3) AS SCORE_MATCHING_PROM,
        ROUND(AVG(TASA_CONCILIACION_LOTE), 3) AS TASA_CONCILIACION_PROM,
        ROUND(SUM(CASE WHEN REQUIRIO_INTERVENCION = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_INTERVENCION,
        ROUND(AVG(ABS(DIFERENCIA_COP)) / 1e3, 1) AS DIFERENCIA_PROM_K,
        ROUND(AVG(TIEMPO_CONCILIACION_SEG), 1) AS TIEMPO_CONC_SEG,
        ROUND(AVG(HORAS_HASTA_RESOLUCION), 1) AS HORAS_RESOLUCION_PROM,
        ROUND(SUM(CASE WHEN ALERTA_GENERADA = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_ALERTA,
        COUNT(*) AS TOTAL_CONCILIACIONES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Score Matching Prom", "Tasa Conciliacion Prom", "% Intervencion", "Diferencia Prom (K)", "Tiempo Conc (seg)", "Horas Resolucion Prom", "% Alerta Generada", "Total Conciliaciones"],
    "kpi_formats": ["{:.3f}", "{:.3f}", "{:.2f}%", "${:.1f}K", "{:.1f} seg", "{:.1f} h", "{:.2f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_CONCILIACION) AS MES,
        ROUND(AVG(SCORE_MATCHING), 3) AS SCORE_MATCHING,
        ROUND(AVG(TASA_CONCILIACION_LOTE), 3) AS TASA_CONCILIACION
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Score Matching", "Tasa Conciliacion"],
    "treemap_query": """SELECT TIPO_TRANSACCION, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Tipo de Transaccion"},
    "geo_query": None,
    "geo_config": None,
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
st.subheader(":material/bar_chart: Analisis Avanzado de Conciliacion Automatica")

df_heat = run_query(f"SELECT FUENTE_A, FUENTE_B, ROUND(AVG(SCORE_MATCHING), 3) AS SCORE FROM {TABLE} GROUP BY 1, 2")
pivot_heat = df_heat.pivot_table(index="FUENTE_A", columns="FUENTE_B", values="SCORE", aggfunc="mean")
fig_heat = go.Figure(go.Heatmap(z=pivot_heat.values, x=pivot_heat.columns.tolist(), y=pivot_heat.index.tolist(), colorscale=[[0, NAVY[0]], [0.5, NAVY[3]], [1, "#A5F3FC"]], texttemplate="%{z:.3f}", textfont=dict(size=10)))
fig_heat.update_layout(title="Score Matching por Fuente A × Fuente B", **CHART_LAYOUT)
st.plotly_chart(fig_heat, use_container_width=True)

col1, col2 = st.columns(2)

df_scatter = run_query(f"SELECT SCORE_MATCHING, DIFERENCIA_COP / 1e3 AS DIFERENCIA_K, ESTADO_CONCILIACION FROM {TABLE}")
fig_scatter = px.scatter(df_scatter, x="DIFERENCIA_K", y="SCORE_MATCHING", color="ESTADO_CONCILIACION", color_discrete_sequence=NAVY)
fig_scatter.update_layout(title="Score Matching vs Diferencia (K COP)", **CHART_LAYOUT)
col1.plotly_chart(fig_scatter, use_container_width=True)

df_funnel = run_query(f"SELECT COUNT(*) AS TOTAL, SUM(CASE WHEN ESTADO_CONCILIACION = 'Conciliada' THEN 1 ELSE 0 END) AS CONCILIADAS, SUM(CASE WHEN ESTADO_CONCILIACION = 'Conciliada' AND REQUIRIO_INTERVENCION = FALSE THEN 1 ELSE 0 END) AS SIN_INTERVENCION FROM {TABLE}")
fig_funnel = go.Figure(go.Funnel(y=["Total", "Conciliadas", "Sin Intervencion"], x=[df_funnel["TOTAL"].iloc[0], df_funnel["CONCILIADAS"].iloc[0], df_funnel["SIN_INTERVENCION"].iloc[0]], marker=dict(color=NAVY[:3])))
fig_funnel.update_layout(title="Funnel de Conciliacion", **CHART_LAYOUT)
col2.plotly_chart(fig_funnel, use_container_width=True)

col3, col4 = st.columns(2)

df_donut = run_query(f"SELECT METODO_CONCILIACION, COUNT(*) AS N FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC")
fig_donut = go.Figure(go.Pie(labels=df_donut["METODO_CONCILIACION"], values=df_donut["N"], hole=0.5, marker=dict(colors=NAVY)))
fig_donut.update_layout(title="Distribucion por Metodo Conciliacion", **CHART_LAYOUT)
col3.plotly_chart(fig_donut, use_container_width=True)

df_bar = run_query(f"SELECT PERIODICIDAD, ROUND(AVG(HORAS_HASTA_RESOLUCION), 1) AS HORAS_RESOLUCION FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC")
fig_bar = go.Figure(go.Bar(x=df_bar["PERIODICIDAD"], y=df_bar["HORAS_RESOLUCION"], marker_color=NAVY[3]))
fig_bar.update_layout(title="Tiempo Resolucion Promedio por Periodicidad", yaxis_title="Horas", **CHART_LAYOUT)
col4.plotly_chart(fig_bar, use_container_width=True)

df_box = run_query(f"SELECT TIPO_TRANSACCION, DIFERENCIA_COP / 1e3 AS DIFERENCIA_K FROM {TABLE}")
fig_box = px.box(df_box, x="TIPO_TRANSACCION", y="DIFERENCIA_K", color="TIPO_TRANSACCION", color_discrete_sequence=NAVY)
fig_box.update_layout(title="Distribucion Diferencia (K COP) por Tipo Transaccion", showlegend=False, **CHART_LAYOUT)
st.plotly_chart(fig_box, use_container_width=True)
