from app_pages.page_template import render_page

config = {
    "key": "rpa",
    "table": "MGG_BANCA.OPERACIONES_Y_EFICIENCIA.AUTOMATIZACION_RPA",
    "icon": ":material/precision_manufacturing:",
    "title": "Automatizacion RPA",
    "subtitle": "Monitoreo de robots RPA con tasa de exito, ahorro operativo y tiempo manual ahorrado.",
    "cards": [
        "Monitorea ejecuciones de robots RPA con tasa de exito, registros procesados y excepciones por proceso.",
        "Cuantifica ahorro operativo en COP y tiempo manual equivalente ahorrado por cada ejecucion.",
        "Detecta procesos con alta tasa de intervencion humana para priorizar mejoras de automatizacion.",
    ],
    "date_col": "FECHA_HORA_INICIO",
    "filter_cols": ["TIPO_PROCESO", "AREA_NEGOCIO", "ESTADO_EJECUCION"],
    "kpi_query": """SELECT
        ROUND(AVG(TASA_EXITO), 3) AS TASA_EXITO_PROM,
        ROUND(SUM(REGISTROS_PROCESADOS) / 1e3, 1) AS REGISTROS_K,
        ROUND(SUM(AHORRO_ESTIMADO_COP) / 1e6, 1) AS AHORRO_TOTAL_M,
        ROUND(SUM(TIEMPO_MANUAL_EQUIVALENTE_MIN) / 60.0, 1) AS TIEMPO_MANUAL_H,
        ROUND(AVG(DURACION_SEGUNDOS), 1) AS DURACION_PROM_SEG,
        ROUND(SUM(CASE WHEN REQUIRIO_INTERVENCION_HUMANA = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_INTERVENCION,
        ROUND(AVG(EXCEPCIONES_GENERADAS), 1) AS EXCEPCIONES_PROM,
        COUNT(*) AS TOTAL_EJECUCIONES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Tasa Exito Prom", "Registros Procesados (K)", "Ahorro Total (M)", "Tiempo Manual Ahorrado (h)", "Duracion Prom (seg)", "% Intervencion Humana", "Excepciones Prom", "Total Ejecuciones"],
    "kpi_formats": ["{:.3f}", "{:.1f}K", "${:.1f}M", "{:.1f} h", "{:.1f} seg", "{:.2f}%", "{:.1f}", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_HORA_INICIO) AS MES,
        ROUND(AVG(TASA_EXITO), 3) AS TASA_EXITO,
        ROUND(SUM(AHORRO_ESTIMADO_COP) / 1e6, 1) AS AHORRO_M
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Tasa Exito", "Ahorro (M)"],
    "treemap_query": """SELECT TIPO_PROCESO, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Tipo de Proceso"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Automatizacion RPA")

df_heat = run_query(f"SELECT TIPO_PROCESO, AREA_NEGOCIO, ROUND(AVG(TASA_EXITO), 3) AS TASA_EXITO FROM {TABLE} GROUP BY 1, 2")
pivot_heat = df_heat.pivot_table(index="TIPO_PROCESO", columns="AREA_NEGOCIO", values="TASA_EXITO", aggfunc="mean")
fig_heat = go.Figure(go.Heatmap(z=pivot_heat.values, x=pivot_heat.columns.tolist(), y=pivot_heat.index.tolist(), colorscale=[[0, NAVY[0]], [0.5, NAVY[3]], [1, "#A5F3FC"]], texttemplate="%{z:.3f}", textfont=dict(size=10)))
fig_heat.update_layout(title="Tasa de Exito por Tipo Proceso × Area Negocio", **CHART_LAYOUT)
st.plotly_chart(fig_heat, use_container_width=True)

col1, col2 = st.columns(2)

df_scatter = run_query(f"SELECT TASA_EXITO, COMPLEJIDAD_PROCESO, ESTADO_EJECUCION, AHORRO_ESTIMADO_COP FROM {TABLE}")
fig_scatter = px.scatter(df_scatter, x="COMPLEJIDAD_PROCESO", y="TASA_EXITO", color="ESTADO_EJECUCION", size="AHORRO_ESTIMADO_COP", color_discrete_sequence=NAVY)
fig_scatter.update_layout(title="Tasa Exito vs Complejidad", **CHART_LAYOUT)
col1.plotly_chart(fig_scatter, use_container_width=True)

df_funnel = run_query(f"SELECT COUNT(*) AS TOTAL, SUM(CASE WHEN ESTADO_EJECUCION = 'Exitoso' THEN 1 ELSE 0 END) AS EXITOSO, SUM(CASE WHEN ESTADO_EJECUCION = 'Exitoso' AND REQUIRIO_INTERVENCION_HUMANA = FALSE THEN 1 ELSE 0 END) AS SIN_INTERVENCION FROM {TABLE}")
fig_funnel = go.Figure(go.Funnel(y=["Total Ejecuciones", "Exitosas", "Sin Intervencion Humana"], x=[df_funnel["TOTAL"].iloc[0], df_funnel["EXITOSO"].iloc[0], df_funnel["SIN_INTERVENCION"].iloc[0]], marker=dict(color=NAVY[:3])))
fig_funnel.update_layout(title="Funnel de Ejecuciones", **CHART_LAYOUT)
col2.plotly_chart(fig_funnel, use_container_width=True)

col3, col4 = st.columns(2)

df_donut = run_query(f"SELECT PLATAFORMA_RPA, COUNT(*) AS N FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC")
fig_donut = go.Figure(go.Pie(labels=df_donut["PLATAFORMA_RPA"], values=df_donut["N"], hole=0.5, marker=dict(colors=NAVY)))
fig_donut.update_layout(title="Distribucion por Plataforma RPA", **CHART_LAYOUT)
col3.plotly_chart(fig_donut, use_container_width=True)

df_wf = run_query(f"SELECT AREA_NEGOCIO, ROUND(SUM(AHORRO_ESTIMADO_COP) / 1e6, 1) AS AHORRO_M FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC")
fig_wf = go.Figure(go.Waterfall(x=df_wf["AREA_NEGOCIO"], y=df_wf["AHORRO_M"], connector=dict(line=dict(color=NAVY[2])), increasing=dict(marker=dict(color=NAVY[3])), decreasing=dict(marker=dict(color=NAVY[0]))))
fig_wf.update_layout(title="Ahorro Estimado por Area (M COP)", **CHART_LAYOUT)
col4.plotly_chart(fig_wf, use_container_width=True)

df_box = run_query(f"SELECT TIPO_PROCESO, DURACION_SEGUNDOS / 60.0 AS DURACION_MIN FROM {TABLE}")
fig_box = px.box(df_box, x="TIPO_PROCESO", y="DURACION_MIN", color="TIPO_PROCESO", color_discrete_sequence=NAVY)
fig_box.update_layout(title="Distribucion Duracion (min) por Tipo Proceso", showlegend=False, **CHART_LAYOUT)
st.plotly_chart(fig_box, use_container_width=True)
