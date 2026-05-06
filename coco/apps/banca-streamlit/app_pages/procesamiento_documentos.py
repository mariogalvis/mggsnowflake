from app_pages.page_template import render_page

config = {
    "key": "pdc",
    "table": "MGG_BANCA.OPERACIONES_Y_EFICIENCIA.PROCESAMIENTO_DOCUMENTOS",
    "icon": ":material/description:",
    "title": "Procesamiento Documentos",
    "subtitle": "Extraccion inteligente de documentos con OCR+AI, validacion contra BD y deteccion de fraude documental.",
    "cards": [
        "Procesa documentos con OCR+AI extrayendo campos con scoring de confianza por campo detectado.",
        "Valida datos extraidos contra bases de datos internas detectando inconsistencias automaticamente.",
        "Identifica posible fraude documental analizando calidad de imagen, firmas y huellas detectadas.",
    ],
    "date_col": "FECHA_HORA_RECEPCION",
    "filter_cols": ["TIPO_DOCUMENTO", "ESTADO_PROCESAMIENTO", "AREA_SOLICITANTE"],
    "kpi_query": """SELECT
        ROUND(AVG(CONFIANZA_OCR_PROMEDIO), 3) AS CONFIANZA_OCR_PROM,
        ROUND(AVG(CAMPOS_EXTRAIDOS), 1) AS CAMPOS_EXTRAIDOS_PROM,
        ROUND(SUM(CASE WHEN REQUIRIO_REVISION_MANUAL = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_REVISION_MANUAL,
        ROUND(AVG(TIEMPO_PROCESAMIENTO_SEG), 1) AS TIEMPO_PROC_SEG,
        ROUND(SUM(CASE WHEN POSIBLE_FRAUDE_DOCUMENTO = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_FRAUDE_DOC,
        ROUND(AVG(CALIDAD_IMAGEN), 2) AS CALIDAD_IMAGEN_PROM,
        ROUND(SUM(CASE WHEN DATOS_VALIDADOS_CONTRA_BD = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_VALIDADOS_BD,
        COUNT(*) AS TOTAL_DOCUMENTOS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Confianza OCR Prom", "Campos Extraidos Prom", "% Revision Manual", "Tiempo Proc (seg)", "% Fraude Doc", "Calidad Imagen Prom", "% Validados BD", "Total Documentos"],
    "kpi_formats": ["{:.3f}", "{:.1f}", "{:.2f}%", "{:.1f} seg", "{:.2f}%", "{:.2f}", "{:.2f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_HORA_RECEPCION) AS MES,
        ROUND(AVG(CONFIANZA_OCR_PROMEDIO), 3) AS CONFIANZA_OCR,
        ROUND(SUM(CASE WHEN POSIBLE_FRAUDE_DOCUMENTO = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_FRAUDE
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Confianza OCR", "% Fraude"],
    "treemap_query": """SELECT TIPO_DOCUMENTO, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Tipo de Documento"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Procesamiento Documentos")

df_heat = run_query(f"SELECT TIPO_DOCUMENTO, CANAL_INGRESO, ROUND(AVG(CONFIANZA_OCR_PROMEDIO), 3) AS CONFIANZA FROM {TABLE} GROUP BY 1, 2")
pivot_heat = df_heat.pivot_table(index="TIPO_DOCUMENTO", columns="CANAL_INGRESO", values="CONFIANZA", aggfunc="mean")
fig_heat = go.Figure(go.Heatmap(z=pivot_heat.values, x=pivot_heat.columns.tolist(), y=pivot_heat.index.tolist(), colorscale=[[0, NAVY[0]], [0.5, NAVY[3]], [1, "#A5F3FC"]], texttemplate="%{z:.3f}", textfont=dict(size=10)))
fig_heat.update_layout(title="Confianza OCR por Tipo Documento × Canal Ingreso", **CHART_LAYOUT)
st.plotly_chart(fig_heat, use_container_width=True)

col1, col2 = st.columns(2)

df_scatter = run_query(f"SELECT CONFIANZA_OCR_PROMEDIO, CALIDAD_IMAGEN, ESTADO_PROCESAMIENTO FROM {TABLE}")
fig_scatter = px.scatter(df_scatter, x="CALIDAD_IMAGEN", y="CONFIANZA_OCR_PROMEDIO", color="ESTADO_PROCESAMIENTO", color_discrete_sequence=NAVY)
fig_scatter.update_layout(title="Confianza OCR vs Calidad Imagen", **CHART_LAYOUT)
col1.plotly_chart(fig_scatter, use_container_width=True)

df_funnel = run_query(f"SELECT COUNT(*) AS TOTAL, SUM(CASE WHEN ESTADO_PROCESAMIENTO = 'Procesado OK' THEN 1 ELSE 0 END) AS PROCESADOS, SUM(CASE WHEN ESTADO_PROCESAMIENTO = 'Procesado OK' AND REQUIRIO_REVISION_MANUAL = FALSE THEN 1 ELSE 0 END) AS SIN_REVISION, SUM(CASE WHEN DATOS_VALIDADOS_CONTRA_BD = TRUE THEN 1 ELSE 0 END) AS VALIDADOS_BD FROM {TABLE}")
fig_funnel = go.Figure(go.Funnel(y=["Recibidos", "Procesados OK", "Sin Revision Manual", "Validados BD"], x=[df_funnel["TOTAL"].iloc[0], df_funnel["PROCESADOS"].iloc[0], df_funnel["SIN_REVISION"].iloc[0], df_funnel["VALIDADOS_BD"].iloc[0]], marker=dict(color=NAVY[:4])))
fig_funnel.update_layout(title="Funnel de Procesamiento", **CHART_LAYOUT)
col2.plotly_chart(fig_funnel, use_container_width=True)

col3, col4 = st.columns(2)

df_donut = run_query(f"SELECT MOTOR_OCR, COUNT(*) AS N FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC")
fig_donut = go.Figure(go.Pie(labels=df_donut["MOTOR_OCR"], values=df_donut["N"], hole=0.5, marker=dict(colors=NAVY)))
fig_donut.update_layout(title="Distribucion por Motor OCR", **CHART_LAYOUT)
col3.plotly_chart(fig_donut, use_container_width=True)

df_fraude = run_query(f"SELECT TIPO_DOCUMENTO, ROUND(SUM(CASE WHEN POSIBLE_FRAUDE_DOCUMENTO = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_FRAUDE FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC")
fig_fraude = go.Figure(go.Bar(x=df_fraude["TIPO_DOCUMENTO"], y=df_fraude["PCT_FRAUDE"], marker_color=NAVY[0]))
fig_fraude.update_layout(title="% Fraude Documental por Tipo Documento", yaxis_title="% Fraude", **CHART_LAYOUT)
col4.plotly_chart(fig_fraude, use_container_width=True)

df_box = run_query(f"SELECT TIPO_DOCUMENTO, TIEMPO_PROCESAMIENTO_SEG FROM {TABLE}")
fig_box = px.box(df_box, x="TIPO_DOCUMENTO", y="TIEMPO_PROCESAMIENTO_SEG", color="TIPO_DOCUMENTO", color_discrete_sequence=NAVY)
fig_box.update_layout(title="Distribucion Tiempo Procesamiento (seg) por Tipo Documento", showlegend=False, **CHART_LAYOUT)
st.plotly_chart(fig_box, use_container_width=True)
