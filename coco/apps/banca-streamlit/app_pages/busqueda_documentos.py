from app_pages.page_template import render_page

config = {
    "key": "bdc",
    "table": "MGG_BANCA.DATOS_Y_AI.BUSQUEDA_DOCUMENTOS",
    "icon": ":material/search:",
    "title": "Busqueda Documentos",
    "subtitle": "Busqueda semantica en repositorios documentales financieros con RAG y ranking de relevancia.",
    "cards": ["TIPO_DOCUMENTO_TOP1", "METODO_BUSQUEDA", "AREA_SOLICITANTE"],
    "date_col": "FECHA_HORA_BUSQUEDA",
    "filter_cols": ["TIPO_DOCUMENTO_TOP1", "METODO_BUSQUEDA", "AREA_SOLICITANTE"],
    "kpi_query": """SELECT
        AVG(RELEVANCIA_TOP1),
        AVG(CASE WHEN BUSQUEDA_EXITOSA THEN 1 ELSE 0 END)*100,
        AVG(RATING_RESULTADO),
        AVG(LATENCIA_BUSQUEDA_MS),
        AVG(PRECISION_BUSQUEDA),
        AVG(CHUNKS_RELEVANTES),
        AVG(RESULTADOS_ENCONTRADOS),
        COUNT(*)
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Relevancia top1 prom", "% busqueda exitosa", "Rating prom", "Latencia (ms)", "Precision prom", "Chunks relevantes prom", "Resultados prom", "Total busquedas"],
    "kpi_formats": [",.2f", ",.1f", ",.2f", ",.0f", ",.2f", ",.1f", ",.1f", ",.0f"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_HORA_BUSQUEDA::DATE) AS MES, AVG(RELEVANCIA_TOP1) AS RELEVANCIA_TOP1, AVG(LATENCIA_BUSQUEDA_MS) AS LATENCIA_MS FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["RELEVANCIA_TOP1", "LATENCIA_MS"],
    "treemap_query": """SELECT TIPO_DOCUMENTO_TOP1 AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
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
st.subheader(":material/bar_chart: Analisis Avanzado de Busqueda de Documentos")

df_hm = run_query(f"SELECT TIPO_DOCUMENTO_TOP1, AREA_SOLICITANTE, AVG(RELEVANCIA_TOP1) AS RELEVANCIA FROM {TABLE} GROUP BY 1,2")
df_piv = df_hm.pivot(index="TIPO_DOCUMENTO_TOP1", columns="AREA_SOLICITANTE", values="RELEVANCIA").fillna(0)
fig_hm = go.Figure(go.Heatmap(z=df_piv.values, x=df_piv.columns.tolist(), y=df_piv.index.tolist(), colorscale=[[0,"#FAFBFC"],[1,"#0F2B46"]]))
fig_hm.update_layout(**CHART_LAYOUT, title="Relevancia por Tipo Documento y Area", xaxis_title="Area Solicitante", yaxis_title="Tipo Documento")
st.plotly_chart(fig_hm, use_container_width=True)

c1, c2 = st.columns(2)
with c1:
    df_sc = run_query(f"SELECT RELEVANCIA_TOP1, PRECISION_BUSQUEDA, METODO_BUSQUEDA FROM {TABLE}")
    fig_sc = px.scatter(df_sc, x="RELEVANCIA_TOP1", y="PRECISION_BUSQUEDA", color="METODO_BUSQUEDA", color_discrete_sequence=NAVY)
    fig_sc.update_layout(**CHART_LAYOUT, title="Relevancia vs Precision", xaxis_title="Relevancia Top1", yaxis_title="Precision")
    st.plotly_chart(fig_sc, use_container_width=True)
with c2:
    df_dn = run_query(f"SELECT MOTOR_BUSQUEDA, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
    fig_dn = px.pie(df_dn, names="MOTOR_BUSQUEDA", values="N", hole=0.45, color_discrete_sequence=NAVY)
    fig_dn.update_layout(**CHART_LAYOUT, title="Distribucion por Motor de Busqueda")
    st.plotly_chart(fig_dn, use_container_width=True)

df_fn = run_query(f"SELECT COUNT(*) AS TOTAL, SUM(CASE WHEN BUSQUEDA_EXITOSA THEN 1 ELSE 0 END) AS EXITOSA, SUM(CASE WHEN BUSQUEDA_EXITOSA AND RELEVANCIA_TOP1 > 0.7 THEN 1 ELSE 0 END) AS RELEVANTE, SUM(CASE WHEN BUSQUEDA_EXITOSA AND RELEVANCIA_TOP1 > 0.7 AND RATING_RESULTADO >= 4 THEN 1 ELSE 0 END) AS ALTA_RATING FROM {TABLE}")
fig_fn = go.Figure(go.Funnel(y=["Total Busquedas", "Exitosa", "Doc Relevante", "Rating >= 4"], x=[df_fn["TOTAL"].iloc[0], df_fn["EXITOSA"].iloc[0], df_fn["RELEVANTE"].iloc[0], df_fn["ALTA_RATING"].iloc[0]], marker=dict(color=NAVY[:4])))
fig_fn.update_layout(**CHART_LAYOUT, title="Funnel de Busqueda")
st.plotly_chart(fig_fn, use_container_width=True)

df_box = run_query(f"SELECT METODO_BUSQUEDA, LATENCIA_BUSQUEDA_MS FROM {TABLE}")
fig_box = px.box(df_box, x="METODO_BUSQUEDA", y="LATENCIA_BUSQUEDA_MS", color="METODO_BUSQUEDA", color_discrete_sequence=NAVY)
fig_box.update_layout(**CHART_LAYOUT, title="Latencia por Metodo de Busqueda", showlegend=False, xaxis_title="Metodo Busqueda", yaxis_title="Latencia (ms)")
st.plotly_chart(fig_box, use_container_width=True)
