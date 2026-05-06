from app_pages.page_template import render_page

config = {
    "key": "rrg",
    "table": "MGG_BANCA.CUMPLIMIENTO_Y_REGULACION.REPORTES_REGULATORIOS",
    "icon": ":material/gavel:",
    "title": "Reportes Regulatorios",
    "subtitle": "Generacion y envio de reportes a Superfinanciera, UIAF y Banco de la Republica con control de calidad.",
    "cards": ["TIPO_REPORTE", "ENTIDAD_DESTINO", "ESTADO_REPORTE"],
    "date_col": "FECHA_CORTE",
    "filter_cols": ["TIPO_REPORTE", "ENTIDAD_DESTINO", "ESTADO_REPORTE"],
    "kpi_query": """SELECT
        AVG(TASA_CALIDAD_DATOS),
        AVG(CASE WHEN ENTREGADO_A_TIEMPO THEN 1 ELSE 0 END)*100,
        AVG(ERRORES_CRITICOS),
        AVG(TIEMPO_GENERACION_MIN),
        AVG(HORAS_ANTES_DEADLINE),
        AVG(CASE WHEN REQUIRIO_CORRECCION THEN 1 ELSE 0 END)*100,
        AVG(FUENTES_DATOS_INVOLUCRADAS),
        COUNT(*)
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Calidad datos prom", "% a tiempo", "Errores criticos prom", "Tiempo generacion (min)", "Horas antes deadline", "% requirio correccion", "Fuentes datos prom", "Total reportes"],
    "kpi_formats": [",.2f", ",.1f", ",.2f", ",.1f", ",.1f", ",.1f", ",.1f", ",.0f"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_CORTE) AS MES, AVG(TASA_CALIDAD_DATOS) AS CALIDAD_DATOS, AVG(TIEMPO_GENERACION_MIN) AS TIEMPO_GENERACION FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["CALIDAD_DATOS", "TIEMPO_GENERACION"],
    "treemap_query": """SELECT TIPO_REPORTE AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Tipo de Reporte"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Reportes Regulatorios")

df_hm = run_query(f"SELECT TIPO_REPORTE, ENTIDAD_DESTINO, AVG(TASA_CALIDAD_DATOS) AS CALIDAD FROM {TABLE} GROUP BY 1,2")
df_piv = df_hm.pivot(index="TIPO_REPORTE", columns="ENTIDAD_DESTINO", values="CALIDAD").fillna(0)
fig_hm = go.Figure(go.Heatmap(z=df_piv.values, x=df_piv.columns.tolist(), y=df_piv.index.tolist(), colorscale=[[0,"#FAFBFC"],[1,"#0F2B46"]]))
fig_hm.update_layout(**CHART_LAYOUT, title="Calidad de Datos por Tipo Reporte y Entidad", xaxis_title="Entidad Destino", yaxis_title="Tipo Reporte")
st.plotly_chart(fig_hm, use_container_width=True)

c1, c2 = st.columns(2)
with c1:
    df_sc = run_query(f"SELECT TASA_CALIDAD_DATOS, HORAS_ANTES_DEADLINE, ENTIDAD_DESTINO FROM {TABLE}")
    fig_sc = px.scatter(df_sc, x="TASA_CALIDAD_DATOS", y="HORAS_ANTES_DEADLINE", color="ENTIDAD_DESTINO", color_discrete_sequence=NAVY)
    fig_sc.update_layout(**CHART_LAYOUT, title="Calidad vs Horas antes de Deadline", xaxis_title="Tasa Calidad Datos", yaxis_title="Horas antes Deadline")
    st.plotly_chart(fig_sc, use_container_width=True)
with c2:
    df_dn = run_query(f"SELECT METODO_GENERACION, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
    fig_dn = px.pie(df_dn, names="METODO_GENERACION", values="N", hole=0.45, color_discrete_sequence=NAVY)
    fig_dn.update_layout(**CHART_LAYOUT, title="Distribucion por Metodo de Generacion")
    st.plotly_chart(fig_dn, use_container_width=True)

df_fn = run_query(f"SELECT COUNT(*) AS TOTAL, SUM(CASE WHEN ENTREGADO_A_TIEMPO THEN 1 ELSE 0 END) AS A_TIEMPO, SUM(CASE WHEN ENTREGADO_A_TIEMPO AND NOT REQUIRIO_CORRECCION THEN 1 ELSE 0 END) AS SIN_CORRECCION, SUM(CASE WHEN ENTREGADO_A_TIEMPO AND NOT REQUIRIO_CORRECCION AND ERRORES_CRITICOS = 0 THEN 1 ELSE 0 END) AS SIN_ERRORES FROM {TABLE}")
fig_fn = go.Figure(go.Funnel(y=["Total Reportes", "Entregado a Tiempo", "Sin Correcciones", "Sin Errores Criticos"], x=[df_fn["TOTAL"].iloc[0], df_fn["A_TIEMPO"].iloc[0], df_fn["SIN_CORRECCION"].iloc[0], df_fn["SIN_ERRORES"].iloc[0]], marker=dict(color=NAVY[:4])))
fig_fn.update_layout(**CHART_LAYOUT, title="Funnel de Calidad de Entrega")
st.plotly_chart(fig_fn, use_container_width=True)

df_box = run_query(f"SELECT ENTIDAD_DESTINO, TIEMPO_GENERACION_MIN FROM {TABLE}")
fig_box = px.box(df_box, x="ENTIDAD_DESTINO", y="TIEMPO_GENERACION_MIN", color="ENTIDAD_DESTINO", color_discrete_sequence=NAVY)
fig_box.update_layout(**CHART_LAYOUT, title="Tiempo de Generacion por Entidad Destino", showlegend=False, xaxis_title="Entidad Destino", yaxis_title="Tiempo (min)")
st.plotly_chart(fig_box, use_container_width=True)
