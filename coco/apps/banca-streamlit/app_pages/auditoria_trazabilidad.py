from app_pages.page_template import render_page

config = {
    "key": "aud",
    "table": "MGG_BANCA.CUMPLIMIENTO_Y_REGULACION.AUDITORIA_TRAZABILIDAD",
    "icon": ":material/history:",
    "title": "Auditoria y Trazabilidad",
    "subtitle": "Registro y analisis de acciones en sistemas criticos con deteccion de accesos fuera de norma.",
    "cards": ["ACCION_REALIZADA", "SISTEMA_ORIGEN", "CLASIFICACION_RIESGO"],
    "date_col": "FECHA_HORA_EVENTO",
    "filter_cols": ["ACCION_REALIZADA", "SISTEMA_ORIGEN", "CLASIFICACION_RIESGO"],
    "kpi_query": """SELECT
        AVG(CASE WHEN ACCION_SENSIBLE THEN 1 ELSE 0 END)*100,
        AVG(CASE WHEN FUERA_HORARIO_LABORAL THEN 1 ELSE 0 END)*100,
        AVG(CASE WHEN ALERTA_SEGURIDAD THEN 1 ELSE 0 END)*100,
        AVG(CASE WHEN REQUIRIO_DOBLE_AUTORIZACION THEN 1 ELSE 0 END)*100,
        AVG(DURACION_SESION_MIN),
        AVG(CASE WHEN DATOS_CLIENTE_ACCEDIDOS THEN 1 ELSE 0 END)*100,
        AVG(CASE WHEN REVISADO_POR_AUDITORIA THEN 1 ELSE 0 END)*100,
        COUNT(*)
    FROM {table} WHERE {where}""",
    "kpi_labels": ["% acciones sensibles", "% fuera horario", "% alerta seguridad", "% doble autorizacion", "Duracion sesion prom", "% datos cliente accedidos", "% revisado auditoria", "Total logs"],
    "kpi_formats": [",.1f", ",.1f", ",.1f", ",.1f", ",.1f", ",.1f", ",.1f", ",.0f"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_HORA_EVENTO::DATE) AS MES, AVG(CASE WHEN ALERTA_SEGURIDAD THEN 1 ELSE 0 END)*100 AS PCT_ALERTA, COUNT(*) AS TOTAL_LOGS FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["PCT_ALERTA", "TOTAL_LOGS"],
    "treemap_query": """SELECT ACCION_REALIZADA AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Accion Realizada"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Auditoria")

df_hm = run_query(f"SELECT SISTEMA_ORIGEN, AREA_USUARIO, AVG(CASE WHEN ACCION_SENSIBLE THEN 1 ELSE 0 END)*100 AS PCT_SENSIBLE FROM {TABLE} GROUP BY 1,2")
df_piv = df_hm.pivot(index="SISTEMA_ORIGEN", columns="AREA_USUARIO", values="PCT_SENSIBLE").fillna(0)
fig_hm = go.Figure(go.Heatmap(z=df_piv.values, x=df_piv.columns.tolist(), y=df_piv.index.tolist(), colorscale=[[0,"#FAFBFC"],[1,"#0F2B46"]]))
fig_hm.update_layout(**CHART_LAYOUT, title="% Acciones Sensibles por Sistema y Area", xaxis_title="Area Usuario", yaxis_title="Sistema Origen")
st.plotly_chart(fig_hm, use_container_width=True)

c1, c2 = st.columns(2)
with c1:
    df_sc = run_query(f"SELECT AREA_USUARIO, AVG(DURACION_SESION_MIN) AS DURACION, COUNT(*) AS N, CLASIFICACION_RIESGO FROM {TABLE} GROUP BY 1,4")
    fig_sc = px.scatter(df_sc, x="DURACION", y="N", color="CLASIFICACION_RIESGO", color_discrete_sequence=NAVY)
    fig_sc.update_layout(**CHART_LAYOUT, title="Duracion Sesion vs Eventos por Area", xaxis_title="Duracion Sesion (min)", yaxis_title="Cantidad Eventos")
    st.plotly_chart(fig_sc, use_container_width=True)
with c2:
    df_dn = run_query(f"SELECT RESULTADO, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
    fig_dn = px.pie(df_dn, names="RESULTADO", values="N", hole=0.45, color_discrete_sequence=NAVY)
    fig_dn.update_layout(**CHART_LAYOUT, title="Distribucion por Resultado")
    st.plotly_chart(fig_dn, use_container_width=True)

df_fn = run_query(f"SELECT COUNT(*) AS TOTAL, SUM(CASE WHEN ACCION_SENSIBLE THEN 1 ELSE 0 END) AS SENSIBLES, SUM(CASE WHEN FUERA_HORARIO_LABORAL THEN 1 ELSE 0 END) AS FUERA_HORARIO, SUM(CASE WHEN ALERTA_SEGURIDAD THEN 1 ELSE 0 END) AS ALERTAS FROM {TABLE}")
fig_fn = go.Figure(go.Funnel(y=["Total Eventos", "Sensibles", "Fuera Horario", "Alerta Seguridad"], x=[df_fn["TOTAL"].iloc[0], df_fn["SENSIBLES"].iloc[0], df_fn["FUERA_HORARIO"].iloc[0], df_fn["ALERTAS"].iloc[0]], marker=dict(color=NAVY[:4])))
fig_fn.update_layout(**CHART_LAYOUT, title="Funnel de Riesgo")
st.plotly_chart(fig_fn, use_container_width=True)

df_box = run_query(f"SELECT TIPO_ACCESO, DURACION_SESION_MIN FROM {TABLE}")
fig_box = px.box(df_box, x="TIPO_ACCESO", y="DURACION_SESION_MIN", color="TIPO_ACCESO", color_discrete_sequence=NAVY)
fig_box.update_layout(**CHART_LAYOUT, title="Duracion Sesion por Tipo de Acceso", showlegend=False, xaxis_title="Tipo Acceso", yaxis_title="Duracion (min)")
st.plotly_chart(fig_box, use_container_width=True)
