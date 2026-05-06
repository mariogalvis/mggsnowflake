from app_pages.page_template import render_page

config = {
    "key": "cop",
    "table": "MGG_BANCA.DATOS_Y_AI.COPILOTOS_EMPLEADOS",
    "icon": ":material/assistant:",
    "title": "Copilotos Empleados",
    "subtitle": "Uso de copilotos AI por empleados de sucursal para insights de cliente y generacion de ventas.",
    "cards": ["TIPO_INSIGHT", "ROL_EMPLEADO", "CIUDAD_SUCURSAL"],
    "date_col": "FECHA_HORA_USO",
    "filter_cols": ["TIPO_INSIGHT", "ROL_EMPLEADO", "CIUDAD_SUCURSAL"],
    "kpi_query": """SELECT
        AVG(UTILIDAD_REPORTADA),
        AVG(CASE WHEN GENERO_VENTA THEN 1 ELSE 0 END)*100,
        AVG(VALOR_VENTA_COP)/1e6,
        AVG(TIEMPO_AHORRADO_MIN),
        AVG(ADOPCION_SCORE),
        AVG(CASE WHEN FEEDBACK_POSITIVO THEN 1 ELSE 0 END)*100,
        AVG(CLIENTES_BENEFICIADOS),
        COUNT(*)
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Utilidad reportada prom", "% genero venta", "Valor venta (M)", "Tiempo ahorrado (min)", "Adopcion prom", "% feedback positivo", "Clientes beneficiados prom", "Total interacciones"],
    "kpi_formats": [",.2f", ",.1f", ",.1f", ",.1f", ",.2f", ",.1f", ",.1f", ",.0f"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_HORA_USO::DATE) AS MES, AVG(ADOPCION_SCORE) AS ADOPCION_SCORE, AVG(CASE WHEN GENERO_VENTA THEN 1 ELSE 0 END)*100 AS PCT_VENTA FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["ADOPCION_SCORE", "PCT_VENTA"],
    "treemap_query": """SELECT TIPO_INSIGHT AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Tipo de Insight"},
    "geo_query": """SELECT CIUDAD_SUCURSAL AS CITY, AVG(ADOPCION_SCORE) AS VALUE FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Adopcion por Ciudad", "metric": "ADOPCION_SCORE"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Copilotos")

df_hm = run_query(f"SELECT ROL_EMPLEADO, TIPO_INSIGHT, AVG(ADOPCION_SCORE) AS ADOPCION FROM {TABLE} GROUP BY 1,2")
df_piv = df_hm.pivot(index="ROL_EMPLEADO", columns="TIPO_INSIGHT", values="ADOPCION").fillna(0)
fig_hm = go.Figure(go.Heatmap(z=df_piv.values, x=df_piv.columns.tolist(), y=df_piv.index.tolist(), colorscale=[[0,"#FAFBFC"],[1,"#0F2B46"]]))
fig_hm.update_layout(**CHART_LAYOUT, title="Adopcion Score por Rol y Tipo Insight", xaxis_title="Tipo Insight", yaxis_title="Rol Empleado")
st.plotly_chart(fig_hm, use_container_width=True)

c1, c2 = st.columns(2)
with c1:
    df_sc = run_query(f"SELECT ADOPCION_SCORE, UTILIDAD_REPORTADA, ROL_EMPLEADO FROM {TABLE}")
    fig_sc = px.scatter(df_sc, x="ADOPCION_SCORE", y="UTILIDAD_REPORTADA", color="ROL_EMPLEADO", color_discrete_sequence=NAVY)
    fig_sc.update_layout(**CHART_LAYOUT, title="Adopcion vs Utilidad Reportada", xaxis_title="Adopcion Score", yaxis_title="Utilidad Reportada")
    st.plotly_chart(fig_sc, use_container_width=True)
with c2:
    df_dn = run_query(f"SELECT TIPO_TRIGGER, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
    fig_dn = px.pie(df_dn, names="TIPO_TRIGGER", values="N", hole=0.45, color_discrete_sequence=NAVY)
    fig_dn.update_layout(**CHART_LAYOUT, title="Distribucion por Tipo Trigger")
    st.plotly_chart(fig_dn, use_container_width=True)

df_fn = run_query(f"SELECT COUNT(*) AS TOTAL, SUM(CASE WHEN FEEDBACK_POSITIVO THEN 1 ELSE 0 END) AS FEEDBACK_POS, SUM(CASE WHEN GENERO_VENTA THEN 1 ELSE 0 END) AS VENTAS FROM {TABLE}")
fig_fn = go.Figure(go.Funnel(y=["Total Interacciones", "Feedback Positivo", "Genero Venta"], x=[df_fn["TOTAL"].iloc[0], df_fn["FEEDBACK_POS"].iloc[0], df_fn["VENTAS"].iloc[0]], marker=dict(color=NAVY[:3])))
fig_fn.update_layout(**CHART_LAYOUT, title="Funnel de Conversion")
st.plotly_chart(fig_fn, use_container_width=True)

df_box = run_query(f"SELECT ROL_EMPLEADO, TIEMPO_AHORRADO_MIN FROM {TABLE}")
fig_box = px.box(df_box, x="ROL_EMPLEADO", y="TIEMPO_AHORRADO_MIN", color="ROL_EMPLEADO", color_discrete_sequence=NAVY)
fig_box.update_layout(**CHART_LAYOUT, title="Tiempo Ahorrado por Rol", showlegend=False, xaxis_title="Rol Empleado", yaxis_title="Tiempo (min)")
st.plotly_chart(fig_box, use_container_width=True)
