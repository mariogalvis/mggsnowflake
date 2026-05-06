from app_pages.page_template import render_page

config = {
    "key": "flq",
    "table": "MGG_BANCA.TESORERIA_Y_RIESGO_FINANCIERO.FORECAST_LIQUIDEZ",
    "icon": ":material/water_drop:",
    "title": "Forecast Liquidez",
    "subtitle": "Proyeccion de liquidez con escenarios de estres y cumplimiento de indicadores regulatorios.",
    "cards": ["ESCENARIO", "HORIZONTE", "ESTADO_REPORTE"],
    "date_col": "FECHA_PROYECCION",
    "filter_cols": ["ESCENARIO", "HORIZONTE", "ESTADO_REPORTE"],
    "kpi_query": """SELECT
        AVG(RATIO_COBERTURA_LIQUIDEZ),
        AVG(IRL_RATIO),
        AVG(CASE WHEN CUMPLE_REGULATORIO THEN 1 ELSE 0 END)*100,
        SUM(GAP_LIQUIDEZ_COP)/1e9,
        AVG(DIAS_SUPERVIVENCIA_ESTRES),
        SUM(COLCHON_LIQUIDEZ_COP)/1e9,
        AVG(CONFIANZA_PROYECCION),
        COUNT(*)
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Ratio cobertura prom", "IRL prom", "% cumple regulatorio", "GAP liquidez (B)", "Dias supervivencia", "Colchon (B)", "Confianza prom", "Total forecasts"],
    "kpi_formats": [",.2f", ",.2f", ",.1f", ",.1f", ",.1f", ",.1f", ",.2f", ",.0f"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_PROYECCION) AS MES, AVG(RATIO_COBERTURA_LIQUIDEZ) AS RATIO_COBERTURA, AVG(IRL_RATIO) AS IRL_RATIO FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["RATIO_COBERTURA", "IRL_RATIO"],
    "treemap_query": """SELECT ESCENARIO AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Escenario"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Liquidez")

df_io = run_query(f"SELECT ESCENARIO, SUM(INFLOWS_PROYECTADOS_COP)/1e9 AS INFLOWS_B, SUM(OUTFLOWS_PROYECTADOS_COP)/1e9 AS OUTFLOWS_B FROM {TABLE} GROUP BY 1 ORDER BY 1")
fig_io = go.Figure()
fig_io.add_trace(go.Bar(x=df_io["ESCENARIO"], y=df_io["INFLOWS_B"], name="Inflows", marker_color="#0F2B46"))
fig_io.add_trace(go.Bar(x=df_io["ESCENARIO"], y=df_io["OUTFLOWS_B"], name="Outflows", marker_color="#29B5E8"))
fig_io.update_layout(**CHART_LAYOUT, barmode="group", title="Inflows vs Outflows por Escenario", xaxis_title="Escenario", yaxis_title="Monto (Billones COP)")
st.plotly_chart(fig_io, use_container_width=True)

c1, c2 = st.columns(2)
with c1:
    df_sc = run_query(f"SELECT RATIO_COBERTURA_LIQUIDEZ, IRL_RATIO, ESCENARIO FROM {TABLE}")
    fig_sc = px.scatter(df_sc, x="RATIO_COBERTURA_LIQUIDEZ", y="IRL_RATIO", color="ESCENARIO", color_discrete_sequence=NAVY)
    fig_sc.update_layout(**CHART_LAYOUT, title="Cobertura vs IRL Ratio", xaxis_title="Ratio Cobertura Liquidez", yaxis_title="IRL Ratio")
    st.plotly_chart(fig_sc, use_container_width=True)
with c2:
    df_dn = run_query(f"SELECT METODO_PROYECCION, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
    fig_dn = px.pie(df_dn, names="METODO_PROYECCION", values="N", hole=0.45, color_discrete_sequence=NAVY)
    fig_dn.update_layout(**CHART_LAYOUT, title="Distribucion por Metodo de Proyeccion")
    st.plotly_chart(fig_dn, use_container_width=True)

df_box = run_query(f"SELECT ESCENARIO, DIAS_SUPERVIVENCIA_ESTRES FROM {TABLE}")
fig_box = px.box(df_box, x="ESCENARIO", y="DIAS_SUPERVIVENCIA_ESTRES", color="ESCENARIO", color_discrete_sequence=NAVY)
fig_box.update_layout(**CHART_LAYOUT, title="Dias de Supervivencia en Estres por Escenario", showlegend=False, xaxis_title="Escenario", yaxis_title="Dias")
st.plotly_chart(fig_box, use_container_width=True)

df_fn = run_query(f"SELECT COUNT(*) AS TOTAL, SUM(CASE WHEN CUMPLE_REGULATORIO THEN 1 ELSE 0 END) AS CUMPLE, SUM(CASE WHEN CONFIANZA_PROYECCION > 0.8 THEN 1 ELSE 0 END) AS ALTA_CONFIANZA FROM {TABLE}")
fig_fn = go.Figure(go.Funnel(y=["Total Forecasts", "Cumple Regulatorio", "Confianza > 0.8"], x=[df_fn["TOTAL"].iloc[0], df_fn["CUMPLE"].iloc[0], df_fn["ALTA_CONFIANZA"].iloc[0]], marker=dict(color=NAVY[:3])))
fig_fn.update_layout(**CHART_LAYOUT, title="Funnel de Cumplimiento")
st.plotly_chart(fig_fn, use_container_width=True)
