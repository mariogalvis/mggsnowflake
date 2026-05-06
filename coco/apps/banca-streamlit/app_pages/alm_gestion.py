from app_pages.page_template import render_page

config = {
    "key": "alm",
    "table": "MGG_BANCA.TESORERIA_Y_RIESGO_FINANCIERO.ALM_GESTION",
    "icon": ":material/account_balance:",
    "title": "ALM Gestion",
    "subtitle": "Gestion de activos y pasivos con analisis de gaps, duration y sensibilidad a tasas.",
    "cards": ["TIPO_INSTRUMENTO", "CLASIFICACION_ALM", "BANDA_TEMPORAL"],
    "date_col": "FECHA_CORTE",
    "filter_cols": ["TIPO_INSTRUMENTO", "CLASIFICACION_ALM", "BANDA_TEMPORAL"],
    "kpi_query": """SELECT
        SUM(MONTO_NOMINAL_COP)/1e9,
        SUM(MONTO_VALOR_PRESENTE_COP)/1e9,
        AVG(DURATION_ANOS),
        AVG(SENSIBILIDAD_100BPS_PCT),
        SUM(GAP_ACUMULADO_COP)/1e9,
        AVG(RATIO_COBERTURA),
        AVG(TASA_PACTADA_EA_PCT),
        COUNT(*)
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Monto nominal (B)", "VP total (B)", "Duration prom", "Sensibilidad 100bps", "GAP acum (B)", "Ratio cobertura", "Tasa pactada prom", "Total posiciones"],
    "kpi_formats": [",.1f", ",.1f", ",.2f", ",.2f", ",.1f", ",.2f", ",.2f", ",.0f"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_CORTE) AS MES, SUM(MONTO_NOMINAL_COP)/1e9 AS MONTO_NOMINAL_B, AVG(DURATION_ANOS) AS DURATION_PROM FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["MONTO_NOMINAL_B", "DURATION_PROM"],
    "treemap_query": """SELECT TIPO_INSTRUMENTO AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Tipo de Instrumento"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de ALM")

df_bar = run_query(f"SELECT BANDA_TEMPORAL, CLASIFICACION_ALM, SUM(MONTO_VALOR_PRESENTE_COP)/1e6 AS VP_M FROM {TABLE} GROUP BY 1,2 ORDER BY 1")
fig_bar = px.bar(df_bar, x="BANDA_TEMPORAL", y="VP_M", color="CLASIFICACION_ALM", color_discrete_sequence=NAVY, barmode="stack")
fig_bar.update_layout(**CHART_LAYOUT, title="Monto VP por Banda Temporal y Clasificacion ALM", xaxis_title="Banda Temporal", yaxis_title="VP (Millones COP)")
st.plotly_chart(fig_bar, use_container_width=True)

c1, c2 = st.columns(2)
with c1:
    df_sc = run_query(f"SELECT DURATION_ANOS, SENSIBILIDAD_100BPS_PCT, TIPO_INSTRUMENTO FROM {TABLE}")
    fig_sc = px.scatter(df_sc, x="DURATION_ANOS", y="SENSIBILIDAD_100BPS_PCT", color="TIPO_INSTRUMENTO", color_discrete_sequence=NAVY)
    fig_sc.update_layout(**CHART_LAYOUT, title="Duration vs Sensibilidad 100bps", xaxis_title="Duration (anos)", yaxis_title="Sensibilidad 100bps (%)")
    st.plotly_chart(fig_sc, use_container_width=True)
with c2:
    df_dn = run_query(f"SELECT TIPO_TASA, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
    fig_dn = px.pie(df_dn, names="TIPO_TASA", values="N", hole=0.45, color_discrete_sequence=NAVY)
    fig_dn.update_layout(**CHART_LAYOUT, title="Distribucion por Tipo de Tasa")
    st.plotly_chart(fig_dn, use_container_width=True)

df_box = run_query(f"SELECT TIPO_INSTRUMENTO, TASA_PACTADA_EA_PCT FROM {TABLE}")
fig_box = px.box(df_box, x="TIPO_INSTRUMENTO", y="TASA_PACTADA_EA_PCT", color="TIPO_INSTRUMENTO", color_discrete_sequence=NAVY)
fig_box.update_layout(**CHART_LAYOUT, title="Tasa Pactada EA por Tipo de Instrumento", showlegend=False, xaxis_title="Tipo Instrumento", yaxis_title="Tasa EA (%)")
st.plotly_chart(fig_box, use_container_width=True)

df_wf = run_query(f"SELECT BANDA_TEMPORAL, SUM(GAP_ACUMULADO_COP)/1e6 AS GAP_M FROM {TABLE} GROUP BY 1 ORDER BY 1")
fig_wf = go.Figure(go.Waterfall(x=df_wf["BANDA_TEMPORAL"], y=df_wf["GAP_M"], connector=dict(line=dict(color="#29B5E8")), increasing=dict(marker=dict(color="#0F2B46")), decreasing=dict(marker=dict(color="#2E7D8C"))))
fig_wf.update_layout(**CHART_LAYOUT, title="GAP Acumulado por Banda Temporal", xaxis_title="Banda Temporal", yaxis_title="GAP (Millones COP)")
st.plotly_chart(fig_wf, use_container_width=True)
