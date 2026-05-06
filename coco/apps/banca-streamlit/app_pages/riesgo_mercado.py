from app_pages.page_template import render_page

config = {
    "key": "rmk",
    "table": "MGG_BANCA.TESORERIA_Y_RIESGO_FINANCIERO.RIESGO_MERCADO",
    "icon": ":material/show_chart:",
    "title": "Riesgo de Mercado",
    "subtitle": "Medicion de VaR, stress testing y P&L con monitoreo de limites y backtesting.",
    "cards": ["PORTAFOLIO", "ESCENARIO_STRESS", "LIBRO"],
    "date_col": "FECHA_VALORACION",
    "filter_cols": ["PORTAFOLIO", "ESCENARIO_STRESS", "LIBRO"],
    "kpi_query": """SELECT
        AVG(VAR_95_1D_COP)/1e6,
        AVG(VAR_99_1D_COP)/1e6,
        AVG(CVAR_99_1D_COP)/1e6,
        AVG(USO_LIMITE_PCT),
        AVG(RATIO_SHARPE),
        AVG(VOLATILIDAD_ANUALIZADA_PCT),
        AVG(PNL_DIARIO_COP)/1e6,
        COUNT(*)
    FROM {table} WHERE {where}""",
    "kpi_labels": ["VaR 95 (M)", "VaR 99 (M)", "CVaR 99 (M)", "Uso limite prom", "Sharpe prom", "Volatilidad prom", "PnL diario prom (M)", "Total registros"],
    "kpi_formats": [",.1f", ",.1f", ",.1f", ",.2f", ",.2f", ",.2f", ",.1f", ",.0f"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_VALORACION) AS MES, AVG(VAR_99_1D_COP)/1e6 AS VAR_99_M, AVG(PNL_DIARIO_COP)/1e6 AS PNL_DIARIO_M FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["VAR_99_M", "PNL_DIARIO_M"],
    "treemap_query": """SELECT PORTAFOLIO AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Portafolio"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Riesgo de Mercado")

df_var = run_query(f"SELECT PORTAFOLIO, AVG(VAR_95_1D_COP)/1e6 AS VAR_95_M, AVG(VAR_99_1D_COP)/1e6 AS VAR_99_M FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC")
fig_var = go.Figure()
fig_var.add_trace(go.Bar(x=df_var["PORTAFOLIO"], y=df_var["VAR_95_M"], name="VaR 95", marker_color="#0F2B46"))
fig_var.add_trace(go.Bar(x=df_var["PORTAFOLIO"], y=df_var["VAR_99_M"], name="VaR 99", marker_color="#29B5E8"))
fig_var.update_layout(**CHART_LAYOUT, barmode="group", title="VaR 95 y VaR 99 por Portafolio", xaxis_title="Portafolio", yaxis_title="VaR (Millones COP)")
st.plotly_chart(fig_var, use_container_width=True)

c1, c2 = st.columns(2)
with c1:
    df_sc = run_query(f"SELECT RATIO_SHARPE, VOLATILIDAD_ANUALIZADA_PCT, PORTAFOLIO, VALOR_MERCADO_COP/1e9 AS VM_B FROM {TABLE}")
    fig_sc = px.scatter(df_sc, x="RATIO_SHARPE", y="VOLATILIDAD_ANUALIZADA_PCT", color="PORTAFOLIO", size="VM_B", color_discrete_sequence=NAVY)
    fig_sc.update_layout(**CHART_LAYOUT, title="Sharpe vs Volatilidad", xaxis_title="Ratio Sharpe", yaxis_title="Volatilidad (%)")
    st.plotly_chart(fig_sc, use_container_width=True)
with c2:
    df_dn = run_query(f"SELECT FACTOR_RIESGO_PRINCIPAL, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
    fig_dn = px.pie(df_dn, names="FACTOR_RIESGO_PRINCIPAL", values="N", hole=0.45, color_discrete_sequence=NAVY)
    fig_dn.update_layout(**CHART_LAYOUT, title="Distribucion por Factor de Riesgo")
    st.plotly_chart(fig_dn, use_container_width=True)

df_hm = run_query(f"SELECT PORTAFOLIO, ESCENARIO_STRESS, AVG(USO_LIMITE_PCT) AS USO FROM {TABLE} GROUP BY 1,2")
df_piv = df_hm.pivot(index="PORTAFOLIO", columns="ESCENARIO_STRESS", values="USO").fillna(0)
fig_hm = go.Figure(go.Heatmap(z=df_piv.values, x=df_piv.columns.tolist(), y=df_piv.index.tolist(), colorscale=[[0,"#FAFBFC"],[1,"#0F2B46"]]))
fig_hm.update_layout(**CHART_LAYOUT, title="Uso Limite (%) por Portafolio y Escenario Stress", xaxis_title="Escenario Stress", yaxis_title="Portafolio")
st.plotly_chart(fig_hm, use_container_width=True)

df_box = run_query(f"SELECT PORTAFOLIO, PNL_DIARIO_COP/1e6 AS PNL_M FROM {TABLE}")
fig_box = px.box(df_box, x="PORTAFOLIO", y="PNL_M", color="PORTAFOLIO", color_discrete_sequence=NAVY)
fig_box.update_layout(**CHART_LAYOUT, title="PnL Diario por Portafolio", showlegend=False, xaxis_title="Portafolio", yaxis_title="PnL (Millones COP)")
st.plotly_chart(fig_box, use_container_width=True)
