from app_pages.page_template import render_page

config = {
    "key": "per",
    "table": "MGG_TELCO.GESTION_DE_RED.PLANIFICACION_EXPANSION_RED",
    "icon": ":material/map:",
    "title": "Expansion de Red",
    "subtitle": "Planificacion de proyectos de expansion priorizados por demanda, ROI y estrategia competitiva.",
    "cards": [
        "Prioriza donde expandir la red basado en demanda proyectada, gap de cobertura vs competencia y viabilidad financiera de cada proyecto.",
        "Integra demanda proyectada, cobertura actual vs competencia, crecimiento de trafico, inversion requerida y ROI esperado para cada zona.",
        "Maximiza retorno de CAPEX de expansion. Reduce time-to-market en zonas criticas y cierra gap competitivo en cobertura.",
    ],
    "date_col": "FECHA_PROPUESTA",
    "filter_cols": ["TECNOLOGIA", "DEPARTAMENTO", "ESTADO"],
    "kpi_query": """SELECT
        COUNT(*) AS PROYECTOS,
        ROUND(SUM(INVERSION_COP)/1e9, 1) AS INV_B,
        ROUND(AVG(ROI_PROYECTADO)*100, 1) AS ROI_PCT,
        ROUND(AVG(PAYBACK_MESES), 0) AS PAYBACK,
        ROUND(SUM(POBLACION_BENEFICIADA)/1e6, 2) AS POB_M,
        ROUND(AVG(COBERTURA_OBJETIVO_PCT)*100, 1) AS COB_OBJ,
        ROUND(SUM(SITIOS_NUEVOS), 0) AS SITIOS,
        ROUND(AVG(SCORE_PRIORIZACION), 1) AS SCORE_PRIO
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Proyectos", "Inversion (B)", "ROI prom.", "Payback (meses)", "Poblacion (M)", "Cobertura obj.", "Sitios nuevos", "Score priorizacion"],
    "kpi_formats": ["{:,.0f}", "${:.1f}B", "{:.1f}%", "{:.0f}", "{:.2f}M", "{:.1f}%", "{:,.0f}", "{:.1f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_PROPUESTA) AS MES,
        COUNT(*) AS PROYECTOS,
        ROUND(AVG(ROI_PROYECTADO)*100, 1) AS ROI
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Proyectos", "ROI (%)"],
    "treemap_query": """SELECT TECNOLOGIA, ROUND(SUM(INVERSION_COP)/1e9, 1) AS INV_B
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Inversion por tecnologia (B COP)"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Expansion")

# SCATTER - FULL WIDTH
st.markdown("**Scatter: ROI Proyectado vs Inversion (Tamano = Poblacion Beneficiada)**")
sc_df = run_query(f"""
    SELECT CODIGO_PROYECTO, TECNOLOGIA,
        ROUND(ROI_PROYECTADO*100, 1) AS ROI_PCT,
        ROUND(INVERSION_COP/1e9, 2) AS INV_B,
        POBLACION_BENEFICIADA,
        PAYBACK_MESES
    FROM {TABLE}
""")
if not sc_df.empty:
    fig_sc = px.scatter(sc_df, x="INV_B", y="ROI_PCT", size="POBLACION_BENEFICIADA", color="TECNOLOGIA",
                       color_discrete_sequence=NAVY, size_max=45,
                       hover_name="CODIGO_PROYECTO",
                       hover_data={"PAYBACK_MESES": True, "POBLACION_BENEFICIADA": ":,.0f"},
                       labels={"INV_B": "Inversion (B COP)", "ROI_PCT": "ROI Proyectado (%)"})
    fig_sc.update_layout(**CHART_LAYOUT, height=450, margin=dict(l=40, r=20, t=30, b=40),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.5, xanchor="center"))
    st.plotly_chart(fig_sc, use_container_width=True)

# BAR + FUNNEL side by side
col_a, col_b = st.columns(2)

with col_a:
    st.markdown("**Bar: Inversion por Departamento (Top 10)**")
    bar_df = run_query(f"""
        SELECT DEPARTAMENTO, ROUND(SUM(INVERSION_COP)/1e9, 2) AS INV_B
        FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """)
    if not bar_df.empty:
        fig_bar = go.Figure(go.Bar(
            y=bar_df["DEPARTAMENTO"].tolist()[::-1],
            x=bar_df["INV_B"].tolist()[::-1],
            orientation="h",
            marker_color="#1B3A5C",
            text=[f"${v:.2f}B" for v in bar_df["INV_B"].tolist()[::-1]],
            textposition="outside",
        ))
        fig_bar.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=120, r=40, t=30, b=40),
                            xaxis=dict(title="Inversion (B COP)", gridcolor="#E2E8F0"))
        st.plotly_chart(fig_bar, use_container_width=True)

with col_b:
    st.markdown("**Funnel: Proyectos por Estado**")
    fn_df = run_query(f"""
        SELECT ESTADO, COUNT(*) AS N
        FROM {TABLE} GROUP BY 1 ORDER BY N DESC
    """)
    if not fn_df.empty:
        fig_fn = go.Figure(go.Funnel(
            y=fn_df["ESTADO"].tolist(),
            x=fn_df["N"].tolist(),
            textposition="inside",
            textinfo="value+percent initial",
            marker=dict(color=NAVY[:len(fn_df)]),
            connector=dict(line=dict(color="#E2E8F0", width=1)),
        ))
        fig_fn.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=20, r=20, t=30, b=20))
        st.plotly_chart(fig_fn, use_container_width=True)

# STACKED BAR - FULL WIDTH
st.markdown("**Stacked Bar: Proyectos por Tecnologia y Estado**")
stack_df = run_query(f"""
    SELECT TECNOLOGIA, ESTADO, COUNT(*) AS N
    FROM {TABLE} GROUP BY 1, 2 ORDER BY 1
""")
if not stack_df.empty:
    fig_stack = px.bar(stack_df, x="TECNOLOGIA", y="N", color="ESTADO",
                      color_discrete_sequence=NAVY,
                      labels={"N": "Proyectos", "TECNOLOGIA": ""})
    fig_stack.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=40, r=20, t=30, b=60),
                          barmode="stack", yaxis=dict(gridcolor="#E2E8F0"),
                          legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.5, xanchor="center"))
    st.plotly_chart(fig_stack, use_container_width=True)

# HISTOGRAM - FULL WIDTH
st.markdown("**Histograma: Distribucion de Payback (meses)**")
hist_df = run_query(f"SELECT PAYBACK_MESES FROM {TABLE}")
if not hist_df.empty:
    fig_hist = px.histogram(hist_df, x="PAYBACK_MESES", nbins=20,
                           color_discrete_sequence=["#1B3A5C"],
                           labels={"PAYBACK_MESES": "Payback (meses)"})
    fig_hist.add_vline(x=36, line_dash="dash", line_color="#E85D29", annotation_text="Meta 36 meses",
                      annotation_position="top right")
    fig_hist.update_layout(**CHART_LAYOUT, height=350, margin=dict(l=40, r=20, t=30, b=40),
                          yaxis=dict(title="Frecuencia", gridcolor="#E2E8F0"),
                          xaxis=dict(title="Payback (meses)"),
                          bargap=0.05, showlegend=False)
    st.plotly_chart(fig_hist, use_container_width=True)
