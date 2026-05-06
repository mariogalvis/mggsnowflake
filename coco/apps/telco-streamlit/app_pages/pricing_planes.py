from app_pages.page_template import render_page

config = {
    "key": "ppl",
    "table": "MGG_TELCO.MONETIZACION_Y_REVENUE.PRICING_PLANES",
    "icon": ":material/sell:",
    "title": "Pricing Planes",
    "subtitle": "Optimizacion de precios basada en elasticidad, competencia, willingness-to-pay y simulacion de impacto.",
    "cards": [
        "Determina el precio optimo de cada plan balanceando elasticidad de demanda, posicionamiento competitivo y maximizacion de revenue.",
        "Analiza precio vs competencia, elasticidad, willingness-to-pay, perceived value y simula impacto en suscriptores y revenue.",
        "Maximiza revenue sin perder competitividad. Cada punto de optimizacion en pricing puede representar miles de millones adicionales.",
    ],
    "date_col": "FECHA_ANALISIS",
    "filter_cols": ["ESTRATEGIA", "ESTADO", "PLAN"],
    "kpi_query": """SELECT
        ROUND(AVG(PRECIO_ACTUAL_COP)/1e3, 1) AS PRECIO_ACT_K,
        ROUND(AVG(PRECIO_SUGERIDO_COP)/1e3, 1) AS PRECIO_SUG_K,
        ROUND(AVG(CAMBIO_PRECIO_PCT), 1) AS CAMBIO_PCT,
        ROUND(AVG(ELASTICIDAD_PRECIO), 2) AS ELASTICIDAD,
        ROUND(AVG(IMPACTO_REVENUE_PCT), 1) AS IMP_REV_PCT,
        ROUND(AVG(WILLINGNESS_TO_PAY)*100, 1) AS WTP_PCT,
        ROUND(AVG(CONFIANZA)*100, 1) AS CONFIANZA,
        COUNT(*) AS ANALISIS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Precio actual (K)", "Precio sugerido (K)", "Cambio precio", "Elasticidad", "Impacto revenue", "WTP", "Confianza", "Analisis"],
    "kpi_formats": ["${:.1f}K", "${:.1f}K", "{:.1f}%", "{:.2f}", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_ANALISIS) AS MES,
        ROUND(AVG(IMPACTO_REVENUE_PCT), 1) AS IMPACTO_REV,
        ROUND(AVG(ELASTICIDAD_PRECIO), 2) AS ELASTICIDAD
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Impacto revenue (%)", "Elasticidad"],
    "treemap_query": """SELECT ESTRATEGIA, COUNT(*) AS N
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Analisis por estrategia"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Pricing")

# SCATTER - FULL WIDTH
st.markdown("**Scatter: Precio Actual vs Precio Competencia (Tamano = Suscriptores)**")
sc_df = run_query(f"""
    SELECT PLAN, ESTRATEGIA,
        ROUND(PRECIO_ACTUAL_COP/1e3, 1) AS PRECIO_K,
        ROUND(PRECIO_COMPETENCIA_COP/1e3, 1) AS COMP_K,
        SUSCRIPTORES_ACTUALES AS SUBS,
        ROUND(ELASTICIDAD_PRECIO, 2) AS ELASTICIDAD
    FROM {TABLE}
""")
if not sc_df.empty:
    fig_sc = px.scatter(sc_df, x="PRECIO_K", y="COMP_K", size="SUBS", color="ESTRATEGIA",
                       hover_name="PLAN", color_discrete_sequence=NAVY, size_max=45,
                       hover_data={"ELASTICIDAD": ":.2f", "SUBS": ":,.0f"},
                       labels={"PRECIO_K": "Precio actual (K COP)", "COMP_K": "Precio competencia (K COP)"})
    fig_sc.add_shape(type="line", x0=0, y0=0, x1=sc_df["PRECIO_K"].max()*1.1, y1=sc_df["PRECIO_K"].max()*1.1,
                    line=dict(dash="dash", color="#E85D29", width=1.5))
    fig_sc.update_layout(**CHART_LAYOUT, height=450, margin=dict(l=40, r=20, t=30, b=40),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.5, xanchor="center"))
    st.plotly_chart(fig_sc, use_container_width=True)

# RADAR + DONUT side by side
col_a, col_b = st.columns(2)

with col_a:
    st.markdown("**Radar: Indicadores por Estrategia de Pricing**")
    radar_df = run_query(f"""
        SELECT ESTRATEGIA,
            ROUND(AVG(WILLINGNESS_TO_PAY)*100, 1) AS WTP,
            ROUND(AVG(PERCEIVED_VALUE)*100, 1) AS PERCEIVED,
            ROUND(ABS(AVG(ELASTICIDAD_PRECIO))*50, 1) AS ELASTICIDAD_N,
            ROUND(AVG(CONFIANZA)*100, 1) AS CONFIANZA,
            ROUND(AVG(IMPACTO_REVENUE_PCT)+50, 1) AS IMPACTO
        FROM {TABLE} GROUP BY 1
    """)
    if not radar_df.empty:
        categories = ["WTP", "Perceived Value", "Elasticidad", "Confianza", "Impacto Rev."]
        fig_radar = go.Figure()
        for i, row in radar_df.iterrows():
            vals = [row["WTP"], row["PERCEIVED"], row["ELASTICIDAD_N"], row["CONFIANZA"], row["IMPACTO"]]
            fig_radar.add_trace(go.Scatterpolar(
                r=vals + [vals[0]], theta=categories + [categories[0]],
                fill="toself", name=row["ESTRATEGIA"],
                line=dict(color=NAVY[i % len(NAVY)]),
                fillcolor=f"rgba({int(NAVY[i % len(NAVY)][1:3], 16)},{int(NAVY[i % len(NAVY)][3:5], 16)},{int(NAVY[i % len(NAVY)][5:7], 16)},0.15)",
            ))
        fig_radar.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=60, r=60, t=30, b=30),
                               polar=dict(radialaxis=dict(visible=True, range=[0, 100]), bgcolor="#FAFBFC"),
                               legend=dict(orientation="h", yanchor="bottom", y=-0.15, x=0.5, xanchor="center"),
                               showlegend=True)
        st.plotly_chart(fig_radar, use_container_width=True)

with col_b:
    st.markdown("**Donut: Analisis por Estrategia**")
    donut_df = run_query(f"""
        SELECT ESTRATEGIA, COUNT(*) AS N
        FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC
    """)
    if not donut_df.empty:
        fig_donut = go.Figure(go.Pie(
            labels=donut_df["ESTRATEGIA"].tolist(),
            values=donut_df["N"].tolist(),
            hole=0.5,
            marker=dict(colors=NAVY[:len(donut_df)]),
            textinfo="label+percent",
            textfont=dict(size=11),
            hovertemplate="%{label}<br>%{value:,.0f} analisis<br>%{percent}<extra></extra>",
        ))
        fig_donut.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=10, r=10, t=30, b=10),
                               showlegend=True, legend=dict(orientation="h", y=-0.1, x=0.5, xanchor="center"))
        st.plotly_chart(fig_donut, use_container_width=True)

# BAR ELASTICIDAD - FULL WIDTH
st.markdown("**Bar: Elasticidad de Precio por Plan (Top 10)**")
bar_df = run_query(f"""
    SELECT PLAN, ROUND(AVG(ELASTICIDAD_PRECIO), 2) AS ELASTICIDAD
    FROM {TABLE} GROUP BY 1 ORDER BY ABS(AVG(ELASTICIDAD_PRECIO)) DESC LIMIT 10
""")
if not bar_df.empty:
    colors = ["#C0392B" if v < -1 else "#E8963A" if v < 0 else "#2E7D8C" for v in bar_df["ELASTICIDAD"]]
    fig_bar = go.Figure(go.Bar(
        y=bar_df["PLAN"].tolist()[::-1],
        x=bar_df["ELASTICIDAD"].tolist()[::-1],
        orientation="h",
        marker_color=colors[::-1],
        text=[f"{v:.2f}" for v in bar_df["ELASTICIDAD"].tolist()[::-1]],
        textposition="outside",
    ))
    fig_bar.add_vline(x=-1, line_dash="dash", line_color="#E85D29", annotation_text="Elastico (-1)")
    fig_bar.update_layout(**CHART_LAYOUT, height=420, margin=dict(l=160, r=60, t=30, b=40),
                        xaxis=dict(title="Elasticidad", gridcolor="#E2E8F0"))
    st.plotly_chart(fig_bar, use_container_width=True)

# WATERFALL - FULL WIDTH
st.markdown("**Waterfall: Impacto en Revenue por Estrategia**")
wf_df = run_query(f"""
    SELECT ESTRATEGIA,
        ROUND(SUM(REVENUE_ESTIMADO_COP - REVENUE_ACTUAL_COP)/1e9, 2) AS DELTA_B
    FROM {TABLE} GROUP BY 1 ORDER BY DELTA_B DESC
""")
if not wf_df.empty:
    labels = wf_df["ESTRATEGIA"].tolist() + ["Total"]
    values = wf_df["DELTA_B"].tolist() + [0]
    measures = ["relative"] * len(wf_df) + ["total"]
    fig_wf = go.Figure(go.Waterfall(
        x=labels, y=values, measure=measures,
        connector=dict(line=dict(color="#1B3A5C", width=1.5)),
        increasing=dict(marker=dict(color="#2E7D8C")),
        decreasing=dict(marker=dict(color="#C0392B")),
        totals=dict(marker=dict(color="#0F2B46")),
        textposition="outside",
    ))
    fig_wf.update_layout(**CHART_LAYOUT, height=380, margin=dict(l=40, r=20, t=30, b=60),
                       yaxis=dict(title="Delta Revenue (B COP)", gridcolor="#E2E8F0"), showlegend=False)
    st.plotly_chart(fig_wf, use_container_width=True)
