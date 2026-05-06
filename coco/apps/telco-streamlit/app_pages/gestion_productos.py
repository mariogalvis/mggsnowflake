from app_pages.page_template import render_page

config = {
    "key": "gpr",
    "table": "MGG_TELCO.MONETIZACION_Y_REVENUE.GESTION_PRODUCTOS",
    "icon": ":material/inventory:",
    "title": "Gestion Productos",
    "subtitle": "Analisis de portafolio de productos por ciclo de vida, rentabilidad y posicionamiento competitivo (BCG).",
    "cards": [
        "Evalua el rendimiento de cada producto/plan del portafolio para decidir inversion, rediseno o descontinuacion.",
        "Matriz BCG basada en growth y share. Mide ARPU, churn, engagement, margen y propension a upgrade/downgrade por plan.",
        "Optimiza el mix de portafolio. Enfoca inversion en estrellas, ordeña vacas y retira productos sin diferenciacion.",
    ],
    "date_col": "FECHA_CORTE",
    "filter_cols": ["CATEGORIA", "ESTADO", "CUADRANTE_BCG"],
    "kpi_query": """SELECT
        ROUND(AVG(ARPU_COP)/1e3, 1) AS ARPU_K,
        ROUND(AVG(MARGEN_PCT)*100, 1) AS MARGEN_PCT,
        ROUND(AVG(TASA_CHURN)*100, 1) AS CHURN_PCT,
        ROUND(SUM(REVENUE_TOTAL_COP)/1e9, 1) AS REV_B,
        ROUND(AVG(NPS_PRODUCTO), 1) AS NPS,
        ROUND(AVG(ENGAGEMENT_SCORE)*100, 1) AS ENGAGE,
        ROUND(AVG(PROPENSION_UPGRADE)*100, 1) AS UPGRADE_PCT,
        COUNT(*) AS PRODUCTOS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["ARPU (K)", "Margen", "Churn", "Revenue (B)", "NPS", "Engagement", "Prop. upgrade", "Productos"],
    "kpi_formats": ["${:.1f}K", "{:.1f}%", "{:.1f}%", "${:.1f}B", "{:.1f}", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_CORTE) AS MES,
        ROUND(AVG(ARPU_COP)/1e3, 1) AS ARPU,
        ROUND(AVG(MARGEN_PCT)*100, 1) AS MARGEN
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["ARPU (K)", "Margen (%)"],
    "treemap_query": """SELECT CUADRANTE_BCG, COUNT(*) AS N
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Productos por cuadrante BCG"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Portafolio")

# SCATTER BCG - FULL WIDTH
st.markdown("**Matriz BCG: Crecimiento vs Share de Revenue**")
bcg_df = run_query(f"""
    SELECT PLAN, CUADRANTE_BCG,
        ROUND(AVG(CRECIMIENTO_SUSCRIPTORES_MOM_PCT), 1) AS CRECIMIENTO,
        ROUND(AVG(SHARE_REVENUE)*100, 1) AS SHARE_REV,
        ROUND(SUM(REVENUE_TOTAL_COP)/1e6, 1) AS REVENUE_M,
        ROUND(AVG(SUSCRIPTORES), 0) AS SUBS
    FROM {TABLE} GROUP BY 1, 2
""")
if not bcg_df.empty:
    fig_bcg = px.scatter(bcg_df, x="SHARE_REV", y="CRECIMIENTO", size="REVENUE_M", color="CUADRANTE_BCG",
                        hover_name="PLAN", color_discrete_sequence=NAVY, size_max=50,
                        hover_data={"SUBS": ":,.0f", "REVENUE_M": ":,.0f"},
                        labels={"SHARE_REV": "Share Revenue (%)", "CRECIMIENTO": "Crecimiento suscriptores MoM (%)"})
    fig_bcg.update_layout(**CHART_LAYOUT, height=450, margin=dict(l=40, r=20, t=30, b=40),
                         legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.5, xanchor="center"))
    st.plotly_chart(fig_bcg, use_container_width=True)

# RADAR + DONUT side by side
col_a, col_b = st.columns(2)

with col_a:
    st.markdown("**Radar: Metricas por Cuadrante BCG**")
    radar_df = run_query(f"""
        SELECT CUADRANTE_BCG,
            ROUND(AVG(ARPU_COP)/1e3, 1) AS ARPU_K,
            ROUND(AVG(MARGEN_PCT)*100, 1) AS MARGEN,
            ROUND(AVG(ENGAGEMENT_SCORE)*100, 1) AS ENGAGEMENT,
            ROUND(AVG(NPS_PRODUCTO), 1) AS NPS,
            ROUND(AVG(PROPENSION_UPGRADE)*100, 1) AS UPGRADE
        FROM {TABLE} GROUP BY 1
    """)
    if not radar_df.empty:
        categories = ["ARPU", "Margen", "Engagement", "NPS", "Upgrade"]
        fig_radar = go.Figure()
        for i, row in radar_df.iterrows():
            vals = [row["ARPU_K"], row["MARGEN"], row["ENGAGEMENT"], row["NPS"]*10, row["UPGRADE"]]
            fig_radar.add_trace(go.Scatterpolar(
                r=vals + [vals[0]], theta=categories + [categories[0]],
                fill="toself", name=row["CUADRANTE_BCG"],
                line=dict(color=NAVY[i % len(NAVY)]),
                fillcolor=f"rgba({int(NAVY[i % len(NAVY)][1:3], 16)},{int(NAVY[i % len(NAVY)][3:5], 16)},{int(NAVY[i % len(NAVY)][5:7], 16)},0.15)",
            ))
        fig_radar.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=60, r=60, t=30, b=30),
                               polar=dict(radialaxis=dict(visible=True, range=[0, 100]), bgcolor="#FAFBFC"),
                               legend=dict(orientation="h", yanchor="bottom", y=-0.15, x=0.5, xanchor="center"),
                               showlegend=True)
        st.plotly_chart(fig_radar, use_container_width=True)

with col_b:
    st.markdown("**Donut: Distribucion por Cuadrante BCG**")
    donut_df = run_query(f"""
        SELECT CUADRANTE_BCG, COUNT(*) AS N
        FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC
    """)
    if not donut_df.empty:
        fig_donut = go.Figure(go.Pie(
            labels=donut_df["CUADRANTE_BCG"].tolist(),
            values=donut_df["N"].tolist(),
            hole=0.5,
            marker=dict(colors=NAVY[:len(donut_df)]),
            textinfo="label+percent",
            textfont=dict(size=11),
            hovertemplate="%{label}<br>%{value:,.0f} productos<br>%{percent}<extra></extra>",
        ))
        fig_donut.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=10, r=10, t=30, b=10),
                               showlegend=True, legend=dict(orientation="h", y=-0.1, x=0.5, xanchor="center"))
        st.plotly_chart(fig_donut, use_container_width=True)

# BAR + WATERFALL side by side
col_c, col_d = st.columns(2)

with col_c:
    st.markdown("**Bar: ARPU y Margen por Categoria**")
    bar_df = run_query(f"""
        SELECT CATEGORIA,
            ROUND(AVG(ARPU_COP)/1e3, 1) AS ARPU_K,
            ROUND(AVG(MARGEN_PCT)*100, 1) AS MARGEN
        FROM {TABLE} GROUP BY 1 ORDER BY ARPU_K DESC
    """)
    if not bar_df.empty:
        fig_bar = go.Figure()
        fig_bar.add_trace(go.Bar(x=bar_df["CATEGORIA"], y=bar_df["ARPU_K"], name="ARPU (K)",
                                marker_color="#1B3A5C"))
        fig_bar.add_trace(go.Bar(x=bar_df["CATEGORIA"], y=bar_df["MARGEN"], name="Margen (%)",
                                marker_color="#29B5E8"))
        fig_bar.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=40, r=20, t=30, b=60),
                            barmode="group", yaxis=dict(gridcolor="#E2E8F0"),
                            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.5, xanchor="center"))
        st.plotly_chart(fig_bar, use_container_width=True)

with col_d:
    st.markdown("**Waterfall: Revenue por Categoria**")
    wf_df = run_query(f"""
        SELECT CATEGORIA, ROUND(SUM(REVENUE_TOTAL_COP)/1e9, 2) AS REV_B
        FROM {TABLE} GROUP BY 1 ORDER BY REV_B DESC
    """)
    if not wf_df.empty:
        labels = wf_df["CATEGORIA"].tolist() + ["Total"]
        values = wf_df["REV_B"].tolist() + [0]
        measures = ["relative"] * len(wf_df) + ["total"]
        fig_wf = go.Figure(go.Waterfall(
            x=labels, y=values, measure=measures,
            connector=dict(line=dict(color="#1B3A5C", width=1.5)),
            increasing=dict(marker=dict(color="#2E7D8C")),
            totals=dict(marker=dict(color="#0F2B46")),
            textposition="outside",
            text=[f"${v:.2f}B" for v in wf_df["REV_B"].tolist()] + [f"${sum(wf_df['REV_B']):.2f}B"],
        ))
        fig_wf.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=40, r=20, t=30, b=60),
                           yaxis=dict(title="Revenue (B COP)", gridcolor="#E2E8F0"), showlegend=False)
        st.plotly_chart(fig_wf, use_container_width=True)

# HISTOGRAM - FULL WIDTH
st.markdown("**Histograma: Distribucion de Tasa de Churn por Producto**")
hist_df = run_query(f"SELECT TASA_CHURN FROM {TABLE}")
if not hist_df.empty:
    fig_hist = px.histogram(hist_df, x="TASA_CHURN", nbins=25,
                           color_discrete_sequence=["#1B3A5C"],
                           labels={"TASA_CHURN": "Tasa de churn"})
    fig_hist.add_vline(x=0.05, line_dash="dash", line_color="#E85D29", annotation_text="Meta (5%)",
                      annotation_position="top right")
    fig_hist.update_layout(**CHART_LAYOUT, height=350, margin=dict(l=40, r=20, t=30, b=40),
                          yaxis=dict(title="Frecuencia", gridcolor="#E2E8F0"),
                          xaxis=dict(title="Tasa de churn", tickformat=".0%"),
                          bargap=0.05, showlegend=False)
    st.plotly_chart(fig_hist, use_container_width=True)
