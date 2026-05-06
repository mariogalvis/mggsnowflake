from app_pages.page_template import render_page

config = {
    "key": "ore",
    "table": "MGG_TELCO.CLIENTE_Y_RETENCION.OPTIMIZACION_RETENCION",
    "icon": ":material/loyalty:",
    "title": "Optimizacion Retencion",
    "subtitle": "Gestion inteligente de acciones de retencion con ROI medido y recomendacion de mejor oferta.",
    "cards": [
        "Optimiza las acciones de retencion asignando la mejor oferta al cliente correcto en el momento adecuado para maximizar permanencia.",
        "Motor de decisiones que evalua churn score, LTV, costo de retencion y ROI para recomendar accion y canal optimos.",
        "Maximiza ROI de retencion. Reduce costo por cliente retenido y aumenta NPS post-gestion con ofertas relevantes.",
    ],
    "date_col": "FECHA_GESTION",
    "filter_cols": ["ACCION_OFRECIDA", "CANAL_CONTACTO", "TRIGGER_RETENCION"],
    "kpi_query": """SELECT
        ROUND(SUM(CASE WHEN CLIENTE_RETENIDO THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS RETENCION_PCT,
        ROUND(SUM(CASE WHEN OFERTA_ACEPTADA THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS ACEPTACION_PCT,
        ROUND(AVG(ROI_RETENCION), 1) AS ROI_PROM,
        ROUND(AVG(COSTO_RETENCION_COP)/1e3, 1) AS COSTO_K,
        ROUND(AVG(NPS_POST_GESTION), 1) AS NPS_POST,
        ROUND(AVG(CONFIANZA_OFERTA)*100, 1) AS CONFIANZA,
        ROUND(SUM(CASE WHEN NOT CHURN_POST_90D THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS PERM_90D_PCT,
        COUNT(*) AS GESTIONES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Retencion", "Aceptacion", "ROI prom.", "Costo (K)", "NPS post", "Confianza", "Permanencia 90d", "Gestiones"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "{:.1f}x", "${:.1f}K", "{:.1f}", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_GESTION) AS MES,
        ROUND(SUM(CASE WHEN CLIENTE_RETENIDO THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS RETENCION,
        ROUND(AVG(ROI_RETENCION), 1) AS ROI
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Retencion (%)", "ROI"],
    "treemap_query": """SELECT ACCION_OFRECIDA, COUNT(*) AS N
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Acciones de retencion"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Retencion")

# HEATMAP - FULL WIDTH
st.markdown("**Heatmap: Tasa Retencion por Accion y Canal**")
hm_df = run_query(f"""
    SELECT ACCION_OFRECIDA, CANAL_CONTACTO,
        ROUND(SUM(CASE WHEN CLIENTE_RETENIDO THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS RETENCION
    FROM {TABLE} GROUP BY 1, 2 ORDER BY 1, 2
""")
if not hm_df.empty:
    pivot = hm_df.pivot_table(index="ACCION_OFRECIDA", columns="CANAL_CONTACTO", values="RETENCION", aggfunc="mean").fillna(0)
    fig_hm = go.Figure(go.Heatmap(
        z=pivot.values,
        x=pivot.columns.tolist(),
        y=pivot.index.tolist(),
        colorscale=[[0, "#C0392B"], [0.3, "#E8963A"], [0.6, "#29B5E8"], [1, "#0F2B46"]],
        text=[[f"{v:.1f}%" for v in row] for row in pivot.values],
        texttemplate="%{text}",
        textfont=dict(size=11, color="white"),
        hovertemplate="Accion: %{y}<br>Canal: %{x}<br>Retencion: %{z:.1f}%<extra></extra>",
        colorbar=dict(title="Retencion %"),
    ))
    fig_hm.update_layout(**CHART_LAYOUT, height=420, margin=dict(l=160, r=20, t=30, b=60),
                        xaxis=dict(title="Canal"), yaxis=dict(title=""))
    st.plotly_chart(fig_hm, use_container_width=True)

# SCATTER + FUNNEL side by side
col_a, col_b = st.columns(2)

with col_a:
    st.markdown("**Scatter: ROI vs Costo de Retencion por Accion**")
    sc_df = run_query(f"""
        SELECT ACCION_OFRECIDA,
            ROUND(AVG(ROI_RETENCION), 2) AS ROI,
            ROUND(AVG(COSTO_RETENCION_COP)/1e3, 1) AS COSTO_K,
            ROUND(AVG(LTV_COP)/1e6, 2) AS LTV_M,
            COUNT(*) AS N
        FROM {TABLE} GROUP BY 1
    """)
    if not sc_df.empty:
        fig_sc = px.scatter(sc_df, x="COSTO_K", y="ROI", size="LTV_M", color="ACCION_OFRECIDA",
                           color_discrete_sequence=NAVY, size_max=50,
                           hover_data={"N": ":,.0f", "LTV_M": ":,.2f"},
                           labels={"COSTO_K": "Costo retencion (K COP)", "ROI": "ROI (x)"})
        fig_sc.add_hline(y=1, line_dash="dash", line_color="#E85D29", annotation_text="Break-even")
        fig_sc.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=40, r=20, t=30, b=40),
                           legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.5, xanchor="center"))
        st.plotly_chart(fig_sc, use_container_width=True)

with col_b:
    st.markdown("**Funnel: Gestion → Oferta Aceptada → Retenido → Permanece 90d**")
    fn_data = run_query(f"""
        SELECT
            COUNT(*) AS GESTIONES,
            SUM(CASE WHEN OFERTA_ACEPTADA THEN 1 ELSE 0 END) AS ACEPTARON,
            SUM(CASE WHEN CLIENTE_RETENIDO THEN 1 ELSE 0 END) AS RETENIDOS,
            SUM(CASE WHEN NOT CHURN_POST_90D THEN 1 ELSE 0 END) AS PERMANECEN_90D
        FROM {TABLE}
    """)
    if not fn_data.empty:
        r = fn_data.iloc[0]
        fig_fn = go.Figure(go.Funnel(
            y=["Gestiones totales", "Oferta aceptada", "Cliente retenido", "Permanece 90d"],
            x=[int(r["GESTIONES"]), int(r["ACEPTARON"]), int(r["RETENIDOS"]), int(r["PERMANECEN_90D"])],
            textposition="inside",
            textinfo="value+percent initial",
            marker=dict(color=["#0F2B46", "#1B3A5C", "#2E7D8C", "#29B5E8"]),
            connector=dict(line=dict(color="#E2E8F0", width=2)),
        ))
        fig_fn.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=20, r=20, t=30, b=20))
        st.plotly_chart(fig_fn, use_container_width=True)

# BAR NPS + DONUT side by side
col_c, col_d = st.columns(2)

with col_c:
    st.markdown("**Bar: NPS Antes vs Despues por Trigger**")
    bar_df = run_query(f"""
        SELECT TRIGGER_RETENCION,
            ROUND(AVG(NPS_PREVIO), 1) AS NPS_PRE,
            ROUND(AVG(NPS_POST_GESTION), 1) AS NPS_POST
        FROM {TABLE} GROUP BY 1 ORDER BY NPS_POST DESC
    """)
    if not bar_df.empty:
        fig_bar = go.Figure()
        fig_bar.add_trace(go.Bar(x=bar_df["TRIGGER_RETENCION"], y=bar_df["NPS_PRE"], name="NPS previo",
                                marker_color="#C0392B"))
        fig_bar.add_trace(go.Bar(x=bar_df["TRIGGER_RETENCION"], y=bar_df["NPS_POST"], name="NPS post gestion",
                                marker_color="#2E7D8C"))
        fig_bar.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=40, r=20, t=30, b=80),
                            barmode="group", yaxis=dict(title="NPS", gridcolor="#E2E8F0"),
                            xaxis=dict(tickangle=-30),
                            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.5, xanchor="center"))
        st.plotly_chart(fig_bar, use_container_width=True)

with col_d:
    st.markdown("**Donut: Motor de Recomendacion de Oferta**")
    donut_df = run_query(f"""
        SELECT MOTOR_OFERTA, COUNT(*) AS N
        FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC
    """)
    if not donut_df.empty:
        fig_donut = go.Figure(go.Pie(
            labels=donut_df["MOTOR_OFERTA"].tolist(),
            values=donut_df["N"].tolist(),
            hole=0.5,
            marker=dict(colors=NAVY[:len(donut_df)]),
            textinfo="label+percent",
            textfont=dict(size=11),
            hovertemplate="%{label}<br>%{value:,.0f} gestiones<br>%{percent}<extra></extra>",
        ))
        fig_donut.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=10, r=10, t=30, b=10),
                               showlegend=True, legend=dict(orientation="h", y=-0.1, x=0.5, xanchor="center"))
        st.plotly_chart(fig_donut, use_container_width=True)

# BOX PLOT - FULL WIDTH
st.markdown("**Box Plot: LTV por Accion Ofrecida**")
box_df = run_query(f"SELECT ACCION_OFRECIDA, LTV_COP/1e6 AS LTV_M FROM {TABLE}")
if not box_df.empty:
    fig_box = px.box(box_df, x="ACCION_OFRECIDA", y="LTV_M", color="ACCION_OFRECIDA",
                    color_discrete_sequence=NAVY,
                    labels={"LTV_M": "LTV (M COP)", "ACCION_OFRECIDA": ""})
    fig_box.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=40, r=20, t=30, b=80),
                        showlegend=False, yaxis=dict(gridcolor="#E2E8F0"),
                        xaxis=dict(tickangle=-20))
    st.plotly_chart(fig_box, use_container_width=True)
