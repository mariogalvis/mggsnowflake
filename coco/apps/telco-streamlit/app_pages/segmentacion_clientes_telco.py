from app_pages.page_template import render_page

config = {
    "key": "sct",
    "table": "MGG_TELCO.CLIENTE_Y_RETENCION.SEGMENTACION_CLIENTES_TELCO",
    "icon": ":material/groups:",
    "title": "Segmentacion Clientes",
    "subtitle": "Segmentacion avanzada por valor, comportamiento digital y propension de upgrade.",
    "cards": [
        "Agrupa clientes en segmentos accionables para personalizar comunicacion, ofertas y prioridad de servicio.",
        "Combina ARPU, consumo de datos/voz, engagement digital, NPS, propension a upgrade y riesgo de churn para asignar segmento.",
        "Permite tratamiento diferenciado: proteger alto valor, desarrollar potencial, rentabilizar bajo consumo y gestionar riesgo.",
    ],
    "date_col": "FECHA_SEGMENTACION",
    "filter_cols": ["SEGMENTO", "PLAN", "CIUDAD"],
    "kpi_query": """SELECT
        ROUND(AVG(ARPU_COP)/1e3, 1) AS ARPU_K,
        ROUND(AVG(DATOS_GB_MES), 1) AS DATOS_GB,
        ROUND(AVG(CHURN_RISK)*100, 1) AS CHURN_PCT,
        ROUND(AVG(LTV_COP)/1e6, 1) AS LTV_M,
        ROUND(AVG(NPS), 1) AS NPS_PROM,
        ROUND(AVG(ENGAGEMENT_DIGITAL)*100, 1) AS ENGAGE_PCT,
        ROUND(AVG(PROPENSION_UPGRADE)*100, 1) AS UPGRADE_PCT,
        COUNT(*) AS CLIENTES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["ARPU (K)", "Datos GB/mes", "Churn risk", "LTV (M)", "NPS", "Engagement", "Propension upgrade", "Clientes"],
    "kpi_formats": ["${:.1f}K", "{:.1f}", "{:.1f}%", "${:.1f}M", "{:.1f}", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_SEGMENTACION) AS MES,
        ROUND(AVG(ARPU_COP)/1e3, 1) AS ARPU_K,
        ROUND(AVG(NPS), 1) AS NPS
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["ARPU (K COP)", "NPS"],
    "treemap_query": """SELECT SEGMENTO, COUNT(*) AS N
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Clientes por segmento"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(ARPU_COP)/1e3, 1) AS ARPU
    FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "ARPU por ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "ARPU", "caption": "Tamano: clientes | Color: ARPU promedio"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Segmentacion")

# SCATTER VALOR - FULL WIDTH
st.markdown("**Scatter: ARPU vs LTV por Segmento (Tamano = Clientes)**")
sc_df = run_query(f"""
    SELECT SEGMENTO,
        ROUND(AVG(ARPU_COP)/1e3, 1) AS ARPU_K,
        ROUND(AVG(LTV_COP)/1e6, 2) AS LTV_M,
        COUNT(*) AS CLIENTES,
        ROUND(AVG(CHURN_RISK)*100, 1) AS CHURN_PCT
    FROM {TABLE} GROUP BY 1
""")
if not sc_df.empty:
    fig_sc = px.scatter(sc_df, x="ARPU_K", y="LTV_M", size="CLIENTES", color="SEGMENTO",
                       color_discrete_sequence=NAVY, size_max=55,
                       hover_data={"CHURN_PCT": ":.1f%", "CLIENTES": ":,.0f"},
                       labels={"ARPU_K": "ARPU (K COP)", "LTV_M": "LTV (M COP)"})
    fig_sc.update_layout(**CHART_LAYOUT, height=450, margin=dict(l=40, r=20, t=30, b=40),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.5, xanchor="center"))
    st.plotly_chart(fig_sc, use_container_width=True)

# RADAR + DONUT side by side
col_a, col_b = st.columns(2)

with col_a:
    st.markdown("**Radar: Perfil por Segmento**")
    radar_df = run_query(f"""
        SELECT SEGMENTO,
            ROUND(AVG(ARPU_COP)/1e3, 1) AS ARPU,
            ROUND(AVG(ENGAGEMENT_DIGITAL)*100, 1) AS ENGAGEMENT,
            ROUND(AVG(NPS)*10, 1) AS NPS_N,
            ROUND(AVG(PROPENSION_UPGRADE)*100, 1) AS UPGRADE,
            ROUND((1-AVG(CHURN_RISK))*100, 1) AS ESTABILIDAD
        FROM {TABLE} GROUP BY 1
    """)
    if not radar_df.empty:
        categories = ["ARPU", "Engagement", "NPS", "Upgrade", "Estabilidad"]
        fig_radar = go.Figure()
        for i, row in radar_df.iterrows():
            vals = [row["ARPU"], row["ENGAGEMENT"], row["NPS_N"], row["UPGRADE"], row["ESTABILIDAD"]]
            fig_radar.add_trace(go.Scatterpolar(
                r=vals + [vals[0]], theta=categories + [categories[0]],
                fill="toself", name=row["SEGMENTO"],
                line=dict(color=NAVY[i % len(NAVY)]),
                fillcolor=f"rgba({int(NAVY[i % len(NAVY)][1:3], 16)},{int(NAVY[i % len(NAVY)][3:5], 16)},{int(NAVY[i % len(NAVY)][5:7], 16)},0.12)",
            ))
        fig_radar.update_layout(**CHART_LAYOUT, height=420, margin=dict(l=60, r=60, t=30, b=30),
                               polar=dict(radialaxis=dict(visible=True, range=[0, 100]), bgcolor="#FAFBFC"),
                               legend=dict(orientation="h", yanchor="bottom", y=-0.2, x=0.5, xanchor="center"),
                               showlegend=True)
        st.plotly_chart(fig_radar, use_container_width=True)

with col_b:
    st.markdown("**Donut: Clientes por Tipo de Cuenta**")
    donut_df = run_query(f"""
        SELECT TIPO_CUENTA, COUNT(*) AS N
        FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC
    """)
    if not donut_df.empty:
        fig_donut = go.Figure(go.Pie(
            labels=donut_df["TIPO_CUENTA"].tolist(),
            values=donut_df["N"].tolist(),
            hole=0.5,
            marker=dict(colors=NAVY[:len(donut_df)]),
            textinfo="label+percent",
            textfont=dict(size=12),
            hovertemplate="%{label}<br>%{value:,.0f} clientes<br>%{percent}<extra></extra>",
        ))
        fig_donut.update_layout(**CHART_LAYOUT, height=420, margin=dict(l=10, r=10, t=30, b=10),
                               showlegend=True, legend=dict(orientation="h", y=-0.1, x=0.5, xanchor="center"))
        st.plotly_chart(fig_donut, use_container_width=True)

# BAR + SUNBURST side by side
col_c, col_d = st.columns(2)

with col_c:
    st.markdown("**Bar: ARPU y Engagement por Plan (Top 8)**")
    bar_df = run_query(f"""
        SELECT PLAN,
            ROUND(AVG(ARPU_COP)/1e3, 1) AS ARPU_K,
            ROUND(AVG(ENGAGEMENT_DIGITAL)*100, 1) AS ENGAGE
        FROM {TABLE}
        WHERE PLAN IN (SELECT PLAN FROM {TABLE} GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 8)
        GROUP BY 1 ORDER BY ARPU_K DESC
    """)
    if not bar_df.empty:
        fig_bar = go.Figure()
        fig_bar.add_trace(go.Bar(x=bar_df["PLAN"], y=bar_df["ARPU_K"], name="ARPU (K)",
                                marker_color="#1B3A5C"))
        fig_bar.add_trace(go.Bar(x=bar_df["PLAN"], y=bar_df["ENGAGE"], name="Engagement (%)",
                                marker_color="#29B5E8"))
        fig_bar.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=40, r=20, t=30, b=80),
                            barmode="group", yaxis=dict(gridcolor="#E2E8F0"),
                            xaxis=dict(tickangle=-30),
                            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.5, xanchor="center"))
        st.plotly_chart(fig_bar, use_container_width=True)

with col_d:
    st.markdown("**Sunburst: Segmento → Convergente/No Convergente**")
    sun_df = run_query(f"""
        SELECT SEGMENTO,
            CASE WHEN CONVERGENTE THEN 'Convergente' ELSE 'No convergente' END AS CONV,
            COUNT(*) AS N
        FROM {TABLE} GROUP BY 1, 2
    """)
    if not sun_df.empty:
        fig_sun = px.sunburst(sun_df, path=["SEGMENTO", "CONV"], values="N",
                             color="N", color_continuous_scale=["#E8F4F8", "#2E7D8C", "#0F2B46"])
        fig_sun.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=10, r=10, t=30, b=10))
        fig_sun.update_traces(textinfo="label+percent parent", insidetextorientation="radial")
        st.plotly_chart(fig_sun, use_container_width=True)

# BOX PLOT - FULL WIDTH
st.markdown("**Box Plot: Datos GB/mes por Segmento**")
box_df = run_query(f"SELECT SEGMENTO, DATOS_GB_MES FROM {TABLE}")
if not box_df.empty:
    fig_box = px.box(box_df, x="SEGMENTO", y="DATOS_GB_MES", color="SEGMENTO",
                    color_discrete_sequence=NAVY,
                    labels={"DATOS_GB_MES": "Datos GB/mes", "SEGMENTO": ""})
    fig_box.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=40, r=20, t=30, b=60),
                        showlegend=False, yaxis=dict(gridcolor="#E2E8F0"))
    st.plotly_chart(fig_box, use_container_width=True)
