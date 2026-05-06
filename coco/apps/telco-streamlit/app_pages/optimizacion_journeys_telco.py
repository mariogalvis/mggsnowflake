from app_pages.page_template import render_page

config = {
    "key": "ojt",
    "table": "MGG_TELCO.EXPERIENCIA_Y_ATENCION.OPTIMIZACION_JOURNEYS_TELCO",
    "icon": ":material/route:",
    "title": "Optimizacion Journeys",
    "subtitle": "Analisis de journeys de cliente para reducir friccion, abandono y mejorar conversion end-to-end.",
    "cards": [
        "Identifica puntos de abandono y friccion en los journeys criticos del cliente (activacion, compra, soporte) para optimizarlos.",
        "Mide conversion por paso, friction score, cambios de canal, errores y SLA. Compara variantes UX para decisiones data-driven.",
        "Reduce abandono en journeys criticos. Mejora conversion de venta y reduce costo de servicio al eliminar pasos innecesarios.",
    ],
    "date_col": "FECHA",
    "filter_cols": ["TIPO_JOURNEY", "CANAL", "VARIANTE_UX"],
    "kpi_query": """SELECT
        ROUND(AVG(TASA_CONVERSION)*100, 1) AS CONV_PCT,
        ROUND(AVG(FRICTION_SCORE)*100, 1) AS FRICTION_PCT,
        ROUND(SUM(CASE WHEN JOURNEY_COMPLETADO THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS COMPLETADO_PCT,
        ROUND(AVG(TIEMPO_TOTAL_SEG)/60, 1) AS TIEMPO_MIN,
        ROUND(SUM(CASE WHEN ABANDONO_EN_PASO THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS ABANDONO_PCT,
        ROUND(SUM(CASE WHEN DENTRO_SLA THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS SLA_PCT,
        ROUND(AVG(NPS_JOURNEY), 1) AS NPS_J,
        COUNT(*) AS REGISTROS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Conversion", "Friction", "Completado", "Tiempo (min)", "Abandono", "SLA", "NPS Journey", "Registros"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.1f}", "{:.1f}%", "{:.1f}%", "{:.1f}", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA) AS MES,
        ROUND(AVG(TASA_CONVERSION)*100, 1) AS CONVERSION,
        ROUND(AVG(FRICTION_SCORE)*100, 1) AS FRICTION
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Conversion (%)", "Friction (%)"],
    "treemap_query": """SELECT TIPO_JOURNEY, COUNT(*) AS N
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Registros por tipo de journey"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Journeys")

# FUNNEL POR PASO - FULL WIDTH
st.markdown("**Funnel: Tasa de Completacion por Paso del Journey**")
fn_df = run_query(f"""
    SELECT NUMERO_PASO,
        COUNT(*) AS TOTAL,
        SUM(CASE WHEN PASO_COMPLETADO THEN 1 ELSE 0 END) AS COMPLETADOS
    FROM {TABLE}
    WHERE NUMERO_PASO <= 7
    GROUP BY 1 ORDER BY 1
""")
if not fn_df.empty:
    fig_fn = go.Figure(go.Funnel(
        y=[f"Paso {int(r['NUMERO_PASO'])}" for _, r in fn_df.iterrows()],
        x=fn_df["COMPLETADOS"].tolist(),
        textposition="inside",
        textinfo="value+percent initial",
        marker=dict(color=NAVY[:len(fn_df)]),
        connector=dict(line=dict(color="#E2E8F0", width=2)),
    ))
    fig_fn.update_layout(**CHART_LAYOUT, height=380, margin=dict(l=20, r=20, t=30, b=20))
    st.plotly_chart(fig_fn, use_container_width=True)

# HEATMAP - FULL WIDTH
st.markdown("**Heatmap: Friction Score por Tipo Journey y Canal**")
hm_df = run_query(f"""
    SELECT TIPO_JOURNEY, CANAL,
        ROUND(AVG(FRICTION_SCORE)*100, 1) AS FRICTION
    FROM {TABLE} GROUP BY 1, 2 ORDER BY 1, 2
""")
if not hm_df.empty:
    pivot = hm_df.pivot_table(index="TIPO_JOURNEY", columns="CANAL", values="FRICTION", aggfunc="mean").fillna(0)
    fig_hm = go.Figure(go.Heatmap(
        z=pivot.values,
        x=pivot.columns.tolist(),
        y=pivot.index.tolist(),
        colorscale=[[0, "#E8F4F8"], [0.3, "#29B5E8"], [0.6, "#E8963A"], [1, "#C0392B"]],
        text=[[f"{v:.1f}%" for v in row] for row in pivot.values],
        texttemplate="%{text}",
        textfont=dict(size=11, color="white"),
        hovertemplate="Journey: %{y}<br>Canal: %{x}<br>Friction: %{z:.1f}%<extra></extra>",
        colorbar=dict(title="Friction %"),
    ))
    fig_hm.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=140, r=20, t=30, b=60),
                        xaxis=dict(title="Canal"), yaxis=dict(title=""))
    st.plotly_chart(fig_hm, use_container_width=True)

# SCATTER + RADAR side by side
col_a, col_b = st.columns(2)

with col_a:
    st.markdown("**Scatter: Conversion vs Friction por Tipo Journey**")
    sc_df = run_query(f"""
        SELECT TIPO_JOURNEY,
            ROUND(AVG(TASA_CONVERSION)*100, 1) AS CONVERSION,
            ROUND(AVG(FRICTION_SCORE)*100, 1) AS FRICTION,
            ROUND(AVG(TIEMPO_TOTAL_SEG)/60, 1) AS TIEMPO_MIN,
            COUNT(*) AS N
        FROM {TABLE} GROUP BY 1
    """)
    if not sc_df.empty:
        fig_sc = px.scatter(sc_df, x="FRICTION", y="CONVERSION", size="N", color="TIPO_JOURNEY",
                           color_discrete_sequence=NAVY, size_max=50,
                           hover_data={"TIEMPO_MIN": ":.1f", "N": ":,.0f"},
                           labels={"FRICTION": "Friction (%)", "CONVERSION": "Conversion (%)"})
        fig_sc.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=40, r=20, t=30, b=40),
                           legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.5, xanchor="center"))
        st.plotly_chart(fig_sc, use_container_width=True)

with col_b:
    st.markdown("**Radar: Metricas CX por Variante UX**")
    radar_df = run_query(f"""
        SELECT VARIANTE_UX,
            ROUND(AVG(TASA_CONVERSION)*100, 1) AS CONVERSION,
            ROUND((1-AVG(FRICTION_SCORE))*100, 1) AS FLUIDEZ,
            ROUND(AVG(SATISFACCION_STEP)*20, 1) AS SATISFACCION,
            ROUND(AVG(NPS_JOURNEY)+50, 1) AS NPS_NORM,
            ROUND(SUM(CASE WHEN DENTRO_SLA THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS SLA
        FROM {TABLE} GROUP BY 1
    """)
    if not radar_df.empty:
        categories = ["Conversion", "Fluidez", "Satisfaccion", "NPS", "SLA"]
        fig_radar = go.Figure()
        for i, row in radar_df.iterrows():
            vals = [row["CONVERSION"], row["FLUIDEZ"], row["SATISFACCION"], row["NPS_NORM"], row["SLA"]]
            fig_radar.add_trace(go.Scatterpolar(
                r=vals + [vals[0]], theta=categories + [categories[0]],
                fill="toself", name=row["VARIANTE_UX"],
                line=dict(color=NAVY[i % len(NAVY)]),
                fillcolor=f"rgba({int(NAVY[i % len(NAVY)][1:3], 16)},{int(NAVY[i % len(NAVY)][3:5], 16)},{int(NAVY[i % len(NAVY)][5:7], 16)},0.12)",
            ))
        fig_radar.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=60, r=60, t=30, b=30),
                               polar=dict(radialaxis=dict(visible=True, range=[0, 100]), bgcolor="#FAFBFC"),
                               legend=dict(orientation="h", yanchor="bottom", y=-0.15, x=0.5, xanchor="center"),
                               showlegend=True)
        st.plotly_chart(fig_radar, use_container_width=True)

# BAR ABANDONOS + DONUT side by side
col_c, col_d = st.columns(2)

with col_c:
    st.markdown("**Bar: Tasa de Abandono por Paso**")
    bar_df = run_query(f"""
        SELECT PASO_ACTUAL,
            ROUND(SUM(CASE WHEN ABANDONO_EN_PASO THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS ABANDONO_PCT
        FROM {TABLE}
        WHERE PASO_ACTUAL IN (SELECT PASO_ACTUAL FROM {TABLE} GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 8)
        GROUP BY 1 ORDER BY 2 DESC
    """)
    if not bar_df.empty:
        colors = ["#C0392B" if v > 30 else "#E8963A" if v > 15 else "#2E7D8C" for v in bar_df["ABANDONO_PCT"]]
        fig_bar = go.Figure(go.Bar(
            y=bar_df["PASO_ACTUAL"].tolist()[::-1],
            x=bar_df["ABANDONO_PCT"].tolist()[::-1],
            orientation="h",
            marker_color=colors[::-1],
            text=[f"{v:.1f}%" for v in bar_df["ABANDONO_PCT"].tolist()[::-1]],
            textposition="outside",
        ))
        fig_bar.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=160, r=40, t=30, b=40),
                            xaxis=dict(title="Abandono (%)", gridcolor="#E2E8F0"))
        st.plotly_chart(fig_bar, use_container_width=True)

with col_d:
    st.markdown("**Donut: Journeys por Canal**")
    donut_df = run_query(f"""
        SELECT CANAL, COUNT(*) AS N
        FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC
    """)
    if not donut_df.empty:
        fig_donut = go.Figure(go.Pie(
            labels=donut_df["CANAL"].tolist(),
            values=donut_df["N"].tolist(),
            hole=0.5,
            marker=dict(colors=NAVY[:len(donut_df)]),
            textinfo="label+percent",
            textfont=dict(size=11),
            hovertemplate="%{label}<br>%{value:,.0f} journeys<br>%{percent}<extra></extra>",
        ))
        fig_donut.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=10, r=10, t=30, b=10),
                               showlegend=True, legend=dict(orientation="h", y=-0.1, x=0.5, xanchor="center"))
        st.plotly_chart(fig_donut, use_container_width=True)

# BOX PLOT - FULL WIDTH
st.markdown("**Box Plot: Tiempo Total del Journey por Tipo (minutos)**")
box_df = run_query(f"SELECT TIPO_JOURNEY, TIEMPO_TOTAL_SEG/60.0 AS TIEMPO_MIN FROM {TABLE}")
if not box_df.empty:
    fig_box = px.box(box_df, x="TIPO_JOURNEY", y="TIEMPO_MIN", color="TIPO_JOURNEY",
                    color_discrete_sequence=NAVY,
                    labels={"TIEMPO_MIN": "Tiempo total (min)", "TIPO_JOURNEY": ""})
    fig_box.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=40, r=20, t=30, b=60),
                        showlegend=False, yaxis=dict(gridcolor="#E2E8F0"))
    st.plotly_chart(fig_box, use_container_width=True)
