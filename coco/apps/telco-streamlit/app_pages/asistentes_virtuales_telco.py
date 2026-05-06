from app_pages.page_template import render_page

config = {
    "key": "avt",
    "table": "MGG_TELCO.EXPERIENCIA_Y_ATENCION.ASISTENTES_VIRTUALES_TELCO",
    "icon": ":material/smart_toy:",
    "title": "Asistentes Virtuales",
    "subtitle": "Monitoreo de rendimiento de chatbots y asistentes conversacionales con analisis de ahorro vs agente humano.",
    "cards": [
        "Mide la efectividad de los asistentes virtuales en resolver consultas sin escalar a agente, generando ahorro operativo.",
        "Analiza confianza de intencion, tasa de resolucion, satisfaccion, transferencias a agente, sentimiento y costo por interaccion.",
        "Reduce costos de atencion en 60-80% por interaccion resuelta. Mejora disponibilidad 24/7 y reduce tiempos de espera.",
    ],
    "date_col": "TIMESTAMP_INICIO",
    "filter_cols": ["INTENCION_DETECTADA", "CANAL", "RESULTADO"],
    "kpi_query": """SELECT
        ROUND(SUM(CASE WHEN RESUELTO_SIN_AGENTE THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS RESOL_PCT,
        ROUND(AVG(SATISFACCION_USUARIO), 1) AS SATISF_PROM,
        ROUND(AVG(CONFIANZA_INTENCION)*100, 1) AS CONFIANZA,
        ROUND(AVG(AHORRO_VS_AGENTE_PCT), 1) AS AHORRO_PCT,
        ROUND(SUM(CASE WHEN TRANSFERIDO_AGENTE THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS TRANSF_PCT,
        ROUND(AVG(TURNOS_CONVERSACION), 1) AS TURNOS_PROM,
        ROUND(SUM(CASE WHEN SENTIMIENTO_POSITIVO THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS SENTIM_POS_PCT,
        COUNT(*) AS INTERACCIONES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Resolucion bot", "Satisfaccion", "Confianza", "Ahorro vs agente", "Transferencia", "Turnos prom.", "Sentimiento (+)", "Interacciones"],
    "kpi_formats": ["{:.1f}%", "{:.1f}", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.1f}", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', TIMESTAMP_INICIO) AS MES,
        ROUND(SUM(CASE WHEN RESUELTO_SIN_AGENTE THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS RESOLUCION,
        ROUND(AVG(SATISFACCION_USUARIO), 1) AS SATISFACCION
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Resolucion bot (%)", "Satisfaccion"],
    "treemap_query": """SELECT INTENCION_DETECTADA, COUNT(*) AS N
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC LIMIT 10""",
    "treemap_config": {"title": "Intenciones mas frecuentes"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Asistentes Virtuales")

# FUNNEL CONVERSION - FULL WIDTH
st.markdown("**Funnel: Interacciones → Resueltas sin Agente → Satisfaccion Positiva**")
fn_data = run_query(f"""
    SELECT
        COUNT(*) AS TOTAL,
        SUM(CASE WHEN RESUELTO_SIN_AGENTE THEN 1 ELSE 0 END) AS RESUELTAS,
        SUM(CASE WHEN RESUELTO_SIN_AGENTE AND SATISFACCION_USUARIO >= 4 THEN 1 ELSE 0 END) AS SATISFECHAS,
        SUM(CASE WHEN RESUELTO_SIN_AGENTE AND SATISFACCION_USUARIO >= 4 AND SENTIMIENTO_POSITIVO THEN 1 ELSE 0 END) AS POSITIVAS
    FROM {TABLE}
""")
if not fn_data.empty:
    r = fn_data.iloc[0]
    fig_fn = go.Figure(go.Funnel(
        y=["Total interacciones", "Resueltas sin agente", "Satisfaccion >= 4", "Sentimiento positivo"],
        x=[int(r["TOTAL"]), int(r["RESUELTAS"]), int(r["SATISFECHAS"]), int(r["POSITIVAS"])],
        textposition="inside",
        textinfo="value+percent initial",
        marker=dict(color=["#0F2B46", "#1B3A5C", "#2E7D8C", "#29B5E8"]),
        connector=dict(line=dict(color="#E2E8F0", width=2)),
    ))
    fig_fn.update_layout(**CHART_LAYOUT, height=320, margin=dict(l=20, r=20, t=30, b=20))
    st.plotly_chart(fig_fn, use_container_width=True)

# HEATMAP - FULL WIDTH
st.markdown("**Heatmap: Tasa Resolucion por Intencion y Canal**")
hm_df = run_query(f"""
    SELECT INTENCION_DETECTADA, CANAL,
        ROUND(SUM(CASE WHEN RESUELTO_SIN_AGENTE THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS RESOL_PCT
    FROM {TABLE}
    WHERE INTENCION_DETECTADA IN (SELECT INTENCION_DETECTADA FROM {TABLE} GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 8)
    GROUP BY 1, 2 ORDER BY 1, 2
""")
if not hm_df.empty:
    pivot = hm_df.pivot_table(index="INTENCION_DETECTADA", columns="CANAL", values="RESOL_PCT", aggfunc="mean").fillna(0)
    fig_hm = go.Figure(go.Heatmap(
        z=pivot.values,
        x=pivot.columns.tolist(),
        y=pivot.index.tolist(),
        colorscale=[[0, "#C0392B"], [0.3, "#E8963A"], [0.6, "#29B5E8"], [1, "#0F2B46"]],
        text=[[f"{v:.0f}%" for v in row] for row in pivot.values],
        texttemplate="%{text}",
        textfont=dict(size=10, color="white"),
        hovertemplate="Intencion: %{y}<br>Canal: %{x}<br>Resolucion: %{z:.1f}%<extra></extra>",
        colorbar=dict(title="Resolucion %"),
    ))
    fig_hm.update_layout(**CHART_LAYOUT, height=450, margin=dict(l=160, r=20, t=30, b=60),
                        xaxis=dict(title="Canal"), yaxis=dict(title=""))
    st.plotly_chart(fig_hm, use_container_width=True)

# SCATTER + DONUT side by side
col_a, col_b = st.columns(2)

with col_a:
    st.markdown("**Scatter: Confianza vs Satisfaccion por Motor**")
    sc_df = run_query(f"""
        SELECT MOTOR_CONVERSACION,
            ROUND(AVG(CONFIANZA_INTENCION)*100, 1) AS CONFIANZA,
            ROUND(AVG(SATISFACCION_USUARIO), 2) AS SATISF,
            ROUND(AVG(AHORRO_VS_AGENTE_PCT), 1) AS AHORRO,
            COUNT(*) AS N
        FROM {TABLE} GROUP BY 1
    """)
    if not sc_df.empty:
        fig_sc = px.scatter(sc_df, x="CONFIANZA", y="SATISF", size="N", color="MOTOR_CONVERSACION",
                           color_discrete_sequence=NAVY, size_max=50,
                           hover_data={"AHORRO": ":.1f%", "N": ":,.0f"},
                           labels={"CONFIANZA": "Confianza intencion (%)", "SATISF": "Satisfaccion (0-5)"})
        fig_sc.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=40, r=20, t=30, b=40),
                           legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.5, xanchor="center"))
        st.plotly_chart(fig_sc, use_container_width=True)

with col_b:
    st.markdown("**Donut: Resultado de Interacciones**")
    donut_df = run_query(f"""
        SELECT RESULTADO, COUNT(*) AS N
        FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC
    """)
    if not donut_df.empty:
        fig_donut = go.Figure(go.Pie(
            labels=donut_df["RESULTADO"].tolist(),
            values=donut_df["N"].tolist(),
            hole=0.5,
            marker=dict(colors=NAVY[:len(donut_df)]),
            textinfo="label+percent",
            textfont=dict(size=11),
            hovertemplate="%{label}<br>%{value:,.0f}<br>%{percent}<extra></extra>",
        ))
        fig_donut.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=10, r=10, t=30, b=10),
                               showlegend=True, legend=dict(orientation="h", y=-0.1, x=0.5, xanchor="center"))
        st.plotly_chart(fig_donut, use_container_width=True)

# WATERFALL + BAR side by side
col_c, col_d = st.columns(2)

with col_c:
    st.markdown("**Waterfall: Ahorro Acumulado vs Agente Humano**")
    wf_df = run_query(f"""
        SELECT CANAL,
            ROUND(SUM(COSTO_EQUIVALENTE_AGENTE_COP - COSTO_INTERACCION_COP)/1e6, 1) AS AHORRO_M
        FROM {TABLE} GROUP BY 1 ORDER BY AHORRO_M DESC
    """)
    if not wf_df.empty:
        labels = wf_df["CANAL"].tolist() + ["Total ahorro"]
        values = wf_df["AHORRO_M"].tolist() + [0]
        measures = ["relative"] * len(wf_df) + ["total"]
        fig_wf = go.Figure(go.Waterfall(
            x=labels, y=values, measure=measures,
            connector=dict(line=dict(color="#1B3A5C", width=1.5)),
            increasing=dict(marker=dict(color="#2E7D8C")),
            totals=dict(marker=dict(color="#0F2B46")),
            textposition="outside",
            text=[f"${v:.1f}M" for v in wf_df["AHORRO_M"]] + [f"${sum(wf_df['AHORRO_M']):.1f}M"],
        ))
        fig_wf.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=40, r=20, t=30, b=60),
                           yaxis=dict(title="Ahorro (M COP)", gridcolor="#E2E8F0"), showlegend=False)
        st.plotly_chart(fig_wf, use_container_width=True)

with col_d:
    st.markdown("**Bar: Intentos Fallidos Promedio por Intencion (Top 8)**")
    bar_df = run_query(f"""
        SELECT INTENCION_DETECTADA, ROUND(AVG(INTENTOS_FALLIDOS), 2) AS FALLIDOS
        FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC LIMIT 8
    """)
    if not bar_df.empty:
        fig_bar = go.Figure(go.Bar(
            y=bar_df["INTENCION_DETECTADA"].tolist()[::-1],
            x=bar_df["FALLIDOS"].tolist()[::-1],
            orientation="h",
            marker_color="#C0392B",
            text=[f"{v:.2f}" for v in bar_df["FALLIDOS"].tolist()[::-1]],
            textposition="outside",
        ))
        fig_bar.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=160, r=40, t=30, b=40),
                            xaxis=dict(title="Intentos fallidos prom.", gridcolor="#E2E8F0"))
        st.plotly_chart(fig_bar, use_container_width=True)

# HISTOGRAM - FULL WIDTH
st.markdown("**Histograma: Distribucion de Duracion de Conversaciones**")
hist_df = run_query(f"SELECT DURACION_SEG/60.0 AS DURACION_MIN FROM {TABLE}")
if not hist_df.empty:
    fig_hist = px.histogram(hist_df, x="DURACION_MIN", nbins=30,
                           color_discrete_sequence=["#1B3A5C"],
                           labels={"DURACION_MIN": "Duracion (minutos)"})
    fig_hist.add_vline(x=5, line_dash="dash", line_color="#E85D29", annotation_text="Meta 5 min",
                      annotation_position="top right")
    fig_hist.update_layout(**CHART_LAYOUT, height=350, margin=dict(l=40, r=20, t=30, b=40),
                          yaxis=dict(title="Frecuencia", gridcolor="#E2E8F0"),
                          bargap=0.05, showlegend=False)
    st.plotly_chart(fig_hist, use_container_width=True)
