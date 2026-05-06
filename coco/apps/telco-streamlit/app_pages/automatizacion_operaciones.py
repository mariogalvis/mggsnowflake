from app_pages.page_template import render_page

config = {
    "key": "aop",
    "table": "MGG_TELCO.OPERACIONES_Y_MANTENIMIENTO.AUTOMATIZACION_OPERACIONES",
    "icon": ":material/precision_manufacturing:",
    "title": "Automatizacion Operaciones",
    "subtitle": "Medicion de avance, ahorro y ROI de la automatizacion de procesos operativos (RPA, ML, orquestadores).",
    "cards": [
        "Mide y prioriza la automatizacion de procesos operativos para reducir costos, errores y tiempo de ejecucion.",
        "Evalua volumen, tiempo manual vs automatizado, tasa de error, ahorro, ROI y payback por proceso. Prioriza por impacto.",
        "Ahorro operativo de millones anuales. Reduce errores humanos en 90% y libera personal para tareas de mayor valor.",
    ],
    "date_col": "FECHA_MEDICION",
    "filter_cols": ["AREA", "ESTADO_AUTOMATIZACION", "COMPLEJIDAD"],
    "kpi_query": """SELECT
        ROUND(AVG(PCT_AUTOMATIZACION)*100, 1) AS AUTO_PCT,
        ROUND(AVG(REDUCCION_TIEMPO_PCT), 1) AS REDUC_TIEMPO,
        ROUND(SUM(AHORRO_MENSUAL_COP)/1e6, 1) AS AHORRO_M,
        ROUND(AVG(ROI), 1) AS ROI_PROM,
        ROUND(AVG(TASA_ERROR_MANUAL)*100, 1) AS ERR_MANUAL,
        ROUND(AVG(TASA_ERROR_AUTOMATIZADO)*100, 1) AS ERR_AUTO,
        ROUND(AVG(PAYBACK_MESES), 0) AS PAYBACK,
        COUNT(*) AS PROCESOS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Automatizacion", "Reduc. tiempo", "Ahorro (M/mes)", "ROI", "Error manual", "Error auto", "Payback (meses)", "Procesos"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "${:.1f}M", "{:.1f}x", "{:.1f}%", "{:.1f}%", "{:.0f}", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_MEDICION) AS MES,
        ROUND(AVG(PCT_AUTOMATIZACION)*100, 1) AS AUTOMATIZACION,
        ROUND(AVG(ROI), 1) AS ROI
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Automatizacion (%)", "ROI"],
    "treemap_query": """SELECT AREA, ROUND(SUM(AHORRO_MENSUAL_COP)/1e6, 1) AS AHORRO_M
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Ahorro mensual por area (M COP)"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Automatizacion")

# HEATMAP - FULL WIDTH
st.markdown("**Heatmap: % Automatizacion por Area y Tecnologia**")
hm_df = run_query(f"""
    SELECT AREA, TECNOLOGIA,
        ROUND(AVG(PCT_AUTOMATIZACION)*100, 1) AS AUTO_PCT
    FROM {TABLE} GROUP BY 1, 2 ORDER BY 1, 2
""")
if not hm_df.empty:
    pivot = hm_df.pivot_table(index="AREA", columns="TECNOLOGIA", values="AUTO_PCT", aggfunc="mean").fillna(0)
    fig_hm = go.Figure(go.Heatmap(
        z=pivot.values,
        x=pivot.columns.tolist(),
        y=pivot.index.tolist(),
        colorscale=[[0, "#E8F4F8"], [0.3, "#29B5E8"], [0.7, "#2E7D8C"], [1, "#0F2B46"]],
        text=[[f"{v:.0f}%" for v in row] for row in pivot.values],
        texttemplate="%{text}",
        textfont=dict(size=11, color="white"),
        hovertemplate="Area: %{y}<br>Tecnologia: %{x}<br>Automatizacion: %{z:.1f}%<extra></extra>",
        colorbar=dict(title="Auto %"),
    ))
    fig_hm.update_layout(**CHART_LAYOUT, height=420, margin=dict(l=140, r=20, t=30, b=60),
                        xaxis=dict(title="Tecnologia"), yaxis=dict(title=""))
    st.plotly_chart(fig_hm, use_container_width=True)

# SCATTER + FUNNEL side by side
col_a, col_b = st.columns(2)

with col_a:
    st.markdown("**Scatter: ROI vs Complejidad (Tamano = Ahorro)**")
    sc_df = run_query(f"""
        SELECT ESTADO_AUTOMATIZACION,
            ROUND(AVG(ROI), 1) AS ROI,
            ROUND(AVG(COMPLEJIDAD)*100, 1) AS COMPLEJIDAD,
            ROUND(SUM(AHORRO_MENSUAL_COP)/1e6, 1) AS AHORRO_M,
            COUNT(*) AS N
        FROM {TABLE} GROUP BY 1
    """)
    if not sc_df.empty:
        fig_sc = px.scatter(sc_df, x="COMPLEJIDAD", y="ROI", size="AHORRO_M", color="ESTADO_AUTOMATIZACION",
                           color_discrete_sequence=NAVY, size_max=50,
                           hover_data={"N": ":,.0f", "AHORRO_M": ":,.0f"},
                           labels={"COMPLEJIDAD": "Complejidad (%)", "ROI": "ROI (x)"})
        fig_sc.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=40, r=20, t=30, b=40),
                           legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.5, xanchor="center"))
        st.plotly_chart(fig_sc, use_container_width=True)

with col_b:
    st.markdown("**Funnel: Procesos por Estado de Automatizacion**")
    fn_df = run_query(f"""
        SELECT ESTADO_AUTOMATIZACION, COUNT(*) AS N
        FROM {TABLE} GROUP BY 1 ORDER BY N DESC
    """)
    if not fn_df.empty:
        fig_fn = go.Figure(go.Funnel(
            y=fn_df["ESTADO_AUTOMATIZACION"].tolist(),
            x=fn_df["N"].tolist(),
            textposition="inside",
            textinfo="value+percent initial",
            marker=dict(color=NAVY[:len(fn_df)]),
            connector=dict(line=dict(color="#E2E8F0", width=1)),
        ))
        fig_fn.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=20, r=20, t=30, b=20))
        st.plotly_chart(fig_fn, use_container_width=True)

# WATERFALL - FULL WIDTH
st.markdown("**Waterfall: Ahorro por Area (Costo Manual → Costo Automatizado)**")
wf_df = run_query(f"""
    SELECT AREA,
        ROUND(SUM(COSTO_MANUAL_COP)/1e6, 1) AS MANUAL_M,
        ROUND(SUM(COSTO_AUTOMATIZADO_COP)/1e6, 1) AS AUTO_M,
        ROUND(SUM(AHORRO_MENSUAL_COP)/1e6, 1) AS AHORRO_M
    FROM {TABLE} GROUP BY 1 ORDER BY AHORRO_M DESC LIMIT 6
""")
if not wf_df.empty:
    labels = []
    values = []
    measures = []
    for _, row in wf_df.iterrows():
        labels.append(f"{row['AREA']}")
        values.append(row["AHORRO_M"])
        measures.append("relative")
    labels.append("Total ahorro")
    values.append(0)
    measures.append("total")
    fig_wf = go.Figure(go.Waterfall(
        x=labels, y=values, measure=measures,
        connector=dict(line=dict(color="#1B3A5C", width=1.5)),
        increasing=dict(marker=dict(color="#2E7D8C")),
        totals=dict(marker=dict(color="#0F2B46")),
        textposition="outside",
        text=[f"${v:.1f}M" for v in wf_df["AHORRO_M"].tolist()] + [f"${sum(wf_df['AHORRO_M']):.1f}M"],
    ))
    fig_wf.update_layout(**CHART_LAYOUT, height=380, margin=dict(l=40, r=20, t=30, b=60),
                       yaxis=dict(title="Ahorro mensual (M COP)", gridcolor="#E2E8F0"), showlegend=False)
    st.plotly_chart(fig_wf, use_container_width=True)

# BAR HORIZONTAL - FULL WIDTH
st.markdown("**Bar: Reduccion de Errores (Manual vs Automatizado) por Area**")
bar_df = run_query(f"""
    SELECT AREA,
        ROUND(AVG(TASA_ERROR_MANUAL)*100, 1) AS ERR_MANUAL,
        ROUND(AVG(TASA_ERROR_AUTOMATIZADO)*100, 1) AS ERR_AUTO
    FROM {TABLE} GROUP BY 1 ORDER BY ERR_MANUAL DESC LIMIT 8
""")
if not bar_df.empty:
    fig_bar = go.Figure()
    fig_bar.add_trace(go.Bar(x=bar_df["AREA"], y=bar_df["ERR_MANUAL"], name="Error manual",
                            marker_color="#C0392B"))
    fig_bar.add_trace(go.Bar(x=bar_df["AREA"], y=bar_df["ERR_AUTO"], name="Error automatizado",
                            marker_color="#2E7D8C"))
    fig_bar.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=40, r=20, t=30, b=60),
                        barmode="group", yaxis=dict(title="Tasa error (%)", gridcolor="#E2E8F0"),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.5, xanchor="center"))
    st.plotly_chart(fig_bar, use_container_width=True)
