import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from app_pages.page_template import render_page, COLORS
from app_pages.conn_helper import run_query

config = {
    "key": "ec",
    "table": "MGG_SEGUROS.SINIESTROS.ESTIMACION_COSTO_SINIESTRO",
    "icon": ":material/calculate:",
    "title": "Estimacion de Costo",
    "subtitle": "Prediccion del costo final de un siniestro desglosado por componentes usando modelos de ML vs estimacion tradicional.",
    "cards": [
        "Reservas mas precisas desde el dia 1. Reduce sorpresas de desarrollo y mejora la suficiencia de reservas reportada a la Superfinanciera.",
        "Modelo ML que estima costo final integrando tipo de siniestro, complejidad, componentes de costo y patrones historicos de desarrollo. Compara con estimacion inicial del ajustador.",
        "Reservas mas precisas = mejor resultado tecnico. Reduce el run-off negativo y permite pricing mas competitivo al tener mejor estimacion de siniestralidad ultima.",
    ],
    "date_col": "FECHA_ESTIMACION",
    "filter_cols": ["TIPO_SINIESTRO", "RAMO", "ESTADO_SINIESTRO"],
    "kpi_query": """SELECT
        ROUND(AVG(COSTO_ESTIMADO_ML_COP)/1e6, 1) AS COSTO_ML_MM,
        ROUND(AVG(ERROR_ESTIMACION_PCT), 1) AS ERROR_PCT,
        ROUND(AVG(CONFIANZA_ESTIMACION)*100, 1) AS CONFIANZA,
        ROUND(SUM(RESERVA_ACTUAL_COP)/1e9, 1) AS RESERVAS_B,
        ROUND(AVG(DESARROLLO_VS_RESERVA_PCT), 1) AS DESARROLLO_PCT,
        ROUND(AVG(COMPLEJIDAD_CASO), 1) AS COMPLEJIDAD,
        ROUND(AVG(DIAS_DESARROLLO), 0) AS DIAS_DEV,
        COUNT(*) AS ESTIMACIONES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Costo ML prom.", "Error estimacion", "Confianza modelo", "Reservas totales", "Desarrollo vs reserva", "Complejidad prom.", "Dias desarrollo", "Estimaciones"],
    "kpi_formats": ["${:.1f}M", "{:.1f}%", "{:.1f}%", "${:.1f}B", "{:.1f}%", "{:.1f}", "{:.0f}", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_ESTIMACION) AS MES,
        ROUND(AVG(ERROR_ESTIMACION_PCT), 1) AS ERROR,
        ROUND(AVG(CONFIANZA_ESTIMACION)*100, 1) AS CONFIANZA
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Error estimacion (%)", "Confianza (%)"],
    "treemap_query": """SELECT COMPONENTE_MAYOR_COSTO, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Componente mayor costo"},
    "geo_query": None, "geo_config": None, "diagnostics": None, "simulator": None,
}

render_page(config)

TABLE = config["table"]
CHART_LAYOUT = dict(
    template="plotly_white", paper_bgcolor="#FAFBFC", plot_bgcolor="#FAFBFC",
    font=dict(family="Inter, sans-serif", color="#334155"),
    margin=dict(l=40, r=40, t=40, b=40),
)

st.divider()
st.subheader(":material/insights: Analisis Avanzado de Estimaciones")

col_a, col_b = st.columns(2)

with col_a:
    st.markdown("**Precision del Modelo: Estimacion ML vs Costo Real**")
    scatter_df = run_query(f"""
        SELECT COSTO_ESTIMADO_ML_COP/1e6 AS ML_EST,
               COSTO_FINAL_REAL_COP/1e6 AS REAL,
               TIPO_SINIESTRO, COMPLEJIDAD_CASO
        FROM {TABLE} WHERE COSTO_FINAL_REAL_COP > 0
        ORDER BY RANDOM() LIMIT 500
    """)
    if not scatter_df.empty:
        fig_scatter = go.Figure()
        fig_scatter.add_trace(go.Scatter(
            x=scatter_df["ML_EST"], y=scatter_df["REAL"],
            mode="markers", marker=dict(
                size=scatter_df["COMPLEJIDAD_CASO"] * 2 + 4,
                color=scatter_df["COMPLEJIDAD_CASO"],
                colorscale=[[0, "#06D6A0"], [0.5, "#F4A261"], [1, "#E63946"]],
                opacity=0.7, colorbar=dict(title="Complejidad"),
            ),
            text=scatter_df["TIPO_SINIESTRO"],
            hovertemplate="<b>%{text}</b><br>ML: $%{x:.1f}M<br>Real: $%{y:.1f}M<extra></extra>",
        ))
        max_val = max(scatter_df["ML_EST"].max(), scatter_df["REAL"].max()) * 1.05
        fig_scatter.add_trace(go.Scatter(
            x=[0, max_val], y=[0, max_val], mode="lines",
            line=dict(color="#94A3B8", dash="dash", width=1.5),
            showlegend=False, hoverinfo="skip",
        ))
        fig_scatter.update_layout(
            **CHART_LAYOUT, height=400,
            xaxis=dict(title="Estimacion ML ($M COP)", gridcolor="#E2E8F0"),
            yaxis=dict(title="Costo Real ($M COP)", gridcolor="#E2E8F0"),
        )
        fig_scatter.add_annotation(
            x=max_val * 0.75, y=max_val * 0.6,
            text="Linea de prediccion<br>perfecta", showarrow=False,
            font=dict(size=10, color="#94A3B8"),
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

with col_b:
    st.markdown("**Rendimiento por Modelo de Estimacion**")
    model_df = run_query(f"""
        SELECT MODELO_ESTIMACION,
               ROUND(AVG(ABS(ERROR_ESTIMACION_PCT)), 1) AS ERROR_ABS_PROM,
               ROUND(AVG(CONFIANZA_ESTIMACION)*100, 1) AS CONFIANZA_PROM,
               COUNT(*) AS N
        FROM {TABLE}
        GROUP BY 1 ORDER BY 2
    """)
    if not model_df.empty:
        fig_model = go.Figure()
        fig_model.add_trace(go.Bar(
            x=model_df["MODELO_ESTIMACION"], y=model_df["ERROR_ABS_PROM"],
            name="Error Absoluto (%)", marker_color="#E63946",
            text=model_df["ERROR_ABS_PROM"].apply(lambda x: f"{x:.1f}%"),
            textposition="outside",
        ))
        fig_model.add_trace(go.Scatter(
            x=model_df["MODELO_ESTIMACION"], y=model_df["CONFIANZA_PROM"],
            name="Confianza (%)", mode="lines+markers+text",
            line=dict(color="#06D6A0", width=3), marker=dict(size=10),
            text=model_df["CONFIANZA_PROM"].apply(lambda x: f"{x:.0f}%"),
            textposition="top center", yaxis="y2",
        ))
        fig_model.update_layout(
            **CHART_LAYOUT, height=400,
            yaxis=dict(title="Error Absoluto (%)", gridcolor="#E2E8F0"),
            yaxis2=dict(title="Confianza (%)", side="right", overlaying="y", showgrid=False, range=[0, 100]),
            legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5),
            barmode="group",
        )
        st.plotly_chart(fig_model, use_container_width=True)

st.divider()
col_c, col_d = st.columns(2)

with col_c:
    st.markdown("**Desglose de Componentes de Costo Promedio**")
    comp_df = run_query(f"""
        SELECT ROUND(AVG(PCT_DANO_MATERIAL)*100, 1) AS DANO_MATERIAL,
               ROUND(AVG(PCT_GASTOS_MEDICOS)*100, 1) AS GASTOS_MEDICOS,
               ROUND(AVG(PCT_LUCRO_CESANTE)*100, 1) AS LUCRO_CESANTE,
               ROUND(AVG(PCT_HONORARIOS_LEGALES)*100, 1) AS HONORARIOS_LEGALES
        FROM {TABLE}
    """)
    if not comp_df.empty:
        labels = ["Dano Material", "Gastos Medicos", "Lucro Cesante", "Honorarios Legales"]
        values = [comp_df["DANO_MATERIAL"].iloc[0], comp_df["GASTOS_MEDICOS"].iloc[0],
                  comp_df["LUCRO_CESANTE"].iloc[0], comp_df["HONORARIOS_LEGALES"].iloc[0]]
        colors_donut = ["#E63946", "#F4A261", "#2A9D8F", "#06D6A0"]
        fig_donut = go.Figure(go.Pie(
            labels=labels, values=values, hole=0.55,
            marker=dict(colors=colors_donut, line=dict(color="white", width=2)),
            textinfo="label+percent", textfont=dict(size=11),
            hovertemplate="<b>%{label}</b><br>%{value:.1f}%<extra></extra>",
        ))
        fig_donut.update_layout(**CHART_LAYOUT, height=380, showlegend=False)
        fig_donut.add_annotation(
            text="<b>Composicion<br>de Costo</b>", x=0.5, y=0.5,
            font=dict(size=13, color="#264653"), showarrow=False,
        )
        st.plotly_chart(fig_donut, use_container_width=True)

with col_d:
    st.markdown("**Complejidad vs Error de Estimacion por Ramo**")
    bubble_df = run_query(f"""
        SELECT RAMO,
               ROUND(AVG(COMPLEJIDAD_CASO), 2) AS COMPLEJIDAD_PROM,
               ROUND(AVG(ABS(ERROR_ESTIMACION_PCT)), 1) AS ERROR_PROM,
               ROUND(SUM(COSTO_FINAL_REAL_COP)/1e9, 2) AS COSTO_TOTAL_B,
               COUNT(*) AS N
        FROM {TABLE}
        WHERE COSTO_FINAL_REAL_COP > 0
        GROUP BY 1
    """)
    if not bubble_df.empty:
        fig_bubble = go.Figure()
        fig_bubble.add_trace(go.Scatter(
            x=bubble_df["COMPLEJIDAD_PROM"], y=bubble_df["ERROR_PROM"],
            mode="markers+text", text=bubble_df["RAMO"],
            textposition="top center", textfont=dict(size=9, color="#334155"),
            marker=dict(
                size=bubble_df["COSTO_TOTAL_B"] * 20 + 15,
                color=bubble_df["ERROR_PROM"],
                colorscale=[[0, "#06D6A0"], [0.5, "#F4A261"], [1, "#E63946"]],
                opacity=0.75, line=dict(width=1, color="white"),
            ),
            hovertemplate="<b>%{text}</b><br>Complejidad: %{x:.1f}<br>Error: %{y:.1f}%<br><extra></extra>",
        ))
        fig_bubble.update_layout(
            **CHART_LAYOUT, height=380,
            xaxis=dict(title="Complejidad Promedio", gridcolor="#E2E8F0"),
            yaxis=dict(title="Error Absoluto Promedio (%)", gridcolor="#E2E8F0"),
            showlegend=False,
        )
        st.plotly_chart(fig_bubble, use_container_width=True)

st.divider()
col_e, col_f = st.columns(2)

with col_e:
    st.markdown("**Estimacion Inicial vs ML vs Real — por Tipo de Siniestro**")
    compare_df = run_query(f"""
        SELECT TIPO_SINIESTRO,
               ROUND(AVG(COSTO_ESTIMADO_INICIAL_COP)/1e6, 1) AS INICIAL,
               ROUND(AVG(COSTO_ESTIMADO_ML_COP)/1e6, 1) AS ML,
               ROUND(AVG(COSTO_FINAL_REAL_COP)/1e6, 1) AS REAL
        FROM {TABLE}
        WHERE COSTO_FINAL_REAL_COP > 0
        GROUP BY 1 ORDER BY 4 DESC LIMIT 8
    """)
    if not compare_df.empty:
        fig_comp = go.Figure()
        fig_comp.add_trace(go.Bar(
            x=compare_df["TIPO_SINIESTRO"], y=compare_df["INICIAL"],
            name="Estimacion Inicial", marker_color="#94A3B8",
        ))
        fig_comp.add_trace(go.Bar(
            x=compare_df["TIPO_SINIESTRO"], y=compare_df["ML"],
            name="Estimacion ML", marker_color="#2A9D8F",
        ))
        fig_comp.add_trace(go.Bar(
            x=compare_df["TIPO_SINIESTRO"], y=compare_df["REAL"],
            name="Costo Real", marker_color="#264653",
        ))
        fig_comp.update_layout(
            **CHART_LAYOUT, height=400, barmode="group",
            xaxis=dict(tickangle=-30),
            yaxis=dict(title="Costo Promedio ($M COP)", gridcolor="#E2E8F0"),
            legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5),
        )
        st.plotly_chart(fig_comp, use_container_width=True)

with col_f:
    st.markdown("**Distribucion de Confianza del Modelo**")
    conf_df = run_query(f"""
        SELECT
            CASE
                WHEN CONFIANZA_ESTIMACION >= 0.9 THEN 'Muy Alta (90%+)'
                WHEN CONFIANZA_ESTIMACION >= 0.75 THEN 'Alta (75-90%)'
                WHEN CONFIANZA_ESTIMACION >= 0.6 THEN 'Media (60-75%)'
                ELSE 'Baja (<60%)'
            END AS RANGO_CONFIANZA,
            COUNT(*) AS N,
            ROUND(AVG(ABS(ERROR_ESTIMACION_PCT)), 1) AS ERROR_REAL
        FROM {TABLE}
        GROUP BY 1
        ORDER BY MIN(CONFIANZA_ESTIMACION) DESC
    """)
    if not conf_df.empty:
        color_map = {"Muy Alta (90%+)": "#1D7847", "Alta (75-90%)": "#2A9D8F",
                     "Media (60-75%)": "#F4A261", "Baja (<60%)": "#E63946"}
        fig_conf = go.Figure()
        fig_conf.add_trace(go.Bar(
            x=conf_df["RANGO_CONFIANZA"], y=conf_df["N"],
            marker_color=[color_map.get(r, "#2A9D8F") for r in conf_df["RANGO_CONFIANZA"]],
            text=conf_df["N"].apply(lambda x: f"{x:,.0f}"),
            textposition="outside", name="Estimaciones",
        ))
        fig_conf.add_trace(go.Scatter(
            x=conf_df["RANGO_CONFIANZA"], y=conf_df["ERROR_REAL"],
            mode="lines+markers+text", name="Error Real (%)",
            line=dict(color="#F5B7B1", width=2.5), marker=dict(size=9, color="#E74C3C"),
            text=conf_df["ERROR_REAL"].apply(lambda x: f"{x:.1f}%"),
            textposition="top center", yaxis="y2",
        ))
        fig_conf.update_layout(
            **CHART_LAYOUT, height=400,
            yaxis=dict(title="Cantidad de Estimaciones", gridcolor="#E2E8F0"),
            yaxis2=dict(title="Error Real Promedio (%)", side="right", overlaying="y", showgrid=False),
            legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5),
        )
        st.plotly_chart(fig_conf, use_container_width=True)

st.divider()
st.markdown("**:material/query_stats: Indicadores Operativos**")
op1, op2, op3 = st.columns(3)

with op1:
    peritaje_df = run_query(f"""
        SELECT REQUIERE_PERITAJE, COUNT(*) AS N,
               ROUND(AVG(DIAS_DESARROLLO), 0) AS DIAS_PROM,
               ROUND(AVG(ABS(ERROR_ESTIMACION_PCT)), 1) AS ERROR_PROM
        FROM {TABLE} GROUP BY 1
    """)
    if not peritaje_df.empty:
        st.markdown("**Impacto del Peritaje**")
        for _, row in peritaje_df.iterrows():
            label = "Con peritaje" if row["REQUIERE_PERITAJE"] else "Sin peritaje"
            st.markdown(f"**{label}** — {int(row['N']):,} casos")
            c_l, c_r = st.columns(2)
            c_l.metric("Dias desarrollo", f"{int(row['DIAS_PROM'])}")
            c_r.metric("Error prom.", f"{row['ERROR_PROM']:.1f}%")

with op2:
    terceros_df = run_query(f"""
        SELECT INVOLUCRA_TERCEROS, COUNT(*) AS N,
               ROUND(AVG(COSTO_FINAL_REAL_COP)/1e6, 1) AS COSTO_PROM_M,
               ROUND(AVG(COMPLEJIDAD_CASO), 1) AS COMPLEJIDAD
        FROM {TABLE} GROUP BY 1
    """)
    if not terceros_df.empty:
        st.markdown("**Impacto de Terceros**")
        for _, row in terceros_df.iterrows():
            label = "Con terceros" if row["INVOLUCRA_TERCEROS"] else "Sin terceros"
            st.markdown(f"**{label}** — {int(row['N']):,} casos")
            c_l, c_r = st.columns(2)
            c_l.metric("Costo prom.", f"${row['COSTO_PROM_M']:.1f}M")
            c_r.metric("Complejidad", f"{row['COMPLEJIDAD']:.1f}")

with op3:
    versiones_df = run_query(f"""
        SELECT VERSIONES_ESTIMACION AS VERSIONES, COUNT(*) AS N,
               ROUND(AVG(ABS(ERROR_ESTIMACION_PCT)), 1) AS ERROR_PROM
        FROM {TABLE} GROUP BY 1 ORDER BY 1
    """)
    if not versiones_df.empty:
        st.markdown("**Error por # de Re-estimaciones**")
        fig_ver = go.Figure(go.Bar(
            x=versiones_df["VERSIONES"], y=versiones_df["ERROR_PROM"],
            marker=dict(
                color=versiones_df["ERROR_PROM"],
                colorscale=[[0, "#06D6A0"], [1, "#E63946"]],
            ),
            text=versiones_df["ERROR_PROM"].apply(lambda x: f"{x:.1f}%"),
            textposition="outside",
        ))
        fig_ver.update_layout(
            **CHART_LAYOUT, height=280,
            xaxis=dict(title="Versiones de estimacion", dtick=1),
            yaxis=dict(title="Error (%)", gridcolor="#E2E8F0"),
            showlegend=False,
        )
        st.plotly_chart(fig_ver, use_container_width=True)
