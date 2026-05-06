import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from app_pages.page_template import render_page, COLORS
from app_pages.conn_helper import run_query

config = {
    "key": "sfc",
    "table": "MGG_SEGUROS.FRAUDE_EN_SEGUROS.SCORING_FRAUDE_CLIENTE",
    "icon": ":material/fingerprint:",
    "title": "Scoring Fraude Cliente",
    "subtitle": "Score de riesgo de fraude evaluando historial, comportamiento atipico, conexiones y listas restrictivas.",
    "cards": [
        "Identifica clientes con alto riesgo de fraude antes de que presenten siniestros. Permite acciones preventivas como inspecciones adicionales o exclusiones.",
        "Score que integra historial de reclamaciones, monto historico, aseguradoras previas, cambios recientes de cobertura/beneficiario, conexion con redes de fraude y listas restrictivas.",
        "Prevencion de fraude es 10x mas efectiva que deteccion post-siniestro. Reduce siniestralidad y protege la rentabilidad del portafolio.",
    ],
    "date_col": "FECHA_EVALUACION",
    "filter_cols": ["PERFIL_RIESGO_FRAUDE", "RAMO", "CIUDAD"],
    "kpi_query": """SELECT
        ROUND(AVG(SCORE_FRAUDE), 0) AS SCORE_AVG,
        ROUND(AVG(CONFIANZA_SCORE)*100, 1) AS CONFIANZA,
        ROUND(AVG(INDICADORES_ACTIVOS), 1) AS INDICADORES,
        ROUND(SUM(MONTO_RECLAMADO_HISTORICO_COP)/1e9, 1) AS MONTO_HIST_B,
        ROUND(COUNT(CASE WHEN EN_BLACKLIST THEN 1 END)*100.0/COUNT(*), 1) AS BLACKLIST_PCT,
        ROUND(COUNT(CASE WHEN CONEXION_RED_FRAUDE THEN 1 END)*100.0/COUNT(*), 1) AS RED_FRAUDE_PCT,
        ROUND(COUNT(CASE WHEN REQUIERE_INVESTIGACION THEN 1 END)*100.0/COUNT(*), 1) AS REQ_INV_PCT,
        COUNT(*) AS EVALUACIONES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Score fraude prom.", "Confianza score", "Indicadores activos", "Monto historico", "En blacklist", "Conexion red fraude", "Req. investigacion", "Evaluaciones"],
    "kpi_formats": ["{:.0f}", "{:.1f}%", "{:.1f}", "${:.1f}B", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_EVALUACION) AS MES,
        ROUND(AVG(SCORE_FRAUDE), 0) AS SCORE,
        COUNT(CASE WHEN REQUIERE_INVESTIGACION THEN 1 END) AS INVESTIGACIONES
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Score fraude", "Investigaciones"],
    "treemap_query": """SELECT INDICADOR_PRINCIPAL, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Indicadores principales de fraude", "colorscale": [[0, "#FFF3E0"], [0.5, "#FF8B00"], [1, "#DE350B"]]},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(SCORE_FRAUDE),0) AS SCORE FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "SCORE", "title": "Score fraude por ciudad", "colorscale": ["#36B37E", "#FF8B00", "#DE350B"]},
    "diagnostics": None, "simulator": None,
}

render_page(config)

TABLE = config["table"]
CHART_LAYOUT = dict(
    template="plotly_white", paper_bgcolor="#FAFBFC", plot_bgcolor="#FAFBFC",
    font=dict(family="Inter, sans-serif", color="#334155"),
    margin=dict(l=40, r=40, t=40, b=40),
)

st.divider()
st.subheader(":material/shield: Analisis Avanzado de Fraude")

st.markdown("**Mapa de Calor: Score Fraude Promedio por Ramo y Perfil de Riesgo**")
heatmap_df = run_query(f"""
    SELECT RAMO, PERFIL_RIESGO_FRAUDE,
           ROUND(AVG(SCORE_FRAUDE), 0) AS SCORE_PROM
    FROM {TABLE}
    GROUP BY 1, 2
""")
if not heatmap_df.empty:
    pivot = heatmap_df.pivot(index="RAMO", columns="PERFIL_RIESGO_FRAUDE", values="SCORE_PROM")
    col_order = ["Sin riesgo", "Riesgo bajo", "Riesgo medio", "Riesgo alto", "Riesgo critico", "En blacklist"]
    col_order = [c for c in col_order if c in pivot.columns]
    pivot = pivot[col_order]
    pivot = pivot.sort_values(col_order[-1], ascending=False) if col_order else pivot

    z_vals = pivot.values
    text_vals = [[f"{int(v)}" if not np.isnan(v) else "" for v in row] for row in z_vals]

    fig_heat = go.Figure(go.Heatmap(
        z=z_vals, x=pivot.columns.tolist(), y=pivot.index.tolist(),
        text=text_vals, texttemplate="%{text}", textfont=dict(size=13, color="white"),
        colorscale=[[0, "#06D6A0"], [0.3, "#2A9D8F"], [0.5, "#F4A261"], [0.75, "#E63946"], [1, "#9B1B30"]],
        colorbar=dict(title="Score"),
        hovertemplate="<b>%{y}</b> × %{x}<br>Score: %{z}<extra></extra>",
    ))
    fig_heat.update_layout(
        **CHART_LAYOUT, height=420,
        xaxis=dict(side="bottom", tickangle=-30),
        yaxis=dict(autorange="reversed"),
    )
    st.plotly_chart(fig_heat, use_container_width=True)

st.divider()
col_a, col_b = st.columns(2)

with col_a:
    st.markdown("**Score Fraude vs Monto Reclamado Historico**")
    scatter_df = run_query(f"""
        SELECT SCORE_FRAUDE, MONTO_RECLAMADO_HISTORICO_COP/1e6 AS MONTO_M,
               PERFIL_RIESGO_FRAUDE, INDICADORES_ACTIVOS, RAMO
        FROM {TABLE}
        ORDER BY RANDOM() LIMIT 600
    """)
    if not scatter_df.empty:
        color_map = {
            "Sin riesgo": "#06D6A0", "Riesgo bajo": "#2A9D8F",
            "Riesgo medio": "#F4A261", "Riesgo alto": "#E63946",
            "Riesgo critico": "#9B1B30", "En blacklist": "#1B1B1B",
        }
        fig_scatter = go.Figure()
        for perfil in scatter_df["PERFIL_RIESGO_FRAUDE"].unique():
            mask = scatter_df["PERFIL_RIESGO_FRAUDE"] == perfil
            sub = scatter_df[mask]
            fig_scatter.add_trace(go.Scatter(
                x=sub["SCORE_FRAUDE"], y=sub["MONTO_M"],
                mode="markers", name=perfil,
                marker=dict(
                    size=sub["INDICADORES_ACTIVOS"] * 3 + 5,
                    color=color_map.get(perfil, "#94A3B8"),
                    opacity=0.65, line=dict(width=0.5, color="white"),
                ),
                text=sub["RAMO"],
                hovertemplate="<b>%{text}</b><br>Score: %{x}<br>Monto: $%{y:.1f}M<extra></extra>",
            ))
        fig_scatter.update_layout(
            **CHART_LAYOUT, height=420,
            xaxis=dict(title="Score Fraude", gridcolor="#E2E8F0"),
            yaxis=dict(title="Monto Reclamado ($M COP)", gridcolor="#E2E8F0"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5, font=dict(size=9)),
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

with col_b:
    st.markdown("**Rendimiento por Modelo de Scoring**")
    model_df = run_query(f"""
        SELECT MODELO,
               ROUND(AVG(SCORE_FRAUDE), 0) AS SCORE_PROM,
               ROUND(AVG(CONFIANZA_SCORE)*100, 1) AS CONFIANZA_PROM,
               ROUND(AVG(INDICADORES_ACTIVOS), 1) AS INDICADORES_PROM,
               COUNT(*) AS N
        FROM {TABLE}
        GROUP BY 1 ORDER BY 2 DESC
    """)
    if not model_df.empty:
        fig_model = go.Figure()
        fig_model.add_trace(go.Bar(
            x=model_df["MODELO"], y=model_df["SCORE_PROM"],
            name="Score Prom.", marker_color="#264653",
            text=model_df["SCORE_PROM"].apply(lambda x: f"{x:.0f}"),
            textposition="outside",
        ))
        fig_model.add_trace(go.Scatter(
            x=model_df["MODELO"], y=model_df["CONFIANZA_PROM"],
            name="Confianza (%)", mode="lines+markers+text",
            line=dict(color="#06D6A0", width=3), marker=dict(size=10),
            text=model_df["CONFIANZA_PROM"].apply(lambda x: f"{x:.0f}%"),
            textposition="top center", yaxis="y2",
        ))
        fig_model.update_layout(
            **CHART_LAYOUT, height=420,
            yaxis=dict(title="Score Promedio", gridcolor="#E2E8F0"),
            yaxis2=dict(title="Confianza (%)", side="right", overlaying="y", showgrid=False, range=[0, 100]),
            legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5),
            barmode="group",
        )
        st.plotly_chart(fig_model, use_container_width=True)

st.divider()
col_c, col_d = st.columns(2)

with col_c:
    st.markdown("**Distribucion de Indicadores Principales**")
    ind_df = run_query(f"""
        SELECT INDICADOR_PRINCIPAL, COUNT(*) AS N,
               ROUND(AVG(SCORE_FRAUDE), 0) AS SCORE_PROM
        FROM {TABLE}
        WHERE INDICADOR_PRINCIPAL != 'Sin indicador relevante'
        GROUP BY 1 ORDER BY 3 DESC
    """)
    if not ind_df.empty:
        colors_donut = ["#9B1B30", "#E63946", "#F4A261", "#E9C46A", "#2A9D8F", "#06D6A0", "#1D7847", "#264653"]
        fig_donut = go.Figure(go.Pie(
            labels=ind_df["INDICADOR_PRINCIPAL"], values=ind_df["N"],
            hole=0.55,
            marker=dict(colors=colors_donut[:len(ind_df)], line=dict(color="white", width=2)),
            textinfo="label+percent", textfont=dict(size=10),
            hovertemplate="<b>%{label}</b><br>Casos: %{value:,}<br>Score prom: " +
                          ind_df["SCORE_PROM"].astype(str).tolist().__repr__() + "<extra></extra>",
        ))
        fig_donut.update_layout(**CHART_LAYOUT, height=400, showlegend=False)
        fig_donut.add_annotation(
            text="<b>Indicadores<br>de Fraude</b>", x=0.5, y=0.5,
            font=dict(size=12, color="#264653"), showarrow=False,
        )
        st.plotly_chart(fig_donut, use_container_width=True)

with col_d:
    st.markdown("**Mapa de Calor: Indicador vs Ramo (Score Promedio)**")
    heat2_df = run_query(f"""
        SELECT INDICADOR_PRINCIPAL, RAMO,
               ROUND(AVG(SCORE_FRAUDE), 0) AS SCORE_PROM
        FROM {TABLE}
        WHERE INDICADOR_PRINCIPAL != 'Sin indicador relevante'
        GROUP BY 1, 2
    """)
    if not heat2_df.empty:
        pivot2 = heat2_df.pivot(index="INDICADOR_PRINCIPAL", columns="RAMO", values="SCORE_PROM")
        z2 = pivot2.values
        text2 = [[f"{int(v)}" if not pd.isna(v) else "" for v in row] for row in z2]
        fig_heat2 = go.Figure(go.Heatmap(
            z=z2, x=pivot2.columns.tolist(), y=pivot2.index.tolist(),
            text=text2, texttemplate="%{text}", textfont=dict(size=11, color="white"),
            colorscale=[[0, "#06D6A0"], [0.4, "#2A9D8F"], [0.6, "#F4A261"], [0.8, "#E63946"], [1, "#9B1B30"]],
            colorbar=dict(title="Score"),
            hovertemplate="<b>%{y}</b><br>Ramo: %{x}<br>Score: %{z}<extra></extra>",
        ))
        fig_heat2.update_layout(
            **CHART_LAYOUT, height=400,
            xaxis=dict(tickangle=-30),
            yaxis=dict(autorange="reversed"),
        )
        st.plotly_chart(fig_heat2, use_container_width=True)

st.divider()
col_e, col_f = st.columns(2)

with col_e:
    st.markdown("**Clientes por Rango de Edad y Perfil de Riesgo**")
    age_df = run_query(f"""
        SELECT
            CASE
                WHEN EDAD < 25 THEN '18-24'
                WHEN EDAD < 35 THEN '25-34'
                WHEN EDAD < 45 THEN '35-44'
                WHEN EDAD < 55 THEN '45-54'
                WHEN EDAD < 65 THEN '55-64'
                ELSE '65+'
            END AS RANGO_EDAD,
            PERFIL_RIESGO_FRAUDE,
            COUNT(*) AS N
        FROM {TABLE}
        GROUP BY 1, 2
        ORDER BY MIN(EDAD)
    """)
    if not age_df.empty:
        age_order = ["18-24", "25-34", "35-44", "45-54", "55-64", "65+"]
        risk_colors = {
            "Sin riesgo": "#06D6A0", "Riesgo bajo": "#2A9D8F",
            "Riesgo medio": "#F4A261", "Riesgo alto": "#E63946",
            "Riesgo critico": "#9B1B30", "En blacklist": "#1B1B1B",
        }
        fig_age = go.Figure()
        for perfil in ["Sin riesgo", "Riesgo bajo", "Riesgo medio", "Riesgo alto", "Riesgo critico", "En blacklist"]:
            sub = age_df[age_df["PERFIL_RIESGO_FRAUDE"] == perfil]
            if not sub.empty:
                sub_ordered = sub.set_index("RANGO_EDAD").reindex(age_order).fillna(0).reset_index()
                fig_age.add_trace(go.Bar(
                    x=sub_ordered["RANGO_EDAD"], y=sub_ordered["N"],
                    name=perfil, marker_color=risk_colors.get(perfil, "#94A3B8"),
                ))
        fig_age.update_layout(
            **CHART_LAYOUT, height=400, barmode="stack",
            xaxis=dict(title="Rango de Edad", categoryorder="array", categoryarray=age_order),
            yaxis=dict(title="Cantidad de Clientes", gridcolor="#E2E8F0"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5, font=dict(size=9)),
        )
        st.plotly_chart(fig_age, use_container_width=True)

with col_f:
    st.markdown("**Cambios Recientes vs Score (Comportamiento Sospechoso)**")
    changes_df = run_query(f"""
        SELECT CAMBIOS_COBERTURA_12M + CAMBIOS_BENEFICIARIO_12M AS CAMBIOS_TOTAL,
               SCORE_FRAUDE, PERFIL_RIESGO_FRAUDE, RAMO
        FROM {TABLE}
        WHERE CAMBIOS_COBERTURA_12M + CAMBIOS_BENEFICIARIO_12M > 0
        ORDER BY RANDOM() LIMIT 400
    """)
    if not changes_df.empty:
        fig_changes = go.Figure()
        fig_changes.add_trace(go.Scatter(
            x=changes_df["CAMBIOS_TOTAL"], y=changes_df["SCORE_FRAUDE"],
            mode="markers",
            marker=dict(
                size=8, opacity=0.6,
                color=changes_df["SCORE_FRAUDE"],
                colorscale=[[0, "#06D6A0"], [0.5, "#F4A261"], [1, "#E63946"]],
                colorbar=dict(title="Score"),
                line=dict(width=0.3, color="white"),
            ),
            text=changes_df["RAMO"],
            hovertemplate="<b>%{text}</b><br>Cambios: %{x}<br>Score: %{y}<extra></extra>",
        ))
        fig_changes.update_layout(
            **CHART_LAYOUT, height=400,
            xaxis=dict(title="Cambios (Cobertura + Beneficiario) 12M", gridcolor="#E2E8F0", dtick=1),
            yaxis=dict(title="Score Fraude", gridcolor="#E2E8F0"),
            showlegend=False,
        )
        st.plotly_chart(fig_changes, use_container_width=True)

st.divider()
st.markdown("**:material/query_stats: Indicadores Operativos de Riesgo**")
op1, op2, op3 = st.columns(3)

with op1:
    bl_df = run_query(f"""
        SELECT EN_BLACKLIST, COUNT(*) AS N,
               ROUND(AVG(SCORE_FRAUDE), 0) AS SCORE_PROM,
               ROUND(AVG(MONTO_RECLAMADO_HISTORICO_COP)/1e6, 1) AS MONTO_M
        FROM {TABLE} GROUP BY 1
    """)
    if not bl_df.empty:
        st.markdown("**Impacto Blacklist**")
        for _, row in bl_df.iterrows():
            label = "En blacklist" if row["EN_BLACKLIST"] else "Fuera blacklist"
            st.markdown(f"**{label}** — {int(row['N']):,} clientes")
            c_l, c_r = st.columns(2)
            c_l.metric("Score prom.", f"{int(row['SCORE_PROM'])}")
            c_r.metric("Monto hist.", f"${row['MONTO_M']:.1f}M")

with op2:
    pep_df = run_query(f"""
        SELECT PEP, COUNT(*) AS N,
               ROUND(AVG(SCORE_FRAUDE), 0) AS SCORE_PROM,
               ROUND(AVG(INDICADORES_ACTIVOS), 1) AS IND_PROM
        FROM {TABLE} GROUP BY 1
    """)
    if not pep_df.empty:
        st.markdown("**Personas Expuestas Politicamente (PEP)**")
        for _, row in pep_df.iterrows():
            label = "PEP" if row["PEP"] else "No PEP"
            st.markdown(f"**{label}** — {int(row['N']):,} clientes")
            c_l, c_r = st.columns(2)
            c_l.metric("Score prom.", f"{int(row['SCORE_PROM'])}")
            c_r.metric("Indicadores", f"{row['IND_PROM']:.1f}")

with op3:
    aseg_df = run_query(f"""
        SELECT ASEGURADORAS_PREVIAS, COUNT(*) AS N,
               ROUND(AVG(SCORE_FRAUDE), 0) AS SCORE_PROM
        FROM {TABLE}
        GROUP BY 1 ORDER BY 1
    """)
    if not aseg_df.empty:
        st.markdown("**Score por # Aseguradoras Previas**")
        fig_aseg = go.Figure(go.Bar(
            x=aseg_df["ASEGURADORAS_PREVIAS"], y=aseg_df["SCORE_PROM"],
            marker=dict(
                color=aseg_df["SCORE_PROM"],
                colorscale=[[0, "#06D6A0"], [0.5, "#F4A261"], [1, "#E63946"]],
            ),
            text=aseg_df["SCORE_PROM"].apply(lambda x: f"{x:.0f}"),
            textposition="outside",
        ))
        fig_aseg.update_layout(
            **CHART_LAYOUT, height=280,
            xaxis=dict(title="# Aseguradoras previas", dtick=1),
            yaxis=dict(title="Score Fraude", gridcolor="#E2E8F0"),
            showlegend=False,
        )
        st.plotly_chart(fig_aseg, use_container_width=True)
