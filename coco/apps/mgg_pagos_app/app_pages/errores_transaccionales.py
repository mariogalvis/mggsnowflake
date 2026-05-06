import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from decimal import Decimal

TABLE = "MGG_PAGOS.DISPONIBILIDAD_Y_PERFORMANCE.ERRORES_TRANSACCIONALES"

COLORS = ["#29B5E8", "#11567F", "#71D4F0", "#0E3A53", "#A3E4F7", "#1B8BBF", "#5BC3E8", "#083248"]


from app_pages.conn_helper import run_query



def color_tag(value, good, bad, fmt="{:.1f}%", inverse=False):
    v = float(value)
    if not inverse:
        if v >= good:
            return f":green[**{fmt.format(v)}**]"
        elif v >= bad:
            return f":orange[**{fmt.format(v)}**]"
        else:
            return f":red[**{fmt.format(v)}**]"
    else:
        if v <= good:
            return f":green[**{fmt.format(v)}**]"
        elif v <= bad:
            return f":orange[**{fmt.format(v)}**]"
        else:
            return f":red[**{fmt.format(v)}**]"


@st.cache_data(ttl=300, show_spinner=False)
def get_filter_options():
    tipos = run_query(f"SELECT DISTINCT TIPO_ERROR FROM {TABLE} ORDER BY 1")["TIPO_ERROR"].tolist()
    componentes = run_query(f"SELECT DISTINCT COMPONENTE_ORIGEN FROM {TABLE} ORDER BY 1")["COMPONENTE_ORIGEN"].tolist()
    severidades = run_query(f"SELECT DISTINCT SEVERIDAD FROM {TABLE} ORDER BY 1")["SEVERIDAD"].tolist()
    impactos = run_query(f"SELECT DISTINCT IMPACTO FROM {TABLE} ORDER BY 1")["IMPACTO"].tolist()
    fechas = run_query(f"SELECT MIN(FECHA_HORA)::DATE AS FMIN, MAX(FECHA_HORA)::DATE AS FMAX FROM {TABLE}")
    return tipos, componentes, severidades, impactos, fechas


st.header(":material/bug_report: Errores transaccionales")
st.caption("Clasificacion, severidad, impacto financiero, resolucion y analisis de causa raiz de errores en el ecosistema de pagos.")

c1, c2, c3 = st.columns(3)
with c1:
    with st.container(border=True):
        st.markdown("**:material/lightbulb: Que resuelve**")
        st.markdown("Visibilidad completa de errores por tipo, componente y severidad para priorizar la resolucion y minimizar el impacto en el negocio.")
with c2:
    with st.container(border=True):
        st.markdown("**:material/settings: Como funciona**")
        st.markdown("Cada error se clasifica por severidad (Critico/Alto/Medio/Bajo), componente de origen, tasa de error y tiempo de resolucion. Incluye trazabilidad de RCA y fix permanente.")
with c3:
    with st.container(border=True):
        st.markdown("**:material/trending_up: Valor de negocio**")
        st.markdown("Reducir tasa de error mejora conversion, confianza del ecosistema y reduce costos operativos de soporte e incidentes repetitivos.")

tipos, componentes, severidades, impactos, fechas_df = get_filter_options()
fmin = pd.to_datetime(fechas_df["FMIN"].iloc[0]).date()
fmax = pd.to_datetime(fechas_df["FMAX"].iloc[0]).date()

dx_kpi = run_query(f"""
    SELECT
        ROUND(AVG(TASA_ERROR_PCT), 2) AS TASA_PROM,
        SUM(TRANSACCIONES_AFECTADAS) AS TX_AFECT,
        ROUND(SUM(IMPACTO_FINANCIERO_COP)/1e6, 1) AS IMPACTO_M,
        ROUND(AVG(TIEMPO_RESOLUCION_MIN), 1) AS MTTR,
        SUM(CASE WHEN RESUELTA THEN 1 ELSE 0 END) AS RESUELTAS,
        COUNT(*) AS TOTAL,
        SUM(CASE WHEN FIX_PERMANENTE_APLICADO THEN 1 ELSE 0 END) AS FIX_PERM,
        SUM(RECURRENCIAS_30D) AS RECURRENCIAS
    FROM {TABLE}
""")

if not dx_kpi.empty and dx_kpi["TOTAL"].iloc[0] > 0:
    dx = dx_kpi.iloc[0]
    dx_tasa = float(dx["TASA_PROM"])
    dx_mttr = float(dx["MTTR"])
    dx_resueltas_pct = round(float(dx["RESUELTAS"]) / float(dx["TOTAL"]) * 100, 1)
    dx_fix_pct = round(float(dx["FIX_PERM"]) / float(dx["TOTAL"]) * 100, 1)

    dx_top_comp = run_query(f"SELECT COMPONENTE_ORIGEN, COUNT(*) AS N FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC LIMIT 1")
    dx_top_sev = run_query(f"SELECT SEVERIDAD, COUNT(*) AS N FROM {TABLE} WHERE SEVERIDAD IN ('Critico','Alto') GROUP BY 1 ORDER BY 2 DESC LIMIT 1")

    if dx_tasa <= 1.0:
        dx_estado = f"Tasa de error promedio ({color_tag(dx_tasa, 0, 2, inverse=True)}) :green[**controlada**]. Resolucion: {dx_resueltas_pct:.1f}%."
    elif dx_tasa <= 3.0:
        dx_estado = f"Tasa de error ({color_tag(dx_tasa, 0, 2, inverse=True)}) en :orange[**zona de atencion**]. MTTR: {dx_mttr:.0f} min."
    else:
        dx_estado = f"Tasa de error ({color_tag(dx_tasa, 0, 2, inverse=True)}) :red[**elevada**]. Impacto: ${float(dx['IMPACTO_M']):.1f}M COP."

    dx_lines = [dx_estado]
    if not dx_top_comp.empty:
        dx_lines.append(f"- Componente con mas errores: **{dx_top_comp.iloc[0]['COMPONENTE_ORIGEN']}** ({int(dx_top_comp.iloc[0]['N'])} errores)")
    dx_lines.append(f"- Fix permanente aplicado: **{dx_fix_pct:.1f}%** — Recurrencias 30d: **{int(dx['RECURRENCIAS']):,}**")
    if not dx_top_sev.empty:
        dx_lines.append(f"- Errores criticos/altos: **{dx_top_sev.iloc[0]['SEVERIDAD']}** ({int(dx_top_sev.iloc[0]['N'])} eventos)")

    st.markdown("")
    d1, d2 = st.columns(2)
    with d1:
        with st.container(border=True):
            st.markdown("**:material/monitoring: Diagnostico CEO**")
            st.markdown("\n".join(dx_lines))
    with d2:
        with st.container(border=True):
            st.markdown("**:material/recommend: Recomendaciones**")
            recs = []
            if dx_tasa > 2:
                recs.append("1. :red[**Reducir tasa de error**]: por encima del umbral aceptable (2%). Priorizar los codigos Pareto.")
            if dx_fix_pct < 50:
                recs.append("2. :orange[**Incrementar fix permanentes**]: menos de la mitad tienen solucion definitiva, causando recurrencias.")
            if dx_mttr > 60:
                recs.append("3. :orange[**Acelerar MTTR**]: tiempo de resolucion elevado. Automatizar deteccion y respuesta.")
            if int(dx["RECURRENCIAS"]) > 500:
                recs.append("4. :red[**Atacar recurrencias**]: alto volumen de errores repetitivos impacta confiabilidad.")
            if not recs:
                recs.append(":green[**Gestion de errores dentro de parametros.**] Mantener cultura de post-mortem y mejora continua.")
            st.markdown("\n".join(recs))

st.divider()

fc1, fc2, fc3, fc4, fc5 = st.columns(5)
with fc1:
    fecha_rng = st.date_input("Periodo", value=(fmin, fmax), min_value=fmin, max_value=fmax, key="err_fecha")
    if isinstance(fecha_rng, (list, tuple)) and len(fecha_rng) == 2:
        fecha_inicio, fecha_fin = fecha_rng
    else:
        fecha_inicio, fecha_fin = fmin, fmax
with fc2:
    tipo_sel = st.selectbox("Tipo error", ["Todos"] + tipos, key="err_tipo")
with fc3:
    comp_sel = st.selectbox("Componente", ["Todos"] + componentes, key="err_comp")
with fc4:
    sev_sel = st.selectbox("Severidad", ["Todos"] + severidades, key="err_sev")
with fc5:
    imp_sel = st.selectbox("Impacto", ["Todos"] + impactos, key="err_imp")

WHERE = f"FECHA_HORA::DATE BETWEEN '{fecha_inicio}' AND '{fecha_fin}'"
if tipo_sel != "Todos":
    WHERE += f" AND TIPO_ERROR = '{tipo_sel}'"
if comp_sel != "Todos":
    WHERE += f" AND COMPONENTE_ORIGEN = '{comp_sel}'"
if sev_sel != "Todos":
    WHERE += f" AND SEVERIDAD = '{sev_sel}'"
if imp_sel != "Todos":
    WHERE += f" AND IMPACTO = '{imp_sel}'"

kpi_df = run_query(f"""
    SELECT
        ROUND(AVG(TASA_ERROR_PCT), 2) AS TASA_PROM,
        SUM(TRANSACCIONES_AFECTADAS) AS TX_AFECT,
        ROUND(SUM(IMPACTO_FINANCIERO_COP)/1e6, 1) AS IMPACTO_M,
        ROUND(AVG(TIEMPO_RESOLUCION_MIN), 1) AS MTTR,
        SUM(CASE WHEN RESUELTA THEN 1 ELSE 0 END) AS RESUELTAS,
        COUNT(*) AS TOTAL,
        SUM(CASE WHEN FIX_PERMANENTE_APLICADO THEN 1 ELSE 0 END) AS FIX_PERM,
        SUM(RECURRENCIAS_30D) AS RECURRENCIAS
    FROM {TABLE}
    WHERE {WHERE}
""")

if kpi_df.empty or kpi_df["TOTAL"].iloc[0] == 0:
    st.info("No hay datos para los filtros seleccionados.")
else:
    r = kpi_df.iloc[0]
    total = int(r["TOTAL"])

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Tasa error prom", f"{float(r['TASA_PROM']):.2f}%")
    k2.metric("Errores registrados", f"{total:,}")
    k3.metric("Txns afectadas", f"{int(r['TX_AFECT']):,}")
    k4.metric("Impacto financiero", f"${float(r['IMPACTO_M']):,.1f}M COP")

    k5, k6, k7, k8 = st.columns(4)
    k5.metric("MTTR promedio", f"{float(r['MTTR']):.1f} min")
    k6.metric("Resueltos", f"{round(float(r['RESUELTAS'])/total*100, 1):.1f}%")
    k7.metric("Fix permanente", f"{round(float(r['FIX_PERM'])/total*100, 1):.1f}%")
    k8.metric("Recurrencias 30d", f"{int(r['RECURRENCIAS']):,}")

    st.divider()

    st.markdown("**Analisis de Pareto — Codigos de error (80/20)**")
    pareto_df = run_query(f"""
        SELECT CODIGO_ERROR, SUM(FRECUENCIA_DIA) AS FREQ
        FROM {TABLE}
        WHERE {WHERE}
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 20
    """)
    if not pareto_df.empty:
        total_freq = pareto_df["FREQ"].sum()
        pareto_df["CUM_PCT"] = (pareto_df["FREQ"].cumsum() / total_freq * 100).round(1)
        fig_pareto = go.Figure()
        fig_pareto.add_trace(go.Bar(
            x=pareto_df["CODIGO_ERROR"].tolist(),
            y=pareto_df["FREQ"].tolist(),
            name="Frecuencia",
            marker=dict(color=COLORS[0]),
            hovertemplate="<b>%{x}</b><br>Frecuencia: %{y:,}<extra></extra>",
        ))
        fig_pareto.add_trace(go.Scatter(
            x=pareto_df["CODIGO_ERROR"].tolist(),
            y=pareto_df["CUM_PCT"].tolist(),
            name="% Acumulado",
            yaxis="y2",
            line=dict(color=COLORS[1], width=2),
            marker=dict(size=6),
            hovertemplate="<b>%{x}</b><br>Acumulado: %{y:.1f}%<extra></extra>",
        ))
        fig_pareto.add_hline(y=80, line_dash="dash", line_color="#DE350B", opacity=0.5,
                             annotation_text="80%", yref="y2")
        fig_pareto.update_layout(
            template="plotly_white", paper_bgcolor="#FFFFFF", height=420,
            margin=dict(l=60, r=60, t=10, b=80),
            xaxis=dict(tickangle=-35, tickfont=dict(size=10)),
            yaxis=dict(title="Frecuencia"),
            yaxis2=dict(title="% Acumulado", overlaying="y", side="right", range=[0, 105], ticksuffix="%"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        st.plotly_chart(fig_pareto, use_container_width=True, key="err_pareto")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Matriz de riesgo — Severidad x Impacto**")
        matrix_df = run_query(f"""
            SELECT SEVERIDAD, IMPACTO, COUNT(*) AS N
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1, 2
        """)
        if not matrix_df.empty:
            pivot_m = matrix_df.pivot_table(index="SEVERIDAD", columns="IMPACTO", values="N", fill_value=0)
            sev_order = ["Bajo", "Medio", "Alto", "Critico"]
            existing_sevs = [s for s in sev_order if s in pivot_m.index]
            if existing_sevs:
                pivot_m = pivot_m.reindex(existing_sevs)
            fig_matrix = go.Figure(go.Heatmap(
                z=pivot_m.values.tolist(),
                x=pivot_m.columns.tolist(),
                y=pivot_m.index.tolist(),
                colorscale=[[0, "#E8F5E9"], [0.3, "#FFF8E1"], [0.6, "#FFAB00"], [1, "#DE350B"]],
                text=[[f"{int(v)}" for v in row] for row in pivot_m.values.tolist()],
                texttemplate="%{text}",
                textfont=dict(size=14, color="white"),
                hovertemplate="Severidad: %{y}<br>Impacto: %{x}<br>Errores: %{z}<extra></extra>",
                colorbar=dict(title="Errores"),
            ))
            fig_matrix.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=380,
                margin=dict(l=100, r=20, t=10, b=60),
                xaxis=dict(title="Impacto", tickfont=dict(size=12)),
                yaxis=dict(title="Severidad", tickfont=dict(size=12)),
            )
            st.plotly_chart(fig_matrix, use_container_width=True, key="err_matrix")

    with col2:
        st.markdown("**MTTR por severidad — Distribucion**")
        mttr_df = run_query(f"""
            SELECT SEVERIDAD, TIEMPO_RESOLUCION_MIN
            FROM {TABLE}
            WHERE {WHERE} AND RESUELTA = TRUE
        """)
        if not mttr_df.empty:
            fig_mttr = go.Figure()
            sev_order = ["Bajo", "Medio", "Alto", "Critico"]
            sev_colors = {"Bajo": "#36B37E", "Medio": COLORS[0], "Alto": "#FFAB00", "Critico": "#DE350B"}
            for sev in sev_order:
                sub = mttr_df[mttr_df["SEVERIDAD"] == sev]
                if not sub.empty:
                    fig_mttr.add_trace(go.Box(
                        y=sub["TIEMPO_RESOLUCION_MIN"].tolist(),
                        name=sev,
                        marker=dict(color=sev_colors.get(sev, COLORS[0])),
                        boxmean="sd",
                    ))
            fig_mttr.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=380,
                margin=dict(l=60, r=20, t=10, b=60),
                yaxis=dict(title="Tiempo resolucion (min)"),
                showlegend=False,
            )
            st.plotly_chart(fig_mttr, use_container_width=True, key="err_mttr_box")

    col3, col4 = st.columns(2)

    with col3:
        st.markdown("**Tipo de resolucion por componente**")
        res_df = run_query(f"""
            SELECT COMPONENTE_ORIGEN, TIPO_RESOLUCION, COUNT(*) AS N
            FROM {TABLE}
            WHERE {WHERE} AND RESUELTA = TRUE
            GROUP BY 1, 2
            ORDER BY 1, 2
        """)
        if not res_df.empty:
            fig_res = go.Figure()
            res_types = sorted(res_df["TIPO_RESOLUCION"].unique().tolist())
            comps = sorted(res_df["COMPONENTE_ORIGEN"].unique().tolist())
            for j, rt in enumerate(res_types):
                sub = res_df[res_df["TIPO_RESOLUCION"] == rt]
                vals = []
                for comp in comps:
                    match = sub[sub["COMPONENTE_ORIGEN"] == comp]
                    vals.append(int(match["N"].iloc[0]) if not match.empty else 0)
                fig_res.add_trace(go.Bar(
                    x=comps, y=vals, name=rt,
                    marker=dict(color=COLORS[j % len(COLORS)]),
                    hovertemplate=f"<b>%{{x}}</b><br>{rt}: %{{y:,}}<extra></extra>",
                ))
            fig_res.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=60, r=20, t=10, b=80),
                barmode="stack",
                xaxis=dict(tickangle=-30, tickfont=dict(size=10)),
                yaxis=dict(title="Errores resueltos"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, font=dict(size=10)),
            )
            st.plotly_chart(fig_res, use_container_width=True, key="err_resolution")

    with col4:
        st.markdown("**Errores cronicos — Recurrencia vs impacto financiero**")
        rec_df = run_query(f"""
            SELECT CODIGO_ERROR,
                   SUM(RECURRENCIAS_30D) AS RECURRENCIAS,
                   ROUND(SUM(IMPACTO_FINANCIERO_COP)/1e6, 2) AS IMPACTO_M,
                   SUM(FRECUENCIA_DIA) AS FREQ,
                   MAX(CASE WHEN FIX_PERMANENTE_APLICADO THEN 'Si' ELSE 'No' END) AS FIX
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1
            HAVING SUM(RECURRENCIAS_30D) > 0
            ORDER BY 3 DESC
            LIMIT 30
        """)
        if not rec_df.empty:
            fig_rec = go.Figure()
            max_freq = max(rec_df["FREQ"].max(), 1)
            for fix_val in ["No", "Si"]:
                sub = rec_df[rec_df["FIX"] == fix_val]
                if sub.empty:
                    continue
                sizes = (sub["FREQ"] / max_freq * 30 + 6).tolist()
                fig_rec.add_trace(go.Scatter(
                    x=sub["RECURRENCIAS"].tolist(),
                    y=sub["IMPACTO_M"].tolist(),
                    mode="markers+text",
                    text=sub["CODIGO_ERROR"].tolist(),
                    textposition="top center",
                    textfont=dict(size=8),
                    name=f"Fix permanente: {fix_val}",
                    marker=dict(
                        size=sizes,
                        color="#36B37E" if fix_val == "Si" else "#DE350B",
                        opacity=0.7,
                        line=dict(width=1, color="#11567F"),
                    ),
                    hovertemplate="<b>%{text}</b><br>Recurrencias: %{x}<br>Impacto: $%{y:.2f}M COP<extra></extra>",
                ))
            fig_rec.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=60, r=20, t=10, b=60),
                xaxis=dict(title="Recurrencias 30d"),
                yaxis=dict(title="Impacto financiero ($M COP)"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            st.plotly_chart(fig_rec, use_container_width=True, key="err_recurrence")

    st.markdown("**Tendencia diaria — Tasa de error**")
    trend_df = run_query(f"""
        SELECT FECHA_HORA::DATE AS DIA,
               ROUND(AVG(TASA_ERROR_PCT), 3) AS TASA_PROM,
               ROUND(MIN(TASA_ERROR_PCT), 3) AS TASA_MIN,
               ROUND(MAX(TASA_ERROR_PCT), 3) AS TASA_MAX
        FROM {TABLE}
        WHERE {WHERE}
        GROUP BY 1
        ORDER BY 1
    """)
    if not trend_df.empty:
        dias = trend_df["DIA"].tolist()
        fig_trend = go.Figure()
        fig_trend.add_trace(go.Scatter(
            x=dias, y=trend_df["TASA_MAX"].tolist(),
            mode="lines", line=dict(width=0), showlegend=False,
            hoverinfo="skip",
        ))
        fig_trend.add_trace(go.Scatter(
            x=dias, y=trend_df["TASA_MIN"].tolist(),
            mode="lines", line=dict(width=0), fill="tonexty",
            fillcolor="rgba(41,181,232,0.15)", showlegend=False,
            hoverinfo="skip",
        ))
        fig_trend.add_trace(go.Scatter(
            x=dias, y=trend_df["TASA_PROM"].tolist(),
            mode="lines+markers", name="Tasa error prom",
            line=dict(color=COLORS[0], width=2),
            marker=dict(size=4),
            hovertemplate="<b>%{x}</b><br>Tasa: %{y:.3f}%<extra></extra>",
        ))
        fig_trend.update_layout(
            template="plotly_white", paper_bgcolor="#FFFFFF", height=380,
            margin=dict(l=60, r=20, t=10, b=60),
            yaxis=dict(title="Tasa de error (%)"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        st.plotly_chart(fig_trend, use_container_width=True, key="err_trend")
