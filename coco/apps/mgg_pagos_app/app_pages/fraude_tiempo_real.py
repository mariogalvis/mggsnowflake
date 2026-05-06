import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import pydeck as pdk

TABLE = "MGG_PAGOS.FRAUDE_Y_SEGURIDAD_PAGOS.FRAUDE_TIEMPO_REAL"

COLORS = ["#29B5E8", "#FF8B00", "#36B37E", "#6554C0", "#DE350B", "#11567F", "#FFAB00", "#00A3BF"]

COORDS_CIUDAD = {
    "Bogota": (4.6097, -74.0817), "Medellin": (6.2442, -75.5812),
    "Cali": (3.4516, -76.5320), "Barranquilla": (10.9685, -74.7813),
    "Bucaramanga": (7.1254, -73.1198), "Cartagena": (10.3997, -75.5144),
    "Cucuta": (7.8939, -72.5078), "Ibague": (4.4389, -75.2322),
    "Pereira": (4.8133, -75.6961), "Manizales": (5.0689, -75.5174),
}


from app_pages.conn_helper import run_query



@st.cache_data(ttl=300, show_spinner=False)
def get_filter_options():
    canales = run_query(f"SELECT DISTINCT CANAL FROM {TABLE} ORDER BY 1")["CANAL"].tolist()
    decisiones = run_query(f"SELECT DISTINCT DECISION FROM {TABLE} ORDER BY 1")["DECISION"].tolist()
    modelos = run_query(f"SELECT DISTINCT MODELO FROM {TABLE} ORDER BY 1")["MODELO"].tolist()
    ciudades = run_query(f"SELECT DISTINCT CIUDAD FROM {TABLE} ORDER BY 1")["CIUDAD"].tolist()
    reglas = run_query(f"SELECT DISTINCT REGLA_PRINCIPAL FROM {TABLE} ORDER BY 1")["REGLA_PRINCIPAL"].tolist()
    fechas = run_query(f"SELECT MIN(FECHA_HORA)::DATE AS FMIN, MAX(FECHA_HORA)::DATE AS FMAX FROM {TABLE}")
    return canales, decisiones, modelos, ciudades, reglas, fechas


def build_where(fecha_ini, fecha_f, canal_s, decision_s, modelo_s, ciudad_s,
                all_canales, all_decisiones, all_modelos, all_ciudades):
    clauses = [f"FECHA_HORA::DATE BETWEEN '{fecha_ini}' AND '{fecha_f}'"]
    if canal_s and canal_s != "Todos" and canal_s in all_canales:
        clauses.append(f"CANAL = '{canal_s}'")
    if decision_s and decision_s != "Todos" and decision_s in all_decisiones:
        clauses.append(f"DECISION = '{decision_s}'")
    if modelo_s and modelo_s != "Todos" and modelo_s in all_modelos:
        clauses.append(f"MODELO = '{modelo_s}'")
    if ciudad_s and ciudad_s != "Todos" and ciudad_s in all_ciudades:
        clauses.append(f"CIUDAD = '{ciudad_s}'")
    return " AND ".join(clauses)


def color_tag(value, good_threshold, bad_threshold, fmt="{:.1f}%", inverse=False):
    v = float(value)
    if not inverse:
        if v >= good_threshold:
            return f":green[**{fmt.format(v)}**]"
        elif v >= bad_threshold:
            return f":orange[**{fmt.format(v)}**]"
        else:
            return f":red[**{fmt.format(v)}**]"
    else:
        if v <= good_threshold:
            return f":green[**{fmt.format(v)}**]"
        elif v <= bad_threshold:
            return f":orange[**{fmt.format(v)}**]"
        else:
            return f":red[**{fmt.format(v)}**]"


st.header(":material/security: Fraude en tiempo real")
st.caption("Evaluacion de fraude en tiempo real por cada transaccion. Score ML, reglas activadas, decision automatica, latencia de evaluacion y trazabilidad completa.")

c1, c2, c3 = st.columns(3)
with c1:
    with st.container(border=True):
        st.markdown("**:material/lightbulb: Que resuelve**")
        st.markdown("Necesidad de tomar decisiones de aprobacion o bloqueo en milisegundos para proteger al tarjetahabiente y al comercio sin generar friccion innecesaria.")
with c2:
    with st.container(border=True):
        st.markdown("**:material/settings: Como funciona**")
        st.markdown("Cada transaccion pasa por un motor de scoring ML que evalua mas de 50 variables: monto, comercio, dispositivo, ubicacion, velocidad de gasto, patrones historicos y reglas de negocio.")
with c3:
    with st.container(border=True):
        st.markdown("**:material/trending_up: Valor de negocio**")
        st.markdown("Reducir perdidas por fraude manteniendo experiencia fluida. Un motor bien calibrado puede reducir el fraude en un 40% sin aumentar los falsos positivos.")

canales, decisiones, modelos, ciudades, reglas, fechas_df = get_filter_options()
fmin = pd.to_datetime(fechas_df["FMIN"].iloc[0]).date()
fmax = pd.to_datetime(fechas_df["FMAX"].iloc[0]).date()

dx_where = f"FECHA_HORA::DATE BETWEEN '{fmin}' AND '{fmax}'"

dx_kpi = run_query(f"""
    SELECT
        COUNT(*) AS TOTAL_EVAL,
        SUM(CASE WHEN FRAUDE_CONFIRMADO THEN 1 ELSE 0 END) AS FRAUDES_CONF,
        SUM(CASE WHEN FALSO_POSITIVO THEN 1 ELSE 0 END) AS FP,
        ROUND(AVG(SCORE_FRAUDE)*100, 1) AS SCORE_PROM,
        ROUND(AVG(LATENCIA_EVALUACION_MS), 0) AS LATENCIA_PROM,
        ROUND(SUM(MONTO_COP)/1e9, 1) AS MONTO_TOTAL_B
    FROM {TABLE}
    WHERE {dx_where}
""")

if not dx_kpi.empty and dx_kpi["TOTAL_EVAL"].iloc[0] > 0:
    dx_r = dx_kpi.iloc[0]
    dx_total = int(dx_r["TOTAL_EVAL"])
    dx_fraudes = int(dx_r["FRAUDES_CONF"])
    dx_fp = int(dx_r["FP"])
    dx_tasa_fraude = round(dx_fraudes / dx_total * 100, 2)
    dx_tasa_fp = round(dx_fp / dx_total * 100, 2)
    dx_precision = round(dx_fraudes / max(dx_fraudes + dx_fp, 1) * 100, 1)
    dx_latencia = float(dx_r["LATENCIA_PROM"])

    dx_worst_canal = run_query(f"""
        SELECT CANAL, ROUND(SUM(CASE WHEN FRAUDE_CONFIRMADO THEN 1 ELSE 0 END)*100.0/COUNT(*), 2) AS TASA
        FROM {TABLE} WHERE {dx_where}
        GROUP BY 1 ORDER BY 2 DESC LIMIT 1
    """)
    dx_worst_regla = run_query(f"""
        SELECT REGLA_PRINCIPAL, COUNT(*) AS N
        FROM {TABLE} WHERE {dx_where} AND FRAUDE_CONFIRMADO
        GROUP BY 1 ORDER BY 2 DESC LIMIT 1
    """)

    if dx_tasa_fraude <= 0.5:
        dx_estado = f"Tasa de fraude confirmado ({color_tag(dx_tasa_fraude, 0.5, 1.0, fmt='{:.2f}%', inverse=True)}) esta :green[**controlada**]. Precision del modelo: {dx_precision:.1f}%."
    elif dx_tasa_fraude <= 1.5:
        dx_estado = f"Tasa de fraude ({color_tag(dx_tasa_fraude, 0.5, 1.5, fmt='{:.2f}%', inverse=True)}) en rango :orange[**de atencion**]. Precision del modelo: {dx_precision:.1f}%."
    else:
        dx_estado = f"Tasa de fraude ({color_tag(dx_tasa_fraude, 0.5, 1.5, fmt='{:.2f}%', inverse=True)}) :red[**elevada**]. Requiere :red[**revision inmediata**] del motor. Precision: {dx_precision:.1f}%."

    dx_lines = [dx_estado]
    if not dx_worst_canal.empty:
        wc = dx_worst_canal.iloc[0]
        dx_lines.append(f"- Canal mas expuesto: **{wc['CANAL']}** ({color_tag(float(wc['TASA']), 0.5, 1.5, fmt='{:.2f}%', inverse=True)})")
    if not dx_worst_regla.empty:
        dx_lines.append(f"- Regla mas frecuente en fraudes: :red[**{dx_worst_regla.iloc[0]['REGLA_PRINCIPAL']}**]")
    if dx_tasa_fp > 2:
        dx_lines.append(f"- Falsos positivos: :red[**{dx_tasa_fp:.2f}%**] — genera friccion innecesaria")
    elif dx_tasa_fp > 1:
        dx_lines.append(f"- Falsos positivos: :orange[**{dx_tasa_fp:.2f}%**] — monitorear")
    else:
        dx_lines.append(f"- Falsos positivos: :green[**{dx_tasa_fp:.2f}%**] — niveles optimos")
    dx_lines.append(f"- Latencia promedio: **{dx_latencia:.0f} ms** {'— :green[**dentro de SLA**]' if dx_latencia < 100 else '— :red[**excede SLA 100ms**]'}")

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
            if dx_tasa_fraude > 1.0:
                recs.append("1. :red[**Recalibrar modelo ML**]: la tasa de fraude esta por encima del umbral aceptable.")
            if dx_tasa_fp > 2:
                recs.append("2. :orange[**Reducir falsos positivos**]: ajustar umbrales de score para mejorar experiencia del cliente.")
            if not dx_worst_canal.empty and float(dx_worst_canal.iloc[0]["TASA"]) > 1.0:
                recs.append(f"3. :orange[**Reforzar canal {dx_worst_canal.iloc[0]['CANAL']}**]: mayor tasa de fraude detectada.")
            if dx_latencia > 100:
                recs.append("4. :red[**Optimizar latencia**]: SLA de 100ms excedido, impacta experiencia de pago.")
            if not recs:
                recs.append(":green[**Motor de fraude operando dentro de parametros optimos.**] Mantener monitoreo continuo.")
            st.markdown("\n".join(recs))

st.divider()

tab1, tab2 = st.tabs(["Dashboard Ejecutivo", "Simulador Predictivo"])

with tab1:
    fc1, fc2, fc3, fc4, fc5 = st.columns(5)
    with fc1:
        fecha_rng = st.date_input("Periodo", value=(fmin, fmax), min_value=fmin, max_value=fmax, key="fr_fecha")
        if isinstance(fecha_rng, (list, tuple)) and len(fecha_rng) == 2:
            fecha_inicio, fecha_fin = fecha_rng
        else:
            fecha_inicio, fecha_fin = fmin, fmax
    with fc2:
        canal_sel = st.selectbox("Canal", ["Todos"] + canales, key="fr_canal")
    with fc3:
        decision_sel = st.selectbox("Decision", ["Todos"] + decisiones, key="fr_decision")
    with fc4:
        modelo_sel = st.selectbox("Modelo", ["Todos"] + modelos, key="fr_modelo")
    with fc5:
        ciudad_sel = st.selectbox("Ciudad", ["Todos"] + ciudades, key="fr_ciudad")

    WHERE = build_where(fecha_inicio, fecha_fin, canal_sel, decision_sel, modelo_sel, ciudad_sel,
                        canales, decisiones, modelos, ciudades)

    kpi_df = run_query(f"""
        SELECT
            COUNT(*) AS TOTAL_EVAL,
            SUM(CASE WHEN FRAUDE_CONFIRMADO THEN 1 ELSE 0 END) AS FRAUDES_CONF,
            SUM(CASE WHEN FALSO_POSITIVO THEN 1 ELSE 0 END) AS FP,
            ROUND(AVG(SCORE_FRAUDE)*100, 1) AS SCORE_PROM,
            ROUND(AVG(LATENCIA_EVALUACION_MS), 0) AS LATENCIA_PROM,
            ROUND(AVG(CONFIANZA_MODELO)*100, 1) AS CONFIANZA_PROM,
            ROUND(SUM(MONTO_COP)/1e9, 1) AS MONTO_TOTAL_B,
            ROUND(AVG(REGLAS_ACTIVADAS_TOTAL), 1) AS REGLAS_PROM
        FROM {TABLE}
        WHERE {WHERE}
    """)

    if kpi_df.empty or kpi_df["TOTAL_EVAL"].iloc[0] == 0:
        st.info("No hay datos para los filtros seleccionados.")
    else:
        r = kpi_df.iloc[0]
        total = int(r["TOTAL_EVAL"])
        fraudes = int(r["FRAUDES_CONF"])
        fp = int(r["FP"])
        tasa_fraude = round(fraudes / total * 100, 2)
        tasa_fp = round(fp / total * 100, 2)
        precision = round(fraudes / max(fraudes + fp, 1) * 100, 1)

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Evaluaciones", f"{total:,}")
        k2.metric("Fraudes confirmados", f"{fraudes:,}", delta=f"{tasa_fraude:.2f}%", delta_color="inverse")
        k3.metric("Falsos positivos", f"{fp:,}", delta=f"{tasa_fp:.2f}%", delta_color="inverse")
        k4.metric("Precision del modelo", f"{precision:.1f}%")

        k5, k6, k7, k8 = st.columns(4)
        k5.metric("Score fraude prom", f"{float(r['SCORE_PROM']):.1f}%")
        k6.metric("Latencia promedio", f"{float(r['LATENCIA_PROM']):.0f} ms")
        k7.metric("Confianza modelo", f"{float(r['CONFIANZA_PROM']):.1f}%")
        k8.metric("Monto evaluado", f"${float(r['MONTO_TOTAL_B']):.1f}B COP")

        st.divider()

        # --- CHART ROW 1: Area trend + Waterfall decisions ---
        trend_df = run_query(f"""
            SELECT DATE_TRUNC('MONTH', FECHA_HORA) AS MES,
                   COUNT(*) AS TOTAL,
                   SUM(CASE WHEN FRAUDE_CONFIRMADO THEN 1 ELSE 0 END) AS FRAUDES,
                   SUM(CASE WHEN FALSO_POSITIVO THEN 1 ELSE 0 END) AS FP
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1 ORDER BY 1
        """)

        col_chart1, col_chart2 = st.columns(2)

        with col_chart1:
            st.markdown("**Evolucion mensual — Fraude vs Falsos positivos**")
            if trend_df.empty:
                st.info("Sin datos de tendencia.")
            else:
                meses = trend_df["MES"].tolist()
                fraude_vals = [round(float(f) / float(t) * 100, 2) if float(t) > 0 else 0
                               for f, t in zip(trend_df["FRAUDES"], trend_df["TOTAL"])]
                fp_vals = [round(float(f) / float(t) * 100, 2) if float(t) > 0 else 0
                           for f, t in zip(trend_df["FP"], trend_df["TOTAL"])]
                fig_trend = go.Figure()
                fig_trend.add_trace(go.Scatter(
                    x=meses, y=fraude_vals,
                    name="Tasa fraude %", mode="lines+markers",
                    fill="tozeroy", fillcolor="rgba(222,53,11,0.12)",
                    line=dict(color="#DE350B", width=3), marker=dict(size=7),
                ))
                fig_trend.add_trace(go.Scatter(
                    x=meses, y=fp_vals,
                    name="Tasa FP %", mode="lines+markers",
                    fill="tozeroy", fillcolor="rgba(255,171,0,0.10)",
                    line=dict(color="#FFAB00", width=2, dash="dot"), marker=dict(size=5),
                ))
                if len(fraude_vals) >= 2:
                    delta = fraude_vals[-1] - fraude_vals[-2]
                    arrow = "▲" if delta >= 0 else "▼"
                    fig_trend.add_annotation(
                        x=meses[-1], y=fraude_vals[-1],
                        text=f"{arrow} {delta:+.2f}pp", showarrow=True,
                        arrowhead=2, arrowcolor="#DE350B",
                        font=dict(size=11, color="#DE350B", weight="bold"),
                        ax=40, ay=-30,
                    )
                fig_trend.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF",
                    height=380, margin=dict(l=40, r=20, t=30, b=40),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    yaxis=dict(title="% del total", ticksuffix="%"),
                )
                st.plotly_chart(fig_trend, use_container_width=True)

        with col_chart2:
            st.markdown("**Distribucion de decisiones del motor**")
            dec_df = run_query(f"""
                SELECT DECISION, COUNT(*) AS N,
                       ROUND(SUM(MONTO_COP)/1e6, 0) AS MONTO_MM
                FROM {TABLE}
                WHERE {WHERE}
                GROUP BY 1 ORDER BY 2 DESC
            """)
            if dec_df.empty:
                st.info("Sin datos de decisiones.")
            else:
                decision_colors = {
                    "Aprobado": "#36B37E", "Aprobado con alerta": "#FFAB00",
                    "Bloqueado": "#DE350B", "Derivado analista": "#6554C0",
                    "Desafio 3DS": "#29B5E8", "Pendiente revision": "#FF8B00",
                }
                labels = dec_df["DECISION"].tolist()
                values = [float(v) for v in dec_df["N"]]
                colors = [decision_colors.get(d, "#999999") for d in labels]
                fig_dec = go.Figure(go.Pie(
                    labels=labels, values=values,
                    hole=0.55,
                    marker=dict(colors=colors, line=dict(color="white", width=2)),
                    textinfo="percent+label",
                    textposition="outside",
                    textfont=dict(size=10),
                    hovertemplate="<b>%{label}</b><br>Transacciones: %{value:,}<br>%{percent:.1%}<extra></extra>",
                    pull=[0.05 if d == "Bloqueado" else 0 for d in labels],
                ))
                total_txns = int(sum(values))
                fig_dec.add_annotation(
                    text=f"<b>{total_txns:,}</b><br>evaluaciones",
                    x=0.5, y=0.5, showarrow=False,
                    font=dict(size=13, color="#11567F"),
                )
                fig_dec.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF",
                    height=380, margin=dict(l=5, r=5, t=30, b=5),
                    showlegend=False,
                )
                st.plotly_chart(fig_dec, use_container_width=True)

        # --- CHART ROW 2a: Heatmap (full width) ---
        st.markdown("**Heatmap — Score fraude por canal y regla**")
        heat_df = run_query(f"""
            SELECT CANAL, REGLA_PRINCIPAL,
                   ROUND(AVG(SCORE_FRAUDE)*100, 1) AS SCORE_PROM
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1, 2
            ORDER BY 1, 2
        """)
        if heat_df.empty:
            st.info("Sin datos para heatmap.")
        else:
            pivot = heat_df.pivot_table(index="REGLA_PRINCIPAL", columns="CANAL", values="SCORE_PROM", fill_value=0)
            fig_heat = go.Figure(go.Heatmap(
                z=pivot.values.tolist(),
                x=pivot.columns.tolist(),
                y=pivot.index.tolist(),
                colorscale=[[0, "#E8F5E9"], [0.3, "#FFF3E0"], [0.6, "#FF8B00"], [1, "#DE350B"]],
                text=[[f"{v:.1f}%" for v in row] for row in pivot.values.tolist()],
                texttemplate="%{text}",
                textfont=dict(size=13),
                hovertemplate="Canal: %{x}<br>Regla: %{y}<br>Score: %{z:.1f}%<extra></extra>",
                colorbar=dict(title="Score %", ticksuffix="%"),
            ))
            fig_heat.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF",
                height=500, margin=dict(l=160, r=40, t=30, b=80),
                xaxis=dict(tickangle=-45, tickfont=dict(size=12)),
                yaxis=dict(tickfont=dict(size=12)),
            )
            st.plotly_chart(fig_heat, use_container_width=True)

        # --- CHART ROW 2b: Radar + Funnel ---
        col_chart4, col_chart5 = st.columns(2)

        with col_chart4:
            st.markdown("**Radar — Rendimiento por modelo**")
            model_df = run_query(f"""
                SELECT MODELO,
                       ROUND(AVG(SCORE_FRAUDE)*100, 1) AS SCORE,
                       ROUND(AVG(CONFIANZA_MODELO)*100, 1) AS CONFIANZA,
                       ROUND(AVG(LATENCIA_EVALUACION_MS), 0) AS LATENCIA,
                       ROUND(AVG(REGLAS_ACTIVADAS_TOTAL), 1) AS REGLAS,
                       ROUND(AVG(FEATURES_EVALUADAS), 0) AS FEATURES
                FROM {TABLE}
                WHERE {WHERE}
                GROUP BY 1
            """)
            if model_df.empty:
                st.info("Sin datos por modelo.")
            else:
                cats = ["Score fraude", "Confianza", "Inv. latencia", "Reglas", "Features"]
                fig_radar = go.Figure()
                model_colors = {"ML ensemble": "#29B5E8", "Neural network": "#6554C0", "Rules + ML": "#36B37E"}
                for _, row in model_df.iterrows():
                    inv_lat = round(max(0, 100 - float(row["LATENCIA"])), 1)
                    vals = [float(row["SCORE"]), float(row["CONFIANZA"]), inv_lat,
                            float(row["REGLAS"]) * 10, float(row["FEATURES"])]
                    fig_radar.add_trace(go.Scatterpolar(
                        r=vals + [vals[0]], theta=cats + [cats[0]],
                        name=str(row["MODELO"]), fill="toself",
                        fillcolor=model_colors.get(str(row["MODELO"]), "#999999").replace("#", "rgba(") + ")" if False else f"rgba({int(model_colors.get(str(row['MODELO']), '#999999')[1:3], 16)},{int(model_colors.get(str(row['MODELO']), '#999999')[3:5], 16)},{int(model_colors.get(str(row['MODELO']), '#999999')[5:7], 16)},0.15)",
                        line=dict(color=model_colors.get(str(row["MODELO"]), "#999999"), width=2),
                    ))
                fig_radar.update_layout(
                    polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
                    template="plotly_white", paper_bgcolor="#FFFFFF",
                    height=380, margin=dict(l=60, r=60, t=40, b=40),
                    legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5),
                    showlegend=True,
                )
                st.plotly_chart(fig_radar, use_container_width=True)

        with col_chart5:
            st.markdown("**Funnel — Reglas que detectan fraude**")
            regla_df = run_query(f"""
                SELECT REGLA_PRINCIPAL, COUNT(*) AS ACTIVACIONES,
                       SUM(CASE WHEN FRAUDE_CONFIRMADO THEN 1 ELSE 0 END) AS FRAUDES_DETECT
                FROM {TABLE}
                WHERE {WHERE} AND REGLA_PRINCIPAL != 'Sin regla activada'
                GROUP BY 1 ORDER BY 3 DESC
                LIMIT 8
            """)
            if regla_df.empty:
                st.info("Sin datos de reglas.")
            else:
                reglas_list = regla_df["REGLA_PRINCIPAL"].tolist()
                fraudes_list = [int(v) for v in regla_df["FRAUDES_DETECT"]]
                max_f = max(fraudes_list) if fraudes_list else 1
                funnel_colors = ["#DE350B" if v >= max_f * 0.7 else "#FF8B00" if v >= max_f * 0.3 else "#FFAB00" for v in fraudes_list]
                fig_funnel = go.Figure(go.Funnel(
                    y=reglas_list, x=fraudes_list,
                    textinfo="value+percent initial",
                    texttemplate="%{value:,} (%{percentInitial:.1%})",
                    marker=dict(color=funnel_colors, line=dict(width=1, color="white")),
                    connector=dict(line=dict(color="#DFE1E6", width=1)),
                ))
                fig_funnel.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF",
                    height=380, margin=dict(l=140, r=20, t=30, b=40),
                    showlegend=False,
                )
                st.plotly_chart(fig_funnel, use_container_width=True)

        st.divider()

        st.markdown("**Mapa de fraude por ciudad**")
        mapa_df = run_query(f"""
            SELECT CIUDAD,
                   COUNT(*) AS EVALUACIONES,
                   SUM(CASE WHEN FRAUDE_CONFIRMADO THEN 1 ELSE 0 END) AS FRAUDES,
                   ROUND(SUM(CASE WHEN FRAUDE_CONFIRMADO THEN 1 ELSE 0 END)*100.0/COUNT(*), 2) AS TASA_FRAUDE,
                   ROUND(SUM(MONTO_COP)/1e6, 0) AS MONTO_MM
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1 ORDER BY 3 DESC
        """)

        if mapa_df.empty:
            st.info("Sin datos geograficos.")
        else:
            mapa_df["LAT"] = mapa_df["CIUDAD"].map(lambda d: COORDS_CIUDAD.get(d, (4.5, -74.0))[0]).astype(float)
            mapa_df["LON"] = mapa_df["CIUDAD"].map(lambda d: COORDS_CIUDAD.get(d, (4.5, -74.0))[1]).astype(float)
            mapa_df["FRAUDES_F"] = mapa_df["FRAUDES"].astype(float)
            mapa_df["TASA_F"] = mapa_df["TASA_FRAUDE"].astype(float)

            tasa_min = float(mapa_df["TASA_F"].min())
            tasa_max = float(mapa_df["TASA_F"].max())

            def tasa_to_color(tasa):
                if tasa_max == tasa_min:
                    norm = 0.5
                else:
                    norm = (tasa - tasa_min) / (tasa_max - tasa_min)
                r = int(54 * (1 - norm) + 222 * norm)
                g = int(179 * (1 - norm) + 53 * norm)
                b = int(126 * (1 - norm) + 11 * norm)
                return [r, g, b, 180]

            mapa_df["COLOR"] = mapa_df["TASA_F"].apply(tasa_to_color)
            fr_max = float(mapa_df["FRAUDES_F"].max()) if float(mapa_df["FRAUDES_F"].max()) > 0 else 1
            mapa_df["RADIUS"] = mapa_df["FRAUDES_F"].apply(lambda v: max(8000, float(v) / fr_max * 45000))

            layer = pdk.Layer(
                "ScatterplotLayer", data=mapa_df,
                get_position=["LON", "LAT"], get_radius="RADIUS",
                get_fill_color="COLOR", pickable=True, opacity=0.8,
            )
            view_state = pdk.ViewState(latitude=5.5, longitude=-74.0, zoom=5.0, pitch=0)
            tooltip = {
                "html": "<b>{CIUDAD}</b><br/>Fraudes: {FRAUDES}<br/>Tasa: {TASA_FRAUDE}%<br/>Monto: ${MONTO_MM}M COP",
                "style": {"backgroundColor": "#11567F", "color": "white", "fontSize": "12px", "padding": "8px"},
            }
            st.pydeck_chart(pdk.Deck(
                layers=[layer], initial_view_state=view_state, tooltip=tooltip,
                map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
            ))
            st.caption("Tamano: volumen de fraudes | Color: verde = baja tasa, rojo = alta tasa de fraude")

        st.divider()

        st.markdown("**Top 10 evaluaciones de mayor riesgo**")
        top_df = run_query(f"""
            SELECT ID_TRANSACCION, CANAL, CIUDAD, DECISION,
                   ROUND(SCORE_FRAUDE*100, 1) AS SCORE,
                   MONTO_COP, REGLA_PRINCIPAL, MODELO,
                   LATENCIA_EVALUACION_MS AS LATENCIA_MS,
                   FRAUDE_CONFIRMADO
            FROM {TABLE}
            WHERE {WHERE}
            ORDER BY SCORE_FRAUDE DESC
            LIMIT 10
        """)
        if top_df.empty:
            st.info("Sin datos.")
        else:
            st.dataframe(top_df, use_container_width=True)

        st.divider()

        st.markdown("**Alertas e insights automaticos**")

        high_fp = run_query(f"""
            SELECT MODELO, ROUND(SUM(CASE WHEN FALSO_POSITIVO THEN 1 ELSE 0 END)*100.0/COUNT(*), 2) AS TASA_FP
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 1
        """)
        high_fraud_canal = run_query(f"""
            SELECT CANAL, SUM(CASE WHEN FRAUDE_CONFIRMADO THEN 1 ELSE 0 END) AS FRAUDES
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 1
        """)
        slow_eval = run_query(f"""
            SELECT COUNT(*) AS N FROM {TABLE}
            WHERE {WHERE} AND LATENCIA_EVALUACION_MS > 100
        """)

        if not high_fp.empty and float(high_fp.iloc[0]["TASA_FP"]) > 2:
            st.warning(f"Modelo **{high_fp.iloc[0]['MODELO']}** tiene la mayor tasa de falsos positivos: **{float(high_fp.iloc[0]['TASA_FP']):.2f}%**. Considerar recalibracion.")
        if not high_fraud_canal.empty:
            st.warning(f"Canal **{high_fraud_canal.iloc[0]['CANAL']}** concentra la mayor cantidad de fraudes: **{int(high_fraud_canal.iloc[0]['FRAUDES']):,}**.")
        if not slow_eval.empty and int(slow_eval.iloc[0]["N"]) > 0:
            n_slow = int(slow_eval.iloc[0]["N"])
            if n_slow > total * 0.1:
                st.error(f"**{n_slow:,}** evaluaciones ({round(n_slow/total*100, 1)}%) superaron el SLA de 100ms.")
            else:
                st.success(f"Solo {n_slow:,} evaluaciones superaron 100ms. Latencia dentro de SLA.")


with tab2:
    st.markdown("**Simulador de riesgo de fraude**")
    st.caption("Estime el score de riesgo de fraude ajustando las variables clave de una transaccion. El modelo pondera cada factor segun su impacto relativo.")

    col_sliders, col_result = st.columns([3, 2])

    with col_sliders:
        sim_monto = st.slider("Monto de transaccion (miles COP)", 10, 10000, 500, step=50, key="fr_sim_monto",
                              help="Transacciones de monto inusualmente alto tienen mayor score de fraude")
        sim_canal_risk = st.slider("Riesgo del canal (0=bajo, 100=alto)", 0, 100, 30, key="fr_sim_canal",
                                   help="E-commerce y MOTO son canales de mayor riesgo")
        sim_device_known = st.slider("Dispositivo conocido (%)", 0, 100, 70, key="fr_sim_device",
                                     help="Dispositivos nuevos o desconocidos incrementan el riesgo")
        sim_geo_normal = st.slider("Geolocalizacion normal (%)", 0, 100, 80, key="fr_sim_geo",
                                   help="Transacciones desde ubicaciones atipicas elevan el score")
        sim_velocidad = st.slider("Velocidad de gasto (txns/hora)", 1, 50, 5, key="fr_sim_vel",
                                  help="Alta velocidad puede indicar uso automatizado o fraude")
        sim_reglas = st.slider("Reglas de negocio activadas", 0, 15, 2, key="fr_sim_reglas",
                               help="Mas reglas activadas = mayor probabilidad de fraude")

    def calcular_score_fraude(monto, canal_risk, device_pct, geo_pct, velocidad, reglas):
        if monto < 100:
            score_monto = 0.2
        elif monto < 500:
            score_monto = 0.3
        elif monto < 2000:
            score_monto = 0.5
        elif monto < 5000:
            score_monto = 0.7
        else:
            score_monto = 1.0

        score_canal = canal_risk / 100
        score_device = 1.0 - (device_pct / 100)
        score_geo = 1.0 - (geo_pct / 100)
        score_vel = min(1.0, velocidad / 30)
        score_reglas = min(1.0, reglas / 10)

        score_final = (
            0.15 * score_monto
            + 0.18 * score_canal
            + 0.20 * score_device
            + 0.17 * score_geo
            + 0.15 * score_vel
            + 0.15 * score_reglas
        )
        return round(min(max(score_final * 100, 1), 99), 1)

    score_pred = calcular_score_fraude(sim_monto, sim_canal_risk, sim_device_known, sim_geo_normal, sim_velocidad, sim_reglas)

    with col_result:
        if score_pred < 20:
            nivel = "Bajo"
            color_nivel = "#36B37E"
            decision_sugerida = "Aprobar"
            interpretacion = "Riesgo bajo. La transaccion cumple con los patrones normales del tarjetahabiente. Aprobacion automatica recomendada."
        elif score_pred < 45:
            nivel = "Medio"
            color_nivel = "#29B5E8"
            decision_sugerida = "Aprobar con monitoreo"
            interpretacion = "Riesgo moderado. Algunos indicadores atipicos pero dentro de tolerancia. Aprobar con alerta para monitoreo posterior."
        elif score_pred < 70:
            nivel = "Alto"
            color_nivel = "#FFAB00"
            decision_sugerida = "Desafio 3DS"
            interpretacion = "Riesgo significativo. Multiples factores de riesgo presentes. Recomendado desafio de autenticacion 3DS antes de aprobar."
        else:
            nivel = "Critico"
            color_nivel = "#DE350B"
            decision_sugerida = "Bloquear"
            interpretacion = "Riesgo critico. Alta probabilidad de fraude. Bloquear transaccion y derivar a analista para revision manual."

        st.metric("Score de riesgo", f"{score_pred:.1f}%")
        if score_pred < 20:
            st.markdown(f"**Nivel:** :green[**{nivel}**] | **Decision:** :green[**{decision_sugerida}**]")
        elif score_pred < 45:
            st.markdown(f"**Nivel:** :blue[**{nivel}**] | **Decision:** :blue[**{decision_sugerida}**]")
        elif score_pred < 70:
            st.markdown(f"**Nivel:** :orange[**{nivel}**] | **Decision:** :orange[**{decision_sugerida}**]")
        else:
            st.markdown(f"**Nivel:** :red[**{nivel}**] | **Decision:** :red[**{decision_sugerida}**]")

        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=float(score_pred),
            number=dict(suffix="%", font=dict(size=36)),
            gauge=dict(
                axis=dict(range=[0, 100], ticksuffix="%"),
                bar=dict(color=color_nivel),
                steps=[
                    dict(range=[0, 20], color="#E8F5E9"),
                    dict(range=[20, 45], color="#E3F2FD"),
                    dict(range=[45, 70], color="#FFF3E0"),
                    dict(range=[70, 100], color="#FFEBEE"),
                ],
                threshold=dict(line=dict(color="#DE350B", width=3), thickness=0.8, value=70),
            ),
        ))
        fig_gauge.update_layout(
            template="plotly_white", paper_bgcolor="#FFFFFF",
            height=250, margin=dict(l=30, r=30, t=30, b=10),
        )
        st.plotly_chart(fig_gauge, use_container_width=True)

        st.info(interpretacion)

    st.divider()

    st.markdown("**Comparador de escenarios**")
    st.caption("Guarde hasta 3 configuraciones para comparar lado a lado.")

    if "fr_escenarios" not in st.session_state:
        st.session_state.fr_escenarios = []

    if st.button("Guardar escenario actual", type="primary", key="fr_save"):
        if len(st.session_state.fr_escenarios) >= 3:
            st.session_state.fr_escenarios.pop(0)
        st.session_state.fr_escenarios.append({
            "Monto (K COP)": sim_monto,
            "Riesgo canal": sim_canal_risk,
            "Device conocido %": sim_device_known,
            "Geo normal %": sim_geo_normal,
            "Velocidad txn/h": sim_velocidad,
            "Reglas activadas": sim_reglas,
            "Score riesgo": f"{score_pred:.1f}%",
            "Nivel": nivel,
            "Decision": decision_sugerida,
        })
        st.rerun()

    if st.session_state.fr_escenarios:
        esc_df = pd.DataFrame(st.session_state.fr_escenarios)
        esc_df.index = [f"Escenario {i+1}" for i in range(len(esc_df))]
        st.dataframe(esc_df.T, use_container_width=True)

        if st.button("Limpiar escenarios", key="fr_clear"):
            st.session_state.fr_escenarios = []
            st.rerun()
    else:
        st.caption("Aun no hay escenarios guardados. Ajuste los sliders y presione 'Guardar escenario actual'.")
