import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go

TABLE = "MGG_PAGOS.PROCESAMIENTO_TRANSACCIONES.ROUTING_TRANSACCIONES"

COLORS = ["#29B5E8", "#FF8B00", "#36B37E", "#6554C0", "#DE350B", "#11567F", "#FFAB00", "#00A3BF"]


from app_pages.conn_helper import run_query



@st.cache_data(ttl=300, show_spinner=False)
def get_filter_options():
    redes_prim = run_query(f"SELECT DISTINCT RED_PRIMARIA FROM {TABLE} ORDER BY 1")["RED_PRIMARIA"].tolist()
    redes_sel = run_query(f"SELECT DISTINCT RED_SELECCIONADA FROM {TABLE} ORDER BY 1")["RED_SELECCIONADA"].tolist()
    reglas = run_query(f"SELECT DISTINCT REGLA_APLICADA FROM {TABLE} ORDER BY 1")["REGLA_APLICADA"].tolist()
    resultados = run_query(f"SELECT DISTINCT RESULTADO_FINAL FROM {TABLE} ORDER BY 1")["RESULTADO_FINAL"].tolist()
    motores = run_query(f"SELECT DISTINCT MOTOR_ROUTING FROM {TABLE} ORDER BY 1")["MOTOR_ROUTING"].tolist()
    fechas = run_query(f"SELECT MIN(FECHA_HORA)::DATE AS FMIN, MAX(FECHA_HORA)::DATE AS FMAX FROM {TABLE}")
    return redes_prim, redes_sel, reglas, resultados, motores, fechas


def build_where(fecha_ini, fecha_f, red_prim_s, red_sel_s, regla_s, resultado_s, motor_s,
                all_rp, all_rs, all_reg, all_res, all_mot):
    clauses = [f"FECHA_HORA::DATE BETWEEN '{fecha_ini}' AND '{fecha_f}'"]
    if red_prim_s and red_prim_s != "Todos" and red_prim_s in all_rp:
        clauses.append(f"RED_PRIMARIA = '{red_prim_s}'")
    if red_sel_s and red_sel_s != "Todos" and red_sel_s in all_rs:
        clauses.append(f"RED_SELECCIONADA = '{red_sel_s}'")
    if regla_s and regla_s != "Todos" and regla_s in all_reg:
        clauses.append(f"REGLA_APLICADA = '{regla_s}'")
    if resultado_s and resultado_s != "Todos" and resultado_s in all_res:
        clauses.append(f"RESULTADO_FINAL = '{resultado_s}'")
    if motor_s and motor_s != "Todos" and motor_s in all_mot:
        clauses.append(f"MOTOR_ROUTING = '{motor_s}'")
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


st.header(":material/route: Routing de transacciones")
st.caption("Decisiones de enrutamiento entre redes y procesadores. Red seleccionada, costo, tasa de aprobacion estimada y uso de fallback.")

tab1, tab2 = st.tabs(["Dashboard Ejecutivo", "Simulador Predictivo"])

with tab1:
    c1, c2, c3 = st.columns(3)
    with c1:
        with st.container(border=True):
            st.markdown("**:material/lightbulb: Que resuelve**")
            st.markdown("Suboptimizacion del costo de procesamiento y tasa de exito por no seleccionar la mejor ruta entre las redes disponibles.")
    with c2:
        with st.container(border=True):
            st.markdown("**:material/settings: Como funciona**")
            st.markdown("Para cada transaccion se evaluan redes disponibles (Visa, MC, Redeban, Credibanco) considerando costo, tasa de aprobacion historica y disponibilidad.")
    with c3:
        with st.container(border=True):
            st.markdown("**:material/trending_up: Valor de negocio**")
            st.markdown("Reducir costo de procesamiento seleccionando la red mas economica y maximizar aprobaciones con fallback inteligente cuando la red primaria falla.")

    redes_prim, redes_sel, reglas, resultados, motores, fechas_df = get_filter_options()
    fmin = pd.to_datetime(fechas_df["FMIN"].iloc[0]).date()
    fmax = pd.to_datetime(fechas_df["FMAX"].iloc[0]).date()

    dx_where = f"FECHA_HORA::DATE BETWEEN '{fmin}' AND '{fmax}'"

    dx_kpi = run_query(f"""
        SELECT
            COUNT(*) AS TOTAL_ROUTING,
            ROUND(SUM(CASE WHEN RESULTADO_FINAL='Aprobada' THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS TASA_APROB,
            ROUND(AVG(COSTO_RED_PCT)*100, 2) AS COSTO_PROM,
            ROUND(SUM(AHORRO_ESTIMADO_COP)/1e6, 1) AS AHORRO_M,
            ROUND(SUM(CASE WHEN USO_FALLBACK THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS FALLBACK_PCT,
            ROUND(AVG(LATENCIA_DECISION_MS), 0) AS LATENCIA_PROM,
            ROUND(AVG(TASA_APROBACION_RED)*100, 1) AS TASA_RED_PROM
        FROM {TABLE}
        WHERE {dx_where}
    """)

    if not dx_kpi.empty and dx_kpi["TOTAL_ROUTING"].iloc[0] > 0:
        dx = dx_kpi.iloc[0]
        dx_total = int(dx["TOTAL_ROUTING"])
        dx_tasa = float(dx["TASA_APROB"])
        dx_ahorro = float(dx["AHORRO_M"])
        dx_fallback = float(dx["FALLBACK_PCT"])

        dx_best_red = run_query(f"""
            SELECT RED_SELECCIONADA, ROUND(SUM(CASE WHEN RESULTADO_FINAL='Aprobada' THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS TASA
            FROM {TABLE} WHERE {dx_where}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 1
        """)
        dx_worst_red = run_query(f"""
            SELECT RED_SELECCIONADA, ROUND(SUM(CASE WHEN RESULTADO_FINAL='Aprobada' THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS TASA
            FROM {TABLE} WHERE {dx_where}
            GROUP BY 1 ORDER BY 2 ASC LIMIT 1
        """)

        if dx_tasa >= 90:
            dx_estado = f"Tasa de aprobacion post-routing ({color_tag(dx_tasa, 90, 80)}) :green[**excelente**]. Ahorro acumulado: ${dx_ahorro:.1f}M COP."
        elif dx_tasa >= 80:
            dx_estado = f"Tasa post-routing ({color_tag(dx_tasa, 90, 80)}) en :orange[**zona de atencion**]. Oportunidad de mejora en seleccion de red."
        else:
            dx_estado = f"Tasa post-routing ({color_tag(dx_tasa, 90, 80)}) :red[**critica**]. Motor de routing requiere recalibracion."

        dx_lines = [dx_estado]
        if not dx_best_red.empty:
            br = dx_best_red.iloc[0]
            dx_lines.append(f"- Mejor red: **{br['RED_SELECCIONADA']}** ({color_tag(float(br['TASA']), 90, 80)})")
        if not dx_worst_red.empty:
            wr = dx_worst_red.iloc[0]
            dx_lines.append(f"- Red mas baja: **{wr['RED_SELECCIONADA']}** ({color_tag(float(wr['TASA']), 90, 80)})")
        dx_lines.append(f"- Uso de fallback: **{dx_fallback:.1f}%** {'— :green[**bajo**]' if dx_fallback < 10 else '— :orange[**elevado, revisar estabilidad de redes**]'}")
        dx_lines.append(f"- Latencia decision: **{float(dx['LATENCIA_PROM']):.0f} ms** — {'— :green[**rapida**]' if float(dx['LATENCIA_PROM']) < 50 else ':orange[**lenta**]'}")

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
                if dx_tasa < 85:
                    recs.append("1. :red[**Recalibrar motor de routing**]: tasa de aprobacion post-routing por debajo del objetivo.")
                if dx_fallback > 15:
                    recs.append("2. :orange[**Investigar fallbacks**]: uso excesivo indica inestabilidad en redes primarias.")
                if dx_ahorro < 50:
                    recs.append("3. :orange[**Optimizar seleccion de red**]: el ahorro acumulado es bajo, revisar reglas de costo.")
                if float(dx["LATENCIA_PROM"]) > 50:
                    recs.append("4. :orange[**Reducir latencia de decision**]: impacta tiempo total de la transaccion.")
                if not recs:
                    recs.append(":green[**Motor de routing operando optimamente.**] Mantener monitoreo de costos y tasas por red.")
                st.markdown("\n".join(recs))

    st.divider()

    fc1, fc2, fc3, fc4, fc5 = st.columns(5)
    with fc1:
        fecha_rng = st.date_input("Periodo", value=(fmin, fmax), min_value=fmin, max_value=fmax, key="rt_fecha")
        if isinstance(fecha_rng, (list, tuple)) and len(fecha_rng) == 2:
            fecha_inicio, fecha_fin = fecha_rng
        else:
            fecha_inicio, fecha_fin = fmin, fmax
    with fc2:
        red_prim_sel = st.selectbox("Red primaria", ["Todos"] + redes_prim, key="rt_rprim")
    with fc3:
        red_sel_sel = st.selectbox("Red seleccionada", ["Todos"] + redes_sel, key="rt_rsel")
    with fc4:
        regla_sel = st.selectbox("Regla aplicada", ["Todos"] + reglas, key="rt_regla")
    with fc5:
        resultado_sel = st.selectbox("Resultado", ["Todos"] + resultados, key="rt_resultado")

    motor_sel = "Todos"

    WHERE = build_where(fecha_inicio, fecha_fin, red_prim_sel, red_sel_sel, regla_sel, resultado_sel, motor_sel,
                        redes_prim, redes_sel, reglas, resultados, motores)

    kpi_df = run_query(f"""
        SELECT
            COUNT(*) AS TOTAL_ROUTING,
            ROUND(SUM(CASE WHEN RESULTADO_FINAL='Aprobada' THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS TASA_APROB,
            ROUND(AVG(COSTO_RED_PCT)*100, 2) AS COSTO_PROM,
            ROUND(SUM(AHORRO_ESTIMADO_COP)/1e6, 1) AS AHORRO_M,
            ROUND(SUM(CASE WHEN USO_FALLBACK THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS FALLBACK_PCT,
            ROUND(AVG(LATENCIA_DECISION_MS), 0) AS LATENCIA_PROM,
            ROUND(AVG(REDES_EVALUADAS), 1) AS REDES_EVAL_PROM,
            ROUND(AVG(CONFIANZA_DECISION)*100, 1) AS CONFIANZA_PROM
        FROM {TABLE}
        WHERE {WHERE}
    """)

    if kpi_df.empty or kpi_df["TOTAL_ROUTING"].iloc[0] == 0:
        st.info("No hay datos para los filtros seleccionados.")
    else:
        r = kpi_df.iloc[0]
        total = int(r["TOTAL_ROUTING"])

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Decisiones routing", f"{total:,}")
        k2.metric("Tasa aprobacion", f"{float(r['TASA_APROB']):.1f}%")
        k3.metric("Costo red prom", f"{float(r['COSTO_PROM']):.2f}%")
        k4.metric("Ahorro acumulado", f"${float(r['AHORRO_M']):.1f}M COP")

        k5, k6, k7, k8 = st.columns(4)
        k5.metric("Uso fallback", f"{float(r['FALLBACK_PCT']):.1f}%")
        k6.metric("Latencia decision", f"{float(r['LATENCIA_PROM']):.0f} ms")
        k7.metric("Redes evaluadas prom", f"{float(r['REDES_EVAL_PROM']):.1f}")
        k8.metric("Confianza prom", f"{float(r['CONFIANZA_PROM']):.1f}%")

        st.divider()

        trend_df = run_query(f"""
            SELECT DATE_TRUNC('MONTH', FECHA_HORA) AS MES,
                   COUNT(*) AS TOTAL,
                   ROUND(SUM(CASE WHEN RESULTADO_FINAL='Aprobada' THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS TASA,
                   ROUND(SUM(AHORRO_ESTIMADO_COP)/1e6, 1) AS AHORRO_M
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1 ORDER BY 1
        """)

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Evolucion mensual — Tasa aprobacion y ahorro**")
            if trend_df.empty:
                st.info("Sin datos de tendencia.")
            else:
                meses = trend_df["MES"].tolist()
                fig_trend = go.Figure()
                fig_trend.add_trace(go.Scatter(
                    x=meses, y=trend_df["TASA"].tolist(), name="% Aprobacion",
                    fill="tozeroy", line=dict(color=COLORS[2], width=2),
                ))
                fig_trend.add_trace(go.Scatter(
                    x=meses, y=trend_df["AHORRO_M"].tolist(), name="Ahorro ($M COP)",
                    yaxis="y2", line=dict(color=COLORS[1], width=2, dash="dot"),
                ))
                fig_trend.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=60, r=60, t=30, b=60),
                    yaxis=dict(title="% Aprobacion"),
                    yaxis2=dict(title="Ahorro ($M COP)", overlaying="y", side="right"),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(fig_trend, use_container_width=True)

        with col2:
            st.markdown("**Treemap — Volumen por regla aplicada y red seleccionada**")
            tree_df = run_query(f"""
                SELECT REGLA_APLICADA, RED_SELECCIONADA, COUNT(*) AS N
                FROM {TABLE} WHERE {WHERE}
                GROUP BY 1, 2 ORDER BY 3 DESC
            """)
            if tree_df.empty:
                st.info("Sin datos.")
            else:
                labels, parents, values, colors_t = [], [], [], []
                regla_totals = tree_df.groupby("REGLA_APLICADA")["N"].sum().sort_values(ascending=False)
                for i, (regla, regla_total) in enumerate(regla_totals.items()):
                    labels.append(str(regla))
                    parents.append("")
                    values.append(int(regla_total))
                    colors_t.append(COLORS[i % len(COLORS)])
                    sub = tree_df[tree_df["REGLA_APLICADA"] == regla]
                    for _, row in sub.iterrows():
                        labels.append(str(row["RED_SELECCIONADA"]))
                        parents.append(str(regla))
                        values.append(int(row["N"]))
                        colors_t.append(COLORS[i % len(COLORS)])
                fig_tree = go.Figure(go.Treemap(
                    labels=labels, parents=parents, values=values,
                    marker=dict(colors=colors_t),
                    textinfo="label+value+percent parent",
                    hovertemplate="<b>%{label}</b><br>Decisiones: %{value:,}<br>%{percentParent:.1%} del padre<extra></extra>",
                ))
                fig_tree.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=10, r=10, t=30, b=10),
                )
                st.plotly_chart(fig_tree, use_container_width=True)

        st.markdown("**Heatmap — Tasa aprobacion por red primaria vs red seleccionada**")
        heat_df = run_query(f"""
            SELECT RED_PRIMARIA, RED_SELECCIONADA,
                   ROUND(SUM(CASE WHEN RESULTADO_FINAL='Aprobada' THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS TASA
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1, 2 ORDER BY 1, 2
        """)
        if heat_df.empty:
            st.info("Sin datos para heatmap.")
        else:
            pivot = heat_df.pivot_table(index="RED_PRIMARIA", columns="RED_SELECCIONADA", values="TASA", fill_value=0)
            fig_heat = go.Figure(go.Heatmap(
                z=pivot.values.tolist(),
                x=pivot.columns.tolist(),
                y=pivot.index.tolist(),
                colorscale=[[0, "#DE350B"], [0.5, "#FFF3E0"], [1, "#36B37E"]],
                text=[[f"{v:.1f}%" for v in row] for row in pivot.values.tolist()],
                texttemplate="%{text}",
                textfont=dict(size=13),
                hovertemplate="Red primaria: %{y}<br>Red seleccionada: %{x}<br>Tasa: %{z:.1f}%<extra></extra>",
                colorbar=dict(title="% Aprob"),
            ))
            fig_heat.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF",
                height=500, margin=dict(l=160, r=40, t=30, b=80),
                xaxis=dict(tickangle=-45, tickfont=dict(size=12)),
                yaxis=dict(tickfont=dict(size=12)),
            )
            st.plotly_chart(fig_heat, use_container_width=True)

        col3, col4 = st.columns(2)

        with col3:
            st.markdown("**Funnel — Resultado del routing**")
            aprobadas = run_query(f"SELECT COUNT(*) AS N FROM {TABLE} WHERE {WHERE} AND RESULTADO_FINAL='Aprobada'")
            declinadas = run_query(f"SELECT COUNT(*) AS N FROM {TABLE} WHERE {WHERE} AND RESULTADO_FINAL='Declinada'")
            fallback_n = run_query(f"SELECT COUNT(*) AS N FROM {TABLE} WHERE {WHERE} AND USO_FALLBACK")
            override_n = run_query(f"SELECT COUNT(*) AS N FROM {TABLE} WHERE {WHERE} AND OVERRIDE_MANUAL")
            funnel_data = [
                ("Total decisiones", total),
                ("Aprobadas", int(aprobadas.iloc[0]["N"]) if not aprobadas.empty else 0),
                ("Con fallback", int(fallback_n.iloc[0]["N"]) if not fallback_n.empty else 0),
                ("Override manual", int(override_n.iloc[0]["N"]) if not override_n.empty else 0),
                ("Declinadas", int(declinadas.iloc[0]["N"]) if not declinadas.empty else 0),
            ]
            fig_funnel = go.Figure(go.Funnel(
                y=[f[0] for f in funnel_data],
                x=[f[1] for f in funnel_data],
                textinfo="value+percent initial",
                marker=dict(color=[COLORS[0], COLORS[2], COLORS[1], COLORS[3], COLORS[4]]),
                hovertemplate="%{y}: %{x:,}<extra></extra>",
            ))
            fig_funnel.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=10, r=10, t=30, b=10),
            )
            st.plotly_chart(fig_funnel, use_container_width=True)

        with col4:
            st.markdown("**Distribucion por motor de routing**")
            motor_df = run_query(f"""
                SELECT MOTOR_ROUTING, COUNT(*) AS N
                FROM {TABLE} WHERE {WHERE}
                GROUP BY 1 ORDER BY 2 DESC
            """)
            if motor_df.empty:
                st.info("Sin datos.")
            else:
                fig_donut = go.Figure(go.Pie(
                    labels=motor_df["MOTOR_ROUTING"].tolist(),
                    values=motor_df["N"].tolist(),
                    hole=0.5,
                    marker=dict(colors=COLORS[:len(motor_df)]),
                    textinfo="label+percent",
                    hovertemplate="%{label}: %{value:,} (%{percent})<extra></extra>",
                ))
                fig_donut.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=10, r=10, t=30, b=10),
                )
                st.plotly_chart(fig_donut, use_container_width=True)

        st.markdown("**Motivos de fallback**")
        fb_df = run_query(f"""
            SELECT MOTIVO_FALLBACK, COUNT(*) AS N
            FROM {TABLE} WHERE {WHERE} AND USO_FALLBACK AND MOTIVO_FALLBACK != 'N/A'
            GROUP BY 1 ORDER BY 2 DESC
        """)
        if not fb_df.empty:
            fig_bar = go.Figure(go.Bar(
                x=fb_df["N"].tolist(),
                y=fb_df["MOTIVO_FALLBACK"].tolist(),
                orientation="h",
                marker=dict(color=COLORS[1]),
                text=[f"{int(n):,}" for n in fb_df["N"]],
                textposition="outside",
                hovertemplate="%{y}: %{x:,}<extra></extra>",
            ))
            fig_bar.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=350,
                margin=dict(l=200, r=80, t=30, b=30),
                xaxis=dict(title="Cantidad"),
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(fig_bar, use_container_width=True)

with tab2:
    st.markdown("**Simulador de optimizacion de routing**")
    st.caption("Estime el resultado de la decision de routing ajustando costos, tasas historicas y parametros de fallback. El modelo simula la seleccion optima de red.")

    col_sliders, col_result = st.columns([3, 2])

    with col_sliders:
        sim_costo_primaria = st.slider("Costo red primaria (%)", 0.1, 5.0, 1.5, step=0.1, key="rt_sim_costo",
                                       help="Comision porcentual de la red primaria por transaccion")
        sim_tasa_primaria = st.slider("Tasa aprobacion red primaria (%)", 50, 99, 88, key="rt_sim_tasa_prim",
                                      help="Tasa historica de aprobacion de la red primaria")
        sim_tasa_alternativa = st.slider("Tasa aprobacion red alternativa (%)", 50, 99, 91, key="rt_sim_tasa_alt",
                                         help="Tasa historica de la red alternativa disponible")
        sim_latencia_decision = st.slider("Latencia de decision esperada (ms)", 5, 200, 30, step=5, key="rt_sim_lat",
                                          help="Tiempo que tarda el motor en evaluar y seleccionar la red")
        sim_fallback_rate = st.slider("Tasa de fallback esperada (%)", 0, 50, 8, key="rt_sim_fb",
                                      help="Porcentaje de transacciones que requieren cambio de red")
        sim_redes_disponibles = st.slider("Redes disponibles para evaluar", 2, 6, 4, key="rt_sim_redes",
                                          help="Mas redes = mas opciones de optimizacion")

    def calcular_score_routing(costo, tasa_p, tasa_a, lat, fb, redes):
        s_costo = max(0.0, 1.0 - (costo / 5.0))
        s_tasa = max(tasa_p, tasa_a) / 100
        s_lat = max(0.0, 1.0 - (lat / 200))
        s_fb = max(0.0, 1.0 - (fb / 50) * 0.9)
        s_redes = min(1.0, redes / 6)
        s_diff = min(1.0, abs(tasa_a - tasa_p) / 20) if tasa_a > tasa_p else 0.0
        score = (0.20 * s_costo + 0.25 * s_tasa + 0.15 * s_lat + 0.15 * s_fb + 0.10 * s_redes + 0.15 * s_diff)
        tasa_final = 0.70 + score * 0.28
        return round(min(max(tasa_final, 0.70), 0.98), 3)

    score_pred = calcular_score_routing(sim_costo_primaria, sim_tasa_primaria, sim_tasa_alternativa,
                                         sim_latencia_decision, sim_fallback_rate, sim_redes_disponibles)
    score_pct = score_pred * 100
    ahorro_est = round((5.0 - sim_costo_primaria) * 10 * (1 + sim_redes_disponibles / 6), 1)
    red_recomendada = "Alternativa" if sim_tasa_alternativa > sim_tasa_primaria else "Primaria"

    with col_result:
        if score_pct >= 92:
            nivel = "Optimo"
            color_nivel = "#36B37E"
            interpretacion = "Routing optimizado. La combinacion de redes, costos y tasas genera el mejor resultado posible. Mantener configuracion actual."
        elif score_pct >= 85:
            nivel = "Bueno"
            color_nivel = "#29B5E8"
            interpretacion = "Routing eficiente con margen de mejora. Considerar evaluar mas redes o ajustar umbrales de fallback para optimizar."
        elif score_pct >= 78:
            nivel = "Aceptable"
            color_nivel = "#FFAB00"
            interpretacion = "Routing funcional pero suboptimo. Revisar costos de red, reducir latencia de decision y evaluar reglas de seleccion."
        else:
            nivel = "Suboptimo"
            color_nivel = "#DE350B"
            interpretacion = "Routing requiere recalibracion. Costos altos, pocas opciones de red o tasas bajas. Accion inmediata recomendada."

        st.metric("Tasa post-routing estimada", f"{score_pct:.1f}%")
        st.metric("Ahorro estimado", f"${ahorro_est:.1f}M COP/mes")
        st.markdown(f"**Red recomendada:** :blue[**{red_recomendada}**]")
        if score_pct >= 92:
            st.markdown(f"**Nivel:** :green[**{nivel}**]")
        elif score_pct >= 85:
            st.markdown(f"**Nivel:** :blue[**{nivel}**]")
        elif score_pct >= 78:
            st.markdown(f"**Nivel:** :orange[**{nivel}**]")
        else:
            st.markdown(f"**Nivel:** :red[**{nivel}**]")

        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=float(score_pct),
            number=dict(suffix="%", font=dict(size=36)),
            gauge=dict(
                axis=dict(range=[70, 98], ticksuffix="%"),
                bar=dict(color=color_nivel),
                steps=[
                    dict(range=[70, 78], color="#FFCDD2"),
                    dict(range=[78, 85], color="#FFF3E0"),
                    dict(range=[85, 92], color="#E3F2FD"),
                    dict(range=[92, 98], color="#E8F5E9"),
                ],
                threshold=dict(line=dict(color="#FF8B00", width=3), thickness=0.8, value=90),
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

    if "rt_escenarios" not in st.session_state:
        st.session_state.rt_escenarios = []

    if st.button("Guardar escenario actual", type="primary", key="rt_save"):
        if len(st.session_state.rt_escenarios) >= 3:
            st.session_state.rt_escenarios.pop(0)
        st.session_state.rt_escenarios.append({
            "Costo prim %": sim_costo_primaria,
            "Tasa prim %": sim_tasa_primaria,
            "Tasa alt %": sim_tasa_alternativa,
            "Latencia (ms)": sim_latencia_decision,
            "Fallback %": sim_fallback_rate,
            "Redes disp": sim_redes_disponibles,
            "Tasa estimada": f"{score_pct:.1f}%",
            "Ahorro": f"${ahorro_est:.1f}M",
            "Nivel": nivel,
        })
        st.rerun()

    if st.session_state.rt_escenarios:
        esc_df = pd.DataFrame(st.session_state.rt_escenarios)
        esc_df.index = [f"Escenario {i+1}" for i in range(len(esc_df))]
        st.dataframe(esc_df.T, use_container_width=True)

        if st.button("Limpiar escenarios", key="rt_clear"):
            st.session_state.rt_escenarios = []
            st.rerun()
    else:
        st.caption("Aun no hay escenarios guardados. Ajuste los sliders y presione 'Guardar escenario actual'.")
