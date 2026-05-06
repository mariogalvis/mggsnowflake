import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from app_pages.conn_helper import run_query
from app_pages.map_helper import colombia_scatter_map, COORDS_COLOMBIA

TABLE = "MGG_SEGUROS.SUSCRIPCION_Y_RIESGO.SCORING_RIESGO"
COLORS = ["#E63946", "#F4A261", "#E9C46A", "#2A9D8F", "#06D6A0", "#1D7847", "#264653", "#F77F00"]


def color_tag(value, good_threshold, bad_threshold, fmt="{:.1f}", inverse=False):
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


st.header(":material/security: Scoring de Riesgo")
st.caption("Evaluacion de probabilidad y severidad de riesgo para cada cliente o activo a asegurar. Modelo predictivo que integra historial, perfil demografico y caracteristicas del riesgo.")

c1, c2, c3 = st.columns(3)
with c1:
    with st.container(border=True):
        st.markdown("**:material/lightbulb: Que resuelve**")
        st.markdown("Estandariza la evaluacion de riesgo eliminando subjetividad. Asigna un score numerico (0-1000) a cada solicitud para decidir si aprobar, condicionar o rechazar la suscripcion con criterios objetivos y auditables.")
with c2:
    with st.container(border=True):
        st.markdown("**:material/settings: Como funciona**")
        st.markdown("Modelo de ML que procesa variables demograficas, historial de siniestros, valor asegurado y antecedentes. Genera score, probabilidad de siniestro, severidad esperada y factor de recargo sugerido.")
with c3:
    with st.container(border=True):
        st.markdown("**:material/trending_up: Valor de negocio**")
        st.markdown("Reduce siniestralidad al filtrar riesgos inadecuados. Mejora velocidad de suscripcion de dias a segundos. Permite pricing granular por perfil y reduce la tasa de rechazo de buenos clientes.")

dx_kpi = run_query(f"""
    SELECT
        ROUND(AVG(SCORE_RIESGO), 0) AS SCORE_PROMEDIO,
        ROUND(AVG(PROBABILIDAD_SINIESTRO)*100, 1) AS PROB_SINIESTRO_PCT,
        ROUND(AVG(FACTOR_RECARGO), 2) AS FACTOR_RECARGO_AVG,
        COUNT(*) AS EVALUACIONES
    FROM {TABLE}
""")

if not dx_kpi.empty and dx_kpi["EVALUACIONES"].iloc[0] > 0:
    r = dx_kpi.iloc[0]
    score_avg = float(r["SCORE_PROMEDIO"])
    prob_avg = float(r["PROB_SINIESTRO_PCT"])

    dx_worst_ramo = run_query(f"""
        SELECT RAMO, ROUND(AVG(PROBABILIDAD_SINIESTRO)*100,1) AS PROB
        FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC LIMIT 1
    """)
    dx_worst_ciudad = run_query(f"""
        SELECT CIUDAD, ROUND(AVG(SCORE_RIESGO),0) AS SCORE
        FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC LIMIT 1
    """)
    dx_alto_riesgo = run_query(f"""
        SELECT ROUND(COUNT(CASE WHEN PERFIL_RIESGO = 'Alto riesgo' THEN 1 END)*100.0/COUNT(*), 1) AS PCT
        FROM {TABLE}
    """)

    dx_lines = []
    if prob_avg <= 8:
        dx_lines.append(f"La probabilidad de siniestro promedio ({color_tag(prob_avg, 5, 10, inverse=True)}%) esta en niveles :green[**controlados**].")
    elif prob_avg <= 15:
        dx_lines.append(f"La probabilidad de siniestro promedio ({color_tag(prob_avg, 5, 10, inverse=True)}%) esta en niveles :orange[**moderados**]. Monitorear tendencia.")
    else:
        dx_lines.append(f"La probabilidad de siniestro promedio ({color_tag(prob_avg, 5, 10, inverse=True)}%) esta :red[**elevada**]. Revisar criterios de aceptacion.")

    if not dx_worst_ramo.empty:
        wr = dx_worst_ramo.iloc[0]
        dx_lines.append(f"- Ramo con mayor siniestralidad esperada: **{wr['RAMO']}** ({color_tag(float(wr['PROB']), 5, 10, inverse=True)}%)")
    if not dx_worst_ciudad.empty:
        wc = dx_worst_ciudad.iloc[0]
        dx_lines.append(f"- Ciudad con mayor score de riesgo: **{wc['CIUDAD']}** (score {int(wc['SCORE'])})")
    if not dx_alto_riesgo.empty:
        pct_alto = float(dx_alto_riesgo.iloc[0]["PCT"])
        dx_lines.append(f"- Solicitudes en alto riesgo: {color_tag(pct_alto, 10, 20, inverse=True)}% del portafolio")

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
            if prob_avg > 10:
                recs.append("1. :red[**Endurecer criterios**] de aceptacion en ramos con siniestralidad > 10%.")
            if not dx_alto_riesgo.empty and float(dx_alto_riesgo.iloc[0]["PCT"]) > 15:
                recs.append("2. :orange[**Revisar politica de alto riesgo**]: mas del 15% del portafolio requiere atencion.")
            if not dx_worst_ramo.empty:
                recs.append(f"3. :orange[**Ajustar modelo para ramo {dx_worst_ramo.iloc[0]['RAMO']}**]: recalibrar pesos de variables.")
            recs.append("4. Implementar monitoreo de drift del modelo para detectar degradacion temprana.")
            if not recs:
                recs.append(":green[**Los indicadores estan dentro de parametros aceptables.**] Mantener monitoreo continuo.")
            st.markdown("\n".join(recs))

st.divider()

tab1, tab2 = st.tabs(["Dashboard Ejecutivo", "Simulador Predictivo"])

with tab1:
    @st.cache_data(ttl=300, show_spinner=False)
    def get_filter_options():
        ramos = run_query(f"SELECT DISTINCT RAMO FROM {TABLE} ORDER BY 1")["RAMO"].tolist()
        ciudades = run_query(f"SELECT DISTINCT CIUDAD FROM {TABLE} ORDER BY 1")["CIUDAD"].tolist()
        perfiles = run_query(f"SELECT DISTINCT PERFIL_RIESGO FROM {TABLE} ORDER BY 1")["PERFIL_RIESGO"].tolist()
        tipos = run_query(f"SELECT DISTINCT TIPO_NEGOCIO FROM {TABLE} ORDER BY 1")["TIPO_NEGOCIO"].tolist()
        fechas = run_query(f"SELECT MIN(FECHA_EVALUACION) AS FMIN, MAX(FECHA_EVALUACION) AS FMAX FROM {TABLE}")
        return ramos, ciudades, perfiles, tipos, fechas

    ramos, ciudades, perfiles, tipos, fechas_df = get_filter_options()
    fmin = pd.to_datetime(fechas_df["FMIN"].iloc[0]).date()
    fmax = pd.to_datetime(fechas_df["FMAX"].iloc[0]).date()

    fc1, fc2, fc3, fc4, fc5 = st.columns(5)
    with fc1:
        fecha_rng = st.date_input("Periodo", value=(fmin, fmax), min_value=fmin, max_value=fmax, key="sr_fecha")
        if isinstance(fecha_rng, (list, tuple)) and len(fecha_rng) == 2:
            fecha_inicio, fecha_fin = fecha_rng
        else:
            fecha_inicio, fecha_fin = fmin, fmax
    with fc2:
        ramo_sel = st.selectbox("Ramo", ["Todos"] + ramos, key="sr_ramo")
    with fc3:
        ciudad_sel = st.selectbox("Ciudad", ["Todos"] + ciudades, key="sr_ciudad")
    with fc4:
        perfil_sel = st.selectbox("Perfil riesgo", ["Todos"] + perfiles, key="sr_perfil")
    with fc5:
        tipo_sel = st.selectbox("Tipo negocio", ["Todos"] + tipos, key="sr_tipo")

    clauses = [f"FECHA_EVALUACION BETWEEN '{fecha_inicio}' AND '{fecha_fin}'"]
    if ramo_sel != "Todos":
        clauses.append(f"RAMO = '{ramo_sel}'")
    if ciudad_sel != "Todos":
        clauses.append(f"CIUDAD = '{ciudad_sel}'")
    if perfil_sel != "Todos":
        clauses.append(f"PERFIL_RIESGO = '{perfil_sel}'")
    if tipo_sel != "Todos":
        clauses.append(f"TIPO_NEGOCIO = '{tipo_sel}'")
    WHERE = " AND ".join(clauses)

    kpi_df = run_query(f"""
        SELECT
            ROUND(AVG(SCORE_RIESGO), 0) AS SCORE_AVG,
            ROUND(AVG(PROBABILIDAD_SINIESTRO)*100, 1) AS PROB_AVG,
            ROUND(AVG(SEVERIDAD_ESPERADA)*100, 1) AS SEVERIDAD_AVG,
            ROUND(AVG(FACTOR_RECARGO), 2) AS RECARGO_AVG,
            ROUND(AVG(VALOR_ASEGURADO_COP)/1e6, 0) AS VALOR_ASEG_MM,
            ROUND(AVG(CONFIANZA_MODELO)*100, 1) AS CONFIANZA_AVG,
            SUM(CASE WHEN REQUIERE_INSPECCION THEN 1 ELSE 0 END) AS INSPECCIONES,
            COUNT(*) AS EVALUACIONES
        FROM {TABLE}
        WHERE {WHERE}
    """)

    if kpi_df.empty or kpi_df["EVALUACIONES"].iloc[0] == 0:
        st.info("No hay datos para los filtros seleccionados.")
    else:
        r = kpi_df.iloc[0]

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Score promedio", f"{int(r['SCORE_AVG'])}")
        k2.metric("Prob. siniestro", f"{float(r['PROB_AVG']):.1f}%")
        k3.metric("Severidad esperada", f"{float(r['SEVERIDAD_AVG']):.1f}%")
        k4.metric("Factor recargo", f"{float(r['RECARGO_AVG']):.2f}x")

        k5, k6, k7, k8 = st.columns(4)
        k5.metric("Valor asegurado prom.", f"${int(r['VALOR_ASEG_MM']):,}M COP")
        k6.metric("Confianza modelo", f"{float(r['CONFIANZA_AVG']):.1f}%")
        k7.metric("Requieren inspeccion", f"{int(r['INSPECCIONES']):,}")
        k8.metric("Evaluaciones", f"{int(r['EVALUACIONES']):,}")

        st.divider()

        col_chart1, col_chart2 = st.columns(2)

        with col_chart1:
            st.markdown("**Evolucion mensual — Score y probabilidad de siniestro**")
            trend_df = run_query(f"""
                SELECT DATE_TRUNC('MONTH', FECHA_EVALUACION) AS MES,
                       ROUND(AVG(SCORE_RIESGO), 0) AS SCORE,
                       ROUND(AVG(PROBABILIDAD_SINIESTRO)*100, 1) AS PROB
                FROM {TABLE}
                WHERE {WHERE}
                GROUP BY 1 ORDER BY 1
            """)
            if not trend_df.empty:
                meses = trend_df["MES"].tolist()
                scores = [float(v) for v in trend_df["SCORE"]]
                probs = [float(v) for v in trend_df["PROB"]]
                fig_trend = go.Figure()
                fig_trend.add_trace(go.Scatter(
                    x=meses, y=scores, name="Score riesgo", mode="lines+markers+text",
                    fill="tozeroy", fillcolor="rgba(42,157,143,0.10)",
                    line=dict(color="#2A9D8F", width=3), marker=dict(size=8),
                    text=[str(int(v)) for v in scores], textposition="top center",
                    textfont=dict(size=10, color="#264653"), yaxis="y",
                ))
                fig_trend.add_trace(go.Scatter(
                    x=meses, y=probs, name="Prob. siniestro (%)", mode="lines+markers",
                    line=dict(color="#F4A261", width=2, dash="dash"), marker=dict(size=6),
                    yaxis="y2",
                ))
                fig_trend.update_layout(
                    template="plotly_white", paper_bgcolor="#FAFBFC", plot_bgcolor="#FAFBFC",
                    height=380, margin=dict(l=40, r=40, t=30, b=40),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    yaxis=dict(title="Score riesgo", side="left", gridcolor="#E2E8F0"),
                    yaxis2=dict(title="Prob. siniestro (%)", side="right", overlaying="y", showgrid=False),
                    font=dict(family="Inter, sans-serif", color="#334155"),
                )
                st.plotly_chart(fig_trend, use_container_width=True)

        with col_chart2:
            st.markdown("**Volumen por ramo y perfil de riesgo**")
            tree_df = run_query(f"""
                SELECT RAMO, PERFIL_RIESGO, COUNT(*) AS N
                FROM {TABLE}
                WHERE {WHERE}
                GROUP BY 1, 2 ORDER BY 3 DESC
            """)
            if not tree_df.empty:
                labels = []
                parents = []
                values = []
                colors_tree = []
                color_map = {"Alto riesgo": "#E63946", "Riesgo moderado": "#F4A261", "Riesgo estandar": "#E9C46A", "Bajo riesgo": "#06D6A0"}
                ramos_u = tree_df["RAMO"].unique().tolist()
                for ramo in ramos_u:
                    labels.append(ramo)
                    parents.append("")
                    ramo_total = int(tree_df[tree_df["RAMO"] == ramo]["N"].sum())
                    values.append(ramo_total)
                    colors_tree.append("#264653")
                for _, row in tree_df.iterrows():
                    labels.append(f"{row['PERFIL_RIESGO']}")
                    parents.append(row["RAMO"])
                    values.append(int(row["N"]))
                    colors_tree.append(color_map.get(row["PERFIL_RIESGO"], "#264653"))
                fig_tree = go.Figure(go.Treemap(
                    labels=labels, parents=parents, values=values,
                    textinfo="label+value+percent root",
                    marker=dict(colors=colors_tree, line=dict(width=2, color="white")),
                    hovertemplate="<b>%{label}</b><br>Evaluaciones: %{value:,}<br>%{percentRoot:.1%} del total<extra></extra>",
                ))
                fig_tree.update_layout(
                    template="plotly_white", paper_bgcolor="#FAFBFC", plot_bgcolor="#FAFBFC",
                    height=380, margin=dict(l=5, r=5, t=30, b=5),
                    font=dict(family="Inter, sans-serif", color="#334155"),
                )
                st.plotly_chart(fig_tree, use_container_width=True)

        col_chart3, col_chart4, col_chart5 = st.columns(3)

        with col_chart3:
            st.markdown("**Radar — Score por ramo**")
            radar_df = run_query(f"""
                SELECT RAMO, ROUND(AVG(SCORE_RIESGO), 0) AS SCORE,
                       ROUND(AVG(PROBABILIDAD_SINIESTRO)*100, 1) AS PROB
                FROM {TABLE}
                WHERE {WHERE}
                GROUP BY 1 ORDER BY 2 DESC
                LIMIT 8
            """)
            if not radar_df.empty:
                cats = radar_df["RAMO"].tolist()
                scores_r = [float(v) for v in radar_df["SCORE"]]
                fig_radar = go.Figure()
                fig_radar.add_trace(go.Scatterpolar(
                    r=scores_r + [scores_r[0]], theta=cats + [cats[0]],
                    name="Score riesgo", fill="toself",
                    fillcolor="rgba(42,157,143,0.15)",
                    line=dict(color="#2A9D8F", width=2), marker=dict(size=6),
                ))
                fig_radar.update_layout(
                    polar=dict(radialaxis=dict(visible=True, range=[0, 1000])),
                    template="plotly_white", paper_bgcolor="#FAFBFC", plot_bgcolor="#FAFBFC",
                    height=380, margin=dict(l=60, r=60, t=40, b=40),
                    showlegend=False,
                    font=dict(family="Inter, sans-serif", color="#334155"),
                )
                st.plotly_chart(fig_radar, use_container_width=True)

        with col_chart4:
            st.markdown("**Distribucion por perfil de riesgo**")
            perfil_df = run_query(f"""
                SELECT PERFIL_RIESGO, COUNT(*) AS N
                FROM {TABLE}
                WHERE {WHERE}
                GROUP BY 1 ORDER BY 2 DESC
            """)
            if not perfil_df.empty:
                color_perfil = {"Alto riesgo": "#E63946", "Riesgo moderado": "#F4A261", "Riesgo estandar": "#E9C46A", "Bajo riesgo": "#06D6A0"}
                fig_donut = go.Figure(go.Pie(
                    labels=perfil_df["PERFIL_RIESGO"].tolist(),
                    values=[int(v) for v in perfil_df["N"]],
                    hole=0.55,
                    marker=dict(
                        colors=[color_perfil.get(p, "#264653") for p in perfil_df["PERFIL_RIESGO"]],
                        line=dict(color="white", width=2),
                    ),
                    textinfo="percent+label", textposition="outside", textfont=dict(size=10),
                ))
                total = int(perfil_df["N"].sum())
                fig_donut.add_annotation(text=f"<b>{total:,}</b><br>evaluaciones", x=0.5, y=0.5, showarrow=False, font=dict(size=14, color="#264653"))
                fig_donut.update_layout(
                    template="plotly_white", paper_bgcolor="#FAFBFC", plot_bgcolor="#FAFBFC",
                    height=380, margin=dict(l=5, r=5, t=30, b=5), showlegend=False,
                    font=dict(family="Inter, sans-serif", color="#334155"),
                )
                st.plotly_chart(fig_donut, use_container_width=True)

        with col_chart5:
            st.markdown("**Factor recargo por tipo negocio**")
            recargo_df = run_query(f"""
                SELECT TIPO_NEGOCIO, ROUND(AVG(FACTOR_RECARGO), 2) AS RECARGO,
                       COUNT(*) AS N
                FROM {TABLE}
                WHERE {WHERE}
                GROUP BY 1 ORDER BY 2 DESC
            """)
            if not recargo_df.empty:
                tipos_list = recargo_df["TIPO_NEGOCIO"].tolist()
                recargos = [float(v) for v in recargo_df["RECARGO"]]
                bar_colors = ["#E63946" if v > 2 else "#F4A261" if v > 1.5 else "#2A9D8F" for v in recargos]
                fig_bar = go.Figure(go.Bar(
                    x=tipos_list, y=recargos,
                    marker=dict(color=bar_colors, line=dict(width=1, color="white")),
                    text=[f"{v:.2f}x" for v in recargos], textposition="outside",
                    textfont=dict(size=11),
                ))
                fig_bar.update_layout(
                    template="plotly_white", paper_bgcolor="#FAFBFC", plot_bgcolor="#FAFBFC",
                    height=380, margin=dict(l=40, r=20, t=30, b=40),
                    yaxis=dict(title="Factor recargo", gridcolor="#E2E8F0"),
                    showlegend=False,
                    font=dict(family="Inter, sans-serif", color="#334155"),
                )
                st.plotly_chart(fig_bar, use_container_width=True)

        st.divider()

        st.markdown("**Score de riesgo por ciudad**")
        mapa_df = run_query(f"""
            SELECT CIUDAD,
                   ROUND(AVG(SCORE_RIESGO), 0) AS SCORE,
                   COUNT(*) AS VOLUMEN,
                   ROUND(AVG(PROBABILIDAD_SINIESTRO)*100, 1) AS PROB
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1 ORDER BY 2 DESC
        """)

        if not mapa_df.empty:
            mapa_df["LAT"] = mapa_df["CIUDAD"].map(lambda c: COORDS_COLOMBIA.get(c, (4.5, -74.0))[0]).astype(float)
            mapa_df["LON"] = mapa_df["CIUDAD"].map(lambda c: COORDS_COLOMBIA.get(c, (4.5, -74.0))[1]).astype(float)
            mapa_df["SCORE_F"] = mapa_df["SCORE"].astype(float)
            mapa_df["VOLUMEN_F"] = mapa_df["VOLUMEN"].astype(float)
            mapa_df["hover"] = mapa_df.apply(lambda row: f"Score: {int(row['SCORE_F'])}<br>Prob: {row['PROB']}%<br>Evaluaciones: {int(row['VOLUMEN_F']):,}", axis=1)
            deck = colombia_scatter_map(
                mapa_df, lat_col="LAT", lon_col="LON", size_col="VOLUMEN_F", color_col="SCORE_F",
                text_col="CIUDAD", hover_col="hover",
                colorscale=["#06D6A0", "#F4A261", "#E63946"],
                colorbar_title="Score Riesgo",
            )
            st.pydeck_chart(deck, key="sr_map")
            st.caption("Tamano: volumen de evaluaciones | Color: verde = bajo riesgo, rojo = alto riesgo")

        st.divider()

        st.markdown("**Top 10 evaluaciones de mayor riesgo**")
        top_df = run_query(f"""
            SELECT CEDULA, RAMO, CIUDAD, PERFIL_RIESGO, SCORE_RIESGO, 
                   ROUND(PROBABILIDAD_SINIESTRO*100,1) AS PROB_PCT,
                   ROUND(VALOR_ASEGURADO_COP/1e6,0) AS VALOR_MM,
                   FACTOR_RECARGO
            FROM {TABLE}
            WHERE {WHERE}
            ORDER BY SCORE_RIESGO DESC
            LIMIT 10
        """)
        if not top_df.empty:
            st.dataframe(top_df, use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("**Alertas e insights automaticos**")
        alto_pct_df = run_query(f"""
            SELECT ROUND(COUNT(CASE WHEN PERFIL_RIESGO = 'Alto riesgo' THEN 1 END)*100.0/COUNT(*), 1) AS PCT
            FROM {TABLE} WHERE {WHERE}
        """)
        if not alto_pct_df.empty:
            pct = float(alto_pct_df.iloc[0]["PCT"])
            if pct > 20:
                st.error(f"**{pct:.1f}%** de las evaluaciones son de alto riesgo. Considerar endurecer criterios de aceptacion.")
            elif pct > 10:
                st.warning(f"**{pct:.1f}%** de evaluaciones en alto riesgo. Monitorear tendencia.")
            else:
                st.success(f"Solo **{pct:.1f}%** de evaluaciones en alto riesgo. Portafolio saludable.")

with tab2:
    st.markdown("**Simulador de Score de Riesgo**")
    st.caption("Predice la probabilidad de siniestro y severidad esperada de un solicitante ajustando las variables clave del modelo.")

    col_sliders, col_result = st.columns([3, 2])

    with col_sliders:
        edad = st.slider("Edad del asegurado", 18, 75, 40, help="Mayor edad, mayor riesgo en vida/salud", key="sim_sr_edad")
        siniestros_hist = st.slider("Siniestros historicos", 0, 10, 1, help="Historial previo es el mayor predictor", key="sim_sr_sin")
        valor_aseg = st.slider("Valor asegurado (M COP)", 10, 2000, 200, step=10, help="A mayor suma, mayor exposicion", key="sim_sr_val")
        exp_conduccion = st.slider("Anos experiencia conduccion", 0, 40, 10, help="Mas experiencia reduce probabilidad auto", key="sim_sr_exp")
        score_bureau = st.slider("Score bureau (0-1000)", 0, 1000, 650, step=10, help="Score crediticio como proxy de comportamiento", key="sim_sr_bur")

    def calcular_score_riesgo(edad, siniestros, valor, exp, bureau):
        n_edad = (edad - 18) / (75 - 18)
        n_sin = siniestros / 10
        n_val = valor / 2000
        n_exp = 1 - (exp / 40)
        n_bur = 1 - (bureau / 1000)
        score = 0.15 * n_edad + 0.30 * n_sin + 0.15 * n_val + 0.20 * n_exp + 0.20 * n_bur
        return round(min(max(score, 0), 1), 3)

    score = calcular_score_riesgo(edad, siniestros_hist, valor_aseg, exp_conduccion, score_bureau)
    score_1000 = int(score * 1000)

    with col_result:
        if score < 0.3:
            nivel = "Bajo Riesgo"
            color_nivel = "#2A9D8F"
            interpretacion = "El perfil presenta indicadores favorables. Se recomienda aprobacion directa con condiciones estandar."
        elif score < 0.6:
            nivel = "Riesgo Medio"
            color_nivel = "#F4A261"
            interpretacion = "Perfil con riesgo moderado. Se sugiere revision manual y posible inspeccion antes de aprobar."
        else:
            nivel = "Alto Riesgo"
            color_nivel = "#E63946"
            interpretacion = "Perfil de alto riesgo. Requiere aprobacion de comite, inspeccion obligatoria y posibles exclusiones."

        st.metric("Score de Riesgo", f"{score_1000}/1000", nivel)

        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=score * 100,
            number={"suffix": "%"},
            gauge={
                "axis": {"range": [0, 100]},
                "bar": {"color": color_nivel},
                "steps": [
                    {"range": [0, 30], "color": "#D8F3DC"},
                    {"range": [30, 60], "color": "#FFF3CD"},
                    {"range": [60, 100], "color": "#F8D7DA"},
                ],
                "threshold": {"line": {"color": "black", "width": 2}, "thickness": 0.75, "value": score * 100},
            },
        ))
        fig_gauge.update_layout(height=250, margin=dict(t=30, b=0, l=30, r=30))
        st.plotly_chart(fig_gauge, use_container_width=True)

        with st.container(border=True):
            st.markdown(f"**Nivel:** :{('green' if score < 0.3 else 'orange' if score < 0.6 else 'red')}[**{nivel}**]")
            st.markdown(interpretacion)
            st.markdown("---")
            st.markdown(f"- Factor recargo sugerido: **{1 + score * 2:.2f}x**")
            st.markdown(f"- Requiere inspeccion: **{'Si' if score > 0.5 else 'No'}**")
            st.markdown(f"- Requiere examenes medicos: **{'Si' if edad > 55 and score > 0.4 else 'No'}**")
