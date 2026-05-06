import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from app_pages.map_helper import colombia_scatter_map

TABLE = "MGG_PAGOS.EXPERIENCIA_Y_AUTORIZACION.TASA_APROBACION"

COLORS = ["#29B5E8", "#FF8B00", "#36B37E", "#6554C0", "#DE350B", "#11567F", "#FFAB00", "#00A3BF"]

COORDS_DEPTO = {
    "Bogota D.C.": (4.6097, -74.0817),
    "Antioquia": (6.2442, -75.5812),
    "Valle del Cauca": (3.4516, -76.5320),
    "Atlantico": (10.9685, -74.7813),
    "Santander": (7.1254, -73.1198),
    "Cundinamarca": (5.0268, -74.0300),
    "Bolivar": (10.3997, -75.5144),
    "Norte de Santander": (7.8939, -72.5078),
    "Tolima": (4.4389, -75.2322),
    "Boyaca": (5.5353, -73.3678),
    "Caldas": (5.0689, -75.5174),
    "Risaralda": (4.8133, -75.6961),
    "Huila": (2.9273, -75.2819),
    "Nariño": (1.2892, -77.3579),
    "Cesar": (10.4631, -73.2532),
    "Meta": (4.1510, -73.6346),
    "Magdalena": (11.2408, -74.1990),
    "Cordoba": (8.7479, -75.8814),
    "Sucre": (9.3017, -75.3972),
    "Quindio": (4.5339, -75.6811),
    "Cauca": (2.4382, -76.6131),
    "La Guajira": (11.5444, -72.9072),
    "Casanare": (5.3378, -72.3959),
    "Arauca": (7.0847, -70.7592),
    "Putumayo": (1.1520, -76.6519),
    "Caqueta": (1.6144, -75.6062),
    "Choco": (5.6919, -76.6583),
    "Amazonas": (-1.0148, -71.9381),
    "San Andres": (12.5567, -81.7185),
    "Guaviare": (2.5703, -72.6450),
    "Vaupes": (1.1983, -70.1819),
    "Vichada": (4.4235, -69.2878),
}


from app_pages.conn_helper import run_query



@st.cache_data(ttl=300, show_spinner=False)
def get_filter_options():
    redes = run_query(f"SELECT DISTINCT RED FROM {TABLE} ORDER BY 1")["RED"].tolist()
    canales = run_query(f"SELECT DISTINCT CANAL FROM {TABLE} ORDER BY 1")["CANAL"].tolist()
    bancos = run_query(f"SELECT DISTINCT BANCO_EMISOR FROM {TABLE} ORDER BY 1")["BANCO_EMISOR"].tolist()
    tipos = run_query(f"SELECT DISTINCT TIPO_TARJETA FROM {TABLE} ORDER BY 1")["TIPO_TARJETA"].tolist()
    deptos = run_query(f"SELECT DISTINCT DEPARTAMENTO FROM {TABLE} ORDER BY 1")["DEPARTAMENTO"].tolist()
    fechas = run_query(f"SELECT MIN(FECHA) AS FMIN, MAX(FECHA) AS FMAX FROM {TABLE}")
    return redes, canales, bancos, tipos, deptos, fechas


def build_where(fecha_ini, fecha_f, red_s, canal_s, banco_s, tipo_s, depto_s,
                all_redes, all_canales, all_bancos, all_tipos, all_deptos):
    clauses = [f"FECHA BETWEEN '{fecha_ini}' AND '{fecha_f}'"]
    if red_s and red_s != "Todos" and red_s in all_redes:
        clauses.append(f"RED = '{red_s}'")
    if canal_s and canal_s != "Todos" and canal_s in all_canales:
        clauses.append(f"CANAL = '{canal_s}'")
    if banco_s and banco_s != "Todos" and banco_s in all_bancos:
        clauses.append(f"BANCO_EMISOR = '{banco_s}'")
    if tipo_s and tipo_s != "Todos" and tipo_s in all_tipos:
        clauses.append(f"TIPO_TARJETA = '{tipo_s}'")
    if depto_s and depto_s != "Todos" and depto_s in all_deptos:
        clauses.append(f"DEPARTAMENTO = '{depto_s}'")
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


st.header(":material/check_circle: Tasa de aprobacion")
st.caption("Metricas de aprobacion y rechazo segmentadas por red, emisor, canal y tipo tarjeta. Identifica motivos de rechazo principales y estima revenue perdido.")

c1, c2, c3 = st.columns(3)
with c1:
    with st.container(border=True):
        st.markdown("**:material/lightbulb: Que resuelve**")
        st.markdown("Visibilidad en tiempo real de la tasa de aprobacion por red, emisor, canal y tipo de tarjeta. Detecta caidas antes de que impacten el revenue y prioriza acciones de mejora con los emisores.")
with c2:
    with st.container(border=True):
        st.markdown("**:material/settings: Como funciona**")
        st.markdown("Consolida transacciones aprobadas y rechazadas, calcula tasas por segmento, compara contra benchmarks de la industria y genera alertas automaticas cuando la aprobacion cae por debajo de umbrales criticos.")
with c3:
    with st.container(border=True):
        st.markdown("**:material/trending_up: Valor de negocio**")
        st.markdown("Cada punto porcentual de mejora en aprobacion representa millones en revenue recuperado. Permite negociar con emisores con datos concretos y optimizar la experiencia de pago del tarjetahabiente.")

redes, canales, bancos, tipos_tarjeta, deptos, fechas_df = get_filter_options()
fmin = pd.to_datetime(fechas_df["FMIN"].iloc[0]).date()
fmax = pd.to_datetime(fechas_df["FMAX"].iloc[0]).date()

dx_where = f"FECHA BETWEEN '{fmin}' AND '{fmax}'"

dx_kpi = run_query(f"""
    SELECT
        ROUND(AVG(TASA_APROBACION)*100, 1) AS TASA_APROBACION_AVG,
        ROUND(AVG(BENCHMARK_INDUSTRIA)*100, 1) AS BENCHMARK_AVG,
        SUM(REVENUE_POTENCIAL_PERDIDO_COP) AS REVENUE_PERDIDO,
        COUNT(*) AS REGISTROS
    FROM {TABLE}
    WHERE {dx_where}
""")

if not dx_kpi.empty and dx_kpi["REGISTROS"].iloc[0] > 0:
    dx_r = dx_kpi.iloc[0]
    dx_tasa = float(dx_r["TASA_APROBACION_AVG"])
    dx_bench = float(dx_r["BENCHMARK_AVG"])
    dx_brecha = round(dx_tasa - dx_bench, 1)
    dx_rev_b = float(dx_r["REVENUE_PERDIDO"]) / 1e9

    dx_worst_red = run_query(f"""
        SELECT RED, ROUND(AVG(TASA_APROBACION)*100,1) AS TASA
        FROM {TABLE} WHERE {dx_where}
        GROUP BY 1 ORDER BY 2 ASC LIMIT 1
    """)
    dx_worst_canal = run_query(f"""
        SELECT CANAL, ROUND(AVG(TASA_APROBACION)*100,1) AS TASA
        FROM {TABLE} WHERE {dx_where}
        GROUP BY 1 ORDER BY 2 ASC LIMIT 1
    """)
    dx_top_motivo = run_query(f"""
        SELECT MOTIVO_RECHAZO_PRINCIPAL AS MOTIVO, COUNT(*) AS N
        FROM {TABLE} WHERE {dx_where}
        GROUP BY 1 ORDER BY 2 DESC LIMIT 1
    """)

    if dx_brecha >= 0:
        dx_estado = f"La tasa de aprobacion ({color_tag(dx_tasa, 90, 85)}) esta :green[**por encima**] del benchmark de la industria ({dx_bench:.1f}%)."
    elif dx_brecha >= -2:
        dx_estado = f"La tasa de aprobacion ({color_tag(dx_tasa, 90, 85)}) esta :orange[**ligeramente por debajo**] del benchmark ({dx_bench:.1f}%). Hay oportunidad de mejora moderada."
    else:
        dx_estado = f"La tasa de aprobacion ({color_tag(dx_tasa, 90, 85)}) esta :red[**significativamente por debajo**] del benchmark ({dx_bench:.1f}%). Se requiere :red[**atencion inmediata**]."

    dx_lines = [dx_estado]
    if not dx_worst_red.empty:
        wr_tasa = float(dx_worst_red.iloc[0]["TASA"])
        dx_lines.append(f"- Red con menor aprobacion: **{dx_worst_red.iloc[0]['RED']}** ({color_tag(wr_tasa, 90, 85)})")
    if not dx_worst_canal.empty:
        wc_tasa = float(dx_worst_canal.iloc[0]["TASA"])
        dx_lines.append(f"- Canal con mayor friccion: **{dx_worst_canal.iloc[0]['CANAL']}** ({color_tag(wc_tasa, 90, 85)})")
    if not dx_top_motivo.empty:
        dx_lines.append(f"- Motivo de rechazo principal: :red[**{dx_top_motivo.iloc[0]['MOTIVO']}**]")
    if dx_rev_b > 50:
        dx_lines.append(f"- Revenue potencial perdido: :red[**${dx_rev_b:.1f}B COP**]")
    elif dx_rev_b > 10:
        dx_lines.append(f"- Revenue potencial perdido: :orange[**${dx_rev_b:.1f}B COP**]")
    else:
        dx_lines.append(f"- Revenue potencial perdido: :green[**${dx_rev_b:.1f}B COP**]")

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
            if dx_brecha < -1:
                recs.append("1. :red[**Negociar con emisores**] los segmentos con mayor brecha vs benchmark para cerrar la diferencia.")
            if not dx_worst_canal.empty and float(dx_worst_canal.iloc[0]["TASA"]) < 85:
                recs.append(f"2. :orange[**Optimizar canal {dx_worst_canal.iloc[0]['CANAL']}**]: revisar parametros de autenticacion y reglas de fraude.")
            if not dx_top_motivo.empty:
                recs.append(f"3. :orange[**Atacar motivo '{dx_top_motivo.iloc[0]['MOTIVO']}'**]: coordinar con emisores y ajustar reglas.")
            if dx_rev_b > 50:
                recs.append("4. :red[**Priorizar recuperacion de revenue**]: cada punto de mejora representa ~$10B COP anuales.")
            if not recs:
                recs.append(":green[**Los indicadores estan dentro de parametros aceptables.**] Mantener monitoreo continuo.")
            st.markdown("\n".join(recs))

st.divider()

tab1, tab2 = st.tabs(["Dashboard Ejecutivo", "Simulador Predictivo"])

with tab1:
    fc1, fc2, fc3, fc4, fc5, fc6 = st.columns(6)
    with fc1:
        fecha_rng = st.date_input("Periodo", value=(fmin, fmax), min_value=fmin, max_value=fmax, key="ta_fecha")
        if isinstance(fecha_rng, (list, tuple)) and len(fecha_rng) == 2:
            fecha_inicio, fecha_fin = fecha_rng
        else:
            fecha_inicio, fecha_fin = fmin, fmax
    with fc2:
        red_sel = st.selectbox("Red", ["Todos"] + redes, key="ta_red")
    with fc3:
        canal_sel = st.selectbox("Canal", ["Todos"] + canales, key="ta_canal")
    with fc4:
        banco_sel = st.selectbox("Banco emisor", ["Todos"] + bancos, key="ta_banco")
    with fc5:
        tipo_sel = st.selectbox("Tipo tarjeta", ["Todos"] + tipos_tarjeta, key="ta_tipo")
    with fc6:
        depto_sel = st.selectbox("Departamento", ["Todos"] + deptos, key="ta_depto")

    WHERE = build_where(fecha_inicio, fecha_fin, red_sel, canal_sel, banco_sel, tipo_sel, depto_sel,
                        redes, canales, bancos, tipos_tarjeta, deptos)

    kpi_df = run_query(f"""
        SELECT
            ROUND(AVG(TASA_APROBACION)*100, 1) AS TASA_APROBACION_AVG,
            ROUND(AVG(TASA_RECHAZO)*100, 1) AS TASA_RECHAZO_AVG,
            SUM(REVENUE_POTENCIAL_PERDIDO_COP) AS REVENUE_PERDIDO,
            SUM(MONTO_RECHAZADO_COP) AS MONTO_RECHAZADO,
            ROUND(AVG(BENCHMARK_INDUSTRIA)*100, 1) AS BENCHMARK_AVG,
            ROUND(AVG(DIFERENCIA_VS_BENCHMARK_PP), 1) AS DIFF_BENCHMARK,
            ROUND(AVG(VARIACION_VS_SEMANA_ANT_PP), 1) AS VAR_SEMANA,
            ROUND(AVG(VARIACION_VS_MES_ANT_PP), 1) AS VAR_MES,
            SUM(CASE WHEN ALERTA_CAIDA_APROBACION THEN 1 ELSE 0 END) AS ALERTAS,
            COUNT(*) AS REGISTROS
        FROM {TABLE}
        WHERE {WHERE}
    """)

    if kpi_df.empty or kpi_df["REGISTROS"].iloc[0] == 0:
        st.info("No hay datos para los filtros seleccionados.")
    else:
        r = kpi_df.iloc[0]

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Tasa de aprobacion", f"{float(r['TASA_APROBACION_AVG']):.1f}%", delta=f"{float(r['VAR_SEMANA'])} pp vs sem ant")
        k2.metric("Revenue perdido", f"${float(r['REVENUE_PERDIDO'])/1e9:.1f}B COP", delta=f"{float(r['VAR_MES'])} pp vs mes ant", delta_color="inverse")
        k3.metric("Monto rechazado", f"${float(r['MONTO_RECHAZADO'])/1e9:.1f}B COP")
        k4.metric("Benchmark industria", f"{float(r['BENCHMARK_AVG']):.1f}%", delta=f"{float(r['DIFF_BENCHMARK'])} pp")

        k5, k6, k7, k8 = st.columns(4)
        k5.metric("Tasa de rechazo", f"{float(r['TASA_RECHAZO_AVG']):.1f}%", delta_color="inverse")
        k6.metric("Alertas activas", f"{int(r['ALERTAS'])}", delta_color="inverse")
        k7.metric("Registros analizados", f"{int(r['REGISTROS']):,}")
        k8.metric("Var. vs mes anterior", f"{float(r['VAR_MES'])} pp")

        st.divider()

        trend_df = run_query(f"""
            SELECT DATE_TRUNC('MONTH', FECHA) AS MES,
                   ROUND(AVG(TASA_APROBACION)*100, 1) AS TASA_APROBACION,
                   ROUND(AVG(BENCHMARK_INDUSTRIA)*100, 1) AS BENCHMARK
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1 ORDER BY 1
        """)

        col_chart1, col_chart2 = st.columns(2)

        with col_chart1:
            st.markdown("**Tendencia mensual — Aprobacion vs Benchmark**")
            if trend_df.empty:
                st.info("Sin datos de tendencia.")
            else:
                tasa_vals = [float(v) for v in trend_df["TASA_APROBACION"]]
                bench_vals = [float(v) for v in trend_df["BENCHMARK"]]
                meses = trend_df["MES"].tolist()
                fig_trend = go.Figure()
                fig_trend.add_trace(go.Scatter(
                    x=meses, y=tasa_vals,
                    name="Tasa aprobacion", mode="lines+markers+text",
                    fill="tozeroy", fillcolor="rgba(41,181,232,0.10)",
                    line=dict(color="#29B5E8", width=3), marker=dict(size=8),
                    text=[f"{v:.1f}%" for v in tasa_vals], textposition="top center",
                    textfont=dict(size=10, color="#29B5E8"),
                ))
                fig_trend.add_trace(go.Scatter(
                    x=meses, y=bench_vals,
                    name="Benchmark industria", mode="lines",
                    line=dict(color="#FF8B00", width=2, dash="dash"),
                ))
                for i in range(len(meses)):
                    if tasa_vals[i] < bench_vals[i]:
                        fig_trend.add_vrect(
                            x0=meses[max(0, i-1)] if i > 0 else meses[i], x1=meses[min(len(meses)-1, i)],
                            fillcolor="rgba(222,53,11,0.07)", line_width=0, layer="below",
                        )
                if len(tasa_vals) >= 2:
                    delta = tasa_vals[-1] - tasa_vals[-2]
                    arrow = "▲" if delta >= 0 else "▼"
                    fig_trend.add_annotation(
                        x=meses[-1], y=tasa_vals[-1],
                        text=f"{arrow} {delta:+.1f}pp", showarrow=True,
                        arrowhead=2, arrowcolor="#29B5E8",
                        font=dict(size=11, color="#11567F", weight="bold"),
                        ax=40, ay=-30,
                    )
                fig_trend.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF",
                    height=380, margin=dict(l=40, r=20, t=30, b=40),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    yaxis=dict(title="% Aprobacion", range=[min(min(tasa_vals), min(bench_vals)) - 3, 100]),
                )
                st.plotly_chart(fig_trend, use_container_width=True)

        with col_chart2:
            st.markdown("**Impacto de motivos de rechazo — Revenue perdido**")
            motivos_df = run_query(f"""
                SELECT MOTIVO_RECHAZO_PRINCIPAL AS MOTIVO, COUNT(*) AS FREQ,
                       ROUND(SUM(REVENUE_POTENCIAL_PERDIDO_COP)/1e6, 0) AS REVENUE_MM
                FROM {TABLE}
                WHERE {WHERE}
                GROUP BY 1 ORDER BY 3 DESC
                LIMIT 10
            """)
            if motivos_df.empty:
                st.info("Sin datos de motivos.")
            else:
                fig_tree = go.Figure(go.Treemap(
                    labels=motivos_df["MOTIVO"].tolist(),
                    values=[float(v) for v in motivos_df["REVENUE_MM"]],
                    parents=[""] * len(motivos_df),
                    textinfo="label+value+percent root",
                    texttemplate="<b>%{label}</b><br>$%{value:,.0f}M<br>%{percentRoot:.1%}",
                    marker=dict(
                        colors=[float(v) for v in motivos_df["REVENUE_MM"]],
                        colorscale=[[0, "#FFF3E0"], [0.3, "#FFAB00"], [0.6, "#FF8B00"], [1, "#DE350B"]],
                        line=dict(width=2, color="white"),
                    ),
                    hovertemplate="<b>%{label}</b><br>Revenue perdido: $%{value:,.0f}M COP<br>%{percentRoot:.1%} del total<extra></extra>",
                ))
                fig_tree.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF",
                    height=380, margin=dict(l=5, r=5, t=30, b=5),
                )
                st.plotly_chart(fig_tree, use_container_width=True)

        col_chart3, col_chart4, col_chart5 = st.columns(3)

        with col_chart3:
            st.markdown("**Radar — Desempeno por red**")
            red_df = run_query(f"""
                SELECT RED, ROUND(AVG(TASA_APROBACION)*100, 1) AS TASA,
                       ROUND(AVG(BENCHMARK_INDUSTRIA)*100, 1) AS BENCHMARK
                FROM {TABLE}
                WHERE {WHERE}
                GROUP BY 1 ORDER BY 2 DESC
            """)
            if red_df.empty:
                st.info("Sin datos por red.")
            else:
                cats = red_df["RED"].tolist()
                tasa_r = [float(v) for v in red_df["TASA"]]
                bench_r = [float(v) for v in red_df["BENCHMARK"]]
                fig_radar = go.Figure()
                fig_radar.add_trace(go.Scatterpolar(
                    r=tasa_r + [tasa_r[0]], theta=cats + [cats[0]],
                    name="Tasa aprobacion", fill="toself",
                    fillcolor="rgba(41,181,232,0.20)",
                    line=dict(color="#29B5E8", width=2),
                    marker=dict(size=6),
                ))
                fig_radar.add_trace(go.Scatterpolar(
                    r=bench_r + [bench_r[0]], theta=cats + [cats[0]],
                    name="Benchmark", fill="none",
                    line=dict(color="#FF8B00", width=2, dash="dash"),
                ))
                fig_radar.update_layout(
                    polar=dict(radialaxis=dict(visible=True, range=[75, 100], ticksuffix="%")),
                    template="plotly_white", paper_bgcolor="#FFFFFF",
                    height=380, margin=dict(l=60, r=60, t=40, b=40),
                    legend=dict(orientation="h", yanchor="bottom", y=-0.15, xanchor="center", x=0.5),
                    showlegend=True,
                )
                st.plotly_chart(fig_radar, use_container_width=True)

        with col_chart4:
            st.markdown("**Funnel — Aprobacion por canal**")
            canal_df = run_query(f"""
                SELECT CANAL, ROUND(AVG(TASA_APROBACION)*100, 1) AS TASA,
                       SUM(TOTAL_TRANSACCIONES) AS VOLUMEN
                FROM {TABLE}
                WHERE {WHERE}
                GROUP BY 1 ORDER BY 2 DESC
            """)
            if canal_df.empty:
                st.info("Sin datos por canal.")
            else:
                canales_list = canal_df["CANAL"].tolist()
                tasas_list = [float(v) for v in canal_df["TASA"]]
                funnel_colors = ["#36B37E" if v >= 90 else "#FFAB00" if v >= 85 else "#DE350B" for v in tasas_list]
                fig_funnel = go.Figure(go.Funnel(
                    y=canales_list,
                    x=tasas_list,
                    textinfo="value+percent initial",
                    texttemplate="%{value:.1f}% (%{percentInitial:.1%})",
                    marker=dict(color=funnel_colors, line=dict(width=1, color="white")),
                    connector=dict(line=dict(color="#DFE1E6", width=1)),
                ))
                fig_funnel.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF",
                    height=380, margin=dict(l=120, r=20, t=30, b=40),
                    showlegend=False,
                )
                st.plotly_chart(fig_funnel, use_container_width=True)

        with col_chart5:
            st.markdown("**Distribucion de motivos de rechazo**")
            donut_df = run_query(f"""
                SELECT MOTIVO_RECHAZO_PRINCIPAL AS MOTIVO, COUNT(*) AS FREQ
                FROM {TABLE}
                WHERE {WHERE}
                GROUP BY 1 ORDER BY 2 DESC
                LIMIT 6
            """)
            if donut_df.empty:
                st.info("Sin datos de motivos.")
            else:
                fig_donut = go.Figure(go.Pie(
                    labels=donut_df["MOTIVO"].tolist(),
                    values=[float(v) for v in donut_df["FREQ"]],
                    hole=0.55,
                    marker=dict(colors=["#DE350B", "#FF8B00", "#FFAB00", "#29B5E8", "#36B37E", "#6554C0"],
                                line=dict(color="white", width=2)),
                    textinfo="percent+label",
                    textposition="outside",
                    textfont=dict(size=10),
                    hovertemplate="<b>%{label}</b><br>Frecuencia: %{value:,}<br>%{percent:.1%}<extra></extra>",
                    pull=[0.05, 0, 0, 0, 0, 0],
                ))
                total_rechazos = int(donut_df["FREQ"].sum())
                fig_donut.add_annotation(
                    text=f"<b>{total_rechazos:,}</b><br>rechazos",
                    x=0.5, y=0.5, showarrow=False,
                    font=dict(size=14, color="#11567F"),
                )
                fig_donut.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF",
                    height=380, margin=dict(l=5, r=5, t=30, b=5),
                    showlegend=False,
                )
                st.plotly_chart(fig_donut, use_container_width=True)

        st.divider()

        st.markdown("**Tasa de aprobacion por departamento**")
        mapa_df = run_query(f"""
            SELECT DEPARTAMENTO,
                   ROUND(AVG(TASA_APROBACION)*100, 1) AS TASA,
                   SUM(TOTAL_TRANSACCIONES) AS VOLUMEN,
                   ROUND(SUM(REVENUE_POTENCIAL_PERDIDO_COP)/1e6, 0) AS REV_PERDIDO_MM
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1 ORDER BY 2 DESC
        """)

        if mapa_df.empty:
            st.info("Sin datos geograficos.")
        else:
            mapa_df["LAT"] = mapa_df["DEPARTAMENTO"].map(lambda d: COORDS_DEPTO.get(d, (4.5, -74.0))[0]).astype(float)
            mapa_df["LON"] = mapa_df["DEPARTAMENTO"].map(lambda d: COORDS_DEPTO.get(d, (4.5, -74.0))[1]).astype(float)
            mapa_df["TASA_F"] = mapa_df["TASA"].astype(float)
            mapa_df["VOLUMEN_F"] = mapa_df["VOLUMEN"].astype(float)
            mapa_df["REV_F"] = mapa_df["REV_PERDIDO_MM"].astype(float)
            mapa_df["hover"] = mapa_df.apply(lambda r: f"{r['DEPARTAMENTO']}<br>Tasa: {r['TASA_F']}%<br>Volumen: {int(r['VOLUMEN_F']):,}<br>Rev. perdido: ${r['REV_F']:.0f}M COP", axis=1)
            deck = colombia_scatter_map(
                mapa_df, lat_col="LAT", lon_col="LON", size_col="VOLUMEN_F", color_col="TASA_F",
                text_col="DEPARTAMENTO", hover_col="hover",
                colorscale=["#DE350B", "#FF8B00", "#36B37E"],
                colorbar_title="Tasa Aprob. (%)",
            )
            st.pydeck_chart(deck, key="tasa_map")

            st.caption("Tamano: volumen de transacciones | Color: rojo = baja aprobacion, verde = alta aprobacion")

        st.divider()

        st.markdown("**Top 10 bancos emisores por oportunidad de mejora**")
        top_bancos = run_query(f"""
            SELECT BANCO_EMISOR,
                   ROUND(AVG(TASA_APROBACION)*100, 1) AS TASA_APROBACION,
                   ROUND(AVG(BENCHMARK_INDUSTRIA)*100, 1) AS BENCHMARK,
                   ROUND(AVG(DIFERENCIA_VS_BENCHMARK_PP), 1) AS BRECHA_PP,
                   SUM(TOTAL_TRANSACCIONES) AS VOLUMEN,
                   ROUND(SUM(REVENUE_POTENCIAL_PERDIDO_COP)/1e6, 0) AS REVENUE_PERDIDO_MM,
                   ROUND(AVG(OPORTUNIDAD_RECUPERACION_PCT)*100, 1) AS OPORTUNIDAD_PCT
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1 ORDER BY 6 DESC
            LIMIT 10
        """)
        if top_bancos.empty:
            st.info("Sin datos de bancos.")
        else:
            st.dataframe(top_bancos, use_container_width=True)

        st.divider()

        st.markdown("**Alertas e insights automaticos**")
        alertas_df = run_query(f"""
            SELECT RED, BANCO_EMISOR, CANAL, ROUND(TASA_APROBACION*100,1) AS TASA,
                   MOTIVO_RECHAZO_PRINCIPAL, ROUND(REVENUE_POTENCIAL_PERDIDO_COP/1e6,1) AS REV_MM,
                   FECHA
            FROM {TABLE}
            WHERE {WHERE} AND ALERTA_CAIDA_APROBACION = TRUE
            ORDER BY REVENUE_POTENCIAL_PERDIDO_COP DESC
            LIMIT 5
        """)

        worst_red = run_query(f"""
            SELECT RED, ROUND(AVG(TASA_APROBACION)*100,1) AS TASA
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1 ORDER BY 2 ASC LIMIT 1
        """)

        worst_canal = run_query(f"""
            SELECT CANAL, ROUND(AVG(TASA_APROBACION)*100,1) AS TASA
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1 ORDER BY 2 ASC LIMIT 1
        """)

        if not worst_red.empty:
            wr = worst_red.iloc[0]
            st.warning(f"La red **{wr['RED']}** tiene la tasa de aprobacion mas baja: **{float(wr['TASA']):.1f}%**. Revisar configuracion con el emisor.")

        if not worst_canal.empty:
            wc = worst_canal.iloc[0]
            st.warning(f"El canal **{wc['CANAL']}** presenta la menor tasa de aprobacion: **{float(wc['TASA']):.1f}%**. Posible friccion en la experiencia de pago.")

        if not alertas_df.empty:
            n = len(alertas_df)
            rev_total = float(alertas_df["REV_MM"].sum())
            st.error(f"Se detectaron **{n}** alertas de caida en aprobacion con revenue potencial perdido de **${rev_total:,.1f}M COP**.")
        else:
            st.success("No se detectaron alertas de caida en la tasa de aprobacion en el periodo seleccionado.")


with tab2:
    st.markdown("**Simulador de tasa de aprobacion**")
    st.caption("Estime la tasa de aprobacion esperada ajustando las variables clave del negocio. El modelo pondera cada factor segun su impacto relativo en la aprobacion de transacciones.")

    col_sliders, col_result = st.columns([3, 2])

    with col_sliders:
        monto_promedio = st.slider(
            "Monto promedio de transaccion (miles COP)", 10, 5000, 500, step=50,
            help="Transacciones de menor monto tienden a tener mayor aprobacion",
        )
        pct_credito = st.slider(
            "% Transacciones con tarjeta credito", 0, 100, 55,
            help="Credito suele tener menor aprobacion por limites y validaciones adicionales",
        )
        pct_ecommerce = st.slider(
            "% Transacciones e-commerce / CNP", 0, 100, 35,
            help="Card-not-present tiene mas friccion y rechazo por fraude",
        )
        pct_internacional = st.slider(
            "% Transacciones internacionales", 0, 100, 15,
            help="Transacciones internacionales enfrentan mas validaciones del emisor",
        )
        indice_fraude = st.slider(
            "Indice de riesgo de fraude (0=bajo, 100=alto)", 0, 100, 20,
            help="Mayor riesgo de fraude reduce la tasa de aprobacion del emisor",
        )
        frecuencia_txn = st.slider(
            "Frecuencia diaria promedio por tarjeta", 1, 50, 5,
            help="Alta frecuencia puede activar reglas de velocidad del emisor",
        )

    def calcular_tasa_aprobacion(monto, pct_cred, pct_ecom, pct_intl, fraude, freq):
        if monto < 50:
            score_monto = 0.3
        elif monto < 200:
            score_monto = 0.6
        elif monto < 1000:
            score_monto = 1.0
        elif monto < 3000:
            score_monto = 0.7
        else:
            score_monto = 0.3

        score_tarjeta = 1.0 - (pct_cred / 100) * 0.7
        score_canal = 1.0 - (pct_ecom / 100) * 0.8
        score_intl = 1.0 - (pct_intl / 100) * 0.9
        score_fraude = 1.0 - (fraude / 100) * 1.0
        score_freq = max(0.0, 1.0 - (freq / 50) * 1.0)

        tasa = (
            0.15 * score_monto
            + 0.18 * score_tarjeta
            + 0.18 * score_canal
            + 0.15 * score_intl
            + 0.22 * score_fraude
            + 0.12 * score_freq
        )

        tasa_final = 0.50 + tasa * 0.48
        return round(min(max(tasa_final, 0.50), 0.98), 3)

    tasa_pred = calcular_tasa_aprobacion(monto_promedio, pct_credito, pct_ecommerce, pct_internacional, indice_fraude, frecuencia_txn)
    tasa_pct = tasa_pred * 100

    with col_result:
        if tasa_pct >= 92:
            nivel = "Alta"
            color_nivel = "#36B37E"
            interpretacion = "Excelente tasa de aprobacion. El perfil transaccional esta optimizado. Mantener las condiciones actuales y monitorear cambios en reglas de emisores."
        elif tasa_pct >= 80:
            nivel = "Media-Alta"
            color_nivel = "#29B5E8"
            interpretacion = "Tasa de aprobacion aceptable pero con espacio de mejora. Revisar segmentos con mayor rechazo (canal CNP o transacciones internacionales) para optimizar."
        elif tasa_pct >= 65:
            nivel = "Media"
            color_nivel = "#FFAB00"
            interpretacion = "Tasa de aprobacion por debajo del benchmark. Priorizar negociacion con emisores en segmentos de mayor volumen y revisar reglas de fraude que puedan ser demasiado estrictas."
        else:
            nivel = "Baja"
            color_nivel = "#DE350B"
            interpretacion = "Tasa de aprobacion critica. Se requiere accion inmediata: revisar parametros de fraude, negociar con emisores, optimizar canales CNP y reducir friccion en transacciones internacionales."

        st.metric("Tasa de aprobacion estimada", f"{tasa_pct:.1f}%")
        if tasa_pct >= 92:
            st.markdown(f"**Nivel:** :green[**{nivel}**]")
        elif tasa_pct >= 80:
            st.markdown(f"**Nivel:** :blue[**{nivel}**]")
        elif tasa_pct >= 65:
            st.markdown(f"**Nivel:** :orange[**{nivel}**]")
        else:
            st.markdown(f"**Nivel:** :red[**{nivel}**]")

        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=float(tasa_pct),
            number=dict(suffix="%", font=dict(size=36)),
            gauge=dict(
                axis=dict(range=[50, 98], ticksuffix="%"),
                bar=dict(color=color_nivel),
                steps=[
                    dict(range=[50, 65], color="#FFCDD2"),
                    dict(range=[65, 75], color="#FFEBEE"),
                    dict(range=[75, 80], color="#FFF3E0"),
                    dict(range=[80, 85], color="#FFF8E1"),
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

    if "escenarios" not in st.session_state:
        st.session_state.escenarios = []

    if st.button("Guardar escenario actual", type="primary"):
        if len(st.session_state.escenarios) >= 3:
            st.session_state.escenarios.pop(0)
        st.session_state.escenarios.append({
            "Monto prom (K COP)": monto_promedio,
            "% Credito": pct_credito,
            "% E-commerce": pct_ecommerce,
            "% Internacional": pct_internacional,
            "Riesgo fraude": indice_fraude,
            "Freq diaria": frecuencia_txn,
            "Tasa estimada": f"{tasa_pct:.1f}%",
            "Nivel": nivel,
        })
        st.rerun()

    if st.session_state.escenarios:
        esc_df = pd.DataFrame(st.session_state.escenarios)
        esc_df.index = [f"Escenario {i+1}" for i in range(len(esc_df))]
        st.dataframe(esc_df.T, use_container_width=True)

        if st.button("Limpiar escenarios"):
            st.session_state.escenarios = []
            st.rerun()
    else:
        st.caption("Aun no hay escenarios guardados. Ajuste los sliders y presione 'Guardar escenario actual'.")
