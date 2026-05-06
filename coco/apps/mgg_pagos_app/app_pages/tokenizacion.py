import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go

TABLE = "MGG_PAGOS.FRAUDE_Y_SEGURIDAD_PAGOS.TOKENIZACION"

COLORS = ["#29B5E8", "#FF8B00", "#36B37E", "#6554C0", "#DE350B", "#11567F", "#FFAB00", "#00A3BF"]


from app_pages.conn_helper import run_query



@st.cache_data(ttl=300, show_spinner=False)
def get_filter_options():
    tipos = run_query(f"SELECT DISTINCT TIPO_TOKEN FROM {TABLE} ORDER BY 1")["TIPO_TOKEN"].tolist()
    redes = run_query(f"SELECT DISTINCT RED FROM {TABLE} ORDER BY 1")["RED"].tolist()
    estados = run_query(f"SELECT DISTINCT ESTADO FROM {TABLE} ORDER BY 1")["ESTADO"].tolist()
    bancos = run_query(f"SELECT DISTINCT BANCO_EMISOR FROM {TABLE} ORDER BY 1")["BANCO_EMISOR"].tolist()
    tipos_tarj = run_query(f"SELECT DISTINCT TIPO_TARJETA FROM {TABLE} ORDER BY 1")["TIPO_TARJETA"].tolist()
    fechas = run_query(f"SELECT MIN(FECHA_CREACION) AS FMIN, MAX(FECHA_CREACION) AS FMAX FROM {TABLE}")
    return tipos, redes, estados, bancos, tipos_tarj, fechas


def build_where(fecha_ini, fecha_f, tipo_s, red_s, estado_s, banco_s,
                all_tipos, all_redes, all_estados, all_bancos):
    clauses = [f"FECHA_CREACION BETWEEN '{fecha_ini}' AND '{fecha_f}'"]
    if tipo_s and tipo_s != "Todos" and tipo_s in all_tipos:
        clauses.append(f"TIPO_TOKEN = '{tipo_s}'")
    if red_s and red_s != "Todos" and red_s in all_redes:
        clauses.append(f"RED = '{red_s}'")
    if estado_s and estado_s != "Todos" and estado_s in all_estados:
        clauses.append(f"ESTADO = '{estado_s}'")
    if banco_s and banco_s != "Todos" and banco_s in all_bancos:
        clauses.append(f"BANCO_EMISOR = '{banco_s}'")
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


st.header(":material/token: Tokenizacion")
st.caption("Gestion de tokens de tarjeta que reemplazan el PAN real. Tipo token, red, estado, tasa de aprobacion y lifecycle management.")

c1, c2, c3 = st.columns(3)
with c1:
    with st.container(border=True):
        st.markdown("**:material/lightbulb: Que resuelve**")
        st.markdown("Exposicion del numero de tarjeta (PAN) en los flujos de pago que incrementa el riesgo de fraude y dificulta el cumplimiento PCI DSS.")
with c2:
    with st.container(border=True):
        st.markdown("**:material/settings: Como funciona**")
        st.markdown("El PAN se reemplaza por un token unico por comercio, dispositivo o red. Se registra el tipo de token, su estado activo/suspendido, tasa de aprobacion y eventos de lifecycle.")
with c3:
    with st.container(border=True):
        st.markdown("**:material/trending_up: Valor de negocio**")
        st.markdown("Mejorar la seguridad eliminando el PAN de los flujos y aumentar la tasa de aprobacion hasta un 10% gracias a la actualizacion automatica de credenciales.")

tipos, redes, estados, bancos, tipos_tarj, fechas_df = get_filter_options()
fmin = pd.to_datetime(fechas_df["FMIN"].iloc[0]).date()
fmax = pd.to_datetime(fechas_df["FMAX"].iloc[0]).date()

dx_where = f"FECHA_CREACION BETWEEN '{fmin}' AND '{fmax}'"

dx_kpi = run_query(f"""
    SELECT
        COUNT(*) AS TOTAL_TOKENS,
        ROUND(AVG(TASA_APROBACION_TOKEN)*100, 1) AS TASA_TOKEN,
        ROUND(AVG(TASA_APROBACION_PAN)*100, 1) AS TASA_PAN,
        ROUND(AVG(MEJORA_APROBACION_PCT)*100, 1) AS MEJORA_PCT,
        SUM(CASE WHEN TOKEN_COMPROMETIDO THEN 1 ELSE 0 END) AS COMPROMETIDOS,
        ROUND(SUM(VOLUMEN_TOTAL_COP)/1e9, 1) AS VOL_B,
        SUM(CASE WHEN ESTADO = 'Activo' THEN 1 ELSE 0 END) AS ACTIVOS
    FROM {TABLE}
    WHERE {dx_where}
""")

if not dx_kpi.empty and dx_kpi["TOTAL_TOKENS"].iloc[0] > 0:
    dx_r = dx_kpi.iloc[0]
    dx_total = int(dx_r["TOTAL_TOKENS"])
    dx_tasa_token = float(dx_r["TASA_TOKEN"])
    dx_tasa_pan = float(dx_r["TASA_PAN"])
    dx_mejora = float(dx_r["MEJORA_PCT"])
    dx_comprometidos = int(dx_r["COMPROMETIDOS"])
    dx_activos = int(dx_r["ACTIVOS"])
    dx_pct_activos = round(dx_activos / dx_total * 100, 1)

    dx_worst_red = run_query(f"""
        SELECT RED, ROUND(AVG(MEJORA_APROBACION_PCT)*100, 1) AS MEJORA
        FROM {TABLE} WHERE {dx_where}
        GROUP BY 1 ORDER BY 2 ASC LIMIT 1
    """)
    dx_best_tipo = run_query(f"""
        SELECT TIPO_TOKEN, ROUND(AVG(TASA_APROBACION_TOKEN)*100, 1) AS TASA
        FROM {TABLE} WHERE {dx_where}
        GROUP BY 1 ORDER BY 2 DESC LIMIT 1
    """)

    if dx_mejora >= 5:
        dx_estado = f"Tokenizacion genera :green[**+{dx_mejora:.1f}pp**] de mejora en aprobacion. Tasa token ({color_tag(dx_tasa_token, 90, 85)}) vs PAN ({dx_tasa_pan:.1f}%)."
    elif dx_mejora >= 0:
        dx_estado = f"Tokenizacion genera :orange[**+{dx_mejora:.1f}pp**] de mejora modesta. Hay oportunidad de optimizacion."
    else:
        dx_estado = f"Tokenizacion :red[**no esta generando mejora**] ({dx_mejora:.1f}pp). Requiere :red[**revision de configuracion**]."

    dx_lines = [dx_estado]
    if not dx_best_tipo.empty:
        dx_lines.append(f"- Mejor tipo de token: **{dx_best_tipo.iloc[0]['TIPO_TOKEN']}** ({color_tag(float(dx_best_tipo.iloc[0]['TASA']), 90, 85)})")
    if not dx_worst_red.empty:
        dx_lines.append(f"- Red con menor mejora: **{dx_worst_red.iloc[0]['RED']}** ({color_tag(float(dx_worst_red.iloc[0]['MEJORA']), 5, 0)})")
    dx_lines.append(f"- Tokens activos: :green[**{dx_pct_activos:.1f}%**] ({dx_activos:,} de {dx_total:,})")
    if dx_comprometidos > 0:
        dx_lines.append(f"- Tokens comprometidos: :red[**{dx_comprometidos:,}**] — requiere remediacion inmediata")

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
            if dx_mejora < 3:
                recs.append("1. :orange[**Optimizar tokenizacion**]: la mejora de aprobacion esta por debajo del potencial. Revisar configuracion con TSPs.")
            if dx_comprometidos > 0:
                recs.append(f"2. :red[**Revocar {dx_comprometidos} tokens comprometidos**] y notificar a emisores para renovacion.")
            if dx_pct_activos < 80:
                recs.append("3. :orange[**Activar tokens pendientes**]: un alto % de tokens inactivos reduce el beneficio de tokenizacion.")
            if not dx_worst_red.empty and float(dx_worst_red.iloc[0]["MEJORA"]) < 2:
                recs.append(f"4. :orange[**Negociar con red {dx_worst_red.iloc[0]['RED']}**]: mejora de aprobacion inferior al promedio.")
            if not recs:
                recs.append(":green[**Tokenizacion operando de forma optima.**] Mantener monitoreo y expandir cobertura.")
            st.markdown("\n".join(recs))

st.divider()

tab1, tab2 = st.tabs(["Dashboard Ejecutivo", "Simulador Predictivo"])

with tab1:
    fc1, fc2, fc3, fc4, fc5 = st.columns(5)
    with fc1:
        fecha_rng = st.date_input("Periodo", value=(fmin, fmax), min_value=fmin, max_value=fmax, key="tk_fecha")
        if isinstance(fecha_rng, (list, tuple)) and len(fecha_rng) == 2:
            fecha_inicio, fecha_fin = fecha_rng
        else:
            fecha_inicio, fecha_fin = fmin, fmax
    with fc2:
        tipo_sel = st.selectbox("Tipo token", ["Todos"] + tipos, key="tk_tipo")
    with fc3:
        red_sel = st.selectbox("Red", ["Todos"] + redes, key="tk_red")
    with fc4:
        estado_sel = st.selectbox("Estado", ["Todos"] + estados, key="tk_estado")
    with fc5:
        banco_sel = st.selectbox("Banco emisor", ["Todos"] + bancos, key="tk_banco")

    WHERE = build_where(fecha_inicio, fecha_fin, tipo_sel, red_sel, estado_sel, banco_sel,
                        tipos, redes, estados, bancos)

    kpi_df = run_query(f"""
        SELECT
            COUNT(*) AS TOTAL,
            ROUND(AVG(TASA_APROBACION_TOKEN)*100, 1) AS TASA_TOKEN,
            ROUND(AVG(TASA_APROBACION_PAN)*100, 1) AS TASA_PAN,
            ROUND(AVG(MEJORA_APROBACION_PCT)*100, 1) AS MEJORA,
            SUM(CASE WHEN TOKEN_COMPROMETIDO THEN 1 ELSE 0 END) AS COMPROMETIDOS,
            ROUND(SUM(VOLUMEN_TOTAL_COP)/1e9, 1) AS VOL_B,
            SUM(CASE WHEN ESTADO = 'Activo' THEN 1 ELSE 0 END) AS ACTIVOS,
            ROUND(AVG(TRANSACCIONES_TOKEN), 0) AS TXN_PROM
        FROM {TABLE}
        WHERE {WHERE}
    """)

    if kpi_df.empty or kpi_df["TOTAL"].iloc[0] == 0:
        st.info("No hay datos para los filtros seleccionados.")
    else:
        r = kpi_df.iloc[0]
        total = int(r["TOTAL"])

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total tokens", f"{total:,}")
        k2.metric("Tasa aprobacion token", f"{float(r['TASA_TOKEN']):.1f}%")
        k3.metric("Tasa aprobacion PAN", f"{float(r['TASA_PAN']):.1f}%")
        k4.metric("Mejora tokenizacion", f"+{float(r['MEJORA']):.1f} pp")

        k5, k6, k7, k8 = st.columns(4)
        k5.metric("Tokens activos", f"{int(r['ACTIVOS']):,}", delta=f"{round(int(r['ACTIVOS'])/total*100, 1)}%")
        k6.metric("Comprometidos", f"{int(r['COMPROMETIDOS']):,}", delta_color="inverse")
        k7.metric("Volumen total", f"${float(r['VOL_B']):.1f}B COP")
        k8.metric("Txns/token prom", f"{float(r['TXN_PROM']):.0f}")

        st.divider()

        # --- ROW 1: Area trend token vs PAN + Treemap por tipo token ---
        trend_df = run_query(f"""
            SELECT DATE_TRUNC('MONTH', FECHA_CREACION) AS MES,
                   ROUND(AVG(TASA_APROBACION_TOKEN)*100, 1) AS TASA_TOKEN,
                   ROUND(AVG(TASA_APROBACION_PAN)*100, 1) AS TASA_PAN,
                   COUNT(*) AS NUEVOS_TOKENS
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1 ORDER BY 1
        """)

        col_chart1, col_chart2 = st.columns(2)

        with col_chart1:
            st.markdown("**Tendencia — Aprobacion Token vs PAN**")
            if trend_df.empty:
                st.info("Sin datos de tendencia.")
            else:
                meses = trend_df["MES"].tolist()
                token_vals = [float(v) for v in trend_df["TASA_TOKEN"]]
                pan_vals = [float(v) for v in trend_df["TASA_PAN"]]
                fig_trend = go.Figure()
                fig_trend.add_trace(go.Scatter(
                    x=meses, y=token_vals,
                    name="Con token", mode="lines+markers",
                    fill="tozeroy", fillcolor="rgba(54,179,126,0.12)",
                    line=dict(color="#36B37E", width=3), marker=dict(size=7),
                    text=[f"{v:.1f}%" for v in token_vals], textposition="top center",
                    textfont=dict(size=10, color="#36B37E"),
                ))
                fig_trend.add_trace(go.Scatter(
                    x=meses, y=pan_vals,
                    name="Sin token (PAN)", mode="lines+markers",
                    line=dict(color="#DE350B", width=2, dash="dash"), marker=dict(size=5),
                ))
                for i in range(len(meses)):
                    if token_vals[i] > pan_vals[i]:
                        fig_trend.add_vrect(
                            x0=meses[max(0, i-1)] if i > 0 else meses[i],
                            x1=meses[min(len(meses)-1, i)],
                            fillcolor="rgba(54,179,126,0.05)", line_width=0, layer="below",
                        )
                if len(token_vals) >= 2:
                    delta = token_vals[-1] - pan_vals[-1]
                    fig_trend.add_annotation(
                        x=meses[-1], y=token_vals[-1],
                        text=f"+{delta:.1f}pp ventaja", showarrow=True,
                        arrowhead=2, arrowcolor="#36B37E",
                        font=dict(size=11, color="#1B5E20", weight="bold"),
                        ax=40, ay=-30,
                    )
                fig_trend.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF",
                    height=380, margin=dict(l=40, r=20, t=30, b=40),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    yaxis=dict(title="% Aprobacion", range=[min(min(token_vals), min(pan_vals)) - 3, 100]),
                )
                st.plotly_chart(fig_trend, use_container_width=True)

        with col_chart2:
            st.markdown("**Volumen transado por tipo de token**")
            tipo_df = run_query(f"""
                SELECT TIPO_TOKEN, COUNT(*) AS N,
                       ROUND(SUM(VOLUMEN_TOTAL_COP)/1e6, 0) AS VOL_MM,
                       ROUND(AVG(MEJORA_APROBACION_PCT)*100, 1) AS MEJORA
                FROM {TABLE}
                WHERE {WHERE}
                GROUP BY 1 ORDER BY 3 DESC
            """)
            if tipo_df.empty:
                st.info("Sin datos por tipo.")
            else:
                fig_tree = go.Figure(go.Treemap(
                    labels=tipo_df["TIPO_TOKEN"].tolist(),
                    values=[float(v) for v in tipo_df["VOL_MM"]],
                    parents=[""] * len(tipo_df),
                    textinfo="label+value+percent root",
                    texttemplate="<b>%{label}</b><br>$%{value:,.0f}M<br>%{percentRoot:.1%}",
                    marker=dict(
                        colors=[float(v) for v in tipo_df["VOL_MM"]],
                        colorscale=[[0, "#E8F5E9"], [0.3, "#36B37E"], [0.6, "#29B5E8"], [1, "#11567F"]],
                        line=dict(width=2, color="white"),
                    ),
                    hovertemplate="<b>%{label}</b><br>Volumen: $%{value:,.0f}M COP<br>%{percentRoot:.1%}<extra></extra>",
                ))
                fig_tree.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF",
                    height=380, margin=dict(l=5, r=5, t=30, b=5),
                )
                st.plotly_chart(fig_tree, use_container_width=True)

        # --- ROW 2: Radar + Funnel + Donut ---
        col_chart3, col_chart4, col_chart5 = st.columns(3)

        with col_chart3:
            st.markdown("**Radar — Mejora por red**")
            red_df = run_query(f"""
                SELECT RED,
                       ROUND(AVG(TASA_APROBACION_TOKEN)*100, 1) AS TASA_TOKEN,
                       ROUND(AVG(TASA_APROBACION_PAN)*100, 1) AS TASA_PAN,
                       ROUND(AVG(MEJORA_APROBACION_PCT)*100, 1) AS MEJORA,
                       ROUND(AVG(TRANSACCIONES_TOKEN), 0) AS TXN_PROM
                FROM {TABLE}
                WHERE {WHERE}
                GROUP BY 1
            """)
            if red_df.empty:
                st.info("Sin datos por red.")
            else:
                cats = red_df["RED"].tolist()
                tasa_t = [float(v) for v in red_df["TASA_TOKEN"]]
                tasa_p = [float(v) for v in red_df["TASA_PAN"]]
                fig_radar = go.Figure()
                fig_radar.add_trace(go.Scatterpolar(
                    r=tasa_t + [tasa_t[0]], theta=cats + [cats[0]],
                    name="Con token", fill="toself",
                    fillcolor="rgba(54,179,126,0.20)",
                    line=dict(color="#36B37E", width=2),
                    marker=dict(size=6),
                ))
                fig_radar.add_trace(go.Scatterpolar(
                    r=tasa_p + [tasa_p[0]], theta=cats + [cats[0]],
                    name="Sin token (PAN)", fill="none",
                    line=dict(color="#DE350B", width=2, dash="dash"),
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
            st.markdown("**Funnel — Estado del lifecycle**")
            estado_df = run_query(f"""
                SELECT ESTADO, COUNT(*) AS N
                FROM {TABLE}
                WHERE {WHERE}
                GROUP BY 1 ORDER BY 2 DESC
            """)
            if estado_df.empty:
                st.info("Sin datos de estados.")
            else:
                estado_colors = {"Activo": "#36B37E", "Pendiente activacion": "#FFAB00",
                                 "Suspendido": "#FF8B00", "Expirado": "#DE350B", "Revocado": "#6554C0"}
                est_list = estado_df["ESTADO"].tolist()
                est_vals = [int(v) for v in estado_df["N"]]
                est_colors = [estado_colors.get(e, "#999") for e in est_list]
                fig_funnel = go.Figure(go.Funnel(
                    y=est_list, x=est_vals,
                    textinfo="value+percent initial",
                    texttemplate="%{value:,} (%{percentInitial:.1%})",
                    marker=dict(color=est_colors, line=dict(width=1, color="white")),
                    connector=dict(line=dict(color="#DFE1E6", width=1)),
                ))
                fig_funnel.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF",
                    height=380, margin=dict(l=140, r=20, t=30, b=40),
                    showlegend=False,
                )
                st.plotly_chart(fig_funnel, use_container_width=True)

        with col_chart5:
            st.markdown("**Distribucion por dominio de token**")
            dom_df = run_query(f"""
                SELECT DOMINIO_TOKEN, COUNT(*) AS N
                FROM {TABLE}
                WHERE {WHERE}
                GROUP BY 1 ORDER BY 2 DESC
            """)
            if dom_df.empty:
                st.info("Sin datos de dominio.")
            else:
                fig_donut = go.Figure(go.Pie(
                    labels=dom_df["DOMINIO_TOKEN"].tolist(),
                    values=[float(v) for v in dom_df["N"]],
                    hole=0.55,
                    marker=dict(colors=["#36B37E", "#29B5E8", "#6554C0", "#FF8B00", "#FFAB00", "#DE350B"],
                                line=dict(color="white", width=2)),
                    textinfo="percent+label",
                    textposition="outside",
                    textfont=dict(size=10),
                    hovertemplate="<b>%{label}</b><br>Tokens: %{value:,}<br>%{percent:.1%}<extra></extra>",
                    pull=[0.05, 0, 0, 0, 0, 0],
                ))
                total_dom = int(dom_df["N"].sum())
                fig_donut.add_annotation(
                    text=f"<b>{total_dom:,}</b><br>tokens",
                    x=0.5, y=0.5, showarrow=False,
                    font=dict(size=13, color="#11567F"),
                )
                fig_donut.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF",
                    height=380, margin=dict(l=5, r=5, t=30, b=5),
                    showlegend=False,
                )
                st.plotly_chart(fig_donut, use_container_width=True)

        st.divider()

        st.markdown("**Top 10 tokens por volumen transado**")
        top_df = run_query(f"""
            SELECT TOKEN_REFERENCIA, TIPO_TOKEN, RED, BANCO_EMISOR, ESTADO,
                   ROUND(TASA_APROBACION_TOKEN*100, 1) AS TASA_TOKEN,
                   ROUND(TASA_APROBACION_PAN*100, 1) AS TASA_PAN,
                   ROUND(MEJORA_APROBACION_PCT*100, 1) AS MEJORA_PP,
                   TRANSACCIONES_TOKEN AS TXNS,
                   ROUND(VOLUMEN_TOTAL_COP/1e6, 0) AS VOL_MM
            FROM {TABLE}
            WHERE {WHERE}
            ORDER BY VOLUMEN_TOTAL_COP DESC
            LIMIT 10
        """)
        if not top_df.empty:
            st.dataframe(top_df, use_container_width=True)

        st.divider()

        st.markdown("**Alertas e insights automaticos**")
        comprometidos = run_query(f"SELECT COUNT(*) AS N FROM {TABLE} WHERE {WHERE} AND TOKEN_COMPROMETIDO")
        baja_mejora = run_query(f"""
            SELECT RED, ROUND(AVG(MEJORA_APROBACION_PCT)*100, 1) AS MEJORA
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1 HAVING AVG(MEJORA_APROBACION_PCT)*100 < 2
            ORDER BY 2 ASC LIMIT 1
        """)
        expirados = run_query(f"SELECT COUNT(*) AS N FROM {TABLE} WHERE {WHERE} AND ESTADO = 'Expirado'")

        if not comprometidos.empty and int(comprometidos.iloc[0]["N"]) > 0:
            st.error(f"**{int(comprometidos.iloc[0]['N']):,}** tokens comprometidos detectados. Iniciar proceso de revocacion inmediata.")
        if not baja_mejora.empty:
            st.warning(f"Red **{baja_mejora.iloc[0]['RED']}** muestra mejora de solo **{float(baja_mejora.iloc[0]['MEJORA']):.1f}pp** con tokenizacion. Revisar configuracion TSP.")
        if not expirados.empty and int(expirados.iloc[0]["N"]) > total * 0.15:
            pct_exp = round(int(expirados.iloc[0]["N"]) / total * 100, 1)
            st.warning(f"**{pct_exp}%** de tokens expirados. Activar renovacion automatica para mantener la tasa de aprobacion.")
        else:
            st.success("Programa de tokenizacion operando dentro de parametros optimos.")


with tab2:
    st.markdown("**Simulador de impacto de tokenizacion**")
    st.caption("Estime el impacto en la tasa de aprobacion al cambiar la cobertura y configuracion de tokenizacion.")

    col_sliders, col_result = st.columns([3, 2])

    with col_sliders:
        sim_cobertura = st.slider("% de transacciones tokenizadas", 0, 100, 50, key="tk_sim_cob",
                                  help="Mayor cobertura de tokenizacion mejora la tasa de aprobacion global")
        sim_network = st.slider("% network tokens (vs merchant/device)", 0, 100, 40, key="tk_sim_net",
                                help="Network tokens tienen la mayor mejora de aprobacion")
        sim_lifecycle = st.slider("% con lifecycle management activo", 0, 100, 60, key="tk_sim_lc",
                                  help="Actualizacion automatica de credenciales previene rechazos")
        sim_multi_tsp = st.slider("TSPs integrados (1-5)", 1, 5, 2, key="tk_sim_tsp",
                                  help="Mas TSPs permiten redundancia y mayor cobertura de redes")
        sim_tasa_base = st.slider("Tasa de aprobacion base (sin token) %", 70, 95, 85, key="tk_sim_base",
                                  help="Tasa actual sin tokenizacion")
        sim_renovacion_auto = st.slider("% renovacion automatica", 0, 100, 50, key="tk_sim_renov",
                                        help="Renovacion automatica previene rechazos por token expirado")

    def calcular_mejora_token(cobertura, network, lifecycle, tsp, tasa_base, renovacion):
        mejora_cobertura = cobertura / 100 * 8.0
        mejora_network = network / 100 * 4.0
        mejora_lifecycle = lifecycle / 100 * 3.0
        mejora_tsp = min(tsp / 5, 1.0) * 2.0
        mejora_renov = renovacion / 100 * 2.0
        mejora_total = mejora_cobertura * 0.35 + mejora_network * 0.25 + mejora_lifecycle * 0.20 + mejora_tsp * 0.10 + mejora_renov * 0.10
        tasa_final = min(99, tasa_base + mejora_total)
        return round(mejora_total, 1), round(tasa_final, 1)

    mejora_pp, tasa_final = calcular_mejora_token(
        sim_cobertura, sim_network, sim_lifecycle, sim_multi_tsp, sim_tasa_base, sim_renovacion_auto
    )

    with col_result:
        if mejora_pp >= 5:
            nivel = "Alto impacto"
            color_nivel = "#36B37E"
            interpretacion = "Excelente configuracion de tokenizacion. Mejora significativa en aprobacion que se traduce en millones de revenue adicional."
        elif mejora_pp >= 3:
            nivel = "Impacto moderado"
            color_nivel = "#29B5E8"
            interpretacion = "Buen nivel de tokenizacion con espacio de optimizacion. Considerar aumentar network tokens y lifecycle management."
        elif mejora_pp >= 1:
            nivel = "Impacto bajo"
            color_nivel = "#FFAB00"
            interpretacion = "Tokenizacion con impacto limitado. Priorizar aumento de cobertura y migracion a network tokens."
        else:
            nivel = "Sin impacto"
            color_nivel = "#DE350B"
            interpretacion = "Tokenizacion sin beneficio medible. Revisar integracion con TSPs, configuracion de lifecycle y cobertura."

        m1, m2 = st.columns(2)
        m1.metric("Mejora estimada", f"+{mejora_pp:.1f} pp")
        m2.metric("Tasa final", f"{tasa_final:.1f}%")

        if mejora_pp >= 5:
            st.markdown(f"**Nivel:** :green[**{nivel}**]")
        elif mejora_pp >= 3:
            st.markdown(f"**Nivel:** :blue[**{nivel}**]")
        elif mejora_pp >= 1:
            st.markdown(f"**Nivel:** :orange[**{nivel}**]")
        else:
            st.markdown(f"**Nivel:** :red[**{nivel}**]")

        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=float(tasa_final),
            number=dict(suffix="%", font=dict(size=36)),
            gauge=dict(
                axis=dict(range=[70, 100], ticksuffix="%"),
                bar=dict(color=color_nivel),
                steps=[
                    dict(range=[70, 80], color="#FFEBEE"),
                    dict(range=[80, 85], color="#FFF3E0"),
                    dict(range=[85, 90], color="#FFF8E1"),
                    dict(range=[90, 95], color="#E3F2FD"),
                    dict(range=[95, 100], color="#E8F5E9"),
                ],
                threshold=dict(line=dict(color="#FF8B00", width=3), thickness=0.8, value=float(sim_tasa_base)),
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

    if "tk_escenarios" not in st.session_state:
        st.session_state.tk_escenarios = []

    if st.button("Guardar escenario actual", type="primary", key="tk_save"):
        if len(st.session_state.tk_escenarios) >= 3:
            st.session_state.tk_escenarios.pop(0)
        st.session_state.tk_escenarios.append({
            "% Cobertura": sim_cobertura,
            "% Network": sim_network,
            "% Lifecycle": sim_lifecycle,
            "TSPs": sim_multi_tsp,
            "Tasa base": f"{sim_tasa_base}%",
            "Mejora": f"+{mejora_pp:.1f}pp",
            "Tasa final": f"{tasa_final:.1f}%",
            "Nivel": nivel,
        })
        st.rerun()

    if st.session_state.tk_escenarios:
        esc_df = pd.DataFrame(st.session_state.tk_escenarios)
        esc_df.index = [f"Escenario {i+1}" for i in range(len(esc_df))]
        st.dataframe(esc_df.T, use_container_width=True)

        if st.button("Limpiar escenarios", key="tk_clear"):
            st.session_state.tk_escenarios = []
            st.rerun()
    else:
        st.caption("Aun no hay escenarios guardados.")
