import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from app_pages.map_helper import colombia_scatter_map
from decimal import Decimal

TABLE = "MGG_PAGOS.EXPERIENCIA_Y_AUTORIZACION.FRICCION_PAGOS"

COLORS = ["#29B5E8", "#11567F", "#71D4F0", "#0E3A53", "#A3E4F7", "#1B8BBF", "#5BC3E8", "#083248"]

COORDS = {
    "Bogota": (4.6097, -74.0817), "Medellin": (6.2442, -75.5812),
    "Cali": (3.4516, -76.5320), "Barranquilla": (10.9685, -74.7813),
    "Bucaramanga": (7.1254, -73.1198), "Cartagena": (10.3997, -75.5144),
    "Cucuta": (7.8939, -72.5078), "Ibague": (4.4389, -75.2322),
    "Manizales": (5.0689, -75.5174), "Pereira": (4.8133, -75.6961),
}


from app_pages.conn_helper import run_query



@st.cache_data(ttl=300, show_spinner=False)
def get_filters():
    puntos = run_query(f"SELECT DISTINCT PUNTO_FRICCION FROM {TABLE} ORDER BY 1")["PUNTO_FRICCION"].tolist()
    canales = run_query(f"SELECT DISTINCT CANAL FROM {TABLE} ORDER BY 1")["CANAL"].tolist()
    plataformas = run_query(f"SELECT DISTINCT PLATAFORMA FROM {TABLE} ORDER BY 1")["PLATAFORMA"].tolist()
    ciudades = run_query(f"SELECT DISTINCT CIUDAD FROM {TABLE} ORDER BY 1")["CIUDAD"].tolist()
    tipos = run_query(f"SELECT DISTINCT TIPO_TARJETA FROM {TABLE} ORDER BY 1")["TIPO_TARJETA"].tolist()
    fechas = run_query(f"SELECT MIN(FECHA_HORA)::DATE AS FMIN, MAX(FECHA_HORA)::DATE AS FMAX FROM {TABLE}")
    return puntos, canales, plataformas, ciudades, tipos, fechas


def build_where(fi, ff, punto_s, canal_s, plat_s, ciudad_s, tipo_s, all_p, all_c, all_pl, all_ci, all_t):
    clauses = [f"FECHA_HORA::DATE BETWEEN '{fi}' AND '{ff}'"]
    if punto_s and punto_s != "Todos" and punto_s in all_p:
        clauses.append(f"PUNTO_FRICCION = '{punto_s}'")
    if canal_s and canal_s != "Todos" and canal_s in all_c:
        clauses.append(f"CANAL = '{canal_s}'")
    if plat_s and plat_s != "Todos" and plat_s in all_pl:
        clauses.append(f"PLATAFORMA = '{plat_s}'")
    if ciudad_s and ciudad_s != "Todos" and ciudad_s in all_ci:
        clauses.append(f"CIUDAD = '{ciudad_s}'")
    if tipo_s and tipo_s != "Todos" and tipo_s in all_t:
        clauses.append(f"TIPO_TARJETA = '{tipo_s}'")
    return " AND ".join(clauses)


def color_tag(value, good, bad, fmt="{:.1f}%", inverse=False):
    v = float(value)
    if not inverse:
        return f":green[**{fmt.format(v)}**]" if v >= good else f":orange[**{fmt.format(v)}**]" if v >= bad else f":red[**{fmt.format(v)}**]"
    else:
        return f":green[**{fmt.format(v)}**]" if v <= good else f":orange[**{fmt.format(v)}**]" if v <= bad else f":red[**{fmt.format(v)}**]"


st.header(":material/speed: Friccion de pagos")
st.caption("Identifica puntos de friccion en el checkout, mide revenue perdido por abandono y permite simular el impacto de optimizaciones en la experiencia de pago.")

c1, c2, c3 = st.columns(3)
with c1:
    with st.container(border=True):
        st.markdown("**:material/lightbulb: Que resuelve**")
        st.markdown("Detectar donde y por que los pagadores abandonan el proceso de pago, cuantificando el impacto financiero de cada punto de friccion.")
with c2:
    with st.container(border=True):
        st.markdown("**:material/settings: Como funciona**")
        st.markdown("Registra cada intento de pago con tiempos de checkout, autenticacion y autorizacion. Clasifica fricciones y calcula revenue perdido por abandono en cada paso.")
with c3:
    with st.container(border=True):
        st.markdown("**:material/trending_up: Valor de negocio**")
        st.markdown("Cada punto de mejora en conversion de checkout representa millones en revenue recuperado. Permite priorizar inversiones en UX con datos concretos.")

puntos, canales, plataformas, ciudades, tipos, fechas_df = get_filters()
fmin = pd.to_datetime(fechas_df["FMIN"].iloc[0]).date()
fmax = pd.to_datetime(fechas_df["FMAX"].iloc[0]).date()

dx_w = f"FECHA_HORA::DATE BETWEEN '{fmin}' AND '{fmax}'"
dx_kpi = run_query(f"""
    SELECT ROUND(AVG(CASE WHEN PAGO_FINAL_EXITOSO THEN 1.0 ELSE 0.0 END)*100,1) AS TASA_EXITO,
           ROUND(AVG(CASE WHEN ABANDONO THEN 1.0 ELSE 0.0 END)*100,1) AS TASA_ABANDONO,
           SUM(REVENUE_PERDIDO_COP) AS REV_PERDIDO,
           ROUND(AVG(SCORE_EXPERIENCIA),1) AS SCORE_AVG,
           COUNT(*) AS TOTAL
    FROM {TABLE} WHERE {dx_w}
""")

if not dx_kpi.empty and dx_kpi["TOTAL"].iloc[0] > 0:
    d = dx_kpi.iloc[0]
    dx_exito = float(d["TASA_EXITO"])
    dx_abandono = float(d["TASA_ABANDONO"])
    dx_rev = float(d["REV_PERDIDO"]) / 1e9
    dx_score = float(d["SCORE_AVG"])

    dx_worst_punto = run_query(f"""
        SELECT PUNTO_FRICCION, SUM(REVENUE_PERDIDO_COP)/1e6 AS REV_MM
        FROM {TABLE} WHERE {dx_w} GROUP BY 1 ORDER BY 2 DESC LIMIT 1
    """)
    dx_worst_plat = run_query(f"""
        SELECT PLATAFORMA, ROUND(AVG(CASE WHEN ABANDONO THEN 1.0 ELSE 0.0 END)*100,1) AS ABAND
        FROM {TABLE} WHERE {dx_w} GROUP BY 1 ORDER BY 2 DESC LIMIT 1
    """)

    dx_lines = []
    if dx_exito >= 85:
        dx_lines.append(f"La tasa de exito de pago es {color_tag(dx_exito, 85, 75)}, :green[**dentro de parametros aceptables**].")
    elif dx_exito >= 75:
        dx_lines.append(f"La tasa de exito de pago es {color_tag(dx_exito, 85, 75)}, :orange[**por debajo del objetivo**]. Se requiere optimizacion.")
    else:
        dx_lines.append(f"La tasa de exito de pago es {color_tag(dx_exito, 85, 75)}, :red[**critica**]. Accion inmediata requerida.")
    dx_lines.append(f"- Revenue perdido por friccion: :red[**${dx_rev:.1f}B COP**]")
    dx_lines.append(f"- Score de experiencia promedio: **{dx_score:.1f}/100**")
    if not dx_worst_punto.empty:
        dx_lines.append(f"- Peor punto de friccion: :red[**{dx_worst_punto.iloc[0]['PUNTO_FRICCION']}**] (${float(dx_worst_punto.iloc[0]['REV_MM']):,.0f}M perdidos)")
    if not dx_worst_plat.empty:
        dx_lines.append(f"- Plataforma con mayor abandono: :orange[**{dx_worst_plat.iloc[0]['PLATAFORMA']}**] ({float(dx_worst_plat.iloc[0]['ABAND']):.1f}%)")

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
            if not dx_worst_punto.empty:
                recs.append(f"1. :red[**Eliminar friccion en '{dx_worst_punto.iloc[0]['PUNTO_FRICCION']}'**] — es el mayor generador de revenue perdido.")
            if dx_score < 70:
                recs.append("2. :orange[**Mejorar score de experiencia**]: optimizar tiempos de checkout y autenticacion.")
            recs.append("3. :blue[**Incrementar adopcion de tarjeta guardada**] y one-click para reducir friccion recurrente.")
            recs.append("4. :green[**Implementar 3DS frictionless**] donde sea posible para reducir abandono en autenticacion.")
            st.markdown("\n".join(recs))

st.divider()

tab1, tab2 = st.tabs(["Dashboard Ejecutivo", "Simulador Predictivo"])

with tab1:
    fc1, fc2, fc3, fc4, fc5, fc6 = st.columns(6)
    with fc1:
        fecha_rng = st.date_input("Periodo", value=(fmin, fmax), min_value=fmin, max_value=fmax, key="fri_fecha")
        if isinstance(fecha_rng, (list, tuple)) and len(fecha_rng) == 2:
            fecha_inicio, fecha_fin = fecha_rng
        else:
            fecha_inicio, fecha_fin = fmin, fmax
    with fc2:
        punto_sel = st.selectbox("Punto friccion", ["Todos"] + puntos, key="fri_punto")
    with fc3:
        canal_sel = st.selectbox("Canal", ["Todos"] + canales, key="fri_canal")
    with fc4:
        plat_sel = st.selectbox("Plataforma", ["Todos"] + plataformas, key="fri_plat")
    with fc5:
        ciudad_sel = st.selectbox("Ciudad", ["Todos"] + ciudades, key="fri_ciudad")
    with fc6:
        tipo_sel = st.selectbox("Tipo tarjeta", ["Todos"] + tipos, key="fri_tipo")

    WHERE = build_where(fecha_inicio, fecha_fin, punto_sel, canal_sel, plat_sel, ciudad_sel, tipo_sel,
                        puntos, canales, plataformas, ciudades, tipos)

    kpi_df = run_query(f"""
        SELECT COUNT(*) AS TOTAL_EVENTOS,
               ROUND(AVG(CASE WHEN PAGO_FINAL_EXITOSO THEN 1.0 ELSE 0.0 END)*100,1) AS TASA_EXITO,
               ROUND(AVG(CASE WHEN ABANDONO THEN 1.0 ELSE 0.0 END)*100,1) AS TASA_ABANDONO,
               SUM(REVENUE_PERDIDO_COP) AS REV_PERDIDO,
               ROUND(AVG(SCORE_EXPERIENCIA),1) AS SCORE_AVG,
               ROUND(AVG(TIEMPO_TOTAL_CHECKOUT_MS)/1000,1) AS CHECKOUT_SEG,
               ROUND(AVG(TIEMPO_AUTENTICACION_MS)/1000,1) AS AUTH_SEG,
               ROUND(AVG(INTENTOS_PAGO),1) AS INTENTOS_AVG
        FROM {TABLE} WHERE {WHERE}
    """)

    if kpi_df.empty or kpi_df["TOTAL_EVENTOS"].iloc[0] == 0:
        st.info("No hay datos para los filtros seleccionados.")
    else:
        r = kpi_df.iloc[0]

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Tasa de exito", f"{float(r['TASA_EXITO']):.1f}%")
        k2.metric("Tasa de abandono", f"{float(r['TASA_ABANDONO']):.1f}%")
        k3.metric("Revenue perdido", f"${float(r['REV_PERDIDO'])/1e9:.1f}B COP")
        k4.metric("Score experiencia", f"{float(r['SCORE_AVG']):.1f}/100")

        k5, k6, k7, k8 = st.columns(4)
        k5.metric("Tiempo checkout", f"{float(r['CHECKOUT_SEG']):.1f} seg")
        k6.metric("Tiempo autenticacion", f"{float(r['AUTH_SEG']):.1f} seg")
        k7.metric("Intentos promedio", f"{float(r['INTENTOS_AVG']):.1f}")
        k8.metric("Eventos analizados", f"{int(r['TOTAL_EVENTOS']):,}")

        st.divider()

        col_a, col_b = st.columns(2)

        with col_a:
            st.markdown("**Funnel de Checkout — Pasos y Abandono**")
            funnel_df = run_query(f"""
                SELECT 'Total intentos' AS PASO, COUNT(*) AS N FROM {TABLE} WHERE {WHERE}
                UNION ALL
                SELECT 'Autenticacion completada', COUNT(*) FROM {TABLE} WHERE {WHERE} AND TIEMPO_AUTENTICACION_MS > 0
                UNION ALL
                SELECT 'Autorizacion enviada', COUNT(*) FROM {TABLE} WHERE {WHERE} AND TIEMPO_RESPUESTA_AUTORIZACION_MS > 0
                UNION ALL
                SELECT 'Pago exitoso', SUM(CASE WHEN PAGO_FINAL_EXITOSO THEN 1 ELSE 0 END) FROM {TABLE} WHERE {WHERE}
            """)
            if not funnel_df.empty:
                fig_funnel = go.Figure(go.Funnel(
                    y=funnel_df["PASO"].tolist(),
                    x=[int(v) for v in funnel_df["N"]],
                    textinfo="value+percent initial",
                    texttemplate="%{value:,} (%{percentInitial:.1%})",
                    marker=dict(color=[COLORS[0], COLORS[5], COLORS[1], "#36B37E"],
                                line=dict(width=1, color="white")),
                    connector=dict(line=dict(color="#DFE1E6", width=1)),
                ))
                fig_funnel.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=160, r=20, t=30, b=40),
                )
                st.plotly_chart(fig_funnel, use_container_width=True, key="fri_funnel")

        with col_b:
            st.markdown("**Revenue Perdido por Punto de Friccion — Waterfall**")
            wf_df = run_query(f"""
                SELECT PUNTO_FRICCION, SUM(REVENUE_PERDIDO_COP)/1e6 AS REV_MM
                FROM {TABLE} WHERE {WHERE}
                GROUP BY 1 ORDER BY 2 DESC LIMIT 8
            """)
            if not wf_df.empty:
                labels = wf_df["PUNTO_FRICCION"].tolist() + ["Total"]
                vals = [float(v) for v in wf_df["REV_MM"]]
                fig_wf = go.Figure(go.Waterfall(
                    x=labels,
                    y=vals + [sum(vals)],
                    measure=["relative"] * len(vals) + ["total"],
                    text=[f"${v:,.0f}M" for v in vals] + [f"${sum(vals):,.0f}M"],
                    textposition="outside",
                    connector=dict(line=dict(color="#DFE1E6")),
                    increasing=dict(marker=dict(color="#DE350B")),
                    totals=dict(marker=dict(color=COLORS[1])),
                ))
                fig_wf.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=50, r=20, t=30, b=100),
                    xaxis=dict(tickangle=-45, tickfont=dict(size=9)),
                    yaxis=dict(title="Revenue Perdido (M COP)"),
                )
                st.plotly_chart(fig_wf, use_container_width=True, key="fri_waterfall")

        col_c, col_d = st.columns(2)

        with col_c:
            st.markdown("**Treemap — Clasificacion de Friccion**")
            tree_df = run_query(f"""
                SELECT PUNTO_FRICCION, CLASIFICACION_FRICCION, COUNT(*) AS CNT
                FROM {TABLE} WHERE {WHERE}
                GROUP BY 1,2 ORDER BY 3 DESC
            """)
            if not tree_df.empty:
                labels = []
                parents = []
                values = []
                punto_totals = tree_df.groupby("PUNTO_FRICCION")["CNT"].sum().to_dict()
                for punto in punto_totals:
                    labels.append(str(punto))
                    parents.append("")
                    values.append(int(punto_totals[punto]))
                for _, row in tree_df.iterrows():
                    labels.append(str(row["CLASIFICACION_FRICCION"]))
                    parents.append(str(row["PUNTO_FRICCION"]))
                    values.append(int(row["CNT"]))
                fig_tree = go.Figure(go.Treemap(
                    labels=labels, parents=parents, values=values,
                    textinfo="label+value+percent parent",
                    marker=dict(colorscale=[[0, "#A3E4F7"], [0.5, "#29B5E8"], [1, "#11567F"]],
                                line=dict(width=2, color="white")),
                    hovertemplate="<b>%{label}</b><br>Eventos: %{value:,}<br>%{percentParent:.1%}<extra></extra>",
                ))
                fig_tree.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=5, r=5, t=30, b=5),
                )
                st.plotly_chart(fig_tree, use_container_width=True, key="fri_treemap")

        with col_d:
            st.markdown("**Distribucion Score Experiencia por Resultado**")
            score_df = run_query(f"""
                SELECT FLOOR(SCORE_EXPERIENCIA/10)*10 AS BIN,
                       SUM(CASE WHEN PAGO_FINAL_EXITOSO THEN 1 ELSE 0 END) AS EXITOSO,
                       SUM(CASE WHEN NOT PAGO_FINAL_EXITOSO THEN 1 ELSE 0 END) AS FALLIDO
                FROM {TABLE} WHERE {WHERE}
                GROUP BY 1 ORDER BY 1
            """)
            if not score_df.empty:
                bins = [f"{int(v)}-{int(v)+9}" for v in score_df["BIN"]]
                fig_score = go.Figure()
                fig_score.add_trace(go.Bar(
                    x=bins, y=[int(v) for v in score_df["EXITOSO"]],
                    name="Pago exitoso", marker=dict(color="#36B37E"),
                ))
                fig_score.add_trace(go.Bar(
                    x=bins, y=[int(v) for v in score_df["FALLIDO"]],
                    name="Pago fallido", marker=dict(color="#DE350B"),
                ))
                fig_score.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=50, r=20, t=30, b=50), barmode="stack",
                    xaxis=dict(title="Score Experiencia"), yaxis=dict(title="Eventos"),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                )
                st.plotly_chart(fig_score, use_container_width=True, key="fri_score_dist")

        st.divider()

        st.markdown("**Revenue perdido por ciudad**")
        map_df = run_query(f"""
            SELECT CIUDAD, SUM(REVENUE_PERDIDO_COP) AS REV_PERDIDO,
                   ROUND(AVG(CASE WHEN ABANDONO THEN 1.0 ELSE 0.0 END)*100,1) AS TASA_ABAND,
                   COUNT(*) AS EVENTOS
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1 ORDER BY 2 DESC
        """)
        if not map_df.empty:
            map_df["LAT"] = map_df["CIUDAD"].map(lambda c: COORDS.get(str(c), (4.5, -74.0))[0]).astype(float)
            map_df["LON"] = map_df["CIUDAD"].map(lambda c: COORDS.get(str(c), (4.5, -74.0))[1]).astype(float)
            map_df["REV_M"] = map_df["REV_PERDIDO"].astype(float) / 1e6
            map_df["hover"] = map_df.apply(lambda r: f"{r['CIUDAD']}<br>Rev. perdido: ${r['REV_PERDIDO']/1e6:,.0f}M COP<br>Abandono: {float(r['TASA_ABAND']):.1f}%<br>Eventos: {int(r['EVENTOS']):,}", axis=1)
            deck = colombia_scatter_map(
                map_df, lat_col="LAT", lon_col="LON", size_col="REV_M", color_col="REV_M",
                text_col="CIUDAD", hover_col="hover",
                colorscale=["#FFAB00", "#FF8B00", "#DE350B"],
                colorbar_title="Rev. Perdido (M COP)",
            )
            st.pydeck_chart(deck, key="fri_map")

        st.divider()

        col_e, col_f = st.columns(2)

        with col_e:
            st.markdown("**Radar — Metricas por Plataforma**")
            plat_df = run_query(f"""
                SELECT PLATAFORMA,
                       ROUND(AVG(CASE WHEN ABANDONO THEN 1.0 ELSE 0.0 END)*100,1) AS ABANDONO,
                       ROUND(AVG(SCORE_EXPERIENCIA),1) AS SCORE,
                       ROUND(AVG(TIEMPO_TOTAL_CHECKOUT_MS)/1000,1) AS CHECKOUT_S,
                       ROUND(AVG(CASE WHEN PAGO_FINAL_EXITOSO THEN 1.0 ELSE 0.0 END)*100,1) AS EXITO
                FROM {TABLE} WHERE {WHERE}
                GROUP BY 1
            """)
            if not plat_df.empty and len(plat_df) >= 2:
                cats = ["Tasa exito", "Score exp.", "Checkout (inv)", "Abandono (inv)"]
                fig_radar = go.Figure()
                for i, (_, row) in enumerate(plat_df.iterrows()):
                    checkout_inv = max(0, 100 - float(row["CHECKOUT_S"]) * 5)
                    abandono_inv = max(0, 100 - float(row["ABANDONO"]))
                    vals = [float(row["EXITO"]), float(row["SCORE"]), checkout_inv, abandono_inv]
                    fig_radar.add_trace(go.Scatterpolar(
                        r=vals + [vals[0]], theta=cats + [cats[0]],
                        name=str(row["PLATAFORMA"]),
                        fill="toself", fillcolor=f"rgba({41 + i*40},{181 - i*30},{232 - i*20},0.15)",
                        line=dict(color=COLORS[i % len(COLORS)], width=2),
                    ))
                fig_radar.update_layout(
                    polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=60, r=60, t=40, b=40),
                    legend=dict(orientation="h", yanchor="bottom", y=-0.15, xanchor="center", x=0.5),
                )
                st.plotly_chart(fig_radar, use_container_width=True, key="fri_radar")

        with col_f:
            st.markdown("**Impacto de Tarjeta Guardada y One-Click**")
            card_df = run_query(f"""
                SELECT
                    ROUND(AVG(CASE WHEN TARJETA_GUARDADA AND PAGO_FINAL_EXITOSO THEN 1.0 WHEN TARJETA_GUARDADA THEN 0.0 END)*100,1) AS EXITO_GUARDADA,
                    ROUND(AVG(CASE WHEN NOT TARJETA_GUARDADA AND PAGO_FINAL_EXITOSO THEN 1.0 WHEN NOT TARJETA_GUARDADA THEN 0.0 END)*100,1) AS EXITO_NO_GUARDADA,
                    ROUND(AVG(CASE WHEN ONE_CLICK_DISPONIBLE AND PAGO_FINAL_EXITOSO THEN 1.0 WHEN ONE_CLICK_DISPONIBLE THEN 0.0 END)*100,1) AS EXITO_ONECLICK,
                    ROUND(AVG(CASE WHEN NOT ONE_CLICK_DISPONIBLE AND PAGO_FINAL_EXITOSO THEN 1.0 WHEN NOT ONE_CLICK_DISPONIBLE THEN 0.0 END)*100,1) AS EXITO_NO_ONECLICK
                FROM {TABLE} WHERE {WHERE}
            """)
            if not card_df.empty:
                cr = card_df.iloc[0]
                cats = ["Tarjeta guardada", "Sin guardar", "One-click", "Sin one-click"]
                vals = [float(cr["EXITO_GUARDADA"]), float(cr["EXITO_NO_GUARDADA"]),
                        float(cr["EXITO_ONECLICK"]), float(cr["EXITO_NO_ONECLICK"])]
                bar_colors = ["#36B37E", "#DE350B", "#36B37E", "#DE350B"]
                fig_card = go.Figure(go.Bar(
                    x=cats, y=vals,
                    marker=dict(color=bar_colors, line=dict(width=1, color="white")),
                    text=[f"{v:.1f}%" for v in vals], textposition="outside",
                    hovertemplate="%{x}: %{y:.1f}%<extra></extra>",
                ))
                fig_card.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=50, r=20, t=30, b=50),
                    yaxis=dict(title="Tasa de exito (%)"),
                )
                st.plotly_chart(fig_card, use_container_width=True, key="fri_saved_card")

with tab2:
    st.markdown("**Simulador de reduccion de friccion**")
    st.caption("Estime el impacto financiero de optimizaciones en la experiencia de checkout. Ajuste los parametros para proyectar revenue recuperado.")

    col_sl, col_res = st.columns([3, 2])

    with col_sl:
        red_friccion = st.slider("Reduccion de friccion general (%)", 0, 50, 15, key="fri_sim_fric",
                                 help="Reduccion porcentual en eventos de friccion")
        red_checkout = st.slider("Reduccion tiempo checkout (%)", 0, 60, 20, key="fri_sim_checkout",
                                 help="Reduccion porcentual en tiempo de checkout")
        opt_3ds = st.slider("Optimizacion 3DS frictionless (%)", 0, 80, 30, key="fri_sim_3ds",
                            help="Porcentaje de autenticaciones que migran a frictionless")
        adopt_tarjeta = st.slider("Adopcion tarjeta guardada (%)", 0, 90, 40, key="fri_sim_tarjeta",
                                  help="Incremento en uso de tarjeta guardada")
        red_intentos = st.slider("Reduccion reintentos (%)", 0, 50, 10, key="fri_sim_intentos",
                                 help="Reduccion de intentos de pago repetidos")
        mejora_score = st.slider("Mejora objetivo en score experiencia (pts)", 0, 30, 10, key="fri_sim_score",
                                 help="Puntos de mejora en score de experiencia")

    base_rev_perdido = float(kpi_df.iloc[0]["REV_PERDIDO"]) / 1e6 if not kpi_df.empty else 0
    base_score = float(kpi_df.iloc[0]["SCORE_AVG"]) if not kpi_df.empty else 50

    recovery_pct = (
        0.30 * (red_friccion / 50) +
        0.15 * (red_checkout / 60) +
        0.20 * (opt_3ds / 80) +
        0.20 * (adopt_tarjeta / 90) +
        0.10 * (red_intentos / 50) +
        0.05 * (mejora_score / 30)
    )
    rev_recuperado = base_rev_perdido * recovery_pct
    new_score = min(100, base_score + mejora_score * (1 + recovery_pct * 0.5))

    with col_res:
        st.metric("Revenue recuperado estimado", f"${rev_recuperado:,.0f}M COP")
        st.metric("Score experiencia proyectado", f"{new_score:.1f}/100")

        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=float(recovery_pct * 100),
            number=dict(suffix="%", font=dict(size=36)),
            title=dict(text="Recuperacion de revenue"),
            gauge=dict(
                axis=dict(range=[0, 100], ticksuffix="%"),
                bar=dict(color="#36B37E"),
                steps=[
                    dict(range=[0, 25], color="#FFCDD2"),
                    dict(range=[25, 50], color="#FFF3E0"),
                    dict(range=[50, 75], color="#E3F2FD"),
                    dict(range=[75, 100], color="#E8F5E9"),
                ],
            ),
        ))
        fig_gauge.update_layout(
            template="plotly_white", paper_bgcolor="#FFFFFF",
            height=250, margin=dict(l=30, r=30, t=50, b=10),
        )
        st.plotly_chart(fig_gauge, use_container_width=True, key="fri_sim_gauge")

        if recovery_pct >= 0.6:
            st.success(f"Escenario optimista: recuperacion de **${rev_recuperado:,.0f}M COP** ({recovery_pct*100:.0f}% del revenue perdido).")
        elif recovery_pct >= 0.3:
            st.info(f"Escenario moderado: recuperacion de **${rev_recuperado:,.0f}M COP** ({recovery_pct*100:.0f}%). Hay espacio de mejora adicional.")
        else:
            st.warning(f"Escenario conservador: recuperacion de **${rev_recuperado:,.0f}M COP** ({recovery_pct*100:.0f}%). Considere incrementar los parametros.")

    st.divider()

    st.markdown("**Comparador de escenarios**")
    st.caption("Guarde hasta 3 configuraciones para comparar lado a lado.")

    if "fri_escenarios" not in st.session_state:
        st.session_state.fri_escenarios = []

    if st.button("Guardar escenario actual", type="primary", key="fri_save_esc"):
        if len(st.session_state.fri_escenarios) >= 3:
            st.session_state.fri_escenarios.pop(0)
        st.session_state.fri_escenarios.append({
            "Red. friccion": f"{red_friccion}%",
            "Red. checkout": f"{red_checkout}%",
            "3DS frictionless": f"{opt_3ds}%",
            "Tarjeta guardada": f"{adopt_tarjeta}%",
            "Red. reintentos": f"{red_intentos}%",
            "Mejora score": f"+{mejora_score} pts",
            "Rev. recuperado": f"${rev_recuperado:,.0f}M",
            "Score proyectado": f"{new_score:.1f}",
        })
        st.rerun()

    if st.session_state.fri_escenarios:
        esc_df = pd.DataFrame(st.session_state.fri_escenarios)
        esc_df.index = [f"Escenario {i+1}" for i in range(len(esc_df))]
        st.dataframe(esc_df.T, use_container_width=True)
        if st.button("Limpiar escenarios", key="fri_clear_esc"):
            st.session_state.fri_escenarios = []
            st.rerun()
    else:
        st.caption("Aun no hay escenarios guardados.")
