import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from decimal import Decimal

TABLE = "MGG_PAGOS.EXPERIENCIA_Y_AUTORIZACION.AUTENTICACION_3DS"

COLORS = ["#29B5E8", "#11567F", "#71D4F0", "#0E3A53", "#A3E4F7", "#1B8BBF", "#5BC3E8", "#083248"]


from app_pages.conn_helper import run_query



@st.cache_data(ttl=300, show_spinner=False)
def get_filters():
    versiones = run_query(f"SELECT DISTINCT VERSION_3DS FROM {TABLE} ORDER BY 1")["VERSION_3DS"].tolist()
    redes = run_query(f"SELECT DISTINCT RED FROM {TABLE} ORDER BY 1")["RED"].tolist()
    bancos = run_query(f"SELECT DISTINCT BANCO_EMISOR FROM {TABLE} ORDER BY 1")["BANCO_EMISOR"].tolist()
    resultados = run_query(f"SELECT DISTINCT RESULTADO_AUTENTICACION FROM {TABLE} ORDER BY 1")["RESULTADO_AUTENTICACION"].tolist()
    plataformas = run_query(f"SELECT DISTINCT PLATAFORMA FROM {TABLE} ORDER BY 1")["PLATAFORMA"].tolist()
    fechas = run_query(f"SELECT MIN(FECHA_HORA)::DATE AS FMIN, MAX(FECHA_HORA)::DATE AS FMAX FROM {TABLE}")
    return versiones, redes, bancos, resultados, plataformas, fechas


def build_where(fi, ff, ver_s, red_s, banco_s, res_s, plat_s, all_v, all_r, all_b, all_res, all_p):
    clauses = [f"FECHA_HORA::DATE BETWEEN '{fi}' AND '{ff}'"]
    if ver_s and ver_s != "Todos" and ver_s in all_v:
        clauses.append(f"VERSION_3DS = '{ver_s}'")
    if red_s and red_s != "Todos" and red_s in all_r:
        clauses.append(f"RED = '{red_s}'")
    if banco_s and banco_s != "Todos" and banco_s in all_b:
        clauses.append(f"BANCO_EMISOR = '{banco_s}'")
    if res_s and res_s != "Todos" and res_s in all_res:
        clauses.append(f"RESULTADO_AUTENTICACION = '{res_s}'")
    if plat_s and plat_s != "Todos" and plat_s in all_p:
        clauses.append(f"PLATAFORMA = '{plat_s}'")
    return " AND ".join(clauses)


def color_tag(value, good, bad, fmt="{:.1f}%", inverse=False):
    v = float(value)
    if not inverse:
        return f":green[**{fmt.format(v)}**]" if v >= good else f":orange[**{fmt.format(v)}**]" if v >= bad else f":red[**{fmt.format(v)}**]"
    else:
        return f":green[**{fmt.format(v)}**]" if v <= good else f":orange[**{fmt.format(v)}**]" if v <= bad else f":red[**{fmt.format(v)}**]"


st.header(":material/security: Autenticacion 3DS")
st.caption("Analisis de autenticacion 3D Secure: tasas de frictionless, abandono en desafio, performance por version, emisor y ACS provider.")

c1, c2, c3 = st.columns(3)
with c1:
    with st.container(border=True):
        st.markdown("**:material/lightbulb: Que resuelve**")
        st.markdown("Visibilidad end-to-end del flujo de autenticacion 3DS para optimizar tasas de frictionless y reducir abandono en desafio de autenticacion.")
with c2:
    with st.container(border=True):
        st.markdown("**:material/settings: Como funciona**")
        st.markdown("Registra cada autenticacion con resultado, version, tiempos, score ACS, tipo de exemption y autorizacion posterior. Compara ACS providers y versiones.")
with c3:
    with st.container(border=True):
        st.markdown("**:material/trending_up: Valor de negocio**")
        st.markdown("Cada punto de mejora en tasa frictionless reduce abandono y mejora conversion. Permite negociar con ACS providers y optimizar reglas de exemption.")

versiones, redes, bancos, resultados, plataformas, fechas_df = get_filters()
fmin = pd.to_datetime(fechas_df["FMIN"].iloc[0]).date()
fmax = pd.to_datetime(fechas_df["FMAX"].iloc[0]).date()

dx_w = f"FECHA_HORA::DATE BETWEEN '{fmin}' AND '{fmax}'"
dx_kpi = run_query(f"""
    SELECT ROUND(AVG(CASE WHEN FUE_FRICTIONLESS THEN 1.0 ELSE 0.0 END)*100,1) AS PCT_FRICTIONLESS,
           ROUND(AVG(CASE WHEN ABANDONO_EN_DESAFIO THEN 1.0 ELSE 0.0 END)*100,1) AS PCT_ABANDONO,
           ROUND(AVG(CASE WHEN AUTORIZACION_POSTERIOR_EXITOSA THEN 1.0 ELSE 0.0 END)*100,1) AS PCT_AUTH_EXITO,
           ROUND(AVG(TIEMPO_AUTENTICACION_MS)/1000,1) AS TIEMPO_AVG_S,
           COUNT(*) AS TOTAL
    FROM {TABLE} WHERE {dx_w}
""")

if not dx_kpi.empty and dx_kpi["TOTAL"].iloc[0] > 0:
    d = dx_kpi.iloc[0]
    dx_frictionless = float(d["PCT_FRICTIONLESS"])
    dx_abandono = float(d["PCT_ABANDONO"])
    dx_auth = float(d["PCT_AUTH_EXITO"])

    dx_worst_banco = run_query(f"""
        SELECT BANCO_EMISOR, ROUND(AVG(CASE WHEN ABANDONO_EN_DESAFIO THEN 1.0 ELSE 0.0 END)*100,1) AS ABAND
        FROM {TABLE} WHERE {dx_w} GROUP BY 1 ORDER BY 2 DESC LIMIT 1
    """)
    dx_best_acs = run_query(f"""
        SELECT ACS_PROVIDER, ROUND(AVG(CASE WHEN AUTORIZACION_POSTERIOR_EXITOSA THEN 1.0 ELSE 0.0 END)*100,1) AS EXITO
        FROM {TABLE} WHERE {dx_w} GROUP BY 1 ORDER BY 2 DESC LIMIT 1
    """)

    dx_lines = []
    if dx_frictionless >= 60:
        dx_lines.append(f"La tasa de frictionless es {color_tag(dx_frictionless, 60, 40)}, :green[**en buen nivel**] para el mercado colombiano.")
    elif dx_frictionless >= 40:
        dx_lines.append(f"La tasa de frictionless es {color_tag(dx_frictionless, 60, 40)}, :orange[**con oportunidad de mejora**].")
    else:
        dx_lines.append(f"La tasa de frictionless es {color_tag(dx_frictionless, 60, 40)}, :red[**muy baja**]. Genera abandono significativo.")
    dx_lines.append(f"- Tasa de abandono en desafio: {color_tag(dx_abandono, 10, 20, inverse=True)}")
    dx_lines.append(f"- Autorizacion posterior exitosa: {color_tag(dx_auth, 85, 75)}")
    if not dx_worst_banco.empty:
        dx_lines.append(f"- Banco con mayor abandono: :red[**{dx_worst_banco.iloc[0]['BANCO_EMISOR']}**] ({float(dx_worst_banco.iloc[0]['ABAND']):.1f}%)")
    if not dx_best_acs.empty:
        dx_lines.append(f"- Mejor ACS provider: :green[**{dx_best_acs.iloc[0]['ACS_PROVIDER']}**] ({float(dx_best_acs.iloc[0]['EXITO']):.1f}% exito)")

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
            if dx_frictionless < 60:
                recs.append("1. :red[**Incrementar tasa de frictionless**] optimizando reglas de exemption y score de riesgo ACS.")
            if dx_abandono > 15:
                recs.append("2. :orange[**Reducir abandono en desafio**] mejorando UX de la pantalla de challenge.")
            recs.append("3. :blue[**Migrar a version 3DS 2.2**] para mejores tasas de frictionless y exemptions.")
            recs.append("4. :green[**Benchmark de ACS providers**] para renegociar con los de menor performance.")
            st.markdown("\n".join(recs))

st.divider()

fc1, fc2, fc3, fc4, fc5, fc6 = st.columns(6)
with fc1:
    fecha_rng = st.date_input("Periodo", value=(fmin, fmax), min_value=fmin, max_value=fmax, key="tds_fecha")
    if isinstance(fecha_rng, (list, tuple)) and len(fecha_rng) == 2:
        fecha_inicio, fecha_fin = fecha_rng
    else:
        fecha_inicio, fecha_fin = fmin, fmax
with fc2:
    ver_sel = st.selectbox("Version 3DS", ["Todos"] + versiones, key="tds_version")
with fc3:
    red_sel = st.selectbox("Red", ["Todos"] + redes, key="tds_red")
with fc4:
    banco_sel = st.selectbox("Banco emisor", ["Todos"] + bancos, key="tds_banco")
with fc5:
    res_sel = st.selectbox("Resultado", ["Todos"] + resultados, key="tds_resultado")
with fc6:
    plat_sel = st.selectbox("Plataforma", ["Todos"] + plataformas, key="tds_plat")

WHERE = build_where(fecha_inicio, fecha_fin, ver_sel, red_sel, banco_sel, res_sel, plat_sel,
                    versiones, redes, bancos, resultados, plataformas)

kpi_df = run_query(f"""
    SELECT COUNT(*) AS TOTAL,
           ROUND(AVG(CASE WHEN FUE_FRICTIONLESS THEN 1.0 ELSE 0.0 END)*100,1) AS PCT_FRICTIONLESS,
           ROUND(AVG(CASE WHEN DESAFIO_PRESENTADO THEN 1.0 ELSE 0.0 END)*100,1) AS PCT_DESAFIO,
           ROUND(AVG(CASE WHEN DESAFIO_COMPLETADO THEN 1.0 ELSE 0.0 END)*100,1) AS PCT_DESAFIO_OK,
           ROUND(AVG(CASE WHEN ABANDONO_EN_DESAFIO THEN 1.0 ELSE 0.0 END)*100,1) AS PCT_ABANDONO,
           ROUND(AVG(CASE WHEN AUTORIZACION_POSTERIOR_EXITOSA THEN 1.0 ELSE 0.0 END)*100,1) AS PCT_AUTH_OK,
           ROUND(AVG(TIEMPO_AUTENTICACION_MS)/1000,2) AS TIEMPO_AUTH_S,
           ROUND(AVG(SCORE_RIESGO_ACS),1) AS SCORE_ACS_AVG
    FROM {TABLE} WHERE {WHERE}
""")

if kpi_df.empty or kpi_df["TOTAL"].iloc[0] == 0:
    st.info("No hay datos para los filtros seleccionados.")
else:
    r = kpi_df.iloc[0]

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("% Frictionless", f"{float(r['PCT_FRICTIONLESS']):.1f}%")
    k2.metric("% Desafio presentado", f"{float(r['PCT_DESAFIO']):.1f}%")
    k3.metric("% Abandono en desafio", f"{float(r['PCT_ABANDONO']):.1f}%")
    k4.metric("Autorizacion exitosa", f"{float(r['PCT_AUTH_OK']):.1f}%")

    k5, k6, k7, k8 = st.columns(4)
    k5.metric("Desafio completado", f"{float(r['PCT_DESAFIO_OK']):.1f}%")
    k6.metric("Tiempo auth promedio", f"{float(r['TIEMPO_AUTH_S']):.2f} seg")
    k7.metric("Score ACS promedio", f"{float(r['SCORE_ACS_AVG']):.1f}")
    k8.metric("Total autenticaciones", f"{int(r['TOTAL']):,}")

    st.divider()

    st.markdown("**Flujo de Autenticacion 3DS — Sankey**")
    sankey_df = run_query(f"""
        SELECT
            COUNT(*) AS TOTAL,
            SUM(CASE WHEN FUE_FRICTIONLESS THEN 1 ELSE 0 END) AS FRICTIONLESS,
            SUM(CASE WHEN DESAFIO_PRESENTADO THEN 1 ELSE 0 END) AS DESAFIO,
            SUM(CASE WHEN DESAFIO_COMPLETADO THEN 1 ELSE 0 END) AS DESAFIO_OK,
            SUM(CASE WHEN ABANDONO_EN_DESAFIO THEN 1 ELSE 0 END) AS DESAFIO_ABANDON,
            SUM(CASE WHEN FUE_FRICTIONLESS AND AUTORIZACION_POSTERIOR_EXITOSA THEN 1 ELSE 0 END) AS FRIC_AUTH_OK,
            SUM(CASE WHEN FUE_FRICTIONLESS AND NOT AUTORIZACION_POSTERIOR_EXITOSA THEN 1 ELSE 0 END) AS FRIC_AUTH_FAIL,
            SUM(CASE WHEN DESAFIO_COMPLETADO AND AUTORIZACION_POSTERIOR_EXITOSA THEN 1 ELSE 0 END) AS DES_AUTH_OK,
            SUM(CASE WHEN DESAFIO_COMPLETADO AND NOT AUTORIZACION_POSTERIOR_EXITOSA THEN 1 ELSE 0 END) AS DES_AUTH_FAIL
        FROM {TABLE} WHERE {WHERE}
    """)
    if not sankey_df.empty:
        s = sankey_df.iloc[0]
        labels = ["Total", "Frictionless", "Desafio", "Completado", "Abandonado", "Autorizado (F)", "Declinado (F)", "Autorizado (D)", "Declinado (D)"]
        source = [0, 0, 2, 2, 1, 1, 3, 3]
        target = [1, 2, 3, 4, 5, 6, 7, 8]
        value = [int(s["FRICTIONLESS"]), int(s["DESAFIO"]), int(s["DESAFIO_OK"]), int(s["DESAFIO_ABANDON"]),
                 int(s["FRIC_AUTH_OK"]), int(s["FRIC_AUTH_FAIL"]), int(s["DES_AUTH_OK"]), int(s["DES_AUTH_FAIL"])]
        colors_link = ["#A3E4F7", "#FFF3E0", "#E8F5E9", "#FFCDD2", "#E8F5E9", "#FFCDD2", "#E8F5E9", "#FFCDD2"]
        colors_node = [COLORS[1], COLORS[0], "#FFAB00", "#36B37E", "#DE350B", "#36B37E", "#DE350B", "#36B37E", "#DE350B"]
        fig_sankey = go.Figure(go.Sankey(
            arrangement="snap",
            node=dict(pad=15, thickness=20, line=dict(color="white", width=1),
                      label=labels, color=colors_node),
            link=dict(source=source, target=target, value=value, color=colors_link),
        ))
        fig_sankey.update_layout(
            template="plotly_white", paper_bgcolor="#FFFFFF", height=450,
            margin=dict(l=20, r=20, t=30, b=20),
        )
        st.plotly_chart(fig_sankey, use_container_width=True, key="tds_sankey")

    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("**Comparacion por Version 3DS**")
        ver_df = run_query(f"""
            SELECT VERSION_3DS,
                   ROUND(AVG(CASE WHEN AUTORIZACION_POSTERIOR_EXITOSA THEN 1.0 ELSE 0.0 END)*100,1) AS EXITO,
                   ROUND(AVG(CASE WHEN FUE_FRICTIONLESS THEN 1.0 ELSE 0.0 END)*100,1) AS FRICTIONLESS,
                   ROUND(AVG(CASE WHEN ABANDONO_EN_DESAFIO THEN 1.0 ELSE 0.0 END)*100,1) AS ABANDONO
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1 ORDER BY 1
        """)
        if not ver_df.empty:
            versions = ver_df["VERSION_3DS"].tolist()
            fig_ver = go.Figure()
            fig_ver.add_trace(go.Bar(x=versions, y=[float(v) for v in ver_df["EXITO"]],
                                     name="Tasa exito", marker=dict(color="#36B37E")))
            fig_ver.add_trace(go.Bar(x=versions, y=[float(v) for v in ver_df["FRICTIONLESS"]],
                                     name="% Frictionless", marker=dict(color=COLORS[0])))
            fig_ver.add_trace(go.Bar(x=versions, y=[float(v) for v in ver_df["ABANDONO"]],
                                     name="% Abandono", marker=dict(color="#DE350B")))
            fig_ver.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=50, r=20, t=30, b=50), barmode="group",
                yaxis=dict(title="Porcentaje (%)"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig_ver, use_container_width=True, key="tds_version_comp")

    with col_b:
        st.markdown("**Benchmark ACS Providers**")
        acs_df = run_query(f"""
            SELECT ACS_PROVIDER,
                   ROUND(AVG(CASE WHEN AUTORIZACION_POSTERIOR_EXITOSA THEN 1.0 ELSE 0.0 END)*100,1) AS EXITO,
                   ROUND(AVG(TIEMPO_AUTENTICACION_MS)/1000,2) AS TIEMPO_S,
                   ROUND(AVG(CASE WHEN ABANDONO_EN_DESAFIO THEN 1.0 ELSE 0.0 END)*100,1) AS ABANDONO
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1 ORDER BY 2 DESC
        """)
        if not acs_df.empty:
            fig_acs = go.Figure()
            fig_acs.add_trace(go.Bar(
                y=acs_df["ACS_PROVIDER"].tolist(),
                x=[float(v) for v in acs_df["EXITO"]],
                name="Tasa exito (%)", orientation="h", marker=dict(color=COLORS[0]),
            ))
            fig_acs.add_trace(go.Bar(
                y=acs_df["ACS_PROVIDER"].tolist(),
                x=[float(v) for v in acs_df["ABANDONO"]],
                name="Abandono (%)", orientation="h", marker=dict(color="#DE350B"),
            ))
            fig_acs.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=120, r=20, t=30, b=50), barmode="group",
                xaxis=dict(title="Porcentaje (%)"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig_acs, use_container_width=True, key="tds_acs_bench")

    col_c, col_d = st.columns(2)

    with col_c:
        st.markdown("**Distribucion Score Riesgo ACS por Resultado**")
        risk_df = run_query(f"""
            SELECT FLOOR(SCORE_RIESGO_ACS/10)*10 AS BIN,
                   RESULTADO_AUTENTICACION,
                   COUNT(*) AS CNT
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1,2 ORDER BY 1
        """)
        if not risk_df.empty:
            result_colors = {"Autenticado": "#36B37E", "No autenticado": "#DE350B",
                             "Intento": "#FFAB00", "Rechazado": "#FF8B00"}
            fig_risk = go.Figure()
            for resultado in risk_df["RESULTADO_AUTENTICACION"].unique():
                sub = risk_df[risk_df["RESULTADO_AUTENTICACION"] == resultado].sort_values("BIN")
                bins = [f"{int(v)}-{int(v)+9}" for v in sub["BIN"]]
                fig_risk.add_trace(go.Bar(
                    x=bins, y=[int(v) for v in sub["CNT"]],
                    name=str(resultado),
                    marker=dict(color=result_colors.get(str(resultado), COLORS[0])),
                ))
            fig_risk.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=50, r=20, t=30, b=50), barmode="stack",
                xaxis=dict(title="Score Riesgo ACS"), yaxis=dict(title="Autenticaciones"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig_risk, use_container_width=True, key="tds_risk_dist")

    with col_d:
        st.markdown("**Analisis de Exemptions**")
        ex_df = run_query(f"""
            SELECT TIPO_EXEMPTION, COUNT(*) AS CNT,
                   ROUND(AVG(CASE WHEN AUTORIZACION_POSTERIOR_EXITOSA THEN 1.0 ELSE 0.0 END)*100,1) AS EXITO
            FROM {TABLE} WHERE {WHERE} AND EXEMPTION_SOLICITADA = TRUE
            GROUP BY 1 ORDER BY 2 DESC
        """)
        if not ex_df.empty:
            fig_ex = go.Figure()
            fig_ex.add_trace(go.Pie(
                labels=ex_df["TIPO_EXEMPTION"].tolist(),
                values=[int(v) for v in ex_df["CNT"]],
                hole=0.5,
                marker=dict(colors=COLORS[:len(ex_df)], line=dict(color="white", width=2)),
                textinfo="percent+label", textposition="outside", textfont=dict(size=10),
                hovertemplate="<b>%{label}</b><br>Cantidad: %{value:,}<br>%{percent:.1%}<extra></extra>",
            ))
            fig_ex.add_annotation(text=f"<b>{int(ex_df['CNT'].sum()):,}</b><br>exemptions",
                                  x=0.5, y=0.5, showarrow=False, font=dict(size=12, color="#11567F"))
            fig_ex.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=5, r=5, t=30, b=5), showlegend=False,
            )
            st.plotly_chart(fig_ex, use_container_width=True, key="tds_exemption_pie")

    st.divider()

    st.markdown("**Abandono en Desafio — Banco Emisor vs Plataforma**")
    heat_df = run_query(f"""
        SELECT BANCO_EMISOR, PLATAFORMA,
               ROUND(AVG(CASE WHEN ABANDONO_EN_DESAFIO THEN 1.0 ELSE 0.0 END)*100,1) AS ABANDONO
        FROM {TABLE} WHERE {WHERE}
        GROUP BY 1,2
    """)
    if not heat_df.empty:
        piv = heat_df.pivot_table(index="BANCO_EMISOR", columns="PLATAFORMA", values="ABANDONO", fill_value=0)
        fig_heat = go.Figure(go.Heatmap(
            z=piv.values.tolist(),
            x=piv.columns.tolist(),
            y=piv.index.tolist(),
            colorscale=[[0, "#E8F5E9"], [0.5, "#FFF3E0"], [1, "#FFCDD2"]],
            text=[[f"{v:.1f}%" for v in row] for row in piv.values.tolist()],
            texttemplate="%{text}",
            textfont=dict(size=11),
            hovertemplate="Banco: %{y}<br>Plataforma: %{x}<br>Abandono: %{z:.1f}%<extra></extra>",
        ))
        fig_heat.update_layout(
            template="plotly_white", paper_bgcolor="#FFFFFF", height=500,
            margin=dict(l=120, r=20, t=30, b=50),
        )
        st.plotly_chart(fig_heat, use_container_width=True, key="tds_heat_abandon")

    st.divider()

    st.markdown("**Tiempo de Autenticacion por Version y Tipo**")
    box_df = run_query(f"""
        SELECT VERSION_3DS,
               CASE WHEN FUE_FRICTIONLESS THEN 'Frictionless' ELSE 'Challenge' END AS TIPO,
               TIEMPO_AUTENTICACION_MS/1000.0 AS TIEMPO_S
        FROM {TABLE} WHERE {WHERE}
        LIMIT 5000
    """)
    if not box_df.empty:
        fig_box = go.Figure()
        for i, tipo in enumerate(["Frictionless", "Challenge"]):
            sub = box_df[box_df["TIPO"] == tipo]
            for j, ver in enumerate(sub["VERSION_3DS"].unique()):
                sv = sub[sub["VERSION_3DS"] == ver]
                fig_box.add_trace(go.Box(
                    y=[float(v) for v in sv["TIEMPO_S"]],
                    name=f"{ver} - {tipo}",
                    marker=dict(color=COLORS[(i * 3 + j) % len(COLORS)]),
                    boxmean=True,
                ))
        fig_box.update_layout(
            template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
            margin=dict(l=50, r=20, t=30, b=80),
            yaxis=dict(title="Tiempo (segundos)"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig_box, use_container_width=True, key="tds_box_time")
