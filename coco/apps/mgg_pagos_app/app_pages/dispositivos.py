import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import pydeck as pdk

TABLE = "MGG_PAGOS.FRAUDE_Y_SEGURIDAD_PAGOS.DISPOSITIVOS"

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
    tipos = run_query(f"SELECT DISTINCT TIPO_DISPOSITIVO FROM {TABLE} ORDER BY 1")["TIPO_DISPOSITIVO"].tolist()
    sos = run_query(f"SELECT DISTINCT SISTEMA_OPERATIVO FROM {TABLE} ORDER BY 1")["SISTEMA_OPERATIVO"].tolist()
    niveles = run_query(f"SELECT DISTINCT NIVEL_RIESGO FROM {TABLE} ORDER BY 1")["NIVEL_RIESGO"].tolist()
    ciudades = run_query(f"SELECT DISTINCT CIUDAD_FRECUENTE FROM {TABLE} ORDER BY 1")["CIUDAD_FRECUENTE"].tolist()
    verificaciones = run_query(f"SELECT DISTINCT ESTADO_VERIFICACION FROM {TABLE} ORDER BY 1")["ESTADO_VERIFICACION"].tolist()
    fechas = run_query(f"SELECT MIN(FECHA_PRIMERA_VEZ) AS FMIN, MAX(FECHA_PRIMERA_VEZ) AS FMAX FROM {TABLE}")
    return tipos, sos, niveles, ciudades, verificaciones, fechas


def build_where(fecha_ini, fecha_f, tipo_s, so_s, nivel_s, ciudad_s,
                all_tipos, all_sos, all_niveles, all_ciudades):
    clauses = [f"FECHA_PRIMERA_VEZ BETWEEN '{fecha_ini}' AND '{fecha_f}'"]
    if tipo_s and tipo_s != "Todos" and tipo_s in all_tipos:
        clauses.append(f"TIPO_DISPOSITIVO = '{tipo_s}'")
    if so_s and so_s != "Todos" and so_s in all_sos:
        clauses.append(f"SISTEMA_OPERATIVO = '{so_s}'")
    if nivel_s and nivel_s != "Todos" and nivel_s in all_niveles:
        clauses.append(f"NIVEL_RIESGO = '{nivel_s}'")
    if ciudad_s and ciudad_s != "Todos" and ciudad_s in all_ciudades:
        clauses.append(f"CIUDAD_FRECUENTE = '{ciudad_s}'")
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


st.header(":material/devices: Dispositivos")
st.caption("Huella digital de dispositivos usados en transacciones. Tipo, SO, IP, geolocalizacion, riesgo, deteccion de emuladores y root/jailbreak.")

c1, c2, c3 = st.columns(3)
with c1:
    with st.container(border=True):
        st.markdown("**:material/lightbulb: Que resuelve**")
        st.markdown("Imposibilidad de correlacionar fraudes a traves de multiples tarjetas o cuentas cuando se originan desde el mismo dispositivo comprometido.")
with c2:
    with st.container(border=True):
        st.markdown("**:material/settings: Como funciona**")
        st.markdown("Se captura el fingerprint de cada dispositivo: tipo, SO, version, IP, geolocalizacion, deteccion de VPN/proxy, emuladores y root/jailbreak. Se asigna un score de riesgo.")
with c3:
    with st.container(border=True):
        st.markdown("**:material/trending_up: Valor de negocio**")
        st.markdown("Detectar redes de fraude organizadas que usan el mismo dispositivo con multiples tarjetas robadas. Bloquear dispositivos comprometidos antes de que generen mas perdidas.")

tipos, sos, niveles, ciudades, verificaciones, fechas_df = get_filter_options()
fmin = pd.to_datetime(fechas_df["FMIN"].iloc[0]).date()
fmax = pd.to_datetime(fechas_df["FMAX"].iloc[0]).date()

dx_where = f"FECHA_PRIMERA_VEZ BETWEEN '{fmin}' AND '{fmax}'"

dx_kpi = run_query(f"""
    SELECT
        COUNT(*) AS TOTAL_DEVICES,
        SUM(CASE WHEN FRAUDE_ASOCIADO THEN 1 ELSE 0 END) AS CON_FRAUDE,
        SUM(CASE WHEN ROOT_JAILBREAK THEN 1 ELSE 0 END) AS ROOTED,
        SUM(CASE WHEN EMULADOR_DETECTADO THEN 1 ELSE 0 END) AS EMULADORES,
        SUM(CASE WHEN VPN_ACTIVA THEN 1 ELSE 0 END) AS VPN,
        SUM(CASE WHEN EN_BLACKLIST THEN 1 ELSE 0 END) AS BLACKLIST,
        ROUND(AVG(SCORE_RIESGO_DEVICE)*100, 1) AS SCORE_PROM
    FROM {TABLE}
    WHERE {dx_where}
""")

if not dx_kpi.empty and dx_kpi["TOTAL_DEVICES"].iloc[0] > 0:
    dx_r = dx_kpi.iloc[0]
    dx_total = int(dx_r["TOTAL_DEVICES"])
    dx_fraude = int(dx_r["CON_FRAUDE"])
    dx_tasa_fraude = round(dx_fraude / dx_total * 100, 1)
    dx_rooted = int(dx_r["ROOTED"])
    dx_emuladores = int(dx_r["EMULADORES"])
    dx_vpn = int(dx_r["VPN"])
    dx_blacklist = int(dx_r["BLACKLIST"])

    dx_worst_tipo = run_query(f"""
        SELECT TIPO_DISPOSITIVO, ROUND(SUM(CASE WHEN FRAUDE_ASOCIADO THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS TASA
        FROM {TABLE} WHERE {dx_where}
        GROUP BY 1 ORDER BY 2 DESC LIMIT 1
    """)

    if dx_tasa_fraude <= 5:
        dx_estado = f"Tasa de dispositivos con fraude ({color_tag(dx_tasa_fraude, 5, 10, inverse=True)}) esta :green[**controlada**]."
    elif dx_tasa_fraude <= 15:
        dx_estado = f"Tasa de dispositivos con fraude ({color_tag(dx_tasa_fraude, 5, 15, inverse=True)}) en rango :orange[**de atencion**]."
    else:
        dx_estado = f"Tasa de dispositivos con fraude ({color_tag(dx_tasa_fraude, 5, 15, inverse=True)}) :red[**elevada**]. Revision inmediata necesaria."

    dx_lines = [dx_estado]
    if not dx_worst_tipo.empty:
        dx_lines.append(f"- Tipo con mayor fraude: **{dx_worst_tipo.iloc[0]['TIPO_DISPOSITIVO']}** ({color_tag(float(dx_worst_tipo.iloc[0]['TASA']), 5, 15, inverse=True)})")
    dx_lines.append(f"- Dispositivos root/jailbreak: {'**:red[' if dx_rooted > dx_total*0.05 else '**:orange['}{dx_rooted:,}]** ({round(dx_rooted/dx_total*100,1)}%)")
    dx_lines.append(f"- Emuladores detectados: {'**:red[' if dx_emuladores > dx_total*0.03 else '**:orange['}{dx_emuladores:,}]**")
    dx_lines.append(f"- En blacklist: :red[**{dx_blacklist:,}**] | VPN activa: :orange[**{dx_vpn:,}**]")

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
            if dx_emuladores > dx_total * 0.02:
                recs.append("1. :red[**Bloquear emuladores**]: alta correlacion con fraude organizado. Implementar deteccion en SDK.")
            if dx_rooted > dx_total * 0.05:
                recs.append("2. :orange[**Restringir root/jailbreak**]: limitar transacciones de alto valor desde dispositivos comprometidos.")
            if dx_blacklist > dx_total * 0.03:
                recs.append("3. :orange[**Revisar blacklist**]: alto volumen de dispositivos bloqueados, evaluar si es por campana de fraude activa.")
            if dx_vpn > dx_total * 0.1:
                recs.append("4. :orange[**Monitorear VPN**]: uso elevado puede ocultar geolocalizacion real del fraudster.")
            if not recs:
                recs.append(":green[**El perfil de dispositivos esta dentro de parametros normales.**] Mantener monitoreo.")
            st.markdown("\n".join(recs))

st.divider()

tab1, tab2 = st.tabs(["Dashboard Ejecutivo", "Simulador Predictivo"])

with tab1:
    fc1, fc2, fc3, fc4, fc5 = st.columns(5)
    with fc1:
        fecha_rng = st.date_input("Periodo", value=(fmin, fmax), min_value=fmin, max_value=fmax, key="dv_fecha")
        if isinstance(fecha_rng, (list, tuple)) and len(fecha_rng) == 2:
            fecha_inicio, fecha_fin = fecha_rng
        else:
            fecha_inicio, fecha_fin = fmin, fmax
    with fc2:
        tipo_sel = st.selectbox("Tipo dispositivo", ["Todos"] + tipos, key="dv_tipo")
    with fc3:
        so_sel = st.selectbox("Sistema operativo", ["Todos"] + sos, key="dv_so")
    with fc4:
        nivel_sel = st.selectbox("Nivel riesgo", ["Todos"] + niveles, key="dv_nivel")
    with fc5:
        ciudad_sel = st.selectbox("Ciudad", ["Todos"] + ciudades, key="dv_ciudad")

    WHERE = build_where(fecha_inicio, fecha_fin, tipo_sel, so_sel, nivel_sel, ciudad_sel,
                        tipos, sos, niveles, ciudades)

    kpi_df = run_query(f"""
        SELECT
            COUNT(*) AS TOTAL,
            SUM(CASE WHEN FRAUDE_ASOCIADO THEN 1 ELSE 0 END) AS CON_FRAUDE,
            ROUND(AVG(SCORE_RIESGO_DEVICE)*100, 1) AS SCORE_PROM,
            ROUND(AVG(TARJETAS_ASOCIADAS), 1) AS TARJETAS_PROM,
            ROUND(AVG(TOTAL_TRANSACCIONES), 0) AS TXN_PROM,
            SUM(CASE WHEN ROOT_JAILBREAK THEN 1 ELSE 0 END) AS ROOTED,
            SUM(CASE WHEN EMULADOR_DETECTADO THEN 1 ELSE 0 END) AS EMULADORES,
            SUM(CASE WHEN EN_BLACKLIST THEN 1 ELSE 0 END) AS BLACKLIST
        FROM {TABLE}
        WHERE {WHERE}
    """)

    if kpi_df.empty or kpi_df["TOTAL"].iloc[0] == 0:
        st.info("No hay datos para los filtros seleccionados.")
    else:
        r = kpi_df.iloc[0]
        total = int(r["TOTAL"])

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total dispositivos", f"{total:,}")
        k2.metric("Con fraude asociado", f"{int(r['CON_FRAUDE']):,}", delta=f"{round(int(r['CON_FRAUDE'])/total*100, 1)}%", delta_color="inverse")
        k3.metric("Score riesgo prom", f"{float(r['SCORE_PROM']):.1f}%")
        k4.metric("Tarjetas/dispositivo", f"{float(r['TARJETAS_PROM']):.1f}")

        k5, k6, k7, k8 = st.columns(4)
        k5.metric("Txns promedio", f"{float(r['TXN_PROM']):.0f}")
        k6.metric("Root/Jailbreak", f"{int(r['ROOTED']):,}", delta_color="inverse")
        k7.metric("Emuladores", f"{int(r['EMULADORES']):,}", delta_color="inverse")
        k8.metric("En blacklist", f"{int(r['BLACKLIST']):,}", delta_color="inverse")

        st.divider()

        # --- ROW 1: Stacked bar tipo + Radar SO ---
        col_chart1, col_chart2 = st.columns(2)

        with col_chart1:
            st.markdown("**Dispositivos por tipo — Riesgo y fraude**")
            tipo_df = run_query(f"""
                SELECT TIPO_DISPOSITIVO,
                       COUNT(*) AS TOTAL,
                       SUM(CASE WHEN FRAUDE_ASOCIADO THEN 1 ELSE 0 END) AS FRAUDE,
                       ROUND(AVG(SCORE_RIESGO_DEVICE)*100, 1) AS SCORE
                FROM {TABLE}
                WHERE {WHERE}
                GROUP BY 1 ORDER BY 2 DESC
            """)
            if tipo_df.empty:
                st.info("Sin datos por tipo.")
            else:
                tipos_list = tipo_df["TIPO_DISPOSITIVO"].tolist()
                total_list = [int(v) for v in tipo_df["TOTAL"]]
                fraude_list = [int(v) for v in tipo_df["FRAUDE"]]
                safe_list = [t - f for t, f in zip(total_list, fraude_list)]
                fig_bar = go.Figure()
                fig_bar.add_trace(go.Bar(
                    x=tipos_list, y=safe_list, name="Sin fraude",
                    marker_color="#29B5E8",
                ))
                fig_bar.add_trace(go.Bar(
                    x=tipos_list, y=fraude_list, name="Con fraude",
                    marker_color="#DE350B",
                    text=[f"{f}" for f in fraude_list], textposition="auto",
                ))
                fig_bar.update_layout(
                    barmode="stack",
                    template="plotly_white", paper_bgcolor="#FFFFFF",
                    height=380, margin=dict(l=40, r=20, t=30, b=100),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    xaxis=dict(tickangle=-45),
                    yaxis=dict(title="Dispositivos"),
                )
                st.plotly_chart(fig_bar, use_container_width=True)

        with col_chart2:
            st.markdown("**Radar — Indicadores de riesgo por SO**")
            so_df = run_query(f"""
                SELECT SISTEMA_OPERATIVO,
                       ROUND(AVG(SCORE_RIESGO_DEVICE)*100, 1) AS SCORE,
                       ROUND(SUM(CASE WHEN FRAUDE_ASOCIADO THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS TASA_FRAUDE,
                       ROUND(SUM(CASE WHEN ROOT_JAILBREAK THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS TASA_ROOT,
                       ROUND(SUM(CASE WHEN VPN_ACTIVA THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS TASA_VPN,
                       ROUND(AVG(TARJETAS_ASOCIADAS), 1) AS TARJETAS_PROM
                FROM {TABLE}
                WHERE {WHERE}
                GROUP BY 1
                HAVING COUNT(*) > 20
                ORDER BY 2 DESC
                LIMIT 6
            """)
            if so_df.empty:
                st.info("Sin datos por SO.")
            else:
                cats = ["Score riesgo", "% Fraude", "% Root/JB", "% VPN", "Tarjetas (x10)"]
                fig_radar = go.Figure()
                so_colors = ["#DE350B", "#FF8B00", "#FFAB00", "#29B5E8", "#36B37E", "#6554C0"]
                for idx, (_, row) in enumerate(so_df.iterrows()):
                    vals = [float(row["SCORE"]), float(row["TASA_FRAUDE"]),
                            float(row["TASA_ROOT"]), float(row["TASA_VPN"]),
                            float(row["TARJETAS_PROM"]) * 10]
                    c = so_colors[idx % len(so_colors)]
                    fig_radar.add_trace(go.Scatterpolar(
                        r=vals + [vals[0]], theta=cats + [cats[0]],
                        name=str(row["SISTEMA_OPERATIVO"]),
                        line=dict(color=c, width=2),
                        fillcolor=f"rgba({int(c[1:3],16)},{int(c[3:5],16)},{int(c[5:7],16)},0.08)",
                        fill="toself",
                    ))
                fig_radar.update_layout(
                    polar=dict(radialaxis=dict(visible=True, range=[0, max(50, so_df["SCORE"].max() * 1.2)])),
                    template="plotly_white", paper_bgcolor="#FFFFFF",
                    height=380, margin=dict(l=60, r=60, t=40, b=40),
                    legend=dict(orientation="h", yanchor="bottom", y=-0.25, xanchor="center", x=0.5, font=dict(size=9)),
                    showlegend=True,
                )
                st.plotly_chart(fig_radar, use_container_width=True)

        # --- ROW 2a: Heatmap (full width) ---
        st.markdown("**Heatmap — Nivel riesgo vs tipo dispositivo**")
        heat_df = run_query(f"""
            SELECT NIVEL_RIESGO, TIPO_DISPOSITIVO, COUNT(*) AS N
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1, 2 ORDER BY 1, 2
        """)
        if heat_df.empty:
            st.info("Sin datos para heatmap.")
        else:
            pivot = heat_df.pivot_table(index="NIVEL_RIESGO", columns="TIPO_DISPOSITIVO", values="N", fill_value=0)
            nivel_order = {"Bajo": 0, "Medio": 1, "Alto": 2, "Critico": 3, "Sin evaluar": 4}
            pivot["_order"] = pivot.index.map(lambda x: nivel_order.get(x, 99))
            pivot = pivot.sort_values("_order").drop(columns=["_order"])
            fig_heat = go.Figure(go.Heatmap(
                z=pivot.values.tolist(),
                x=pivot.columns.tolist(),
                y=pivot.index.tolist(),
                colorscale=[[0, "#E8F5E9"], [0.3, "#FFF3E0"], [0.6, "#FF8B00"], [1, "#DE350B"]],
                text=[[f"{int(v)}" for v in row] for row in pivot.values.tolist()],
                texttemplate="%{text}",
                textfont=dict(size=13),
                hovertemplate="Nivel: %{y}<br>Tipo: %{x}<br>Dispositivos: %{z:,}<extra></extra>",
                colorbar=dict(title="Cant."),
            ))
            fig_heat.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF",
                height=500, margin=dict(l=120, r=40, t=30, b=100),
                xaxis=dict(tickangle=-45, tickfont=dict(size=12)),
                yaxis=dict(tickfont=dict(size=12)),
            )
            st.plotly_chart(fig_heat, use_container_width=True)

        # --- ROW 2b: Donut + Funnel ---
        col_chart4, col_chart5 = st.columns(2)

        with col_chart4:
            st.markdown("**Amenazas detectadas**")
            amenazas = {
                "Root/Jailbreak": int(r["ROOTED"]),
                "Emulador": int(r["EMULADORES"]),
                "Blacklist": int(r["BLACKLIST"]),
            }
            vpn_count = run_query(f"SELECT SUM(CASE WHEN VPN_ACTIVA THEN 1 ELSE 0 END) AS N FROM {TABLE} WHERE {WHERE}")
            if not vpn_count.empty:
                amenazas["VPN activa"] = int(vpn_count.iloc[0]["N"])
            multi_tarj = run_query(f"SELECT COUNT(*) AS N FROM {TABLE} WHERE {WHERE} AND TARJETAS_ASOCIADAS > 3")
            if not multi_tarj.empty:
                amenazas["Multi-tarjeta (>3)"] = int(multi_tarj.iloc[0]["N"])

            labels = list(amenazas.keys())
            values = list(amenazas.values())
            colors_donut = ["#DE350B", "#FF8B00", "#11567F", "#FFAB00", "#6554C0"]
            fig_donut = go.Figure(go.Pie(
                labels=labels, values=values,
                hole=0.55,
                marker=dict(colors=colors_donut[:len(labels)], line=dict(color="white", width=2)),
                textinfo="percent+label",
                textposition="outside",
                textfont=dict(size=10),
                hovertemplate="<b>%{label}</b><br>Dispositivos: %{value:,}<br>%{percent:.1%}<extra></extra>",
                pull=[0.05, 0, 0, 0, 0],
            ))
            total_amenazas = sum(values)
            fig_donut.add_annotation(
                text=f"<b>{total_amenazas:,}</b><br>amenazas",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=13, color="#11567F"),
            )
            fig_donut.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF",
                height=380, margin=dict(l=5, r=5, t=30, b=5),
                showlegend=False,
            )
            st.plotly_chart(fig_donut, use_container_width=True)

        with col_chart5:
            st.markdown("**Funnel — Verificacion de dispositivos**")
            verif_df = run_query(f"""
                SELECT ESTADO_VERIFICACION, COUNT(*) AS N
                FROM {TABLE}
                WHERE {WHERE}
                GROUP BY 1 ORDER BY 2 DESC
            """)
            if verif_df.empty:
                st.info("Sin datos de verificacion.")
            else:
                verif_colors = {"Verificado": "#36B37E", "Pendiente": "#FFAB00", "No verificado": "#FF8B00",
                                "Rechazado": "#DE350B", "En revision": "#29B5E8"}
                v_list = verif_df["ESTADO_VERIFICACION"].tolist()
                v_vals = [int(v) for v in verif_df["N"]]
                v_colors = [verif_colors.get(v, "#999") for v in v_list]
                fig_funnel = go.Figure(go.Funnel(
                    y=v_list, x=v_vals,
                    textinfo="value+percent initial",
                    texttemplate="%{value:,} (%{percentInitial:.1%})",
                    marker=dict(color=v_colors, line=dict(width=1, color="white")),
                    connector=dict(line=dict(color="#DFE1E6", width=1)),
                ))
                fig_funnel.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF",
                    height=380, margin=dict(l=120, r=20, t=30, b=40),
                    showlegend=False,
                )
                st.plotly_chart(fig_funnel, use_container_width=True)

        st.divider()

        st.markdown("**Mapa de dispositivos por ciudad**")
        mapa_df = run_query(f"""
            SELECT CIUDAD_FRECUENTE AS CIUDAD,
                   COUNT(*) AS TOTAL,
                   SUM(CASE WHEN FRAUDE_ASOCIADO THEN 1 ELSE 0 END) AS FRAUDES,
                   ROUND(AVG(SCORE_RIESGO_DEVICE)*100, 1) AS SCORE
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
            mapa_df["SCORE_F"] = mapa_df["SCORE"].astype(float)

            score_min = float(mapa_df["SCORE_F"].min())
            score_max = float(mapa_df["SCORE_F"].max())

            def score_to_color(score):
                if score_max == score_min:
                    norm = 0.5
                else:
                    norm = (score - score_min) / (score_max - score_min)
                r_c = int(54 * (1 - norm) + 222 * norm)
                g_c = int(179 * (1 - norm) + 53 * norm)
                b_c = int(126 * (1 - norm) + 11 * norm)
                return [r_c, g_c, b_c, 180]

            mapa_df["COLOR"] = mapa_df["SCORE_F"].apply(score_to_color)
            fr_max = float(mapa_df["FRAUDES_F"].max()) if float(mapa_df["FRAUDES_F"].max()) > 0 else 1
            mapa_df["RADIUS"] = mapa_df["FRAUDES_F"].apply(lambda v: max(8000, float(v) / fr_max * 45000))

            layer = pdk.Layer(
                "ScatterplotLayer", data=mapa_df,
                get_position=["LON", "LAT"], get_radius="RADIUS",
                get_fill_color="COLOR", pickable=True, opacity=0.8,
            )
            view_state = pdk.ViewState(latitude=5.5, longitude=-74.0, zoom=5.0, pitch=0)
            tooltip = {
                "html": "<b>{CIUDAD}</b><br/>Dispositivos: {TOTAL}<br/>Fraudes: {FRAUDES}<br/>Score: {SCORE}%",
                "style": {"backgroundColor": "#11567F", "color": "white", "fontSize": "12px", "padding": "8px"},
            }
            st.pydeck_chart(pdk.Deck(
                layers=[layer], initial_view_state=view_state, tooltip=tooltip,
                map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
            ))
            st.caption("Tamano: volumen de fraudes | Color: verde = bajo riesgo, rojo = alto riesgo")

        st.divider()

        st.markdown("**Top 10 dispositivos de mayor riesgo**")
        top_df = run_query(f"""
            SELECT DEVICE_ID, TIPO_DISPOSITIVO, SISTEMA_OPERATIVO, NIVEL_RIESGO,
                   ROUND(SCORE_RIESGO_DEVICE*100, 1) AS SCORE,
                   TARJETAS_ASOCIADAS, USUARIOS_ASOCIADOS, TOTAL_TRANSACCIONES,
                   FRAUDE_ASOCIADO, EN_BLACKLIST
            FROM {TABLE}
            WHERE {WHERE}
            ORDER BY SCORE_RIESGO_DEVICE DESC
            LIMIT 10
        """)
        if not top_df.empty:
            st.dataframe(top_df, use_container_width=True)

        st.divider()

        st.markdown("**Alertas e insights automaticos**")
        multi_card = run_query(f"SELECT COUNT(*) AS N FROM {TABLE} WHERE {WHERE} AND TARJETAS_ASOCIADAS > 5")
        emul = run_query(f"SELECT COUNT(*) AS N FROM {TABLE} WHERE {WHERE} AND EMULADOR_DETECTADO AND FRAUDE_ASOCIADO")

        if not multi_card.empty and int(multi_card.iloc[0]["N"]) > 0:
            st.error(f"**{int(multi_card.iloc[0]['N']):,}** dispositivos con mas de 5 tarjetas asociadas — posible red de fraude organizada.")
        if not emul.empty and int(emul.iloc[0]["N"]) > 0:
            st.warning(f"**{int(emul.iloc[0]['N']):,}** emuladores con fraude confirmado. Reforzar deteccion en SDK de pagos.")
        if int(r["BLACKLIST"]) > total * 0.05:
            st.warning(f"**{round(int(r['BLACKLIST'])/total*100, 1)}%** de dispositivos en blacklist. Posible campana de fraude activa.")
        else:
            st.success("Niveles de amenaza de dispositivos dentro de parametros normales.")


with tab2:
    st.markdown("**Simulador de riesgo de dispositivo**")
    st.caption("Estime el score de riesgo de un dispositivo ajustando sus caracteristicas. Permite evaluar politicas de bloqueo.")

    col_sliders, col_result = st.columns([3, 2])

    with col_sliders:
        sim_tarjetas = st.slider("Tarjetas asociadas al dispositivo", 1, 20, 2, key="dv_sim_tarj",
                                 help="Multiples tarjetas en un mismo dispositivo aumentan el riesgo")
        sim_usuarios = st.slider("Usuarios asociados", 1, 10, 1, key="dv_sim_usr",
                                 help="Multiples usuarios desde un dispositivo es atipico")
        sim_txn_30d = st.slider("Transacciones ultimos 30 dias", 1, 200, 20, key="dv_sim_txn",
                                help="Alta frecuencia puede indicar uso automatizado")
        sim_root = st.slider("Root/Jailbreak detectado (0=No, 1=Si)", 0, 1, 0, key="dv_sim_root",
                             help="Dispositivos comprometidos son de alto riesgo")
        sim_emulador = st.slider("Emulador detectado (0=No, 1=Si)", 0, 1, 0, key="dv_sim_emu",
                                 help="Emuladores son usados frecuentemente para fraude automatizado")
        sim_vpn = st.slider("VPN activa (0=No, 1=Si)", 0, 1, 0, key="dv_sim_vpn",
                            help="VPN oculta la ubicacion real del dispositivo")

    def calcular_riesgo_device(tarjetas, usuarios, txn_30d, root, emulador, vpn):
        score_tarj = min(1.0, (tarjetas - 1) / 10)
        score_usr = min(1.0, (usuarios - 1) / 5)
        score_txn = min(1.0, txn_30d / 100)
        score_root = float(root)
        score_emu = float(emulador)
        score_vpn = float(vpn) * 0.5

        score = (
            0.20 * score_tarj
            + 0.15 * score_usr
            + 0.10 * score_txn
            + 0.25 * score_root
            + 0.20 * score_emu
            + 0.10 * score_vpn
        )
        return round(min(max(score * 100, 1), 99), 1)

    score_pred = calcular_riesgo_device(sim_tarjetas, sim_usuarios, sim_txn_30d, sim_root, sim_emulador, sim_vpn)

    with col_result:
        if score_pred < 15:
            nivel = "Bajo"
            color_nivel = "#36B37E"
            accion = "Permitir sin restriccion"
            interpretacion = "Dispositivo de bajo riesgo. Perfil normal de uso. No requiere restricciones adicionales."
        elif score_pred < 40:
            nivel = "Medio"
            color_nivel = "#29B5E8"
            accion = "Monitoreo reforzado"
            interpretacion = "Algunos indicadores atipicos. Aplicar monitoreo reforzado y limitar transacciones de alto valor."
        elif score_pred < 70:
            nivel = "Alto"
            color_nivel = "#FFAB00"
            accion = "Restringir transacciones"
            interpretacion = "Dispositivo de alto riesgo. Restringir montos, exigir autenticacion adicional (3DS, OTP)."
        else:
            nivel = "Critico"
            color_nivel = "#DE350B"
            accion = "Bloquear dispositivo"
            interpretacion = "Dispositivo altamente sospechoso. Bloquear transacciones y agregar a blacklist. Investigar inmediatamente."

        st.metric("Score de riesgo", f"{score_pred:.1f}%")
        if score_pred < 15:
            st.markdown(f"**Nivel:** :green[**{nivel}**] | **Accion:** :green[**{accion}**]")
        elif score_pred < 40:
            st.markdown(f"**Nivel:** :blue[**{nivel}**] | **Accion:** :blue[**{accion}**]")
        elif score_pred < 70:
            st.markdown(f"**Nivel:** :orange[**{nivel}**] | **Accion:** :orange[**{accion}**]")
        else:
            st.markdown(f"**Nivel:** :red[**{nivel}**] | **Accion:** :red[**{accion}**]")

        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=float(score_pred),
            number=dict(suffix="%", font=dict(size=36)),
            gauge=dict(
                axis=dict(range=[0, 100], ticksuffix="%"),
                bar=dict(color=color_nivel),
                steps=[
                    dict(range=[0, 15], color="#E8F5E9"),
                    dict(range=[15, 40], color="#E3F2FD"),
                    dict(range=[40, 70], color="#FFF3E0"),
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

    if "dv_escenarios" not in st.session_state:
        st.session_state.dv_escenarios = []

    if st.button("Guardar escenario actual", type="primary", key="dv_save"):
        if len(st.session_state.dv_escenarios) >= 3:
            st.session_state.dv_escenarios.pop(0)
        st.session_state.dv_escenarios.append({
            "Tarjetas": sim_tarjetas,
            "Usuarios": sim_usuarios,
            "Txns 30d": sim_txn_30d,
            "Root/JB": "Si" if sim_root else "No",
            "Emulador": "Si" if sim_emulador else "No",
            "VPN": "Si" if sim_vpn else "No",
            "Score": f"{score_pred:.1f}%",
            "Nivel": nivel,
            "Accion": accion,
        })
        st.rerun()

    if st.session_state.dv_escenarios:
        esc_df = pd.DataFrame(st.session_state.dv_escenarios)
        esc_df.index = [f"Escenario {i+1}" for i in range(len(esc_df))]
        st.dataframe(esc_df.T, use_container_width=True)

        if st.button("Limpiar escenarios", key="dv_clear"):
            st.session_state.dv_escenarios = []
            st.rerun()
    else:
        st.caption("Aun no hay escenarios guardados.")
