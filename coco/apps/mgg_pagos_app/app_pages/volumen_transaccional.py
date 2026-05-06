import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from app_pages.map_helper import colombia_scatter_map
from decimal import Decimal

TABLE = "MGG_PAGOS.ANALITICA_DE_PAGOS.VOLUMEN_TRANSACCIONAL"

COLORS = ["#29B5E8", "#11567F", "#71D4F0", "#0E3A53", "#A3E4F7", "#1B8BBF", "#5BC3E8", "#083248"]

COORDS_CIUDAD = {
    "Bogota": (4.6097, -74.0817), "Medellin": (6.2442, -75.5812),
    "Cali": (3.4516, -76.5320), "Barranquilla": (10.9685, -74.7813),
    "Bucaramanga": (7.1254, -73.1198), "Cartagena": (10.3997, -75.5144),
    "Cucuta": (7.8939, -72.5078), "Ibague": (4.4389, -75.2322),
    "Pereira": (4.8133, -75.6961), "Manizales": (5.0689, -75.5174),
    "Villavicencio": (4.1420, -73.6266), "Santa Marta": (11.2404, -74.1990),
}


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
    canales = run_query(f"SELECT DISTINCT CANAL FROM {TABLE} ORDER BY 1")["CANAL"].tolist()
    redes = run_query(f"SELECT DISTINCT RED FROM {TABLE} ORDER BY 1")["RED"].tolist()
    tipos_tarjeta = run_query(f"SELECT DISTINCT TIPO_TARJETA FROM {TABLE} ORDER BY 1")["TIPO_TARJETA"].tolist()
    ciudades = run_query(f"SELECT DISTINCT CIUDAD FROM {TABLE} ORDER BY 1")["CIUDAD"].tolist()
    tipos_tx = run_query(f"SELECT DISTINCT TIPO_TRANSACCION FROM {TABLE} ORDER BY 1")["TIPO_TRANSACCION"].tolist()
    fechas = run_query(f"SELECT MIN(FECHA) AS FMIN, MAX(FECHA) AS FMAX FROM {TABLE}")
    return canales, redes, tipos_tarjeta, ciudades, tipos_tx, fechas


st.header(":material/bar_chart: Volumen transaccional")
st.caption("Volumen, revenue, tickets, crecimiento, concentracion y geografia del ecosistema transaccional colombiano.")

c1, c2, c3 = st.columns(3)
with c1:
    with st.container(border=True):
        st.markdown("**:material/lightbulb: Que resuelve**")
        st.markdown("Panorama integral del volumen de transacciones por canal, red, tarjeta y ciudad, con metricas de crecimiento y concentracion del mercado.")
with c2:
    with st.container(border=True):
        st.markdown("**:material/settings: Como funciona**")
        st.markdown("Agrega transacciones diarias con volumen bruto, ticket promedio, tasas de aprobacion/rechazo, crecimiento (DoD, WoW, MoM, YoY) y revenue por comisiones.")
with c3:
    with st.container(border=True):
        st.markdown("**:material/trending_up: Valor de negocio**")
        st.markdown("Entender dinamicas de crecimiento, detectar concentracion excesiva, optimizar mix de canales y priorizar expansion geografica con datos reales.")

canales, redes, tipos_tarjeta, ciudades, tipos_tx, fechas_df = get_filter_options()
fmin = pd.to_datetime(fechas_df["FMIN"].iloc[0]).date()
fmax = pd.to_datetime(fechas_df["FMAX"].iloc[0]).date()

dx_kpi = run_query(f"""
    SELECT
        SUM(NUMERO_TRANSACCIONES) AS TOTAL_TX,
        ROUND(SUM(VOLUMEN_BRUTO_COP)/1e9, 2) AS VOL_B,
        ROUND(AVG(TICKET_PROMEDIO_COP), 0) AS TICKET_PROM,
        ROUND(AVG(TASA_APROBACION)*100, 1) AS APROB_PCT,
        ROUND(AVG(CRECIMIENTO_YOY_PCT), 1) AS YOY,
        ROUND(AVG(CONCENTRACION_TOP10_PCT), 1) AS CONC,
        ROUND(SUM(REVENUE_COMISIONES_COP)/1e6, 1) AS REV_M,
        ROUND(AVG(TASA_FRAUDE)*100, 3) AS FRAUDE_PCT
    FROM {TABLE}
""")

if not dx_kpi.empty and dx_kpi["TOTAL_TX"].iloc[0] > 0:
    dx = dx_kpi.iloc[0]
    dx_yoy = float(dx["YOY"])
    dx_conc = float(dx["CONC"])
    dx_aprob = float(dx["APROB_PCT"])

    dx_top_canal = run_query(f"SELECT CANAL, SUM(VOLUMEN_BRUTO_COP) AS V FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC LIMIT 1")
    dx_top_ciudad = run_query(f"SELECT CIUDAD, SUM(VOLUMEN_BRUTO_COP) AS V FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC LIMIT 1")

    if dx_yoy >= 10:
        dx_estado = f"Crecimiento YoY ({color_tag(dx_yoy, 10, 5, fmt='{:+.1f}%')}) :green[**dinamico**]. Volumen: ${float(dx['VOL_B']):.2f}B COP."
    elif dx_yoy >= 0:
        dx_estado = f"Crecimiento YoY ({color_tag(dx_yoy, 10, 5, fmt='{:+.1f}%')}) :orange[**moderado**]. Aprobacion: {dx_aprob:.1f}%."
    else:
        dx_estado = f"Crecimiento YoY ({color_tag(dx_yoy, 10, 5, fmt='{:+.1f}%')}) :red[**negativo**]. Requiere intervencion."

    dx_lines = [dx_estado]
    if not dx_top_canal.empty:
        dx_lines.append(f"- Canal lider: **{dx_top_canal.iloc[0]['CANAL']}** (mayor volumen bruto)")
    if not dx_top_ciudad.empty:
        dx_lines.append(f"- Ciudad lider: **{dx_top_ciudad.iloc[0]['CIUDAD']}**")
    if dx_conc > 80:
        dx_lines.append(f"- Concentracion Top 10: {color_tag(dx_conc, 0, 80, inverse=True)} — :red[**riesgo de dependencia**]")
    else:
        dx_lines.append(f"- Concentracion Top 10: {color_tag(dx_conc, 0, 80, inverse=True)} — diversificacion adecuada")

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
            if dx_yoy < 5:
                recs.append("1. :orange[**Impulsar crecimiento**]: YoY por debajo del 5%. Revisar estrategia de adquisicion.")
            if dx_conc > 80:
                recs.append("2. :red[**Diversificar**]: alta concentracion en Top 10. Expandir base de comercios.")
            if float(dx["FRAUDE_PCT"]) > 0.5:
                recs.append("3. :orange[**Controlar fraude**]: tasa por encima del benchmark.")
            if dx_aprob < 90:
                recs.append("4. :orange[**Mejorar aprobacion**]: tasa por debajo del 90% impacta revenue.")
            if not recs:
                recs.append(":green[**Ecosistema transaccional saludable.**] Continuar monitoreo de crecimiento y diversificacion.")
            st.markdown("\n".join(recs))

st.divider()

fc1, fc2, fc3, fc4, fc5 = st.columns(5)
with fc1:
    fecha_rng = st.date_input("Periodo", value=(fmin, fmax), min_value=fmin, max_value=fmax, key="vol_fecha")
    if isinstance(fecha_rng, (list, tuple)) and len(fecha_rng) == 2:
        fecha_inicio, fecha_fin = fecha_rng
    else:
        fecha_inicio, fecha_fin = fmin, fmax
with fc2:
    canal_sel = st.selectbox("Canal", ["Todos"] + canales, key="vol_canal")
with fc3:
    red_sel = st.selectbox("Red", ["Todos"] + redes, key="vol_red")
with fc4:
    ciudad_sel = st.selectbox("Ciudad", ["Todos"] + ciudades, key="vol_ciudad")
with fc5:
    tipo_tx_sel = st.selectbox("Tipo txn", ["Todos"] + tipos_tx, key="vol_tipo")

WHERE = f"FECHA BETWEEN '{fecha_inicio}' AND '{fecha_fin}'"
if canal_sel != "Todos":
    WHERE += f" AND CANAL = '{canal_sel}'"
if red_sel != "Todos":
    WHERE += f" AND RED = '{red_sel}'"
if ciudad_sel != "Todos":
    WHERE += f" AND CIUDAD = '{ciudad_sel}'"
if tipo_tx_sel != "Todos":
    WHERE += f" AND TIPO_TRANSACCION = '{tipo_tx_sel}'"

kpi_df = run_query(f"""
    SELECT
        SUM(NUMERO_TRANSACCIONES) AS TOTAL_TX,
        ROUND(SUM(VOLUMEN_BRUTO_COP)/1e9, 2) AS VOL_B,
        ROUND(AVG(TICKET_PROMEDIO_COP), 0) AS TICKET_PROM,
        ROUND(AVG(TICKET_MEDIANO_COP), 0) AS TICKET_MED,
        ROUND(AVG(TASA_APROBACION)*100, 1) AS APROB_PCT,
        ROUND(AVG(TASA_RECHAZO)*100, 1) AS RECH_PCT,
        ROUND(AVG(CRECIMIENTO_YOY_PCT), 1) AS YOY,
        ROUND(SUM(REVENUE_COMISIONES_COP)/1e6, 1) AS REV_M,
        ROUND(AVG(CONCENTRACION_TOP10_PCT), 1) AS CONC,
        SUM(COMERCIOS_ACTIVOS) AS COMERCIOS,
        SUM(TARJETAS_UNICAS) AS TARJETAS,
        ROUND(AVG(TASA_FRAUDE)*100, 3) AS FRAUDE_PCT,
        COUNT(*) AS N_REG
    FROM {TABLE}
    WHERE {WHERE}
""")

if kpi_df.empty or kpi_df["N_REG"].iloc[0] == 0:
    st.info("No hay datos para los filtros seleccionados.")
else:
    r = kpi_df.iloc[0]

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Transacciones", f"{int(r['TOTAL_TX']):,}")
    k2.metric("Volumen bruto", f"${float(r['VOL_B']):.2f}B COP")
    k3.metric("Ticket promedio", f"${float(r['TICKET_PROM']):,.0f} COP")
    k4.metric("Tasa aprobacion", f"{float(r['APROB_PCT']):.1f}%")

    k5, k6, k7, k8 = st.columns(4)
    k5.metric("Crecimiento YoY", f"{float(r['YOY']):+.1f}%")
    k6.metric("Revenue comisiones", f"${float(r['REV_M']):,.1f}M COP")
    k7.metric("Concentracion Top10", f"{float(r['CONC']):.1f}%")
    k8.metric("Tasa fraude", f"{float(r['FRAUDE_PCT']):.3f}%")

    st.divider()

    st.markdown("**Mapa — Volumen transaccional por ciudad**")
    map_df = run_query(f"""
        SELECT CIUDAD, SUM(NUMERO_TRANSACCIONES) AS N, ROUND(SUM(VOLUMEN_BRUTO_COP)/1e6, 1) AS VOL_M
        FROM {TABLE} WHERE {WHERE}
        GROUP BY 1 ORDER BY 2 DESC
    """)
    if not map_df.empty:
        map_data = []
        for _, row in map_df.iterrows():
            city = str(row["CIUDAD"])
            if city in COORDS_CIUDAD:
                lat, lon = COORDS_CIUDAD[city]
                map_data.append({"lat": lat, "lon": lon, "city": city, "n": int(row["N"]), "vol_m": float(row["VOL_M"])})
        if map_data:
            pdf = pd.DataFrame(map_data)
            pdf["hover"] = pdf.apply(lambda r: f"{r['city']}<br>Txns: {r['n']:,}<br>${r['vol_m']:.1f}M COP", axis=1)
            deck = colombia_scatter_map(pdf, colorbar_title="Transacciones")
            st.pydeck_chart(deck, key="vol_map")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Crecimiento MoM por canal — Cascada**")
        growth_df = run_query(f"""
            SELECT CANAL, ROUND(AVG(CRECIMIENTO_MOM_PCT), 2) AS MOM
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1
            ORDER BY 2 DESC
        """)
        if not growth_df.empty:
            fig_wf = go.Figure(go.Waterfall(
                x=growth_df["CANAL"].tolist(),
                y=growth_df["MOM"].tolist(),
                measure=["relative"] * len(growth_df),
                text=[f"{v:+.2f}%" for v in growth_df["MOM"].tolist()],
                textposition="outside",
                connector=dict(line=dict(color="#11567F", width=1)),
                increasing=dict(marker=dict(color="#36B37E")),
                decreasing=dict(marker=dict(color="#DE350B")),
                hovertemplate="<b>%{x}</b><br>MoM: %{y:+.2f}%<extra></extra>",
            ))
            fig_wf.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=60, r=20, t=10, b=80),
                yaxis=dict(title="Crecimiento MoM (%)"),
                xaxis=dict(tickangle=-30),
            )
            st.plotly_chart(fig_wf, use_container_width=True, key="vol_growth_wf")

    with col2:
        st.markdown("**Hora pico por canal**")
        hora_df = run_query(f"""
            SELECT CANAL, HORA_PICO, COUNT(*) AS FREQ
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1, 2
        """)
        if not hora_df.empty:
            pivot_h = hora_df.pivot_table(index="CANAL", columns="HORA_PICO", values="FREQ", fill_value=0)
            pivot_h = pivot_h.reindex(columns=sorted(pivot_h.columns))
            fig_hora = go.Figure(go.Heatmap(
                z=pivot_h.values.tolist(),
                x=[str(int(h)) + "h" for h in pivot_h.columns.tolist()],
                y=pivot_h.index.tolist(),
                colorscale=[[0, "#E3F2FD"], [0.5, "#29B5E8"], [1, "#11567F"]],
                hovertemplate="Canal: %{y}<br>Hora: %{x}<br>Frecuencia: %{z}<extra></extra>",
                colorbar=dict(title="Frecuencia"),
            ))
            fig_hora.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=120, r=20, t=10, b=60),
                xaxis=dict(title="Hora del dia", tickfont=dict(size=10)),
                yaxis=dict(tickfont=dict(size=11)),
            )
            st.plotly_chart(fig_hora, use_container_width=True, key="vol_hora_heatmap")

    col3, col4 = st.columns(2)

    with col3:
        st.markdown("**Ticket promedio por tipo de transaccion**")
        ticket_df = run_query(f"""
            SELECT TIPO_TRANSACCION, TICKET_PROMEDIO_COP
            FROM {TABLE}
            WHERE {WHERE}
        """)
        if not ticket_df.empty:
            fig_ticket = go.Figure()
            ttypes = sorted(ticket_df["TIPO_TRANSACCION"].unique().tolist())
            for i, tt in enumerate(ttypes):
                sub = ticket_df[ticket_df["TIPO_TRANSACCION"] == tt]
                fig_ticket.add_trace(go.Box(
                    y=sub["TICKET_PROMEDIO_COP"].tolist(),
                    name=tt,
                    marker=dict(color=COLORS[i % len(COLORS)]),
                    boxmean="sd",
                ))
            fig_ticket.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=80, r=20, t=10, b=80),
                yaxis=dict(title="Ticket promedio (COP)"),
                showlegend=False,
                xaxis=dict(tickangle=-20),
            )
            st.plotly_chart(fig_ticket, use_container_width=True, key="vol_ticket_box")

    with col4:
        st.markdown("**Indicador de concentracion Top 10**")
        conc_val = float(r["CONC"])
        conc_color = "#DE350B" if conc_val > 80 else ("#FFAB00" if conc_val > 60 else "#36B37E")
        fig_conc = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=conc_val,
            number=dict(suffix="%", font=dict(size=40)),
            delta=dict(reference=70, suffix="%"),
            title=dict(text="Concentracion Top 10 comercios", font=dict(size=14)),
            gauge=dict(
                axis=dict(range=[0, 100], ticksuffix="%"),
                bar=dict(color=conc_color),
                steps=[
                    dict(range=[0, 50], color="#E8F5E9"),
                    dict(range=[50, 70], color="#FFF8E1"),
                    dict(range=[70, 85], color="#FFEBEE"),
                    dict(range=[85, 100], color="#FFCDD2"),
                ],
                threshold=dict(line=dict(color="#DE350B", width=3), thickness=0.8, value=80),
            ),
        ))
        fig_conc.update_layout(
            template="plotly_white", paper_bgcolor="#FFFFFF",
            height=350, margin=dict(l=30, r=30, t=50, b=10),
        )
        st.plotly_chart(fig_conc, use_container_width=True, key="vol_concentration")

    st.markdown("**Radar multi-metrica por red**")
    radar_df = run_query(f"""
        SELECT RED,
               ROUND(AVG(TASA_APROBACION)*100, 1) AS APROB,
               ROUND(100 - AVG(TASA_FRAUDE)*10000, 1) AS SEG,
               ROUND(AVG(CRECIMIENTO_YOY_PCT) + 50, 1) AS CREC,
               ROUND(SUM(REVENUE_COMISIONES_COP)/1e6, 1) AS REV_M,
               ROUND(AVG(TICKET_PROMEDIO_COP)/10000 * 100, 1) AS TICKET_IDX
        FROM {TABLE}
        WHERE {WHERE}
        GROUP BY 1
    """)
    if not radar_df.empty:
        cats = ["Aprobacion", "Seguridad", "Crecimiento", "Ticket"]
        fig_radar = go.Figure()
        for i, (_, row) in enumerate(radar_df.iterrows()):
            vals = [
                min(float(row["APROB"]), 100),
                min(float(row["SEG"]), 100),
                min(max(float(row["CREC"]), 0), 100),
                min(float(row["TICKET_IDX"]), 100),
            ]
            fig_radar.add_trace(go.Scatterpolar(
                r=vals + [vals[0]],
                theta=cats + [cats[0]],
                name=str(row["RED"]),
                line=dict(color=COLORS[i % len(COLORS)], width=2),
                fill="toself", opacity=0.3,
            ))
        fig_radar.update_layout(
            template="plotly_white", paper_bgcolor="#FFFFFF", height=450,
            margin=dict(l=60, r=60, t=30, b=30),
            polar=dict(radialaxis=dict(range=[0, 100], ticksuffix="")),
            legend=dict(orientation="h", yanchor="bottom", y=-0.15),
        )
        st.plotly_chart(fig_radar, use_container_width=True, key="vol_radar")

    st.markdown("**Evolucion del volumen por canal**")
    area_df = run_query(f"""
        SELECT FECHA, CANAL, SUM(VOLUMEN_BRUTO_COP)/1e6 AS VOL_M
        FROM {TABLE}
        WHERE {WHERE}
        GROUP BY 1, 2
        ORDER BY 1, 2
    """)
    if not area_df.empty:
        fig_area = go.Figure()
        canal_list = sorted(area_df["CANAL"].unique().tolist())
        for i, canal in enumerate(canal_list):
            sub = area_df[area_df["CANAL"] == canal].sort_values("FECHA")
            fig_area.add_trace(go.Scatter(
                x=sub["FECHA"].tolist(), y=sub["VOL_M"].tolist(),
                name=canal, stackgroup="one",
                line=dict(color=COLORS[i % len(COLORS)], width=0.5),
                hovertemplate=f"<b>{canal}</b><br>Fecha: %{{x}}<br>Volumen: $%{{y:,.0f}}M COP<extra></extra>",
            ))
        fig_area.update_layout(
            template="plotly_white", paper_bgcolor="#FFFFFF", height=420,
            margin=dict(l=80, r=20, t=10, b=60),
            yaxis=dict(title="Volumen ($M COP)"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        st.plotly_chart(fig_area, use_container_width=True, key="vol_stacked_area")
