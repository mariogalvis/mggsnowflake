import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from app_pages.map_helper import colombia_scatter_map
from decimal import Decimal

TABLE = "MGG_PAGOS.ANALITICA_DE_PAGOS.TENDENCIAS_CONSUMO"

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
    categorias = run_query(f"SELECT DISTINCT CATEGORIA_GASTO FROM {TABLE} ORDER BY 1")["CATEGORIA_GASTO"].tolist()
    ciudades = run_query(f"SELECT DISTINCT CIUDAD FROM {TABLE} ORDER BY 1")["CIUDAD"].tolist()
    segmentos = run_query(f"SELECT DISTINCT SEGMENTO_CONSUMIDOR FROM {TABLE} ORDER BY 1")["SEGMENTO_CONSUMIDOR"].tolist()
    tendencias = run_query(f"SELECT DISTINCT TENDENCIA FROM {TABLE} ORDER BY 1")["TENDENCIA"].tolist()
    fechas = run_query(f"SELECT MIN(FECHA_PERIODO) AS FMIN, MAX(FECHA_PERIODO) AS FMAX FROM {TABLE}")
    return categorias, ciudades, segmentos, tendencias, fechas


st.header(":material/insights: Tendencias de consumo")
st.caption("Patrones de gasto por categoria, estacionalidad, digitalizacion, crecimiento y penetracion en el ecosistema de pagos colombiano.")

c1, c2, c3 = st.columns(3)
with c1:
    with st.container(border=True):
        st.markdown("**:material/lightbulb: Que resuelve**")
        st.markdown("Identificar como evolucionan los habitos de consumo por categoria, segmento y ciudad, detectando tendencias emergentes y cambios de participacion.")
with c2:
    with st.container(border=True):
        st.markdown("**:material/settings: Como funciona**")
        st.markdown("Agrega transacciones por periodo, categoria y segmento. Calcula crecimiento YoY/MoM, estacionalidad, digitalizacion (contactless, e-commerce) e inflacion por categoria.")
with c3:
    with st.container(border=True):
        st.markdown("**:material/trending_up: Valor de negocio**")
        st.markdown("Anticipar cambios en el mix de consumo para ajustar ofertas de adquirencia, priorizar categorias de crecimiento y optimizar la estrategia comercial.")

categorias, ciudades, segmentos, tendencias, fechas_df = get_filter_options()
fmin = pd.to_datetime(fechas_df["FMIN"].iloc[0]).date()
fmax = pd.to_datetime(fechas_df["FMAX"].iloc[0]).date()

dx_kpi = run_query(f"""
    SELECT
        SUM(NUMERO_TRANSACCIONES) AS TOTAL_TX,
        ROUND(SUM(VOLUMEN_COP)/1e9, 2) AS VOL_B,
        ROUND(AVG(TICKET_PROMEDIO_COP), 0) AS TICKET_PROM,
        ROUND(AVG(CRECIMIENTO_YOY_PCT), 1) AS YOY,
        ROUND(AVG(INDICE_DIGITALIZACION)*100, 1) AS DIGIT_IDX,
        ROUND(AVG(PCT_CONTACTLESS)*100, 1) AS CTLS,
        ROUND(AVG(PCT_ECOMMERCE)*100, 1) AS ECOM,
        ROUND(AVG(PENETRACION_DIGITAL_PCT), 1) AS PEN_DIG,
        COUNT(DISTINCT CATEGORIA_GASTO) AS N_CAT
    FROM {TABLE}
""")

if not dx_kpi.empty and dx_kpi["TOTAL_TX"].iloc[0] > 0:
    dx = dx_kpi.iloc[0]
    dx_yoy = float(dx["YOY"])
    dx_digit = float(dx["DIGIT_IDX"])
    dx_ecom = float(dx["ECOM"])

    dx_top_cat = run_query(f"SELECT CATEGORIA_GASTO, SUM(VOLUMEN_COP) AS V FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC LIMIT 1")
    dx_fastest = run_query(f"SELECT CATEGORIA_GASTO, ROUND(AVG(CRECIMIENTO_YOY_PCT), 1) AS YOY FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC LIMIT 1")

    if dx_yoy >= 10:
        dx_estado = f"Consumo creciendo ({color_tag(dx_yoy, 10, 5, fmt='{:+.1f}%')}) :green[**dinamicamente**]. Digitalizacion: {dx_digit:.1f}%."
    elif dx_yoy >= 0:
        dx_estado = f"Consumo con crecimiento ({color_tag(dx_yoy, 10, 5, fmt='{:+.1f}%')}) :orange[**moderado**]. E-commerce: {dx_ecom:.1f}%."
    else:
        dx_estado = f"Consumo ({color_tag(dx_yoy, 10, 5, fmt='{:+.1f}%')}) :red[**en contraccion**]. Revisar categorias afectadas."

    dx_lines = [dx_estado]
    if not dx_top_cat.empty:
        dx_lines.append(f"- Categoria lider: **{dx_top_cat.iloc[0]['CATEGORIA_GASTO']}** (mayor volumen)")
    if not dx_fastest.empty:
        dx_lines.append(f"- Mayor crecimiento YoY: **{dx_fastest.iloc[0]['CATEGORIA_GASTO']}** ({float(dx_fastest.iloc[0]['YOY']):+.1f}%)")
    dx_lines.append(f"- Penetracion digital: **{float(dx['PEN_DIG']):.1f}%** | Contactless: **{float(dx['CTLS']):.1f}%**")

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
            if dx_digit < 50:
                recs.append("1. :orange[**Acelerar digitalizacion**]: indice por debajo del 50%. Incentivar pagos digitales.")
            if dx_ecom < 30:
                recs.append("2. :orange[**Impulsar e-commerce**]: baja penetracion limita crecimiento del canal digital.")
            if dx_yoy < 5:
                recs.append("3. :red[**Reactivar consumo**]: crecimiento bajo en el ecosistema. Revisar categorias rezagadas.")
            if float(dx["CTLS"]) < 20:
                recs.append("4. :orange[**Promover contactless**]: adopcion incipiente, oportunidad de diferenciacion.")
            if not recs:
                recs.append(":green[**Tendencias de consumo saludables.**] Continuar impulsando digitalizacion y monitorear estacionalidad.")
            st.markdown("\n".join(recs))

st.divider()

fc1, fc2, fc3, fc4 = st.columns(4)
with fc1:
    fecha_rng = st.date_input("Periodo", value=(fmin, fmax), min_value=fmin, max_value=fmax, key="ten_fecha")
    if isinstance(fecha_rng, (list, tuple)) and len(fecha_rng) == 2:
        fecha_inicio, fecha_fin = fecha_rng
    else:
        fecha_inicio, fecha_fin = fmin, fmax
with fc2:
    cat_sel = st.selectbox("Categoria", ["Todos"] + categorias, key="ten_cat")
with fc3:
    ciudad_sel = st.selectbox("Ciudad", ["Todos"] + ciudades, key="ten_ciudad")
with fc4:
    seg_sel = st.selectbox("Segmento", ["Todos"] + segmentos, key="ten_seg")

WHERE = f"FECHA_PERIODO BETWEEN '{fecha_inicio}' AND '{fecha_fin}'"
if cat_sel != "Todos":
    WHERE += f" AND CATEGORIA_GASTO = '{cat_sel}'"
if ciudad_sel != "Todos":
    WHERE += f" AND CIUDAD = '{ciudad_sel}'"
if seg_sel != "Todos":
    WHERE += f" AND SEGMENTO_CONSUMIDOR = '{seg_sel}'"

kpi_df = run_query(f"""
    SELECT
        SUM(NUMERO_TRANSACCIONES) AS TOTAL_TX,
        ROUND(SUM(VOLUMEN_COP)/1e9, 2) AS VOL_B,
        ROUND(AVG(TICKET_PROMEDIO_COP), 0) AS TICKET_PROM,
        ROUND(AVG(CRECIMIENTO_YOY_PCT), 1) AS YOY,
        ROUND(AVG(CRECIMIENTO_MOM_PCT), 1) AS MOM,
        ROUND(AVG(INDICE_DIGITALIZACION)*100, 1) AS DIGIT_IDX,
        ROUND(AVG(PCT_CONTACTLESS)*100, 1) AS CTLS,
        ROUND(AVG(PCT_ECOMMERCE)*100, 1) AS ECOM,
        ROUND(AVG(PENETRACION_DIGITAL_PCT), 1) AS PEN_DIG,
        ROUND(AVG(TASA_APROBACION_CATEGORIA)*100, 1) AS APROB,
        ROUND(AVG(TASA_FRAUDE_CATEGORIA)*100, 3) AS FRAUDE,
        ROUND(AVG(INFLACION_CATEGORIA_PCT), 1) AS INFLACION,
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
    k2.metric("Volumen", f"${float(r['VOL_B']):.2f}B COP")
    k3.metric("Ticket promedio", f"${float(r['TICKET_PROM']):,.0f} COP")
    k4.metric("Crecimiento YoY", f"{float(r['YOY']):+.1f}%")

    k5, k6, k7, k8 = st.columns(4)
    k5.metric("Digitalizacion", f"{float(r['DIGIT_IDX']):.1f}%")
    k6.metric("% Contactless", f"{float(r['CTLS']):.1f}%")
    k7.metric("% E-commerce", f"{float(r['ECOM']):.1f}%")
    k8.metric("Inflacion prom", f"{float(r['INFLACION']):+.1f}%")

    st.divider()

    st.markdown("**Ranking de categorias — Bump chart (posicion por participacion)**")
    bump_df = run_query(f"""
        SELECT FECHA_PERIODO, CATEGORIA_GASTO,
               ROUND(AVG(PARTICIPACION_CATEGORIA_PCT), 2) AS PART_PCT
        FROM {TABLE}
        WHERE {WHERE}
        GROUP BY 1, 2
        ORDER BY 1, 3 DESC
    """)
    if not bump_df.empty:
        periods = sorted(bump_df["FECHA_PERIODO"].unique().tolist())
        cats_all = bump_df["CATEGORIA_GASTO"].unique().tolist()

        rank_data = {}
        for p in periods:
            sub = bump_df[bump_df["FECHA_PERIODO"] == p].sort_values("PART_PCT", ascending=False)
            for rank_pos, (_, row) in enumerate(sub.iterrows(), 1):
                cat = row["CATEGORIA_GASTO"]
                if cat not in rank_data:
                    rank_data[cat] = {"periods": [], "ranks": []}
                rank_data[cat]["periods"].append(p)
                rank_data[cat]["ranks"].append(rank_pos)

        fig_bump = go.Figure()
        for i, (cat, data) in enumerate(rank_data.items()):
            fig_bump.add_trace(go.Scatter(
                x=data["periods"], y=data["ranks"],
                mode="lines+markers+text",
                name=cat,
                text=[cat if j == len(data["periods"]) - 1 else "" for j in range(len(data["periods"]))],
                textposition="middle right",
                textfont=dict(size=9),
                line=dict(color=COLORS[i % len(COLORS)], width=2),
                marker=dict(size=8, color=COLORS[i % len(COLORS)]),
                hovertemplate=f"<b>{cat}</b><br>Periodo: %{{x}}<br>Posicion: #%{{y}}<extra></extra>",
            ))
        max_rank = max([r for d in rank_data.values() for r in d["ranks"]], default=10)
        fig_bump.update_layout(
            template="plotly_white", paper_bgcolor="#FFFFFF", height=480,
            margin=dict(l=60, r=140, t=10, b=60),
            yaxis=dict(title="Posicion (1=lider)", autorange="reversed", dtick=1),
            xaxis=dict(tickangle=-30),
            showlegend=False,
        )
        st.plotly_chart(fig_bump, use_container_width=True, key="ten_bump")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Evolucion del volumen por categoria**")
        area_df = run_query(f"""
            SELECT FECHA_PERIODO, CATEGORIA_GASTO, SUM(VOLUMEN_COP)/1e6 AS VOL_M
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1, 2
            ORDER BY 1, 2
        """)
        if not area_df.empty:
            fig_area = go.Figure()
            cat_list = sorted(area_df["CATEGORIA_GASTO"].unique().tolist())
            for i, cat in enumerate(cat_list):
                sub = area_df[area_df["CATEGORIA_GASTO"] == cat].sort_values("FECHA_PERIODO")
                fig_area.add_trace(go.Scatter(
                    x=sub["FECHA_PERIODO"].tolist(), y=sub["VOL_M"].tolist(),
                    name=cat, stackgroup="one",
                    line=dict(color=COLORS[i % len(COLORS)], width=0.5),
                    hovertemplate=f"<b>{cat}</b><br>%{{x}}<br>${{y:,.0f}}M COP<extra></extra>",
                ))
            fig_area.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=420,
                margin=dict(l=80, r=20, t=10, b=60),
                yaxis=dict(title="Volumen ($M COP)"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, font=dict(size=9)),
            )
            st.plotly_chart(fig_area, use_container_width=True, key="ten_stacked_area")

    with col2:
        st.markdown("**Radar de digitalizacion por segmento**")
        radar_df = run_query(f"""
            SELECT SEGMENTO_CONSUMIDOR,
                   ROUND(AVG(INDICE_DIGITALIZACION)*100, 1) AS DIGIT,
                   ROUND(AVG(PCT_CONTACTLESS)*100, 1) AS CTLS,
                   ROUND(AVG(PCT_ECOMMERCE)*100, 1) AS ECOM,
                   ROUND(AVG(PENETRACION_DIGITAL_PCT), 1) AS PEN
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1
        """)
        if not radar_df.empty:
            cats_r = ["Digitalizacion", "Contactless", "E-commerce", "Penetracion digital"]
            fig_radar = go.Figure()
            for i, (_, row) in enumerate(radar_df.iterrows()):
                vals = [float(row["DIGIT"]), float(row["CTLS"]), float(row["ECOM"]), float(row["PEN"])]
                fig_radar.add_trace(go.Scatterpolar(
                    r=vals + [vals[0]],
                    theta=cats_r + [cats_r[0]],
                    name=str(row["SEGMENTO_CONSUMIDOR"]),
                    line=dict(color=COLORS[i % len(COLORS)], width=2),
                    fill="toself", opacity=0.25,
                ))
            fig_radar.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=420,
                margin=dict(l=60, r=60, t=30, b=30),
                polar=dict(radialaxis=dict(range=[0, 100], ticksuffix="%")),
                legend=dict(orientation="h", yanchor="bottom", y=-0.15, font=dict(size=10)),
            )
            st.plotly_chart(fig_radar, use_container_width=True, key="ten_radar")

    st.markdown("**Mapa — Volumen de consumo por ciudad**")
    map_df = run_query(f"""
        SELECT CIUDAD, SUM(NUMERO_TRANSACCIONES) AS N, ROUND(SUM(VOLUMEN_COP)/1e6, 1) AS VOL_M
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
            st.pydeck_chart(deck, key="ten_map")

    col3, col4 = st.columns(2)

    with col3:
        st.markdown("**Portafolio BCG — Crecimiento vs participacion**")
        bcg_df = run_query(f"""
            SELECT CATEGORIA_GASTO,
                   ROUND(AVG(CRECIMIENTO_YOY_PCT), 1) AS YOY,
                   ROUND(AVG(PARTICIPACION_CATEGORIA_PCT), 2) AS PART,
                   SUM(VOLUMEN_COP)/1e6 AS VOL_M
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1
        """)
        if not bcg_df.empty:
            fig_bcg = go.Figure()
            max_vol = max(bcg_df["VOL_M"].max(), 1)
            for i, (_, row) in enumerate(bcg_df.iterrows()):
                sz = max(10, (float(row["VOL_M"]) / max_vol) * 50)
                fig_bcg.add_trace(go.Scatter(
                    x=[float(row["YOY"])], y=[float(row["PART"])],
                    mode="markers+text",
                    text=[str(row["CATEGORIA_GASTO"])],
                    textposition="top center",
                    textfont=dict(size=9),
                    marker=dict(size=sz, color=COLORS[i % len(COLORS)], opacity=0.7,
                                line=dict(width=1, color="#11567F")),
                    showlegend=False,
                    hovertemplate=f"<b>{row['CATEGORIA_GASTO']}</b><br>YoY: {float(row['YOY']):+.1f}%<br>Participacion: {float(row['PART']):.2f}%<br>Volumen: ${float(row['VOL_M']):,.0f}M<extra></extra>",
                ))
            yoy_med = bcg_df["YOY"].median()
            part_med = bcg_df["PART"].median()
            fig_bcg.add_hline(y=part_med, line_dash="dash", line_color="#999", opacity=0.5)
            fig_bcg.add_vline(x=yoy_med, line_dash="dash", line_color="#999", opacity=0.5)
            fig_bcg.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=420,
                margin=dict(l=60, r=20, t=10, b=60),
                xaxis=dict(title="Crecimiento YoY (%)"),
                yaxis=dict(title="Participacion categoria (%)"),
            )
            st.plotly_chart(fig_bcg, use_container_width=True, key="ten_bcg")

    with col4:
        st.markdown("**Indice de estacionalidad por tipo**")
        season_df = run_query(f"""
            SELECT ESTACIONALIDAD, ROUND(AVG(INDICE_ESTACIONALIDAD), 2) AS IDX
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1
            ORDER BY 2 DESC
        """)
        if not season_df.empty:
            colors_bar = [COLORS[i % len(COLORS)] for i in range(len(season_df))]
            fig_season = go.Figure(go.Bar(
                x=season_df["ESTACIONALIDAD"].tolist(),
                y=season_df["IDX"].tolist(),
                marker=dict(color=colors_bar),
                text=[f"{v:.2f}" for v in season_df["IDX"].tolist()],
                textposition="outside",
                hovertemplate="<b>%{x}</b><br>Indice: %{y:.2f}<extra></extra>",
            ))
            fig_season.add_hline(y=1.0, line_dash="dash", line_color="#DE350B", opacity=0.5,
                                 annotation_text="Baseline (1.0)")
            fig_season.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=420,
                margin=dict(l=60, r=20, t=10, b=80),
                yaxis=dict(title="Indice estacionalidad"),
                xaxis=dict(tickangle=-30, tickfont=dict(size=11)),
            )
            st.plotly_chart(fig_season, use_container_width=True, key="ten_seasonality")
