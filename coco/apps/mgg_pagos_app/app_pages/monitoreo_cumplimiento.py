import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from decimal import Decimal

TABLE = "MGG_PAGOS.CUMPLIMIENTO_Y_ESQUEMAS.MONITOREO_CUMPLIMIENTO"

COLORS = ["#29B5E8", "#11567F", "#71D4F0", "#0E3A53", "#A3E4F7", "#1B8BBF", "#5BC3E8", "#083248"]


from app_pages.conn_helper import run_query



@st.cache_data(ttl=300, show_spinner=False)
def get_filters():
    tipos = run_query(f"SELECT DISTINCT TIPO_EVENTO FROM {TABLE} ORDER BY 1")["TIPO_EVENTO"].tolist()
    estados = run_query(f"SELECT DISTINCT ESTADO FROM {TABLE} ORDER BY 1")["ESTADO"].tolist()
    reguladores = run_query(f"SELECT DISTINCT REGULADOR_DESTINO FROM {TABLE} ORDER BY 1")["REGULADOR_DESTINO"].tolist()
    origenes = run_query(f"SELECT DISTINCT ORIGEN_DETECCION FROM {TABLE} ORDER BY 1")["ORIGEN_DETECCION"].tolist()
    categorias = run_query(f"SELECT DISTINCT CATEGORIA_CUMPLIMIENTO FROM {TABLE} ORDER BY 1")["CATEGORIA_CUMPLIMIENTO"].tolist()
    fechas = run_query(f"SELECT MIN(FECHA_HORA)::DATE AS FMIN, MAX(FECHA_HORA)::DATE AS FMAX FROM {TABLE}")
    return tipos, estados, reguladores, origenes, categorias, fechas


def build_where(fi, ff, tipo_s, estado_s, reg_s, origen_s, cat_s, all_t, all_e, all_r, all_o, all_c):
    clauses = [f"FECHA_HORA::DATE BETWEEN '{fi}' AND '{ff}'"]
    if tipo_s and tipo_s != "Todos" and tipo_s in all_t:
        clauses.append(f"TIPO_EVENTO = '{tipo_s}'")
    if estado_s and estado_s != "Todos" and estado_s in all_e:
        clauses.append(f"ESTADO = '{estado_s}'")
    if reg_s and reg_s != "Todos" and reg_s in all_r:
        clauses.append(f"REGULADOR_DESTINO = '{reg_s}'")
    if origen_s and origen_s != "Todos" and origen_s in all_o:
        clauses.append(f"ORIGEN_DETECCION = '{origen_s}'")
    if cat_s and cat_s != "Todos" and cat_s in all_c:
        clauses.append(f"CATEGORIA_CUMPLIMIENTO = '{cat_s}'")
    return " AND ".join(clauses)


def color_tag(value, good, bad, fmt="{:.1f}%", inverse=False):
    v = float(value)
    if not inverse:
        return f":green[**{fmt.format(v)}**]" if v >= good else f":orange[**{fmt.format(v)}**]" if v >= bad else f":red[**{fmt.format(v)}**]"
    else:
        return f":green[**{fmt.format(v)}**]" if v <= good else f":orange[**{fmt.format(v)}**]" if v <= bad else f":red[**{fmt.format(v)}**]"


st.header(":material/shield: Monitoreo de cumplimiento")
st.caption("Pipeline regulatorio: eventos de monitoreo, ROS, reportes a reguladores, analisis de falsos positivos y desempeno por analista.")

c1, c2, c3 = st.columns(3)
with c1:
    with st.container(border=True):
        st.markdown("**:material/lightbulb: Que resuelve**")
        st.markdown("Riesgo de incumplimiento regulatorio por retrasos en reportes, baja calidad de deteccion y carga desbalanceada en el equipo de cumplimiento.")
with c2:
    with st.container(border=True):
        st.markdown("**:material/settings: Como funciona**")
        st.markdown("Registra cada evento de monitoreo desde la deteccion hasta el reporte regulatorio. Mide tiempos de revision, tasas de falso positivo y cumplimiento de SLAs.")
with c3:
    with st.container(border=True):
        st.markdown("**:material/trending_up: Valor de negocio**")
        st.markdown("Evitar sanciones regulatorias, optimizar la carga de trabajo del equipo de cumplimiento y reducir falsos positivos para enfocar recursos en hallazgos reales.")

tipos, estados, reguladores, origenes, categorias, fechas_df = get_filters()
fmin = pd.to_datetime(fechas_df["FMIN"].iloc[0]).date()
fmax = pd.to_datetime(fechas_df["FMAX"].iloc[0]).date()

dx_w = f"FECHA_HORA::DATE BETWEEN '{fmin}' AND '{fmax}'"
dx_kpi = run_query(f"""
    SELECT ROUND(AVG(CASE WHEN DENTRO_PLAZO THEN 1.0 ELSE 0.0 END)*100,1) AS PCT_PLAZO,
           ROUND(AVG(CASE WHEN FALSO_POSITIVO THEN 1.0 ELSE 0.0 END)*100,1) AS PCT_FP,
           ROUND(AVG(CASE WHEN HALLAZGO_RELEVANTE THEN 1.0 ELSE 0.0 END)*100,1) AS PCT_HALLAZGO,
           ROUND(AVG(TIEMPO_REVISION_MIN),1) AS TIEMPO_REV,
           SUM(CASE WHEN ROS_GENERADO THEN 1 ELSE 0 END) AS TOTAL_ROS,
           COUNT(*) AS TOTAL
    FROM {TABLE} WHERE {dx_w}
""")

if not dx_kpi.empty and dx_kpi["TOTAL"].iloc[0] > 0:
    d = dx_kpi.iloc[0]
    dx_plazo = float(d["PCT_PLAZO"])
    dx_fp = float(d["PCT_FP"])
    dx_hallazgo = float(d["PCT_HALLAZGO"])

    dx_worst_reg = run_query(f"""
        SELECT REGULADOR_DESTINO, ROUND(AVG(CASE WHEN DENTRO_PLAZO THEN 1.0 ELSE 0.0 END)*100,1) AS PLAZO
        FROM {TABLE} WHERE {dx_w} GROUP BY 1 ORDER BY 2 ASC LIMIT 1
    """)
    dx_best_origen = run_query(f"""
        SELECT ORIGEN_DETECCION, ROUND(AVG(CASE WHEN HALLAZGO_RELEVANTE THEN 1.0 ELSE 0.0 END)*100,1) AS HALLAZGO
        FROM {TABLE} WHERE {dx_w} GROUP BY 1 ORDER BY 2 DESC LIMIT 1
    """)

    dx_lines = []
    if dx_plazo >= 95:
        dx_lines.append(f"El cumplimiento de plazos regulatorios es {color_tag(dx_plazo, 95, 85)}, :green[**excelente**].")
    elif dx_plazo >= 85:
        dx_lines.append(f"El cumplimiento de plazos es {color_tag(dx_plazo, 95, 85)}, :orange[**con oportunidad de mejora**].")
    else:
        dx_lines.append(f"El cumplimiento de plazos es {color_tag(dx_plazo, 95, 85)}, :red[**riesgo de sancion**].")
    dx_lines.append(f"- Tasa de falso positivo: {color_tag(dx_fp, 30, 50, inverse=True)}")
    dx_lines.append(f"- Tasa de hallazgos relevantes: {color_tag(dx_hallazgo, 10, 5)}")
    if not dx_worst_reg.empty:
        dx_lines.append(f"- Regulador con menor plazo: :red[**{dx_worst_reg.iloc[0]['REGULADOR_DESTINO']}**] ({float(dx_worst_reg.iloc[0]['PLAZO']):.1f}%)")
    if not dx_best_origen.empty:
        dx_lines.append(f"- Mejor fuente de deteccion: :green[**{dx_best_origen.iloc[0]['ORIGEN_DETECCION']}**] ({float(dx_best_origen.iloc[0]['HALLAZGO']):.1f}% hallazgos)")

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
            if dx_plazo < 95:
                recs.append("1. :red[**Reducir tiempos de revision**] para cumplir SLAs regulatorios.")
            if dx_fp > 40:
                recs.append("2. :orange[**Calibrar reglas de deteccion**] para reducir falsos positivos y liberar capacidad.")
            recs.append("3. :blue[**Balancear carga de analistas**] para mejorar tiempos de respuesta.")
            recs.append("4. :green[**Automatizar reportes regulatorios**] para reducir riesgo de incumplimiento.")
            st.markdown("\n".join(recs))

st.divider()

fc1, fc2, fc3, fc4, fc5, fc6 = st.columns(6)
with fc1:
    fecha_rng = st.date_input("Periodo", value=(fmin, fmax), min_value=fmin, max_value=fmax, key="mon_fecha")
    if isinstance(fecha_rng, (list, tuple)) and len(fecha_rng) == 2:
        fecha_inicio, fecha_fin = fecha_rng
    else:
        fecha_inicio, fecha_fin = fmin, fmax
with fc2:
    tipo_sel = st.selectbox("Tipo evento", ["Todos"] + tipos, key="mon_tipo")
with fc3:
    estado_sel = st.selectbox("Estado", ["Todos"] + estados, key="mon_estado")
with fc4:
    reg_sel = st.selectbox("Regulador", ["Todos"] + reguladores, key="mon_reg")
with fc5:
    origen_sel = st.selectbox("Origen deteccion", ["Todos"] + origenes, key="mon_origen")
with fc6:
    cat_sel = st.selectbox("Categoria", ["Todos"] + categorias, key="mon_cat")

WHERE = build_where(fecha_inicio, fecha_fin, tipo_sel, estado_sel, reg_sel, origen_sel, cat_sel,
                    tipos, estados, reguladores, origenes, categorias)

kpi_df = run_query(f"""
    SELECT COUNT(*) AS TOTAL_EVENTOS,
           SUM(CASE WHEN ROS_GENERADO THEN 1 ELSE 0 END) AS TOTAL_ROS,
           SUM(CASE WHEN REPORTE_REGULATORIO_ENVIADO THEN 1 ELSE 0 END) AS REPORTES_ENV,
           ROUND(AVG(CASE WHEN DENTRO_PLAZO THEN 1.0 ELSE 0.0 END)*100,1) AS PCT_PLAZO,
           ROUND(AVG(CASE WHEN FALSO_POSITIVO THEN 1.0 ELSE 0.0 END)*100,1) AS PCT_FP,
           ROUND(AVG(CASE WHEN HALLAZGO_RELEVANTE THEN 1.0 ELSE 0.0 END)*100,1) AS PCT_HALLAZGO,
           ROUND(AVG(TIEMPO_REVISION_MIN),1) AS TIEMPO_REV,
           SUM(MONTO_MONITOREADO_COP) AS MONTO_TOTAL
    FROM {TABLE} WHERE {WHERE}
""")

if kpi_df.empty or kpi_df["TOTAL_EVENTOS"].iloc[0] == 0:
    st.info("No hay datos para los filtros seleccionados.")
else:
    r = kpi_df.iloc[0]

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total eventos", f"{int(r['TOTAL_EVENTOS']):,}")
    k2.metric("ROS generados", f"{int(r['TOTAL_ROS']):,}")
    k3.metric("Reportes enviados", f"{int(r['REPORTES_ENV']):,}")
    k4.metric("Cumplimiento plazo", f"{float(r['PCT_PLAZO']):.1f}%")

    k5, k6, k7, k8 = st.columns(4)
    k5.metric("Falsos positivos", f"{float(r['PCT_FP']):.1f}%")
    k6.metric("Hallazgos relevantes", f"{float(r['PCT_HALLAZGO']):.1f}%")
    k7.metric("Tiempo revision prom.", f"{float(r['TIEMPO_REV']):.1f} min")
    k8.metric("Monto monitoreado", f"${float(r['MONTO_TOTAL'])/1e9:.1f}B COP")

    st.divider()

    st.markdown("**Pipeline Regulatorio**")
    pipe_df = run_query(f"""
        SELECT COUNT(*) AS TOTAL,
               SUM(CASE WHEN ESTADO IN ('En revision','Cerrado','Escalado') THEN 1 ELSE 0 END) AS EN_REVISION,
               SUM(CASE WHEN ROS_GENERADO THEN 1 ELSE 0 END) AS ROS,
               SUM(CASE WHEN REPORTE_REGULATORIO_ENVIADO THEN 1 ELSE 0 END) AS REPORTES,
               SUM(CASE WHEN DENTRO_PLAZO THEN 1 ELSE 0 END) AS DENTRO_PLAZO
        FROM {TABLE} WHERE {WHERE}
    """)
    if not pipe_df.empty:
        p = pipe_df.iloc[0]
        steps = ["Total eventos", "En revision/cerrado", "ROS generado", "Reporte enviado", "Dentro de plazo"]
        vals = [int(p["TOTAL"]), int(p["EN_REVISION"]), int(p["ROS"]), int(p["REPORTES"]), int(p["DENTRO_PLAZO"])]
        fig_funnel = go.Figure(go.Funnel(
            y=steps, x=vals,
            textinfo="value+percent initial",
            texttemplate="%{value:,} (%{percentInitial:.1%})",
            marker=dict(color=[COLORS[0], COLORS[5], "#FFAB00", "#36B37E", COLORS[1]],
                        line=dict(width=1, color="white")),
            connector=dict(line=dict(color="#DFE1E6", width=1)),
        ))
        fig_funnel.update_layout(
            template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
            margin=dict(l=180, r=20, t=30, b=40),
        )
        st.plotly_chart(fig_funnel, use_container_width=True, key="mon_funnel")

    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("**Carga de Trabajo por Analista**")
        analyst_df = run_query(f"""
            SELECT ANALISTA_ASIGNADO, COUNT(*) AS CASOS,
                   ROUND(AVG(TIEMPO_REVISION_MIN),1) AS TIEMPO_PROM
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 15
        """)
        if not analyst_df.empty:
            fig_an = go.Figure()
            fig_an.add_trace(go.Bar(
                x=analyst_df["ANALISTA_ASIGNADO"].tolist(),
                y=[int(v) for v in analyst_df["CASOS"]],
                name="Casos asignados",
                marker=dict(color=COLORS[0]),
                hovertemplate="%{x}: %{y} casos<extra></extra>",
            ))
            fig_an.add_trace(go.Scatter(
                x=analyst_df["ANALISTA_ASIGNADO"].tolist(),
                y=[float(v) for v in analyst_df["TIEMPO_PROM"]],
                name="Tiempo prom. (min)",
                yaxis="y2",
                mode="lines+markers",
                line=dict(color="#DE350B", width=2),
                marker=dict(size=7),
                hovertemplate="%{x}: %{y:.1f} min<extra></extra>",
            ))
            fig_an.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=50, r=50, t=30, b=100),
                xaxis=dict(tickangle=-45, tickfont=dict(size=9)),
                yaxis=dict(title="Numero de casos"),
                yaxis2=dict(title="Tiempo revision (min)", overlaying="y", side="right"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig_an, use_container_width=True, key="mon_analyst")

    with col_b:
        st.markdown("**Falsos Positivos por Origen de Deteccion**")
        fp_df = run_query(f"""
            SELECT ORIGEN_DETECCION,
                   SUM(CASE WHEN FALSO_POSITIVO THEN 1 ELSE 0 END) AS FP,
                   SUM(CASE WHEN NOT FALSO_POSITIVO THEN 1 ELSE 0 END) AS NO_FP
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1 ORDER BY FP DESC
        """)
        if not fp_df.empty:
            fig_fp = go.Figure()
            fig_fp.add_trace(go.Pie(
                labels=fp_df["ORIGEN_DETECCION"].tolist(),
                values=[int(v) for v in fp_df["FP"]],
                hole=0.5,
                marker=dict(colors=COLORS[:len(fp_df)], line=dict(color="white", width=2)),
                textinfo="percent+label", textposition="outside", textfont=dict(size=10),
                hovertemplate="<b>%{label}</b><br>Falsos positivos: %{value:,}<br>%{percent:.1%}<extra></extra>",
            ))
            total_fp = int(fp_df["FP"].sum())
            fig_fp.add_annotation(text=f"<b>{total_fp:,}</b><br>FP totales",
                                  x=0.5, y=0.5, showarrow=False, font=dict(size=12, color="#11567F"))
            fig_fp.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=5, r=5, t=30, b=5), showlegend=False,
            )
            st.plotly_chart(fig_fp, use_container_width=True, key="mon_fp_pie")

    col_c, col_d = st.columns(2)

    with col_c:
        st.markdown("**Distribucion Score Riesgo por Hallazgo**")
        risk_df = run_query(f"""
            SELECT FLOOR(SCORE_RIESGO/10)*10 AS BIN,
                   SUM(CASE WHEN HALLAZGO_RELEVANTE THEN 1 ELSE 0 END) AS RELEVANTE,
                   SUM(CASE WHEN NOT HALLAZGO_RELEVANTE THEN 1 ELSE 0 END) AS NO_RELEVANTE
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1 ORDER BY 1
        """)
        if not risk_df.empty:
            bins = [f"{int(v)}-{int(v)+9}" for v in risk_df["BIN"]]
            fig_risk = go.Figure()
            fig_risk.add_trace(go.Bar(
                x=bins, y=[int(v) for v in risk_df["RELEVANTE"]],
                name="Hallazgo relevante", marker=dict(color="#DE350B"),
            ))
            fig_risk.add_trace(go.Bar(
                x=bins, y=[int(v) for v in risk_df["NO_RELEVANTE"]],
                name="No relevante", marker=dict(color=COLORS[0]),
            ))
            fig_risk.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=50, r=20, t=30, b=50), barmode="stack",
                xaxis=dict(title="Score Riesgo"), yaxis=dict(title="Eventos"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig_risk, use_container_width=True, key="mon_risk_dist")

    with col_d:
        st.markdown("**Cumplimiento de Plazo por Regulador**")
        sla_df = run_query(f"""
            SELECT REGULADOR_DESTINO,
                   ROUND(AVG(CASE WHEN DENTRO_PLAZO THEN 1.0 ELSE 0.0 END)*100,1) AS PCT_PLAZO,
                   COUNT(*) AS EVENTOS
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1 ORDER BY 2 ASC
        """)
        if not sla_df.empty:
            bar_colors = ["#36B37E" if float(v) >= 95 else "#FFAB00" if float(v) >= 85 else "#DE350B" for v in sla_df["PCT_PLAZO"]]
            fig_sla = go.Figure(go.Bar(
                x=sla_df["REGULADOR_DESTINO"].tolist(),
                y=[float(v) for v in sla_df["PCT_PLAZO"]],
                marker=dict(color=bar_colors, line=dict(width=1, color="white")),
                text=[f"{float(v):.1f}%" for v in sla_df["PCT_PLAZO"]],
                textposition="outside",
                hovertemplate="<b>%{x}</b><br>Dentro de plazo: %{y:.1f}%<extra></extra>",
            ))
            fig_sla.add_hline(y=95, line_dash="dash", line_color="#36B37E", annotation_text="Objetivo 95%")
            fig_sla.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=50, r=20, t=30, b=80),
                xaxis=dict(tickangle=-30), yaxis=dict(title="% Dentro de plazo", range=[0, 105]),
            )
            st.plotly_chart(fig_sla, use_container_width=True, key="mon_sla")

    st.divider()

    col_e, col_f = st.columns(2)

    with col_e:
        st.markdown("**Efectividad por Origen de Deteccion**")
        det_df = run_query(f"""
            SELECT ORIGEN_DETECCION,
                   COUNT(*) AS TOTAL,
                   ROUND(AVG(CASE WHEN HALLAZGO_RELEVANTE THEN 1.0 ELSE 0.0 END)*100,1) AS HALLAZGO_PCT,
                   ROUND(AVG(CASE WHEN FALSO_POSITIVO THEN 1.0 ELSE 0.0 END)*100,1) AS FP_PCT
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1 ORDER BY 3 DESC
        """)
        if not det_df.empty:
            fig_det = go.Figure()
            fig_det.add_trace(go.Bar(
                y=det_df["ORIGEN_DETECCION"].tolist(),
                x=[float(v) for v in det_df["HALLAZGO_PCT"]],
                name="% Hallazgo relevante", orientation="h",
                marker=dict(color="#36B37E"),
            ))
            fig_det.add_trace(go.Bar(
                y=det_df["ORIGEN_DETECCION"].tolist(),
                x=[float(v) for v in det_df["FP_PCT"]],
                name="% Falso positivo", orientation="h",
                marker=dict(color="#DE350B"),
            ))
            fig_det.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=140, r=20, t=30, b=50), barmode="group",
                xaxis=dict(title="Porcentaje (%)"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig_det, use_container_width=True, key="mon_detection")

    with col_f:
        st.markdown("**Heatmap — Tipo Evento vs Accion Tomada**")
        heat_df = run_query(f"""
            SELECT TIPO_EVENTO, ACCION_TOMADA, COUNT(*) AS CNT
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1,2
        """)
        if not heat_df.empty:
            piv = heat_df.pivot_table(index="TIPO_EVENTO", columns="ACCION_TOMADA", values="CNT", fill_value=0)
            fig_heat = go.Figure(go.Heatmap(
                z=piv.values.tolist(),
                x=piv.columns.tolist(),
                y=piv.index.tolist(),
                colorscale=[[0, "#E8F5E9"], [0.5, "#FFF3E0"], [1, "#FFCDD2"]],
                text=piv.values.tolist(),
                texttemplate="%{text:,}",
                textfont=dict(size=11),
                hovertemplate="Evento: %{y}<br>Accion: %{x}<br>Cantidad: %{z:,}<extra></extra>",
            ))
            fig_heat.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=140, r=20, t=30, b=80),
                xaxis=dict(tickangle=-30),
            )
            st.plotly_chart(fig_heat, use_container_width=True, key="mon_heatmap")
