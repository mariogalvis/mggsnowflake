import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from decimal import Decimal

TABLE = "MGG_PAGOS.CUMPLIMIENTO_Y_ESQUEMAS.AUDITORIA_PAGOS"

COLORS = ["#29B5E8", "#11567F", "#71D4F0", "#0E3A53", "#A3E4F7", "#1B8BBF", "#5BC3E8", "#083248"]

SEV_COLORS = {"Critica": "#DE350B", "Alta": "#FF8B00", "Media": "#FFAB00", "Baja": "#29B5E8", "Info": "#A3E4F7"}


from app_pages.conn_helper import run_query



@st.cache_data(ttl=300, show_spinner=False)
def get_filters():
    tipos = run_query(f"SELECT DISTINCT TIPO_EVENTO FROM {TABLE} ORDER BY 1")["TIPO_EVENTO"].tolist()
    sistemas = run_query(f"SELECT DISTINCT SISTEMA_ORIGEN FROM {TABLE} ORDER BY 1")["SISTEMA_ORIGEN"].tolist()
    severidades = run_query(f"SELECT DISTINCT SEVERIDAD FROM {TABLE} ORDER BY 1")["SEVERIDAD"].tolist()
    categorias = run_query(f"SELECT DISTINCT CATEGORIA_CONTROL FROM {TABLE} ORDER BY 1")["CATEGORIA_CONTROL"].tolist()
    fechas = run_query(f"SELECT MIN(FECHA_HORA)::DATE AS FMIN, MAX(FECHA_HORA)::DATE AS FMAX FROM {TABLE}")
    return tipos, sistemas, severidades, categorias, fechas


def build_where(fi, ff, tipo_s, sist_s, sev_s, cat_s, all_t, all_s, all_sv, all_c):
    clauses = [f"FECHA_HORA::DATE BETWEEN '{fi}' AND '{ff}'"]
    if tipo_s and tipo_s != "Todos" and tipo_s in all_t:
        clauses.append(f"TIPO_EVENTO = '{tipo_s}'")
    if sist_s and sist_s != "Todos" and sist_s in all_s:
        clauses.append(f"SISTEMA_ORIGEN = '{sist_s}'")
    if sev_s and sev_s != "Todos" and sev_s in all_sv:
        clauses.append(f"SEVERIDAD = '{sev_s}'")
    if cat_s and cat_s != "Todos" and cat_s in all_c:
        clauses.append(f"CATEGORIA_CONTROL = '{cat_s}'")
    return " AND ".join(clauses)


def color_tag(value, good, bad, fmt="{:.1f}%", inverse=False):
    v = float(value)
    if not inverse:
        return f":green[**{fmt.format(v)}**]" if v >= good else f":orange[**{fmt.format(v)}**]" if v >= bad else f":red[**{fmt.format(v)}**]"
    else:
        return f":green[**{fmt.format(v)}**]" if v <= good else f":orange[**{fmt.format(v)}**]" if v <= bad else f":red[**{fmt.format(v)}**]"


st.header(":material/history: Auditoria de pagos")
st.caption("Registro inmutable de eventos de auditoria del ecosistema de pagos. Anomalias, accesos fuera de horario, datos sensibles y pipeline de investigacion.")

c1, c2, c3 = st.columns(3)
with c1:
    with st.container(border=True):
        st.markdown("**:material/lightbulb: Que resuelve**")
        st.markdown("Trazabilidad completa de acciones en el sistema de pagos para cumplimiento normativo, deteccion de anomalias y soporte a investigaciones internas.")
with c2:
    with st.container(border=True):
        st.markdown("**:material/settings: Como funciona**")
        st.markdown("Captura eventos por sistema, actor, severidad y tipo de accion. Detecta anomalias, accesos fuera de horario y acceso a datos sensibles. Exporta a SIEM.")
with c3:
    with st.container(border=True):
        st.markdown("**:material/trending_up: Valor de negocio**")
        st.markdown("Cumplimiento de marcos regulatorios (PCI DSS, SOX), deteccion temprana de amenazas internas y soporte forense para investigaciones.")

tipos, sistemas, severidades, categorias, fechas_df = get_filters()
fmin = pd.to_datetime(fechas_df["FMIN"].iloc[0]).date()
fmax = pd.to_datetime(fechas_df["FMAX"].iloc[0]).date()

dx_w = f"FECHA_HORA::DATE BETWEEN '{fmin}' AND '{fmax}'"
dx_kpi = run_query(f"""
    SELECT ROUND(AVG(CASE WHEN ANOMALIA_DETECTADA THEN 1.0 ELSE 0.0 END)*100,1) AS PCT_ANOMALIA,
           ROUND(AVG(CASE WHEN FUERA_HORARIO_LABORAL THEN 1.0 ELSE 0.0 END)*100,1) AS PCT_FUERA_HORARIO,
           ROUND(AVG(CASE WHEN DATOS_SENSIBLES THEN 1.0 ELSE 0.0 END)*100,1) AS PCT_SENSIBLES,
           ROUND(AVG(CASE WHEN REQUIERE_INVESTIGACION THEN 1.0 ELSE 0.0 END)*100,1) AS PCT_INVESTIGACION,
           COUNT(*) AS TOTAL
    FROM {TABLE} WHERE {dx_w}
""")

if not dx_kpi.empty and dx_kpi["TOTAL"].iloc[0] > 0:
    d = dx_kpi.iloc[0]
    dx_anomalia = float(d["PCT_ANOMALIA"])
    dx_fuera = float(d["PCT_FUERA_HORARIO"])
    dx_investig = float(d["PCT_INVESTIGACION"])

    dx_top_actor = run_query(f"""
        SELECT ACTOR, COUNT(*) AS N, SUM(CASE WHEN ANOMALIA_DETECTADA THEN 1 ELSE 0 END) AS ANOMALIAS
        FROM {TABLE} WHERE {dx_w}
        GROUP BY 1 ORDER BY 3 DESC LIMIT 1
    """)
    dx_top_sist = run_query(f"""
        SELECT SISTEMA_ORIGEN, SUM(CASE WHEN SEVERIDAD IN ('Critica','Alta') THEN 1 ELSE 0 END) AS CRITICOS
        FROM {TABLE} WHERE {dx_w}
        GROUP BY 1 ORDER BY 2 DESC LIMIT 1
    """)

    dx_lines = []
    if dx_anomalia <= 5:
        dx_lines.append(f"La tasa de anomalias es {color_tag(dx_anomalia, 5, 10, inverse=True)}, :green[**bajo control**].")
    elif dx_anomalia <= 10:
        dx_lines.append(f"La tasa de anomalias es {color_tag(dx_anomalia, 5, 10, inverse=True)}, :orange[**merece atencion**].")
    else:
        dx_lines.append(f"La tasa de anomalias es {color_tag(dx_anomalia, 5, 10, inverse=True)}, :red[**elevada**]. Requiere investigacion.")
    dx_lines.append(f"- Accesos fuera de horario: {color_tag(dx_fuera, 5, 15, inverse=True)}")
    dx_lines.append(f"- Eventos con datos sensibles: **{float(d['PCT_SENSIBLES']):.1f}%**")
    dx_lines.append(f"- Requieren investigacion: :orange[**{dx_investig:.1f}%**]")
    if not dx_top_actor.empty:
        dx_lines.append(f"- Actor con mas anomalias: :red[**{dx_top_actor.iloc[0]['ACTOR']}**] ({int(dx_top_actor.iloc[0]['ANOMALIAS'])} eventos)")
    if not dx_top_sist.empty:
        dx_lines.append(f"- Sistema con mas criticos: :red[**{dx_top_sist.iloc[0]['SISTEMA_ORIGEN']}**] ({int(dx_top_sist.iloc[0]['CRITICOS'])} eventos)")

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
            if dx_anomalia > 5:
                recs.append("1. :red[**Investigar anomalias**] de forma prioritaria, especialmente accesos fuera de horario con datos sensibles.")
            if not dx_top_actor.empty and int(dx_top_actor.iloc[0]['ANOMALIAS']) > 10:
                recs.append(f"2. :orange[**Revisar actor '{dx_top_actor.iloc[0]['ACTOR']}'**]: patron inusual de anomalias detectadas.")
            recs.append("3. :blue[**Asegurar exportacion a SIEM**] para todos los eventos criticos y de alta severidad.")
            recs.append("4. :green[**Mantener integridad de logs**] — verificar hash_integridad periodicamente.")
            st.markdown("\n".join(recs))

st.divider()

fc1, fc2, fc3, fc4, fc5 = st.columns(5)
with fc1:
    fecha_rng = st.date_input("Periodo", value=(fmin, fmax), min_value=fmin, max_value=fmax, key="aud_fecha")
    if isinstance(fecha_rng, (list, tuple)) and len(fecha_rng) == 2:
        fecha_inicio, fecha_fin = fecha_rng
    else:
        fecha_inicio, fecha_fin = fmin, fmax
with fc2:
    tipo_sel = st.selectbox("Tipo evento", ["Todos"] + tipos, key="aud_tipo")
with fc3:
    sist_sel = st.selectbox("Sistema origen", ["Todos"] + sistemas, key="aud_sist")
with fc4:
    sev_sel = st.selectbox("Severidad", ["Todos"] + severidades, key="aud_sev")
with fc5:
    cat_sel = st.selectbox("Categoria control", ["Todos"] + categorias, key="aud_cat")

WHERE = build_where(fecha_inicio, fecha_fin, tipo_sel, sist_sel, sev_sel, cat_sel,
                    tipos, sistemas, severidades, categorias)

kpi_df = run_query(f"""
    SELECT COUNT(*) AS TOTAL_EVENTOS,
           ROUND(AVG(CASE WHEN ANOMALIA_DETECTADA THEN 1.0 ELSE 0.0 END)*100,1) AS PCT_ANOMALIA,
           ROUND(AVG(CASE WHEN FUERA_HORARIO_LABORAL THEN 1.0 ELSE 0.0 END)*100,1) AS PCT_FUERA,
           ROUND(AVG(CASE WHEN DATOS_SENSIBLES THEN 1.0 ELSE 0.0 END)*100,1) AS PCT_SENSIBLES,
           ROUND(AVG(CASE WHEN REQUIERE_INVESTIGACION THEN 1.0 ELSE 0.0 END)*100,1) AS PCT_INVEST,
           ROUND(AVG(CASE WHEN EXPORTADO_SIEM THEN 1.0 ELSE 0.0 END)*100,1) AS PCT_SIEM,
           ROUND(AVG(CASE WHEN DOBLE_AUTORIZACION THEN 1.0 ELSE 0.0 END)*100,1) AS PCT_DOBLE_AUTH,
           COUNT(DISTINCT ACTOR) AS ACTORES_UNICOS
    FROM {TABLE} WHERE {WHERE}
""")

if kpi_df.empty or kpi_df["TOTAL_EVENTOS"].iloc[0] == 0:
    st.info("No hay datos para los filtros seleccionados.")
else:
    r = kpi_df.iloc[0]

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total eventos", f"{int(r['TOTAL_EVENTOS']):,}")
    k2.metric("% Anomalias", f"{float(r['PCT_ANOMALIA']):.1f}%")
    k3.metric("% Fuera horario", f"{float(r['PCT_FUERA']):.1f}%")
    k4.metric("% Datos sensibles", f"{float(r['PCT_SENSIBLES']):.1f}%")

    k5, k6, k7, k8 = st.columns(4)
    k5.metric("% Requiere investigacion", f"{float(r['PCT_INVEST']):.1f}%")
    k6.metric("% Exportado a SIEM", f"{float(r['PCT_SIEM']):.1f}%")
    k7.metric("% Doble autorizacion", f"{float(r['PCT_DOBLE_AUTH']):.1f}%")
    k8.metric("Actores unicos", f"{int(r['ACTORES_UNICOS']):,}")

    st.divider()

    st.markdown("**Timeline de Eventos por Severidad**")
    tl_df = run_query(f"""
        SELECT FECHA_HORA::DATE AS FECHA, SEVERIDAD, COUNT(*) AS CNT,
               SUM(CASE WHEN REQUIERE_INVESTIGACION THEN 1 ELSE 0 END) AS INVEST
        FROM {TABLE} WHERE {WHERE}
        GROUP BY 1,2 ORDER BY 1
    """)
    if not tl_df.empty:
        fig_tl = go.Figure()
        for sev in tl_df["SEVERIDAD"].unique():
            sub = tl_df[tl_df["SEVERIDAD"] == sev].sort_values("FECHA")
            sizes = [max(5, min(20, int(v))) for v in sub["INVEST"]]
            fig_tl.add_trace(go.Scatter(
                x=sub["FECHA"].tolist(),
                y=[int(v) for v in sub["CNT"]],
                mode="markers",
                name=str(sev),
                marker=dict(
                    size=sizes,
                    color=SEV_COLORS.get(str(sev), COLORS[0]),
                    opacity=0.7,
                    line=dict(width=1, color="white"),
                ),
                hovertemplate=f"Severidad: {sev}<br>Fecha: %{{x}}<br>Eventos: %{{y:,}}<extra></extra>",
            ))
        fig_tl.update_layout(
            template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
            margin=dict(l=50, r=20, t=30, b=50),
            xaxis=dict(title="Fecha"), yaxis=dict(title="Cantidad de eventos"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig_tl, use_container_width=True, key="aud_timeline")

    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("**Matriz Severidad x Sistema Origen**")
        matrix_df = run_query(f"""
            SELECT SEVERIDAD, SISTEMA_ORIGEN, COUNT(*) AS CNT
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1,2
        """)
        if not matrix_df.empty:
            piv = matrix_df.pivot_table(index="SEVERIDAD", columns="SISTEMA_ORIGEN", values="CNT", fill_value=0)
            sev_order = ["Critica", "Alta", "Media", "Baja", "Info"]
            piv = piv.reindex([s for s in sev_order if s in piv.index])
            fig_matrix = go.Figure(go.Heatmap(
                z=piv.values.tolist(),
                x=piv.columns.tolist(),
                y=piv.index.tolist(),
                colorscale=[[0, "#E8F5E9"], [0.3, "#FFF3E0"], [0.6, "#FFAB00"], [1, "#DE350B"]],
                text=piv.values.tolist(),
                texttemplate="%{text:,}",
                textfont=dict(size=12),
                hovertemplate="Severidad: %{y}<br>Sistema: %{x}<br>Eventos: %{z:,}<extra></extra>",
            ))
            fig_matrix.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=80, r=20, t=30, b=80),
                xaxis=dict(tickangle=-30),
            )
            st.plotly_chart(fig_matrix, use_container_width=True, key="aud_matrix")

    with col_b:
        st.markdown("**Top 15 Actores por Actividad**")
        actor_df = run_query(f"""
            SELECT ACTOR, COUNT(*) AS EVENTOS,
                   SUM(CASE WHEN ANOMALIA_DETECTADA THEN 1 ELSE 0 END) AS ANOMALIAS,
                   ROUND(AVG(CASE WHEN ANOMALIA_DETECTADA THEN 1.0 ELSE 0.0 END)*100,1) AS PCT_ANOMALIA
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 15
        """)
        if not actor_df.empty:
            bar_colors = ["#DE350B" if float(v) > 10 else "#FFAB00" if float(v) > 5 else COLORS[0] for v in actor_df["PCT_ANOMALIA"]]
            fig_actor = go.Figure(go.Bar(
                y=actor_df["ACTOR"].tolist(),
                x=[int(v) for v in actor_df["EVENTOS"]],
                orientation="h",
                marker=dict(color=bar_colors, line=dict(width=1, color="white")),
                text=[f"{int(v)} ({float(p):.0f}% anom.)" for v, p in zip(actor_df["EVENTOS"], actor_df["PCT_ANOMALIA"])],
                textposition="outside", textfont=dict(size=9),
                hovertemplate="<b>%{y}</b><br>Eventos: %{x:,}<extra></extra>",
            ))
            fig_actor.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=120, r=80, t=30, b=50),
                xaxis=dict(title="Eventos"),
            )
            st.plotly_chart(fig_actor, use_container_width=True, key="aud_actor_bar")

    st.divider()

    st.markdown("**Indicadores de Anomalia**")
    gi1, gi2, gi3, gi4 = st.columns(4)
    gauge_data = [
        ("% Anomalias", float(r["PCT_ANOMALIA"]), 5, 10),
        ("% Fuera horario", float(r["PCT_FUERA"]), 10, 20),
        ("% Datos sensibles", float(r["PCT_SENSIBLES"]), 15, 30),
        ("% Requiere invest.", float(r["PCT_INVEST"]), 5, 15),
    ]
    for i, (col, (label, val, good_th, bad_th)) in enumerate(zip([gi1, gi2, gi3, gi4], gauge_data)):
        with col:
            bar_color = "#36B37E" if val <= good_th else "#FFAB00" if val <= bad_th else "#DE350B"
            fig_g = go.Figure(go.Indicator(
                mode="gauge+number",
                value=val,
                number=dict(suffix="%", font=dict(size=24)),
                title=dict(text=label, font=dict(size=11)),
                gauge=dict(
                    axis=dict(range=[0, max(50, val * 1.5)], ticksuffix="%"),
                    bar=dict(color=bar_color),
                    steps=[
                        dict(range=[0, good_th], color="#E8F5E9"),
                        dict(range=[good_th, bad_th], color="#FFF3E0"),
                        dict(range=[bad_th, max(50, val * 1.5)], color="#FFCDD2"),
                    ],
                ),
            ))
            fig_g.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF",
                height=200, margin=dict(l=20, r=20, t=40, b=10),
            )
            st.plotly_chart(fig_g, use_container_width=True, key=f"aud_gauge_{i}")

    st.divider()

    col_c, col_d = st.columns(2)

    with col_c:
        st.markdown("**Estado de Exportacion a SIEM**")
        siem_df = run_query(f"""
            SELECT CASE WHEN EXPORTADO_SIEM THEN 'Exportado' ELSE 'No exportado' END AS ESTADO,
                   COUNT(*) AS CNT
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1
        """)
        if not siem_df.empty:
            fig_siem = go.Figure(go.Pie(
                labels=siem_df["ESTADO"].tolist(),
                values=[int(v) for v in siem_df["CNT"]],
                hole=0.5,
                marker=dict(colors=["#36B37E", "#DE350B"], line=dict(color="white", width=2)),
                textinfo="percent+label", textposition="outside",
                hovertemplate="<b>%{label}</b><br>Eventos: %{value:,}<br>%{percent:.1%}<extra></extra>",
            ))
            fig_siem.add_annotation(text=f"<b>{int(siem_df['CNT'].sum()):,}</b><br>total",
                                    x=0.5, y=0.5, showarrow=False, font=dict(size=12, color="#11567F"))
            fig_siem.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=380,
                margin=dict(l=5, r=5, t=30, b=5), showlegend=False,
            )
            st.plotly_chart(fig_siem, use_container_width=True, key="aud_siem_pie")

    with col_d:
        st.markdown("**Treemap — Categoria Control x Tipo Evento**")
        tree_df = run_query(f"""
            SELECT CATEGORIA_CONTROL, TIPO_EVENTO, COUNT(*) AS CNT
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1,2 ORDER BY 3 DESC
        """)
        if not tree_df.empty:
            labels = []
            parents = []
            values = []
            cat_totals = tree_df.groupby("CATEGORIA_CONTROL")["CNT"].sum().to_dict()
            for cat in cat_totals:
                labels.append(str(cat))
                parents.append("")
                values.append(int(cat_totals[cat]))
            for _, row in tree_df.iterrows():
                labels.append(str(row["TIPO_EVENTO"]))
                parents.append(str(row["CATEGORIA_CONTROL"]))
                values.append(int(row["CNT"]))
            fig_tree = go.Figure(go.Treemap(
                labels=labels, parents=parents, values=values,
                textinfo="label+value+percent parent",
                marker=dict(colorscale=[[0, "#A3E4F7"], [0.5, "#29B5E8"], [1, "#11567F"]],
                            line=dict(width=2, color="white")),
                hovertemplate="<b>%{label}</b><br>Eventos: %{value:,}<br>%{percentParent:.1%}<extra></extra>",
            ))
            fig_tree.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=380,
                margin=dict(l=5, r=5, t=30, b=5),
            )
            st.plotly_chart(fig_tree, use_container_width=True, key="aud_treemap")

    st.divider()

    st.markdown("**Pipeline de Investigacion**")
    inv_df = run_query(f"""
        SELECT COUNT(*) AS TOTAL,
               SUM(CASE WHEN ANOMALIA_DETECTADA THEN 1 ELSE 0 END) AS ANOMALIAS,
               SUM(CASE WHEN REQUIERE_INVESTIGACION THEN 1 ELSE 0 END) AS REQ_INVEST,
               SUM(CASE WHEN EXPORTADO_SIEM THEN 1 ELSE 0 END) AS SIEM
        FROM {TABLE} WHERE {WHERE}
    """)
    if not inv_df.empty:
        iv = inv_df.iloc[0]
        steps = ["Total eventos", "Anomalia detectada", "Requiere investigacion", "Exportado a SIEM"]
        vals = [int(iv["TOTAL"]), int(iv["ANOMALIAS"]), int(iv["REQ_INVEST"]), int(iv["SIEM"])]
        fig_inv = go.Figure(go.Funnel(
            y=steps, x=vals,
            textinfo="value+percent initial",
            texttemplate="%{value:,} (%{percentInitial:.1%})",
            marker=dict(color=[COLORS[0], "#FFAB00", "#DE350B", COLORS[1]],
                        line=dict(width=1, color="white")),
            connector=dict(line=dict(color="#DFE1E6", width=1)),
        ))
        fig_inv.update_layout(
            template="plotly_white", paper_bgcolor="#FFFFFF", height=350,
            margin=dict(l=200, r=20, t=30, b=40),
        )
        st.plotly_chart(fig_inv, use_container_width=True, key="aud_invest_funnel")
