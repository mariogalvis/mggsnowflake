import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from app_pages.conn_helper import run_query
from app_pages.map_helper import colombia_scatter_map, COORDS_COLOMBIA

COLORS = ["#29B5E8", "#1B3A5C", "#2E7D8C", "#0F4C75", "#3282B8", "#11567F", "#1A5276", "#154360"]


@st.cache_data(ttl=300, show_spinner=False)
def _get_ai_diagnostic(title, subtitle, kpi_summary):
    prompt = f"""Eres un analista senior de telecomunicaciones en Colombia. Analiza estos KPIs del modulo "{title}" ({subtitle}).

KPIs actuales: {kpi_summary}

Responde en espanol con EXACTAMENTE 2 frases separadas por el caracter |:
DIAGNOSTICO: Una frase directa sobre el estado actual y hallazgo clave.
|RECOMENDACION: Una accion concreta que el equipo debe tomar.

No uses listas, bullets, saltos de linea ni markdown. Solo 2 frases separadas por |."""
    prompt_escaped = prompt.replace("'", "''")
    try:
        sql = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', '{prompt_escaped}') AS RESP"
        resp_df = run_query(sql)
        if not resp_df.empty:
            return str(resp_df["RESP"].iloc[0]).strip()
    except Exception:
        pass
    return None


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


def render_page(config):
    table = config["table"]
    icon = config.get("icon", ":material/analytics:")
    title = config["title"]
    subtitle = config["subtitle"]
    cards = config["cards"]
    date_col = config["date_col"]
    filter_cols = config["filter_cols"]
    kpi_query = config["kpi_query"]
    kpi_labels = config["kpi_labels"]
    kpi_formats = config["kpi_formats"]
    trend_query = config.get("trend_query")
    trend_cols = config.get("trend_cols")
    treemap_query = config.get("treemap_query")
    treemap_config = config.get("treemap_config")
    geo_query = config.get("geo_query")
    geo_config = config.get("geo_config")
    diagnostics = config.get("diagnostics")
    simulator = config.get("simulator")

    st.header(f"{icon} {title}")
    st.caption(subtitle)

    c1, c2, c3 = st.columns(3)
    with c1:
        with st.container(border=True):
            st.markdown("**:material/lightbulb: Que resuelve**")
            st.markdown(cards[0])
    with c2:
        with st.container(border=True):
            st.markdown("**:material/settings: Como funciona**")
            st.markdown(cards[1])
    with c3:
        with st.container(border=True):
            st.markdown("**:material/trending_up: Valor de negocio**")
            st.markdown(cards[2])

    if diagnostics:
        dx_data = run_query(diagnostics["query"])
        if not dx_data.empty:
            st.markdown("")
            d1, d2 = st.columns(2)
            with d1:
                with st.container(border=True):
                    st.markdown("**:material/monitoring: Diagnostico CEO**")
                    lines = diagnostics["build_lines"](dx_data)
                    st.markdown("\n".join(lines))
            with d2:
                with st.container(border=True):
                    st.markdown("**:material/recommend: Recomendaciones**")
                    recs = diagnostics["build_recs"](dx_data)
                    st.markdown("\n".join(recs))

    st.divider()

    tabs = ["Dashboard Ejecutivo"]
    if simulator:
        tabs.append("Simulador Predictivo")
    tab_objs = st.tabs(tabs)

    with tab_objs[0]:
        fechas_df = run_query(f"SELECT MIN({date_col}) AS FMIN, MAX({date_col}) AS FMAX FROM {table}")
        if fechas_df.empty:
            st.info("No hay datos disponibles.")
            return
        fmin = pd.to_datetime(fechas_df["FMIN"].iloc[0]).date()
        fmax = pd.to_datetime(fechas_df["FMAX"].iloc[0]).date()

        n_filters = len(filter_cols) + 1
        cols_f = st.columns(min(n_filters, 6))
        with cols_f[0]:
            fecha_rng = st.date_input("Periodo", value=(fmin, fmax), min_value=fmin, max_value=fmax, key=f"{config['key']}_fecha")
            if isinstance(fecha_rng, (list, tuple)) and len(fecha_rng) == 2:
                fecha_inicio, fecha_fin = fecha_rng
            else:
                fecha_inicio, fecha_fin = fmin, fmax

        clauses = [f"{date_col} BETWEEN '{fecha_inicio}' AND '{fecha_fin}'"]
        filter_selections = {}
        for i, fc in enumerate(filter_cols):
            col_idx = min(i + 1, len(cols_f) - 1)
            opts = run_query(f"SELECT DISTINCT {fc} FROM {table} ORDER BY 1")[fc].tolist()
            with cols_f[col_idx]:
                sel = st.selectbox(fc.replace("_", " ").title(), ["Todos"] + opts, key=f"{config['key']}_{fc}")
            if sel != "Todos":
                clauses.append(f"{fc} = '{sel}'")
            filter_selections[fc] = sel
        WHERE = " AND ".join(clauses)

        kpi_sql = kpi_query.format(table=table, where=WHERE)
        kpi_df = run_query(kpi_sql)

        if kpi_df.empty or (kpi_df.columns[0] != "" and len(kpi_df) == 0):
            st.info("No hay datos para los filtros seleccionados.")
            return

        r = kpi_df.iloc[0]
        n_kpis = len(kpi_labels)
        row1 = min(4, n_kpis)
        k_cols1 = st.columns(row1)
        for i in range(row1):
            col_name = kpi_df.columns[i]
            val = r[col_name]
            fmt = kpi_formats[i]
            if isinstance(val, (int, float)):
                k_cols1[i].metric(kpi_labels[i], fmt.format(val))
            else:
                k_cols1[i].metric(kpi_labels[i], str(val))

        if n_kpis > 4:
            row2 = min(4, n_kpis - 4)
            k_cols2 = st.columns(row2)
            for i in range(row2):
                col_name = kpi_df.columns[i + 4]
                val = r[col_name]
                fmt = kpi_formats[i + 4]
                if isinstance(val, (int, float)):
                    k_cols2[i].metric(kpi_labels[i + 4], fmt.format(val))
                else:
                    k_cols2[i].metric(kpi_labels[i + 4], str(val))

        kpi_parts = []
        for i in range(n_kpis):
            col_name = kpi_df.columns[i]
            val = r[col_name]
            if isinstance(val, (int, float)):
                kpi_parts.append(f"{kpi_labels[i]}: {kpi_formats[i].format(val)}")
        kpi_summary = ", ".join(kpi_parts)
        ai_text = _get_ai_diagnostic(title, subtitle, kpi_summary)
        if ai_text:
            parts = ai_text.split("|", 1)
            diag = parts[0].strip()
            rec = parts[1].strip() if len(parts) > 1 else ""
            d1, d2 = st.columns(2)
            with d1:
                with st.container(border=True):
                    st.markdown("**:material/psychology: Diagnostico IA**")
                    st.markdown(diag)
            with d2:
                with st.container(border=True):
                    st.markdown("**:material/lightbulb: Recomendacion IA**")
                    st.markdown(rec)

        st.divider()

        col_c1, col_c2 = st.columns(2)

        with col_c1:
            if trend_query and trend_cols:
                st.markdown(f"**Evolucion mensual**")
                t_sql = trend_query.format(table=table, where=WHERE)
                t_df = run_query(t_sql)
                if not t_df.empty:
                    meses = t_df[t_df.columns[0]].tolist()
                    y1 = [float(v) for v in t_df[t_df.columns[1]]]
                    fig_t = go.Figure()
                    fig_t.add_trace(go.Scatter(
                        x=meses, y=y1, name=trend_cols[0], mode="lines+markers+text",
                        fill="tozeroy", fillcolor="rgba(15,44,70,0.08)",
                        line=dict(color="#1B3A5C", width=3), marker=dict(size=8),
                        text=[f"{v:.1f}" for v in y1], textposition="top center",
                        textfont=dict(size=10, color="#1B3A5C"),
                    ))
                    if len(t_df.columns) > 2 and len(trend_cols) > 1:
                        y2 = [float(v) for v in t_df[t_df.columns[2]]]
                        fig_t.add_trace(go.Scatter(
                            x=meses, y=y2, name=trend_cols[1], mode="lines+markers",
                            line=dict(color="#2E7D8C", width=2, dash="dash"), marker=dict(size=6),
                            yaxis="y2",
                        ))
                        fig_t.update_layout(
                            yaxis2=dict(title=trend_cols[1], side="right", overlaying="y", showgrid=False),
                        )
                    fig_t.update_layout(
                        template="plotly_white", paper_bgcolor="#FAFBFC", plot_bgcolor="#FAFBFC",
                        height=380, margin=dict(l=40, r=40, t=30, b=40),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                        yaxis=dict(title=trend_cols[0], gridcolor="#E2E8F0"),
                        font=dict(family="Inter, sans-serif", color="#334155"),
                    )
                    st.plotly_chart(fig_t, use_container_width=True)

        with col_c2:
            if treemap_query and treemap_config:
                st.markdown(f"**{treemap_config.get('title', 'Composicion')}**")
                tm_sql = treemap_query.format(table=table, where=WHERE)
                tm_df = run_query(tm_sql)
                if not tm_df.empty:
                    fig_tree = go.Figure(go.Treemap(
                        labels=tm_df[tm_df.columns[0]].tolist(),
                        values=[float(v) for v in tm_df[tm_df.columns[1]]],
                        parents=[""] * len(tm_df),
                        textinfo="label+value+percent root",
                        marker=dict(
                            colors=[float(v) for v in tm_df[tm_df.columns[1]]],
                            colorscale=treemap_config.get("colorscale", [[0, "#E8F4F8"], [0.3, "#2E7D8C"], [0.6, "#1B3A5C"], [1, "#0F2B46"]]),
                            line=dict(width=2, color="white"),
                        ),
                    ))
                    fig_tree.update_layout(
                        template="plotly_white", paper_bgcolor="#FAFBFC", plot_bgcolor="#FAFBFC",
                        height=380, margin=dict(l=5, r=5, t=30, b=5),
                        font=dict(family="Inter, sans-serif", color="#334155"),
                    )
                    st.plotly_chart(fig_tree, use_container_width=True)

        if geo_query and geo_config:
            st.divider()
            st.markdown(f"**{geo_config.get('title', 'Distribucion geografica')}**")
            geo_sql = geo_query.format(table=table, where=WHERE)
            geo_df = run_query(geo_sql)
            if not geo_df.empty:
                city_col = geo_config["city_col"]
                size_col = geo_config["size_col"]
                color_col = geo_config["color_col"]
                geo_df["LAT"] = geo_df[city_col].map(lambda c: COORDS_COLOMBIA.get(c, (4.5, -74.0))[0]).astype(float)
                geo_df["LON"] = geo_df[city_col].map(lambda c: COORDS_COLOMBIA.get(c, (4.5, -74.0))[1]).astype(float)
                geo_df["SIZE_F"] = geo_df[size_col].astype(float)
                geo_df["COLOR_F"] = geo_df[color_col].astype(float)
                geo_df["hover"] = geo_df.apply(
                    lambda row: f"{row[city_col]}<br>{size_col}: {row['SIZE_F']:,.0f}<br>{color_col}: {row['COLOR_F']:.1f}",
                    axis=1,
                )
                deck = colombia_scatter_map(
                    geo_df, lat_col="LAT", lon_col="LON", size_col="SIZE_F", color_col="COLOR_F",
                    text_col=city_col, hover_col="hover",
                    colorscale=geo_config.get("colorscale", ["#2E7D8C", "#1B3A5C", "#0F2B46"]),
                )
                st.pydeck_chart(deck, key=f"{config['key']}_map")
                st.caption(geo_config.get("caption", "Tamano: volumen | Color: intensidad del indicador"))

        st.divider()
        st.markdown("**Top 10 registros**")
        top_sql = f"SELECT * FROM {table} WHERE {WHERE} LIMIT 10"
        top_df = run_query(top_sql)
        if not top_df.empty:
            st.dataframe(top_df, use_container_width=True, hide_index=True)

    if simulator and len(tab_objs) > 1:
        with tab_objs[1]:
            st.markdown(f"**{simulator['title']}**")
            st.caption(simulator["desc"])

            col_sliders, col_result = st.columns([3, 2])
            valores = []
            with col_sliders:
                for feat in simulator["features"]:
                    step = feat.get("step", 1 if isinstance(feat["default"], int) else 0.1)
                    val = st.slider(
                        feat["name"], min_value=float(feat["min"]), max_value=float(feat["max"]),
                        value=float(feat["default"]), step=float(step),
                        help=feat.get("desc", ""), key=f"sim_{config['key']}_{feat['name']}",
                    )
                    valores.append(val)

            score = 0.0
            for feat, val in zip(simulator["features"], valores):
                vmin, vmax = feat["min"], feat["max"]
                norm = (val - vmin) / (vmax - vmin) if vmax != vmin else 0.5
                w = feat["weight"]
                if w < 0:
                    norm = 1 - norm
                    score += abs(w) * norm
                else:
                    score += w * norm
            score = round(min(max(score, 0), 1), 3)

            thresholds = simulator.get("thresholds", [0.33, 0.66])
            labels = simulator.get("labels", ["Bajo", "Medio", "Alto"])

            with col_result:
                if score < thresholds[0]:
                    nivel = labels[0]
                    color_nivel = "#2E7D8C"
                elif score < thresholds[1]:
                    nivel = labels[1]
                    color_nivel = "#1B3A5C"
                else:
                    nivel = labels[2]
                    color_nivel = "#0F2B46"

                st.metric("Score Predictivo", f"{score*100:.1f}%", nivel)

                fig_gauge = go.Figure(go.Indicator(
                    mode="gauge+number", value=score * 100, number={"suffix": "%"},
                    gauge={
                        "axis": {"range": [0, 100]},
                        "bar": {"color": color_nivel},
                        "steps": [
                            {"range": [0, thresholds[0] * 100], "color": "#E8F4F8"},
                            {"range": [thresholds[0] * 100, thresholds[1] * 100], "color": "#D4E6F1"},
                            {"range": [thresholds[1] * 100, 100], "color": "#F5B7B1"},
                        ],
                        "threshold": {"line": {"color": "black", "width": 2}, "thickness": 0.75, "value": score * 100},
                    },
                ))
                fig_gauge.update_layout(height=250, margin=dict(t=30, b=0, l=30, r=30))
                st.plotly_chart(fig_gauge, use_container_width=True)

                with st.container(border=True):
                    st.markdown(f"**Nivel:** :{('green' if score < thresholds[0] else 'orange' if score < thresholds[1] else 'red')}[**{nivel}**]")
                    if score < thresholds[0]:
                        st.markdown("Los indicadores muestran una situacion favorable. Mantener monitoreo estandar.")
                    elif score < thresholds[1]:
                        st.markdown("Se recomienda revision preventiva y seguimiento cercano de las variables de riesgo.")
                    else:
                        st.markdown("Accion inmediata requerida. Escalar a comite de decision y activar protocolos de mitigacion.")
