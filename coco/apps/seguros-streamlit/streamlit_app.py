import streamlit as st

st.set_page_config(page_title="MGG Seguros — Centro de Control", page_icon=":material/shield:", layout="wide")

st.logo("https://mgg.com.co/wp-content/uploads/images/logo.png")

st.markdown("""
<style>
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0F2B46 0%, #11354A 100%);
    }
    [data-testid="stSidebar"] * {
        color: #CBD5E1 !important;
    }
    [data-testid="stSidebar"] [data-testid="stMarkdown"] p {
        color: #94A3B8 !important;
        font-weight: 500;
        text-transform: uppercase;
        font-size: 0.7rem;
        letter-spacing: 0.05em;
    }
    [data-testid="stSidebar"] .stPageLink p, [data-testid="stSidebar"] a span {
        color: #E2E8F0 !important;
        text-transform: none;
        font-size: 0.85rem;
    }
    [data-testid="stSidebar"] [aria-selected="true"] {
        background: rgba(41, 181, 232, 0.15) !important;
        border-left: 3px solid #29B5E8;
    }
    [data-testid="stMetric"] {
        background: #F8FAFC;
        border: 1px solid #E2E8F0;
        border-radius: 8px;
        padding: 12px 16px;
        border-left: 4px solid #29B5E8;
    }
    [data-testid="stMetricLabel"] {
        color: #475569 !important;
        font-size: 0.75rem !important;
        text-transform: uppercase;
        letter-spacing: 0.03em;
    }
    [data-testid="stMetricValue"] {
        color: #0F172A !important;
        font-weight: 700 !important;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 0;
        border-bottom: 2px solid #E2E8F0;
    }
    .stTabs [data-baseweb="tab"] {
        color: #64748B;
        font-weight: 600;
        padding: 8px 24px;
        border-bottom: 3px solid transparent;
    }
    .stTabs [aria-selected="true"] {
        color: #0F2B46 !important;
        border-bottom: 3px solid #29B5E8 !important;
        background: transparent;
    }
    h1, h2, h3 {
        color: #0F2B46 !important;
    }
    [data-testid="stHeader"] {
        background: linear-gradient(90deg, #0F2B46 0%, #164E6B 100%);
    }
    div[data-testid="stContainer"] {
        border-color: #E2E8F0 !important;
    }
    .stDivider {
        border-color: #E2E8F0 !important;
    }
</style>
""", unsafe_allow_html=True)

scoring_riesgo = st.Page("app_pages/scoring_riesgo.py", title="Scoring de Riesgo", icon=":material/security:")
pricing_dinamico = st.Page("app_pages/pricing_dinamico.py", title="Pricing Dinamico", icon=":material/paid:")
decision_suscripcion = st.Page("app_pages/decision_suscripcion.py", title="Decision de Suscripcion", icon=":material/gavel:")
optimizacion_portafolio = st.Page("app_pages/optimizacion_portafolio.py", title="Optimizacion Portafolio", icon=":material/pie_chart:")

gestion_siniestros = st.Page("app_pages/gestion_siniestros.py", title="Gestion de Siniestros", icon=":material/report:")
estimacion_costo = st.Page("app_pages/estimacion_costo.py", title="Estimacion de Costo", icon=":material/calculate:")
deteccion_fraude_siniestros = st.Page("app_pages/deteccion_fraude_siniestros.py", title="Deteccion Fraude Siniestros", icon=":material/warning:")
priorizacion_casos = st.Page("app_pages/priorizacion_casos.py", title="Priorizacion de Casos", icon=":material/sort:")

retencion_clientes = st.Page("app_pages/retencion_clientes.py", title="Retencion de Clientes", icon=":material/person_off:")
cross_sell = st.Page("app_pages/cross_sell.py", title="Cross-Sell Seguros", icon=":material/shopping_cart:")
personalizacion_polizas = st.Page("app_pages/personalizacion_polizas.py", title="Personalizacion Polizas", icon=":material/tune:")
optimizacion_ltv = st.Page("app_pages/optimizacion_ltv.py", title="Optimizacion LTV", icon=":material/trending_up:")

scoring_fraude_cliente = st.Page("app_pages/scoring_fraude_cliente.py", title="Scoring Fraude Cliente", icon=":material/fingerprint:")
deteccion_red_fraude = st.Page("app_pages/deteccion_red_fraude.py", title="Deteccion Red Fraude", icon=":material/hub:")
gestion_alertas_fraude = st.Page("app_pages/gestion_alertas_fraude.py", title="Gestion Alertas Fraude", icon=":material/notifications_active:")

optimizacion_canales = st.Page("app_pages/optimizacion_canales.py", title="Optimizacion Canales", icon=":material/store:")
pricing_comercial = st.Page("app_pages/pricing_comercial.py", title="Pricing Comercial", icon=":material/sell:")
gestion_intermediarios = st.Page("app_pages/gestion_intermediarios.py", title="Gestion Intermediarios", icon=":material/handshake:")

optimizacion_renovaciones = st.Page("app_pages/optimizacion_renovaciones.py", title="Optimizacion Renovaciones", icon=":material/autorenew:")
analisis_satisfaccion = st.Page("app_pages/analisis_satisfaccion.py", title="Analisis Satisfaccion", icon=":material/sentiment_satisfied:")
recomendaciones_cliente = st.Page("app_pages/recomendaciones_cliente.py", title="Recomendaciones Cliente", icon=":material/recommend:")

optimizacion_reaseguro = st.Page("app_pages/optimizacion_reaseguro.py", title="Optimizacion Reaseguro", icon=":material/swap_horiz:")
gestion_cesiones = st.Page("app_pages/gestion_cesiones.py", title="Gestion Cesiones", icon=":material/assignment_turned_in:")

ratio_combinado = st.Page("app_pages/ratio_combinado.py", title="Ratio Combinado", icon=":material/analytics:")
forecast_siniestralidad = st.Page("app_pages/forecast_siniestralidad.py", title="Forecast Siniestralidad", icon=":material/show_chart:")
gestion_reservas = st.Page("app_pages/gestion_reservas.py", title="Gestion Reservas", icon=":material/savings:")

automatizacion_procesos = st.Page("app_pages/automatizacion_procesos.py", title="Automatizacion Procesos", icon=":material/smart_toy:")
procesamiento_documentos = st.Page("app_pages/procesamiento_documentos.py", title="Procesamiento Documentos", icon=":material/description:")
control_calidad = st.Page("app_pages/control_calidad.py", title="Control Calidad Operativa", icon=":material/verified:")

pg = st.navigation({
    "Suscripcion y Riesgo": [scoring_riesgo, pricing_dinamico, decision_suscripcion, optimizacion_portafolio],
    "Siniestros": [gestion_siniestros, estimacion_costo, deteccion_fraude_siniestros, priorizacion_casos],
    "Cliente y Crecimiento": [retencion_clientes, cross_sell, personalizacion_polizas, optimizacion_ltv],
    "Fraude en Seguros": [scoring_fraude_cliente, deteccion_red_fraude, gestion_alertas_fraude],
    "Distribucion y Ventas": [optimizacion_canales, pricing_comercial, gestion_intermediarios],
    "Retencion y Experiencia": [optimizacion_renovaciones, analisis_satisfaccion, recomendaciones_cliente],
    "Reaseguros": [optimizacion_reaseguro, gestion_cesiones],
    "Finanzas Tecnicas": [ratio_combinado, forecast_siniestralidad, gestion_reservas],
    "Operaciones": [automatizacion_procesos, procesamiento_documentos, control_calidad],
})

pg.run()
