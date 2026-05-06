import streamlit as st

st.set_page_config(page_title="MGG Banca — Centro de Control", page_icon=":material/account_balance:", layout="wide")

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

scoring_crediticio = st.Page("app_pages/scoring_crediticio.py", title="Scoring Crediticio", icon=":material/credit_score:")
originacion_digital = st.Page("app_pages/originacion_digital.py", title="Originacion Digital", icon=":material/phone_android:")
pricing_riesgo = st.Page("app_pages/pricing_riesgo.py", title="Pricing Riesgo", icon=":material/price_change:")
early_warning = st.Page("app_pages/early_warning.py", title="Early Warning", icon=":material/notification_important:")
recobro_inteligente = st.Page("app_pages/recobro_inteligente.py", title="Recobro Inteligente", icon=":material/account_balance_wallet:")

fraude_transaccional = st.Page("app_pages/fraude_transaccional.py", title="Fraude Transaccional", icon=":material/gpp_bad:")
kyc_onboarding = st.Page("app_pages/kyc_onboarding.py", title="KYC Onboarding", icon=":material/verified_user:")
aml_monitoreo = st.Page("app_pages/aml_monitoreo.py", title="AML Monitoreo", icon=":material/policy:")
autenticacion_adaptativa = st.Page("app_pages/autenticacion_adaptativa.py", title="Autenticacion Adaptativa", icon=":material/fingerprint:")

next_best_offer = st.Page("app_pages/next_best_offer.py", title="Next Best Offer", icon=":material/recommend:")
segmentacion_clientes = st.Page("app_pages/segmentacion_clientes.py", title="Segmentacion Clientes", icon=":material/groups:")
cross_selling = st.Page("app_pages/cross_selling.py", title="Cross Selling", icon=":material/shopping_cart:")
churn_prediction = st.Page("app_pages/churn_prediction.py", title="Prediccion Churn", icon=":material/person_off:")
personalizacion_experiencia = st.Page("app_pages/personalizacion_experiencia.py", title="Personalizacion CX", icon=":material/tune:")

asistentes_virtuales = st.Page("app_pages/asistentes_virtuales.py", title="Asistentes Virtuales", icon=":material/smart_toy:")
sentimiento_call_center = st.Page("app_pages/sentimiento_call_center.py", title="Sentimiento Call Center", icon=":material/call:")
journeys_digitales = st.Page("app_pages/journeys_digitales.py", title="Journeys Digitales", icon=":material/route:")
recomendaciones_real_time = st.Page("app_pages/recomendaciones_real_time.py", title="Recomendaciones RT", icon=":material/auto_awesome:")

automatizacion_rpa = st.Page("app_pages/automatizacion_rpa.py", title="Automatizacion RPA", icon=":material/precision_manufacturing:")
procesamiento_documentos = st.Page("app_pages/procesamiento_documentos.py", title="Procesamiento Documentos", icon=":material/description:")
conciliacion_automatica = st.Page("app_pages/conciliacion_automatica.py", title="Conciliacion Automatica", icon=":material/fact_check:")
optimizacion_costos = st.Page("app_pages/optimizacion_costos.py", title="Optimizacion Costos", icon=":material/savings:")

alm_gestion = st.Page("app_pages/alm_gestion.py", title="ALM Gestion", icon=":material/account_balance:")
riesgo_mercado = st.Page("app_pages/riesgo_mercado.py", title="Riesgo de Mercado", icon=":material/show_chart:")
forecast_liquidez = st.Page("app_pages/forecast_liquidez.py", title="Forecast Liquidez", icon=":material/water_drop:")

reportes_regulatorios = st.Page("app_pages/reportes_regulatorios.py", title="Reportes Regulatorios", icon=":material/gavel:")
monitoreo_transacciones = st.Page("app_pages/monitoreo_transacciones.py", title="Monitoreo Transacciones", icon=":material/monitoring:")
auditoria_trazabilidad = st.Page("app_pages/auditoria_trazabilidad.py", title="Auditoria y Trazabilidad", icon=":material/history:")

agentes_conversacionales = st.Page("app_pages/agentes_conversacionales.py", title="Agentes Conversacionales", icon=":material/psychology:")
copilotos_empleados = st.Page("app_pages/copilotos_empleados.py", title="Copilotos Empleados", icon=":material/assistant:")
busqueda_documentos = st.Page("app_pages/busqueda_documentos.py", title="Busqueda Documentos", icon=":material/search:")
reportes_automaticos = st.Page("app_pages/reportes_automaticos.py", title="Reportes Automaticos", icon=":material/summarize:")
exposicion_crediticia = st.Page("app_pages/exposicion_crediticia.py", title="Exposicion Crediticia", icon=":material/analytics:")

pg = st.navigation({
    "Riesgo y Credito": [scoring_crediticio, originacion_digital, pricing_riesgo, early_warning, recobro_inteligente],
    "Fraude y Seguridad": [fraude_transaccional, kyc_onboarding, aml_monitoreo, autenticacion_adaptativa],
    "Cliente y Crecimiento": [next_best_offer, segmentacion_clientes, cross_selling, churn_prediction, personalizacion_experiencia],
    "Canales y Experiencia": [asistentes_virtuales, sentimiento_call_center, journeys_digitales, recomendaciones_real_time],
    "Operaciones y Eficiencia": [automatizacion_rpa, procesamiento_documentos, conciliacion_automatica, optimizacion_costos],
    "Tesoreria y Riesgo Financiero": [alm_gestion, riesgo_mercado, forecast_liquidez],
    "Cumplimiento y Regulacion": [reportes_regulatorios, monitoreo_transacciones, auditoria_trazabilidad],
    "Datos y AI": [agentes_conversacionales, copilotos_empleados, busqueda_documentos, reportes_automaticos, exposicion_crediticia],
})

pg.run()
