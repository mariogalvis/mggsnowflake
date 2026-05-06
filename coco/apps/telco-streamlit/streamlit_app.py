import streamlit as st

st.set_page_config(page_title="MGG Telco — Centro de Control", page_icon=":material/cell_tower:", layout="wide")

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

deteccion_anomalias_red = st.Page("app_pages/deteccion_anomalias_red.py", title="Deteccion Anomalias Red", icon=":material/warning:")
optimizacion_calidad_servicio = st.Page("app_pages/optimizacion_calidad_servicio.py", title="Calidad de Servicio", icon=":material/high_quality:")
optimizacion_capacidad_red = st.Page("app_pages/optimizacion_capacidad_red.py", title="Capacidad de Red", icon=":material/speed:")
planificacion_expansion_red = st.Page("app_pages/planificacion_expansion_red.py", title="Expansion de Red", icon=":material/map:")

prediccion_churn_telco = st.Page("app_pages/prediccion_churn_telco.py", title="Prediccion Churn", icon=":material/person_off:")
segmentacion_clientes_telco = st.Page("app_pages/segmentacion_clientes_telco.py", title="Segmentacion Clientes", icon=":material/groups:")
optimizacion_retencion = st.Page("app_pages/optimizacion_retencion.py", title="Optimizacion Retencion", icon=":material/loyalty:")
personalizacion_ofertas = st.Page("app_pages/personalizacion_ofertas.py", title="Personalizacion Ofertas", icon=":material/redeem:")

analisis_experiencia_cliente = st.Page("app_pages/analisis_experiencia_cliente.py", title="Experiencia Cliente", icon=":material/sentiment_satisfied:")
asistentes_virtuales_telco = st.Page("app_pages/asistentes_virtuales_telco.py", title="Asistentes Virtuales", icon=":material/smart_toy:")
optimizacion_journeys_telco = st.Page("app_pages/optimizacion_journeys_telco.py", title="Optimizacion Journeys", icon=":material/route:")

optimizacion_facturacion = st.Page("app_pages/optimizacion_facturacion.py", title="Optimizacion Facturacion", icon=":material/receipt_long:")
prediccion_mora = st.Page("app_pages/prediccion_mora.py", title="Prediccion Mora", icon=":material/credit_card_off:")
gestion_cobranza = st.Page("app_pages/gestion_cobranza.py", title="Gestion Cobranza", icon=":material/payments:")

deteccion_fraude_telco = st.Page("app_pages/deteccion_fraude_telco.py", title="Deteccion Fraude", icon=":material/shield:")
gestion_identidad = st.Page("app_pages/gestion_identidad.py", title="Gestion Identidad", icon=":material/fingerprint:")
prevencion_abuso_red = st.Page("app_pages/prevencion_abuso_red.py", title="Prevencion Abuso Red", icon=":material/block:")

gestion_productos = st.Page("app_pages/gestion_productos.py", title="Gestion Productos", icon=":material/inventory:")
optimizacion_arpu = st.Page("app_pages/optimizacion_arpu.py", title="Optimizacion ARPU", icon=":material/trending_up:")
pricing_planes = st.Page("app_pages/pricing_planes.py", title="Pricing Planes", icon=":material/sell:")
upsell_cross_sell_telco = st.Page("app_pages/upsell_cross_sell_telco.py", title="Upsell & Cross-Sell", icon=":material/shopping_cart:")

automatizacion_operaciones = st.Page("app_pages/automatizacion_operaciones.py", title="Automatizacion Operaciones", icon=":material/precision_manufacturing:")
mantenimiento_predictivo = st.Page("app_pages/mantenimiento_predictivo.py", title="Mantenimiento Predictivo", icon=":material/build:")
optimizacion_despacho_tecnicos = st.Page("app_pages/optimizacion_despacho_tecnicos.py", title="Despacho Tecnicos", icon=":material/engineering:")

analisis_trafico_datos = st.Page("app_pages/analisis_trafico_datos.py", title="Analisis Trafico Datos", icon=":material/data_usage:")
insights_cliente_telco = st.Page("app_pages/insights_cliente_telco.py", title="Insights Cliente", icon=":material/psychology:")

pg = st.navigation({
    "Gestion de Red": [deteccion_anomalias_red, optimizacion_calidad_servicio, optimizacion_capacidad_red, planificacion_expansion_red],
    "Cliente y Retencion": [prediccion_churn_telco, segmentacion_clientes_telco, optimizacion_retencion, personalizacion_ofertas],
    "Experiencia y Atencion": [analisis_experiencia_cliente, asistentes_virtuales_telco, optimizacion_journeys_telco],
    "Facturacion y Cobranza": [optimizacion_facturacion, prediccion_mora, gestion_cobranza],
    "Fraude y Seguridad": [deteccion_fraude_telco, gestion_identidad, prevencion_abuso_red],
    "Monetizacion y Revenue": [gestion_productos, optimizacion_arpu, pricing_planes, upsell_cross_sell_telco],
    "Operaciones y Mantenimiento": [automatizacion_operaciones, mantenimiento_predictivo, optimizacion_despacho_tecnicos],
    "Analitica de Red y Cliente": [analisis_trafico_datos, insights_cliente_telco],
})

pg.run()
