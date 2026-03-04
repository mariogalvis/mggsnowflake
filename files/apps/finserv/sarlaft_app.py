import streamlit as st
from snowflake.snowpark.context import get_active_session
from datetime import datetime
from io import BytesIO
import json
import re
import base64

st.set_page_config(page_title="Registro SARLAFT", page_icon="🏛️", layout="wide")

st.markdown("""
<style>
    .stApp { background-color: #f5f7fa; }
    .main-header {
        background: linear-gradient(135deg, #1e3a5f 0%, #2d5a87 100%);
        padding: 2rem; border-radius: 10px; margin-bottom: 2rem;
        color: white; text-align: center;
    }
    .main-header h1 { color: white; margin: 0; font-size: 2rem; }
    .main-header p { color: #b8d4e8; margin: 0.5rem 0 0 0; }
    .section-header {
        background-color: #1e3a5f; color: white;
        padding: 0.75rem 1rem; border-radius: 5px;
        margin: 1.5rem 0 1rem 0; font-weight: 600;
    }
    .stButton > button[kind="primary"] { background-color: #1e3a5f; border: none; padding: 0.75rem 2rem; }
    .stButton > button[kind="primary"]:hover { background-color: #2d5a87; }
    .info-box {
        background-color: #e8f4f8; border-left: 4px solid #1e3a5f;
        padding: 1rem; margin: 1rem 0; border-radius: 0 5px 5px 0;
    }
    div[data-testid="stFileUploader"] {
        background-color: white; padding: 1rem;
        border-radius: 10px; border: 2px dashed #1e3a5f;
    }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="main-header">
    <h1>🏛️ Registro de Clientes - SARLAFT</h1>
    <p>Sistema de Autocontrol y Gestión del Riesgo de Lavado de Activos y Financiación del Terrorismo</p>
</div>
""", unsafe_allow_html=True)

session = get_active_session()

current_db = session.get_current_database()
current_schema = session.get_current_schema()
STAGE_NAME = f"{current_db}.{current_schema}.SARLAFT_TEMP_STAGE"

def init_stage():
    try:
        session.sql(f"CREATE STAGE IF NOT EXISTS {STAGE_NAME} ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')").collect()
        return True
    except Exception as e:
        st.warning(f"No se pudo crear stage temporal: {e}")
        return False

if "sarlaft_form" not in st.session_state:
    st.session_state.sarlaft_form = {
        "numero_identificacion": "", "nombre_completo": "", "tipo_documento": "",
        "fecha_expedicion": None, "direccion": "", "ciudad": "", "telefono": "",
        "correo_electronico": "", "total_ingresos_mensuales": "", "actividad_economica": "",
        "es_vinculacion": False, "es_pep": False, "total_egresos_mensuales": "",
        "ocupacion_profesion": "", "codigo_ciiu": "", "activos": "", "pasivos": ""
    }
if "sarlaft_status" not in st.session_state:
    st.session_state.sarlaft_status = {}
if "sarlaft_processed" not in st.session_state:
    st.session_state.sarlaft_processed = None
if "stage_ready" not in st.session_state:
    st.session_state.stage_ready = init_stage()

def parse_date(date_str):
    if not date_str:
        return None
    try:
        for fmt in ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d %b %Y", "%d %B %Y"]:
            try:
                return datetime.strptime(str(date_str), fmt).date()
            except:
                continue
        match = re.search(r'(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})', str(date_str))
        if match:
            day, month, year = match.groups()
            return datetime(int(year), int(month), int(day)).date()
    except:
        pass
    return None

def parse_money(value_str):
    if not value_str:
        return 0.0
    try:
        clean = re.sub(r'[^\d.,]', '', str(value_str))
        clean = clean.replace('.', '').replace(',', '.')
        return float(clean) if clean else 0.0
    except:
        return 0.0

def field_indicator(field):
    status = st.session_state.sarlaft_status.get(field, "")
    if status == "detected":
        return " ✓"
    elif status == "not_detected":
        return " ○"
    return ""

st.markdown('<div class="section-header">📄 Carga de Documento</div>', unsafe_allow_html=True)

uploaded_file = st.file_uploader("Seleccione el formulario SARLAFT en PDF", type=["pdf"], help="Formato aceptado: PDF")

if uploaded_file and st.session_state.stage_ready:
    uploaded_file.seek(0)
    file_bytes = uploaded_file.read()
    
    col_info1, col_info2, col_info3 = st.columns(3)
    with col_info1:
        st.metric("Archivo", uploaded_file.name[:25] + "..." if len(uploaded_file.name) > 25 else uploaded_file.name)
    with col_info2:
        st.metric("Tipo", "PDF")
    with col_info3:
        st.metric("Tamaño", f"{len(file_bytes) / 1024:.1f} KB")
    
    current_file_id = f"{uploaded_file.name}_{len(file_bytes)}"
    
    if st.session_state.sarlaft_processed != current_file_id:
        st.markdown('<div class="section-header">⚙️ Procesamiento Automático</div>', unsafe_allow_html=True)
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        try:
            status_text.info("📤 Cargando documento...")
            progress_bar.progress(20)
            
            file_name = f"sarlaft_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
            session.file.put_stream(
                input_stream=BytesIO(file_bytes),
                stage_location=f"@{STAGE_NAME}/{file_name}",
                auto_compress=False, overwrite=True
            )
            
            progress_bar.progress(40)
            status_text.info("🤖 Analizando con IA...")
            
            extract_query = f"""
            SELECT AI_EXTRACT(
                file => TO_FILE('@{STAGE_NAME}', '{file_name}'),
                responseFormat => {{
                    'numero_identificacion': '¿Cuál es el número de identificación o cédula?',
                    'nombre_completo': '¿Cuál es el nombre completo?',
                    'tipo_documento': '¿Cuál es el tipo de documento (CC, CE, NIT, Pasaporte)?',
                    'fecha_expedicion': '¿Cuál es la fecha de expedición del documento?',
                    'direccion': '¿Cuál es la dirección?',
                    'ciudad': '¿Cuál es la ciudad?',
                    'telefono': '¿Cuál es el número de teléfono?',
                    'correo_electronico': '¿Cuál es el correo electrónico?',
                    'total_ingresos_mensuales': '¿Cuál es el total de ingresos mensuales?',
                    'total_egresos_mensuales': '¿Cuál es el total de egresos mensuales?',
                    'actividad_economica': '¿Cuál es la actividad económica principal?',
                    'ocupacion_profesion': '¿Cuál es la ocupación o profesión?',
                    'codigo_ciiu': '¿Cuál es el código CIIU?',
                    'activos': '¿Cuál es el valor total de activos?',
                    'pasivos': '¿Cuál es el valor total de pasivos?',
                    'es_vinculacion': '¿Es vinculación? SI o NO',
                    'es_pep': '¿Es PEP? SI o NO'
                }}
            ) AS extraction_result
            """
            
            progress_bar.progress(70)
            result = session.sql(extract_query).collect()
            
            if result:
                extraction_json = json.loads(result[0]["EXTRACTION_RESULT"])
                if not extraction_json.get("error"):
                    response = extraction_json.get("response", {})
                    st.session_state.sarlaft_status = {}
                    for field, value in response.items():
                        if value and str(value).lower() not in ["none", "null", "n/a", ""]:
                            if field in ["es_vinculacion", "es_pep"]:
                                st.session_state.sarlaft_form[field] = str(value).upper() in ["SI", "SÍ", "YES", "TRUE", "1"]
                            else:
                                st.session_state.sarlaft_form[field] = value
                            st.session_state.sarlaft_status[field] = "detected"
                        else:
                            st.session_state.sarlaft_status[field] = "not_detected"
                    
                    progress_bar.progress(100)
                    st.session_state.sarlaft_processed = current_file_id
                    detected = sum(1 for v in st.session_state.sarlaft_status.values() if v == "detected")
                    status_text.success(f"✅ {detected}/{len(st.session_state.sarlaft_status)} campos detectados")
                    st.rerun()
                else:
                    status_text.error(f"Error: {extraction_json['error']}")
        except Exception as e:
            progress_bar.empty()
            status_text.error(f"Error: {str(e)}")
    else:
        st.markdown('<div class="info-box">✅ Documento procesado. Revise los campos.</div>', unsafe_allow_html=True)

st.markdown('<div class="section-header">📋 Datos del Cliente</div>', unsafe_allow_html=True)

st.markdown("#### Identificación")
col1, col2, col3 = st.columns(3)
with col1:
    tipo_doc_val = st.session_state.sarlaft_form.get("tipo_documento", "").upper()
    tipo_doc_options = ["", "CC", "CE", "NIT", "PA", "TI"]
    tipo_doc_index = next((i for i, opt in enumerate(tipo_doc_options) if opt and tipo_doc_val.startswith(opt)), 0)
    tipo_documento = st.selectbox(f"Tipo de Documento{field_indicator('tipo_documento')}", tipo_doc_options, index=tipo_doc_index)
with col2:
    numero_identificacion = st.text_input(f"Número de Identificación{field_indicator('numero_identificacion')}", 
        value=st.session_state.sarlaft_form.get("numero_identificacion", ""), max_chars=20)
with col3:
    fecha_expedicion = st.date_input(f"Fecha de Expedición{field_indicator('fecha_expedicion')}", 
        value=parse_date(st.session_state.sarlaft_form.get("fecha_expedicion", "")))

st.markdown("#### Información Personal")
col1, col2 = st.columns(2)
with col1:
    nombre_completo = st.text_input(f"Nombre Completo{field_indicator('nombre_completo')}", value=st.session_state.sarlaft_form.get("nombre_completo", ""))
    direccion = st.text_input(f"Dirección{field_indicator('direccion')}", value=st.session_state.sarlaft_form.get("direccion", ""))
    telefono = st.text_input(f"Teléfono{field_indicator('telefono')}", value=st.session_state.sarlaft_form.get("telefono", ""))
with col2:
    ciudad = st.text_input(f"Ciudad{field_indicator('ciudad')}", value=st.session_state.sarlaft_form.get("ciudad", ""))
    correo_electronico = st.text_input(f"Correo Electrónico{field_indicator('correo_electronico')}", value=st.session_state.sarlaft_form.get("correo_electronico", ""))
    c1, c2 = st.columns(2)
    with c1:
        es_vinculacion = st.checkbox(f"Es Vinculación{field_indicator('es_vinculacion')}", value=st.session_state.sarlaft_form.get("es_vinculacion", False))
    with c2:
        es_pep = st.checkbox(f"Es PEP{field_indicator('es_pep')}", value=st.session_state.sarlaft_form.get("es_pep", False))

st.markdown("#### Información Laboral")
col1, col2, col3 = st.columns(3)
with col1:
    actividad_economica = st.text_input(f"Actividad Económica{field_indicator('actividad_economica')}", value=st.session_state.sarlaft_form.get("actividad_economica", ""))
with col2:
    ocupacion_profesion = st.text_input(f"Ocupación/Profesión{field_indicator('ocupacion_profesion')}", value=st.session_state.sarlaft_form.get("ocupacion_profesion", ""))
with col3:
    codigo_ciiu = st.text_input(f"Código CIIU{field_indicator('codigo_ciiu')}", value=st.session_state.sarlaft_form.get("codigo_ciiu", ""), max_chars=10)

st.markdown("#### Información Financiera")
col1, col2 = st.columns(2)
with col1:
    total_ingresos = st.number_input(f"Total Ingresos Mensuales ($){field_indicator('total_ingresos_mensuales')}", 
        min_value=0.0, value=parse_money(st.session_state.sarlaft_form.get("total_ingresos_mensuales", "")), step=100000.0, format="%.2f")
    activos = st.number_input(f"Activos ($){field_indicator('activos')}", 
        min_value=0.0, value=parse_money(st.session_state.sarlaft_form.get("activos", "")), step=1000000.0, format="%.2f")
with col2:
    total_egresos = st.number_input(f"Total Egresos Mensuales ($){field_indicator('total_egresos_mensuales')}", 
        min_value=0.0, value=parse_money(st.session_state.sarlaft_form.get("total_egresos_mensuales", "")), step=100000.0, format="%.2f")
    pasivos = st.number_input(f"Pasivos ($){field_indicator('pasivos')}", 
        min_value=0.0, value=parse_money(st.session_state.sarlaft_form.get("pasivos", "")), step=1000000.0, format="%.2f")

st.markdown('<div class="section-header">💾 Exportar Registro</div>', unsafe_allow_html=True)

errors = []
if not numero_identificacion: errors.append("Número de identificación requerido")
if not nombre_completo: errors.append("Nombre completo requerido")
if not tipo_documento: errors.append("Tipo de documento requerido")
for e in errors: st.error(f"⚠️ {e}")

col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 2])

with col_btn1:
    if len(errors) == 0:
        registro = {
            "numero_identificacion": numero_identificacion.replace(".", "").replace("-", "").replace(" ", ""),
            "nombre_completo": nombre_completo,
            "tipo_documento": tipo_documento,
            "fecha_expedicion": str(fecha_expedicion) if fecha_expedicion else None,
            "direccion": direccion, "ciudad": ciudad, "telefono": telefono,
            "correo_electronico": correo_electronico,
            "total_ingresos_mensuales": total_ingresos, "total_egresos_mensuales": total_egresos,
            "actividad_economica": actividad_economica, "ocupacion_profesion": ocupacion_profesion,
            "codigo_ciiu": codigo_ciiu, "activos": activos, "pasivos": pasivos,
            "es_vinculacion": es_vinculacion, "es_pep": es_pep,
            "fecha_registro": datetime.now().isoformat()
        }
        json_str = json.dumps(registro, indent=2, ensure_ascii=False)
        st.download_button("📥 Descargar JSON", json_str, f"sarlaft_{numero_identificacion}.json", "application/json", use_container_width=True)

with col_btn2:
    if st.button("🔄 Limpiar", use_container_width=True):
        st.session_state.sarlaft_form = {k: False if "es_" in k else (None if k == "fecha_expedicion" else "") for k in st.session_state.sarlaft_form}
        st.session_state.sarlaft_status = {}
        st.session_state.sarlaft_processed = None
        st.rerun()

st.markdown("---")
st.markdown('<p style="text-align:center;color:#6c757d;font-size:0.85rem;">🔒 Sistema SARLAFT | Datos procesados en Snowflake</p>', unsafe_allow_html=True)