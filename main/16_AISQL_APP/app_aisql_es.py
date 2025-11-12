# =============================================================================
# Snowflake AISQL en Acción - Aplicación de Demostración Integral
# =============================================================================
# Una aplicación Streamlit en Snowflake que muestra todas las funciones AISQL
# con ejemplos temáticos de Tasty Bytes
# =============================================================================

import streamlit as st
from snowflake.snowpark.context import get_active_session
import json
import pypdfium2 as pdfium

# Configurar página - DEBE ser el primer comando de Streamlit
st.set_page_config(
    page_title="Snowflake Cortex AISQL Playground",
    page_icon="❄️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Inicializar sesión de Snowflake
session = get_active_session()

# =============================================================================
# CONFIGURACIÓN Y ESTILOS
# =============================================================================

# Colores de marca Snowflake
SNOWFLAKE_BLUE = "#29B5E8"
SNOWFLAKE_DARK_BLUE = "#111827" 
SNOWFLAKE_AQUA = "#00D4FF"
SNOWFLAKE_WHITE = "#FFFFFF"
SNOWFLAKE_LIGHT_GRAY = "#E8EEF2"

# Lista global de modelos AI_COMPLETE (ordenados alfabéticamente)
# Lista curada de los modelos más recientes y capaces
AI_COMPLETE_MODELS = [
    "claude-4-sonnet",
    "claude-haiku-4-5",
    "claude-sonnet-4-5",
    "llama4-maverick",
    "llama4-scout",
    "mistral-large2",
    "openai-gpt-5",
    "openai-gpt-5-mini"
]

# CSS personalizado para marca Snowflake
st.markdown(f"""
<style>
    /* Estilos principales de la aplicación */
    .stApp {{
        background-color: {SNOWFLAKE_WHITE};
    }}
    
    /* Área de contenido principal - dejar que el diseño amplio maneje el ancho */
    .main .block-container {{
        padding-left: 2rem;
        padding-right: 2rem;
        padding-top: 1rem;
    }}
    
    /* Estilos de encabezado */
    .main-header {{
        background: linear-gradient(135deg, {SNOWFLAKE_BLUE} 0%, {SNOWFLAKE_AQUA} 100%);
        padding: 25px;
        border-radius: 12px;
        margin-bottom: 30px;
        color: white;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }}
    
    /* Estilos de tarjetas para ejemplos */
    .demo-card {{
        background: linear-gradient(to right, #F8FAFC 0%, #FFFFFF 100%);
        border-left: 5px solid {SNOWFLAKE_BLUE};
        padding: 24px;
        border-radius: 12px;
        margin: 20px 0;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
        transition: all 0.3s ease;
    }}
    
    .demo-card:hover {{
        box-shadow: 0 4px 12px rgba(41, 181, 232, 0.15);
        transform: translateY(-2px);
    }}
    
    /* Estilos de botones */
    .stButton>button {{
        background: linear-gradient(135deg, {SNOWFLAKE_BLUE} 0%, {SNOWFLAKE_AQUA} 100%);
        color: white;
        border-radius: 8px;
        border: none;
        padding: 12px 28px;
        font-weight: 600;
        font-size: 16px;
        transition: all 0.3s ease;
        box-shadow: 0 2px 4px rgba(41, 181, 232, 0.3);
    }}
    
    .stButton>button:hover {{
        background: linear-gradient(135deg, {SNOWFLAKE_AQUA} 0%, {SNOWFLAKE_BLUE} 100%);
        box-shadow: 0 4px 8px rgba(41, 181, 232, 0.4);
        transform: translateY(-1px);
    }}
    
    /* Estilos de barra lateral - Gris claro moderno */
    [data-testid="stSidebar"] {{
        background-color: {SNOWFLAKE_LIGHT_GRAY};
        border-right: 1px solid #D1D9E0;
    }}
    
    [data-testid="stSidebar"] > div:first-child {{
        background-color: {SNOWFLAKE_LIGHT_GRAY};
    }}
    
    /* Colores de texto de la barra lateral */
    [data-testid="stSidebar"] * {{
        color: #1E293B !important;
    }}
    
    /* Botones de radio de la barra lateral */
    [data-testid="stSidebar"] .stRadio > label {{
        background-color: white;
        padding: 10px 12px;
        border-radius: 8px;
        margin: 4px 0;
        transition: all 0.2s ease;
        border: 1px solid #D1D9E0;
        font-size: 14px;
    }}
    
    [data-testid="stSidebar"] .stRadio > label:hover {{
        background-color: #F0F7FA;
        border-color: {SNOWFLAKE_BLUE};
    }}
    
    /* Botón de radio seleccionado */
    [data-testid="stSidebar"] .stRadio > label[data-baseweb="radio"] > div:first-child {{
        background-color: {SNOWFLAKE_BLUE};
    }}
    
    /* Contenedor de logo de la barra lateral */
    .sidebar-logo {{
        text-align: center;
        padding: 20px 10px;
        margin-bottom: 10px;
        background: white;
        border-radius: 12px;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
    }}
    
    /* Estilos de bloques de código */
    .stCodeBlock {{
        background-color: #1E293B;
        border-left: 4px solid {SNOWFLAKE_BLUE};
        border-radius: 8px;
    }}
    
    /* Estilos de métricas */
    [data-testid="stMetricValue"] {{
        font-size: 28px;
        color: {SNOWFLAKE_BLUE};
        font-weight: 700;
    }}
    
    /* Estilos de expandibles */
    .streamlit-expanderHeader {{
        background-color: #F8FAFC;
        border-radius: 8px;
        border: 1px solid #E2E8F0;
    }}
    
    .streamlit-expanderHeader:hover {{
        background-color: #F0F7FA;
        border-color: {SNOWFLAKE_BLUE};
    }}
</style>
""", unsafe_allow_html=True)

# =============================================================================
# FUNCIONES AUXILIARES
# =============================================================================

def escape_sql_string(text):
    """Escapa apostrofes en texto para consultas SQL"""
    if text is None:
        return ""
    return str(text).replace("'", "''")

def show_header():
    """Muestra el encabezado de la aplicación"""
    st.markdown('<div class="main-header"><h1>❄️ Snowflake Cortex AISQL Playground ❄️</h1><p>Impulsado por Cortex AI - Explora Cada Función AISQL</p></div>', unsafe_allow_html=True)

def execute_query(query):
    """Ejecuta una consulta de Snowflake y devuelve resultados"""
    try:
        result = session.sql(query).collect()
        return result, None
    except Exception as e:
        return None, str(e)

def show_example_card(title, description, example_num):
    """Muestra una tarjeta de ejemplo estilizada"""
    st.markdown(f"""
    <div class="demo-card">
        <h4>📌 Ejemplo {example_num}: {title}</h4>
        <p>{description}</p>
    </div>
    """, unsafe_allow_html=True)

def show_multimodal_support(text=True, images=False, documents=False, audio=False):
    """Muestra indicadores de capacidades multi-modales como badges inline"""
    capabilities = []
    if text:
        capabilities.append("📝 Texto")
    if images:
        capabilities.append("🖼️ Imágenes")
    if documents:
        capabilities.append("📄 Documentos")
    if audio:
        capabilities.append("🎵 Audio")
    
    caps_str = " · ".join(capabilities)
    return f"""<span style='background: linear-gradient(135deg, #E8F4F8 0%, #D6EEF7 100%); 
                           padding: 6px 14px; 
                           border-radius: 6px; 
                           font-size: 13px;
                           font-weight: 500;
                           color: #1E293B;
                           border: 1px solid #B8DCEA;'>
        🎯 {caps_str}
    </span>"""

def display_pdf_page():
    """Muestra la página actual del PDF como imagen"""
    pdf = st.session_state['pdf_doc']
    page_index = st.session_state['pdf_page']
    
    # Renderizar la página como imagen
    page = pdf[page_index]
    pil_image = page.render(scale=2).to_pil()
    
    # Mostrar la imagen
    st.image(pil_image, use_container_width=True)

# =============================================================================
# NAVEGACIÓN DE PÁGINAS
# =============================================================================

# Barra lateral con logo
st.sidebar.markdown("""
<div class="sidebar-logo">
    <img src="https://www.snowflake.com/wp-content/themes/snowflake/assets/img/brand-guidelines/logo-sno-blue-example.svg" width="140"/>
</div>
""", unsafe_allow_html=True)

st.sidebar.markdown("---")

# Definir todas las páginas
pages = {
    "🏠 Inicio": "home",
    "🤖 AI_COMPLETE": "ai_complete",
    "🌍 AI_TRANSLATE": "ai_translate",
    "😊 AI_SENTIMENT": "ai_sentiment",
    "🔍 AI_EXTRACT": "ai_extract",
    "🏷️ AI_CLASSIFY": "ai_classify",
    "🎯 AI_FILTER": "ai_filter",
    "🔗 AI_SIMILARITY": "ai_similarity",
    "🔒 AI_REDACT": "ai_redact",
    "🎙️ AI_TRANSCRIBE": "ai_transcribe",
    "📄 AI_PARSE_DOCUMENT": "ai_parse_document",
    "📝 AI_SUMMARIZE_AGG": "ai_summarize_agg",
    "📊 AI_AGG": "ai_agg"
}

# Selección de página
selected_page = st.sidebar.radio("**Selecciona una Función:**", list(pages.keys()), label_visibility="visible")
current_page = pages[selected_page]

# Información de la barra lateral
st.sidebar.markdown("---")
st.sidebar.markdown("""
<div style='background: white; padding: 16px; border-radius: 8px; border-left: 4px solid #29B5E8;'>
    <h4 style='color: #1E293B; margin-top: 0;'>🍔 Acerca de Tasty Bytes</h4>
    <p style='color: #475569; font-size: 14px; margin-bottom: 0;'>
        Una empresa ficticia de food trucks que sirve deliciosa comida callejera en las principales ciudades. 
        Esta demo usa datos de Tasty Bytes para mostrar las capacidades de AISQL.
    </p>
</div>
""", unsafe_allow_html=True)

# =============================================================================
# PÁGINA: INICIO
# =============================================================================

def page_home():
    show_header()
    
    st.markdown("""
    ## ¡Bienvenido a la Demo Interactiva de AISQL!
    
    Esta aplicación demuestra **12 funciones principales de Snowflake Cortex AISQL** usando escenarios del mundo real 
    del negocio de food trucks Tasty Bytes.
    
    ### ¿Qué es AISQL?
    
    Usa Cortex AISQL en Snowflake para ejecutar análisis no estructurados en texto e imágenes con LLMs líderes de la industria 
    de OpenAI, Anthropic, Meta, Mistral AI y DeepSeek. 
    
    Todos los modelos están completamente alojados en Snowflake, garantizando **rendimiento**, **escalabilidad** y **gobernanza** 
    mientras mantiene tus datos seguros y en su lugar.
    """)
    
    st.success("👈 **¡Selecciona una función de la barra lateral para explorar ejemplos interactivos!**")
    
    # Aviso de disponibilidad de modelos
    st.info("""
    ⚠️ **Nota sobre Disponibilidad de Modelos:** No todos los modelos de IA están disponibles por defecto en cada región de Snowflake. 
    Algunos modelos pueden requerir habilitar **inferencia entre regiones** para acceder a modelos alojados en diferentes regiones. 
    
    📖 Más información: [Documentación de Inferencia entre Regiones](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cross-region-inference#label-use-cross-region-inference)
    """)
    
    # Aviso de precios
    st.warning("""
    💰 **Información de Precios:** Los costos de créditos mostrados en esta aplicación son solo para referencia. 
    Siempre consulta la **[Tabla de Consumo de Créditos de Snowflake (Tabla 6a)](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf)** 
    para obtener la información de precios más actualizada y precisa.
    """)
    
    # Resumen de funciones con enlaces a documentación
    st.markdown("### 📚 Funciones AISQL Disponibles")
    
    st.markdown("""
    **[AI_COMPLETE](https://docs.snowflake.com/en/sql-reference/functions/ai_complete)**  
    Genera una completación para una cadena de texto o imagen dada usando un LLM seleccionado. Usa esta función para la mayoría de tareas de IA generativa.
    
    **[AI_TRANSLATE](https://docs.snowflake.com/en/sql-reference/functions/ai_translate)**  
    Traduce texto entre idiomas soportados.
    
    **[AI_SENTIMENT](https://docs.snowflake.com/en/sql-reference/functions/ai_sentiment)**  
    Extrae el sentimiento del texto.
    
    **[AI_EXTRACT](https://docs.snowflake.com/en/sql-reference/functions/ai_extract)**  
    Extrae información de una cadena de entrada o archivo, por ejemplo, texto, imágenes y documentos. Soporta múltiples idiomas.
    
    **[AI_CLASSIFY](https://docs.snowflake.com/en/sql-reference/functions/ai_classify)**  
    Clasifica texto o imágenes en categorías definidas por el usuario.
    
    **[AI_FILTER](https://docs.snowflake.com/en/sql-reference/functions/ai_filter)**  
    Devuelve Verdadero o Falso para una entrada de texto o imagen dada, permitiéndote filtrar resultados en cláusulas SELECT, WHERE o JOIN ... ON.
    
    **[AI_SIMILARITY](https://docs.snowflake.com/en/sql-reference/functions/ai_similarity)**  
    Calcula la similitud de embeddings entre dos entradas.
    
    **[AI_REDACT](https://docs.snowflake.com/en/sql-reference/functions/ai_redact)**  
    Oculta información de identificación personal (PII) del texto.
    
    **[AI_TRANSCRIBE](https://docs.snowflake.com/en/sql-reference/functions/ai_transcribe)**  
    Transcribe archivos de audio y video almacenados en un stage, extrayendo texto, marcas de tiempo e información del hablante.
    
    **[AI_PARSE_DOCUMENT](https://docs.snowflake.com/en/sql-reference/functions/ai_parse_document)**  
    Extrae texto (usando modo OCR) o texto con información de diseño (usando modo LAYOUT) de documentos en un stage interno o externo.
    
    **[AI_SUMMARIZE_AGG](https://docs.snowflake.com/en/sql-reference/functions/ai_summarize_agg)**  
    Agrega una columna de texto y devuelve un resumen a través de múltiples filas. Esta función no está sujeta a limitaciones de ventana de contexto.
    
    **[AI_AGG](https://docs.snowflake.com/en/sql-reference/functions/ai_agg)**  
    Agrega una columna de texto y devuelve insights a través de múltiples filas basados en un prompt definido por el usuario. Esta función no está sujeta a limitaciones de ventana de contexto.
    """)
    
    st.markdown("---")
    
    # Estadísticas rápidas
    st.markdown("### 📊 Estadísticas de la Base de Datos Demo")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        result, _ = execute_query("SELECT COUNT(*) as cnt FROM AISQL_PLAYGROUND_ES.DEMO.FOOD_TRUCKS")
        if result:
            st.metric("Food Trucks", result[0]['CNT'])
    
    with col2:
        result, _ = execute_query("SELECT COUNT(*) as cnt FROM AISQL_PLAYGROUND_ES.DEMO.CUSTOMER_REVIEWS")
        if result:
            st.metric("Reseñas de Clientes", result[0]['CNT'])
    
    with col3:
        result, _ = execute_query("SELECT COUNT(*) as cnt FROM AISQL_PLAYGROUND_ES.DEMO.MENU_ITEMS")
        if result:
            st.metric("Elementos del Menú", result[0]['CNT'])
    
    with col4:
        result, _ = execute_query("SELECT COUNT(*) as cnt FROM AISQL_PLAYGROUND_ES.DEMO.SUPPORT_TICKETS")
        if result:
            st.metric("Tickets de Soporte", result[0]['CNT'])

# =============================================================================
# PÁGINA: AI_COMPLETE
# =============================================================================

def page_ai_complete():
    show_header()
    
    st.title("🤖 AI_COMPLETE")
    
    # Soporte multi-modal y enlaces de documentación inline
    multimodal_badge = show_multimodal_support(text=True, images=False, documents=False, audio=False)
    st.markdown(f"""
    **AI_COMPLETE** genera completaciones inteligentes usando LLMs de última generación como Claude, GPT y Llama. 
    Úsalo para generación de contenido, respuesta a preguntas, enriquecimiento de datos y tareas creativas.
    
    {multimodal_badge} &nbsp;&nbsp; 📖 [Ver Documentación](https://docs.snowflake.com/en/sql-reference/functions/ai_complete)
    
    **💰 Costo (por 1M tokens):** Varía según el modelo
    - **Claude-4.5-Sonnet**: 2.80 créditos (Más reciente, Mejor calidad)
    - **OpenAI GPT-5**: 1.60 créditos (GPT más reciente)
    - **Llama4-Maverick**: 0.25 créditos (Rentable)
    - Claude-3.5-Sonnet: 2.55 créditos
    
    [Ver precios completos](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf)
    """, unsafe_allow_html=True)
    
    # Aviso de disponibilidad de modelos
    st.info("""
    ⚠️ **Nota sobre Disponibilidad de Modelos:** No todos los modelos de IA están disponibles por defecto en cada región de Snowflake. 
    Algunos modelos pueden requerir habilitar **inferencia entre regiones** para acceder a modelos alojados en diferentes regiones. 
    
    📖 Más información: [Documentación de Inferencia entre Regiones](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cross-region-inference#label-use-cross-region-inference)
    """)
    
    # Ejemplo 1: Generación de Descripciones de Menú
    show_example_card(
        "Generar Descripciones Creativas de Menú",
        "Usa AI_COMPLETE para crear descripciones atractivas de elementos del menú para marketing",
        1
    )
    
    col1, col2 = st.columns([3, 1])
    with col1:
        menu_item = st.selectbox("Selecciona un elemento del menú:", 
                                 ["Taco de Carne Asada", "Ramen Tonkotsu", "Gyro de Cordero", "Poutine Clásica"])
    with col2:
        model_ex1 = st.selectbox("Modelo:", AI_COMPLETE_MODELS, key="model_ex1")
    
    if st.button("Generar Descripción", key="gen_desc"):
        with st.spinner("Generando..."):
            escaped_item = escape_sql_string(menu_item)
            query = f"""
            SELECT AI_COMPLETE(
                '{model_ex1}',
                'Escribe una descripción creativa y apetitosa para un {escaped_item}. 
                Manténla bajo 50 palabras y hazla atractiva para los amantes de la comida.',
                {{'temperature': 0.7}}
            ) as description
            """
            result, error = execute_query(query)
            if result:
                st.success("**Descripción Generada:**")
                st.markdown(result[0]['DESCRIPTION'])
                st.code(query, language="sql")
    
    st.markdown("---")
    
    # Ejemplo 2: Procesamiento Masivo - Enriquecer Todos los Elementos del Menú
    show_example_card(
        "Procesamiento Masivo: Generar Descripciones de Marketing para Todos los Elementos del Menú",
        "Procesa tablas completas con AI_COMPLETE para enriquecer datos a escala",
        2
    )
    
    col1, col2 = st.columns([3, 1])
    with col1:
        st.write("Genera texto de marketing para todos los elementos del menú en la base de datos")
    with col2:
        model_ex2 = st.selectbox("Modelo:", AI_COMPLETE_MODELS, key="model_ex2")
    
    if st.button("Enriquecer Todos los Elementos del Menú", key="bulk_menu"):
        with st.spinner("Procesando todos los elementos del menú..."):
            query = f"""
            SELECT 
                item_name,
                category,
                price,
                description_english as original_description,
                AI_COMPLETE(
                    '{model_ex2}',
                    'Crea una descripción de marketing convincente de 30 palabras para este elemento del menú: ' || item_name || 
                    '. Categoría: ' || category || '. Hazla apetitosa y destaca sabores únicos.',
                    {{'temperature': 0.7}}
                ) as ai_marketing_copy
            FROM AISQL_PLAYGROUND_ES.DEMO.MENU_ITEMS
            LIMIT 10
            """
            result, error = execute_query(query)
            if result:
                st.success(f"**Texto de marketing generado para {len(result)} elementos del menú:**")
                st.dataframe(result, use_container_width=True)
                st.code(query, language="sql")
    
    st.markdown("---")
    
    # Ejemplo 3: Procesamiento Masivo - Categorizar Tickets de Soporte
    show_example_card(
        "Procesamiento Masivo: Auto-Categorizar Todos los Tickets de Soporte",
        "Usa IA para clasificar y priorizar grandes volúmenes de tickets de soporte",
        3
    )
    
    col1, col2 = st.columns([3, 1])
    with col1:
        st.write("Categoriza automáticamente y sugiere acciones para tickets de soporte")
    with col2:
        model_ex3 = st.selectbox("Modelo:", AI_COMPLETE_MODELS, key="model_ex3")
    
    if st.button("Categorizar Tickets", key="bulk_tickets"):
        with st.spinner("Procesando tickets de soporte..."):
            query = f"""
            WITH ai_analysis AS (
                SELECT 
                    ticket_id,
                    customer_name,
                    issue_description,
                    urgency,
                    AI_COMPLETE(
                        '{model_ex3}',
                        'Analiza este ticket de soporte y responde en formato JSON (no generes ```json\\n) con: category (Calidad de Comida/Servicio/Pago/Ubicación/Otro), priority (debe ser exactamente "Alta", "Media" o "Baja"), y suggested_action (una oración). Ticket: ' || issue_description,
                        {{'temperature': 0.3}}
                    ) as ai_analysis_json
                FROM AISQL_PLAYGROUND_ES.DEMO.SUPPORT_TICKETS
            )
            SELECT 
                ticket_id,
                customer_name,
                issue_description,
                urgency,
                TRY_PARSE_JSON(ai_analysis_json):category::STRING as category,
                TRY_PARSE_JSON(ai_analysis_json):priority::STRING as priority,
                TRY_PARSE_JSON(ai_analysis_json):suggested_action::STRING as suggested_action
            FROM ai_analysis;
            """
            result, error = execute_query(query)
            if result:
                st.success(f"**✅ Analizados {len(result)} tickets de soporte (conjunto completo de datos):**")
                st.dataframe(result, use_container_width=True)
                
                # Mostrar estadísticas resumidas
                col1, col2, col3 = st.columns(3)
                with col1:
                    # Buscar "Alta" o "High" (case insensitive, maneja espacios)
                    high_priority = sum(1 for row in result if row['PRIORITY'] and ('alta' in row['PRIORITY'].lower() or 'high' in row['PRIORITY'].lower()))
                    st.metric("Prioridad Alta", high_priority)
                with col2:
                    # Buscar "Media" o "Medium" (case insensitive, maneja espacios)
                    medium_priority = sum(1 for row in result if row['PRIORITY'] and ('media' in row['PRIORITY'].lower() or 'medium' in row['PRIORITY'].lower()))
                    st.metric("Prioridad Media", medium_priority)
                with col3:
                    # Buscar "Baja" o "Low" (case insensitive, maneja espacios)
                    low_priority = sum(1 for row in result if row['PRIORITY'] and ('baja' in row['PRIORITY'].lower() or 'low' in row['PRIORITY'].lower()))
                    st.metric("Prioridad Baja", low_priority)
                
                st.code(query, language="sql")
    
    st.markdown("---")
    
    # Ejemplo 4: Playground de Prompts Personalizados
    show_example_card(
        "Playground de Prompts Personalizados",
        "Prueba tus propios prompts con AI_COMPLETE y diferentes modelos",
        4
    )
    
    custom_prompt = st.text_area("Ingresa tu prompt personalizado:", 
                                 "¿Cuáles son 3 ideas creativas de nombres de food truck para un negocio de sushi?")
    
    col1, col2 = st.columns(2)
    with col1:
        model = st.selectbox("Selecciona modelo:", AI_COMPLETE_MODELS, key="model_ex4")
        
        # Guía de modelos
        model_info = {
            "claude-sonnet-4-5": "🌟 Claude más reciente - Mejor calidad, razonamiento",
            "openai-gpt-5": "🚀 OpenAI más reciente - Capacidades avanzadas",
            "llama4-maverick": "💰 Rentable - Gran rendimiento/precio",
            "claude-3-5-sonnet": "⚡ Calidad probada - Rápido y confiable",
            "llama3.1-70b": "🎯 Open-source - Buen equilibrio"
        }
        st.caption(model_info.get(model, ""))
        
    with col2:
        temperature = st.slider("Temperatura:", 0.0, 1.0, 0.7, 0.1)
        st.caption("Mayor = más creativo, Menor = más enfocado")
    
    if st.button("Ejecutar Prompt Personalizado", key="custom_prompt"):
        with st.spinner("Procesando..."):
            escaped_prompt = escape_sql_string(custom_prompt)
            query = f"""
            SELECT AI_COMPLETE(
                '{model}',
                '{escaped_prompt}',
                {{'temperature': {temperature}}}
            ) as result
            """
            result, error = execute_query(query)
            if result:
                st.success("**Resultado:**")
                st.markdown(result[0]['RESULT'])
                st.code(query, language="sql")
            elif error:
                st.error(f"Error: {error}")

# =============================================================================
# PÁGINA: AI_TRANSLATE
# =============================================================================

def page_ai_translate():
    show_header()
    
    st.title("🌍 AI_TRANSLATE")
    
    multimodal_badge = show_multimodal_support(text=True, images=False, documents=False, audio=False)
    st.markdown(f"""
    **AI_TRANSLATE** proporciona calidad de traducción líder en la industria en 24 idiomas.
    Perfecto para internacionalizar menús, traducir reseñas y crear contenido multilingüe.
    
    {multimodal_badge} &nbsp;&nbsp; 📖 [Ver Documentación](https://docs.snowflake.com/en/sql-reference/functions/ai_translate)
    
    **💰 Costo:** 1.50 créditos por 1M tokens (Tabla 6a)  
    [Ver detalles de precios](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf)
    
    **Idiomas Soportados (24):** Inglés, Español, Francés, Alemán, Japonés, Chino, Portugués, Italiano, 
    Holandés, Ruso, Coreano, Árabe, Hindi, Turco, Polaco, Ucraniano, Rumano, Checo, Sueco, 
    Danés, Finlandés, Noruego, Griego, Hebreo
    """, unsafe_allow_html=True)
    
    # Mapeo completo de idiomas: Nombre para mostrar -> Código ISO (todos los 24 idiomas soportados)
    ALL_LANGUAGES = {
        "Inglés": "en",
        "Español": "es",
        "Francés": "fr",
        "Alemán": "de",
        "Japonés": "ja",
        "Chino": "zh",
        "Portugués": "pt",
        "Italiano": "it",
        "Holandés": "nl",
        "Ruso": "ru",
        "Coreano": "ko",
        "Árabe": "ar",
        "Hindi": "hi",
        "Turco": "tr",
        "Polaco": "pl",
        "Ucraniano": "uk",
        "Rumano": "ro",
        "Checo": "cs",
        "Sueco": "sv",
        "Danés": "da",
        "Finlandés": "fi",
        "Noruego": "no",
        "Griego": "el",
        "Hebreo": "he"
    }
    
    # Ejemplo 1: Traducción de Menú
    show_example_card(
        "Traducir Descripciones del Menú",
        "Traduce elementos del menú a cualquiera de los 24 idiomas soportados",
        1
    )
    
    result, _ = execute_query("SELECT item_name, description_spanish FROM AISQL_PLAYGROUND_ES.DEMO.MENU_ITEMS")
    if result:
        menu_options = {r['ITEM_NAME']: r['DESCRIPTION_SPANISH'] for r in result}
        
        col1, col2 = st.columns(2)
        with col1:
            selected_item = st.selectbox("Selecciona elemento del menú:", list(menu_options.keys()))
        with col2:
            target_lang_display = st.selectbox("Traducir a:", list(ALL_LANGUAGES.keys()))
        
        if st.button("Traducir", key="translate_menu"):
            with st.spinner("Traduciendo..."):
                description = escape_sql_string(menu_options[selected_item])
                target_lang_code = ALL_LANGUAGES[target_lang_display]
                query = f"""
                SELECT AI_TRANSLATE(
                    '{description}',
                    'es',
                    '{target_lang_code}'
                ) as translation
                """
                result, error = execute_query(query)
                if result:
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown("**Original (Español):**")
                        st.write(menu_options[selected_item])
                    with col2:
                        st.markdown(f"**Traducción ({target_lang_display}):**")
                        st.write(result[0]['TRANSLATION'])
                    st.code(query, language="sql")
    
    st.markdown("---")
    
    # Ejemplo 2: Traducción por Lotes
    show_example_card(
        "Traducir Todas las Reseñas de Clientes por Lotes",
        "Traduce todas las reseñas a la vez a cualquier idioma soportado",
        2
    )
    
    batch_target_lang = st.selectbox(
        "Traducir todas las reseñas a:",
        list(ALL_LANGUAGES.keys()),
        index=list(ALL_LANGUAGES.keys()).index("Inglés"),
        key="batch_lang"
    )
    
    if st.button("Traducir Todas las Reseñas", key="batch_translate"):
        with st.spinner(f"Traduciendo todas las reseñas a {batch_target_lang}..."):
            target_code = ALL_LANGUAGES[batch_target_lang]
            query = f"""
            SELECT 
                customer_name,
                food_truck_name,
                review_text as original,
                AI_TRANSLATE(review_text, 'es', '{target_code}') as translation
            FROM AISQL_PLAYGROUND_ES.DEMO.CUSTOMER_REVIEWS
            WHERE language = 'Español'
            """
            result, error = execute_query(query)
            if result:
                st.success(f"**✅ Traducidas {len(result)} reseñas a {batch_target_lang}:**")
                st.dataframe(result, use_container_width=True)
                st.code(query, language="sql")
    
    st.markdown("---")
    
    # Ejemplo 3: Auto-detectar Idioma de Origen
    show_example_card(
        "Auto-Detectar y Traducir",
        "AI_TRANSLATE puede detectar automáticamente el idioma de origen y traducir a cualquier destino",
        3
    )
    
    custom_text = st.text_area("Ingresa texto en cualquier idioma:", 
                               "¡Hola! ¿Cómo estás hoy?")
    auto_target_display = st.selectbox("Traducir a:", list(ALL_LANGUAGES.keys()), key="auto_target")
    
    if st.button("Auto-Traducir", key="auto_translate"):
        with st.spinner("Traduciendo..."):
            escaped_text = escape_sql_string(custom_text)
            auto_target_code = ALL_LANGUAGES[auto_target_display]
            query = f"""
            SELECT AI_TRANSLATE(
                '{escaped_text}',
                '',
                '{auto_target_code}'
            ) as translation
            """
            result, error = execute_query(query)
            if result:
                st.success(f"**Traducción ({auto_target_display}):**")
                st.write(result[0]['TRANSLATION'])
                st.code(query, language="sql")

# =============================================================================
# PÁGINA: AI_SENTIMENT
# =============================================================================

def page_ai_sentiment():
    show_header()
    
    st.title("😊 AI_SENTIMENT")
    
    multimodal_badge = show_multimodal_support(text=True, images=False, documents=False, audio=False)
    st.markdown(f"""
    **AI_SENTIMENT** analiza el sentimiento en texto con puntuaciones específicas por categoría.  
    **SNOWFLAKE.CORTEX.SENTIMENT** devuelve una puntuación numérica simple de -1 a 1.
    
    Esencial para entender comentarios de clientes, redes sociales y tickets de soporte.
    
    {multimodal_badge} &nbsp;&nbsp; 📖 [Documentación AI_SENTIMENT](https://docs.snowflake.com/en/sql-reference/functions/ai_sentiment) | [Documentación SENTIMENT](https://docs.snowflake.com/en/sql-reference/functions/sentiment)
    
    **💰 Costo:** 
    - **AI_SENTIMENT:** 1.60 créditos por 1M tokens
    - **SENTIMENT (legacy):** 0.08 créditos por 1M tokens
    
    [Ver detalles de precios](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf)
    """, unsafe_allow_html=True)
    
    # Ejemplo 1: Análisis de Sentimiento de Reseñas
    show_example_card(
        "Analizar Sentimientos de Todas las Reseñas de Clientes",
        "Extrae puntuaciones de sentimiento numéricas y sentimiento específico por categoría para todas las reseñas",
        1
    )
    
    if st.button("Analizar Todas las Reseñas", key="analyze_sentiment"):
        with st.spinner("Analizando todas las reseñas de clientes..."):
            query = """
            WITH sentiment_analysis AS (
                    SELECT distinct
                        review_id,
                        customer_name,
                        food_truck_name,
                        review_text,
                        rating,
                        SNOWFLAKE.CORTEX.SENTIMENT(review_text) as overall_sentiment,
                        AI_SENTIMENT(review_text, ['Calidad de Comida', 'Servicio', 'Valor', 'Ambiente']) as category_sentiment_json
                    FROM AISQL_PLAYGROUND_ES.DEMO.CUSTOMER_REVIEWS
                )
                SELECT 
                    review_id,
                    customer_name,
                    food_truck_name,
                    rating,
                    overall_sentiment,
                    MAX(CASE WHEN f.value:name::STRING = 'overall' THEN f.value:sentiment::STRING END) as overall,
                    MAX(CASE WHEN f.value:name::STRING = 'Ambiente' THEN f.value:sentiment::STRING END) as atmosphere,
                    MAX(CASE WHEN f.value:name::STRING = 'Calidad de Comida' THEN f.value:sentiment::STRING END) as food_quality,
                    MAX(CASE WHEN f.value:name::STRING = 'Servicio' THEN f.value:sentiment::STRING END) as service,
                    MAX(CASE WHEN f.value:name::STRING = 'Valor' THEN f.value:sentiment::STRING END) as value,
                    review_text
                FROM sentiment_analysis,
                LATERAL FLATTEN(input => category_sentiment_json:categories) f
                GROUP BY review_id, customer_name, food_truck_name, rating, overall_sentiment, category_sentiment_json, review_text
                ORDER BY overall_sentiment DESC;
            """
            result, error = execute_query(query)
            if result:
                st.success(f"**✅ Analizadas {len(result)} reseñas de clientes:**")
                
                # Mostrar estadísticas resumidas
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    avg_sentiment = sum(row['OVERALL_SENTIMENT'] for row in result) / len(result)
                    st.metric("Sentimiento Promedio", f"{avg_sentiment:.3f}")
                with col2:
                    positive_count = sum(1 for row in result if row['OVERALL_SENTIMENT'] > 0.3)
                    st.metric("Reseñas Positivas", positive_count)
                with col3:
                    neutral_count = sum(1 for row in result if -0.3 <= row['OVERALL_SENTIMENT'] <= 0.3)
                    st.metric("Reseñas Neutrales", neutral_count)
                with col4:
                    negative_count = sum(1 for row in result if row['OVERALL_SENTIMENT'] < -0.3)
                    st.metric("Reseñas Negativas", negative_count)
                
                # Mostrar resultados completos
                st.markdown("**📊 Análisis Detallado de Sentimiento:**")
                
                # Crear dataframe de visualización sin review_text para la vista principal
                display_df = []
                for row in result:
                    display_df.append({
                        'ID Reseña': row['REVIEW_ID'],
                        'Cliente': row['CUSTOMER_NAME'],
                        'Food Truck': row['FOOD_TRUCK_NAME'],
                        'Calificación': '⭐' * row['RATING'],
                        'General': f"{row['OVERALL_SENTIMENT']:.3f}",
                        'Calidad Comida': row['FOOD_QUALITY'] or 'N/A',
                        'Servicio': row['SERVICE'] or 'N/A',
                        'Valor': row['VALUE'] or 'N/A',
                        'Ambiente': row['ATMOSPHERE'] or 'N/A',
                        'Reseña': row['REVIEW_TEXT'] or 'N/A'
                    })
                
                st.dataframe(display_df, use_container_width=True)
                
                
                st.code(query, language="sql")
    
    st.markdown("---")
    
    # Ejemplo 2: Análisis de Sentimiento Agregado
    show_example_card(
        "Comparación de Sentimiento de Food Trucks",
        "Compara el sentimiento promedio entre diferentes food trucks usando la función SENTIMENT",
        2
    )
    
    if st.button("Comparar Trucks", key="compare_sentiment"):
        with st.spinner("Analizando..."):
            query = """
            SELECT 
                food_truck_name,
                AVG(rating) as avg_rating,
                COUNT(*) as review_count,
                AVG(SNOWFLAKE.CORTEX.SENTIMENT(review_text)) as avg_sentiment
            FROM AISQL_PLAYGROUND_ES.DEMO.CUSTOMER_REVIEWS
            GROUP BY food_truck_name
            ORDER BY avg_sentiment DESC
            """
            result, error = execute_query(query)
            if result:
                st.dataframe(result, use_container_width=True)
                st.code(query, language="sql")
    
    st.markdown("---")
    
    # Ejemplo 3: Sentimiento de Tickets de Soporte
    show_example_card(
        "Priorizar Tickets de Soporte por Sentimiento",
        "Usa la función SENTIMENT para identificar sentimiento negativo para manejo prioritario",
        3
    )
    
    if st.button("Analizar Tickets", key="ticket_sentiment"):
        with st.spinner("Analizando tickets..."):
            query = """
            SELECT 
                ticket_id,
                customer_name,
                issue_description,
                urgency,
                SNOWFLAKE.CORTEX.SENTIMENT(issue_description) as sentiment_score,
                CASE 
                    WHEN SNOWFLAKE.CORTEX.SENTIMENT(issue_description) < -0.3 THEN 'Urgente - Negativo'
                    WHEN SNOWFLAKE.CORTEX.SENTIMENT(issue_description) < 0 THEN 'Necesita Atención'
                    ELSE 'Positivo/Neutral'
                END as priority_level
            FROM AISQL_PLAYGROUND_ES.DEMO.SUPPORT_TICKETS
            ORDER BY sentiment_score ASC
            LIMIT 10
            """
            result, error = execute_query(query)
            if result:
                st.dataframe(result, use_container_width=True)
                st.code(query, language="sql")

# =============================================================================
# PÁGINA: AI_EXTRACT
# =============================================================================

def page_ai_extract():
    show_header()
    
    st.title("🔍 AI_EXTRACT")
    
    multimodal_badge = show_multimodal_support(text=True, images=True, documents=True, audio=False)
    st.markdown(f"""
    **AI_EXTRACT** extrae información específica de texto, documentos e imágenes basándose en tus preguntas.
    Perfecto para analizar facturas, extraer entidades y extracción de datos estructurados de PDFs.
    
    {multimodal_badge} &nbsp;&nbsp; 📖 [Ver Documentación](https://docs.snowflake.com/en/sql-reference/functions/ai_extract)
    
    **💰 Costo:** 5.00 créditos por 1M tokens (Tabla 6a)  
    [Ver detalles de precios](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf)
    """, unsafe_allow_html=True)
    
    # Ejemplo 1: Extraer Entidades de Reseñas
    show_example_card(
        "Extraer Elementos del Menú Mencionados en Todas las Reseñas",
        "Identifica automáticamente qué elementos del menú mencionan los clientes en todas las reseñas",
        1
    )
    
    if st.button("Extraer de Todas las Reseñas", key="extract_items"):
        with st.spinner("Extrayendo información de todas las reseñas..."):
            query = """
            WITH extracted_data AS (
                SELECT 
                    review_id,
                    customer_name,
                    food_truck_name,
                    rating,
                    review_text,
                    AI_EXTRACT(review_text,
                        {
                            'foods_mentioned': '¿Qué elementos de comida se mencionan?', 
                            'customer_favorite': '¿Qué le gustó más al cliente?',
                            'customer_complaint': '¿De qué se quejó el cliente?'
                         }
                    ) as extracted_details
                FROM AISQL_PLAYGROUND_ES.DEMO.CUSTOMER_REVIEWS
            )
            SELECT 
                review_id,
                customer_name,
                food_truck_name,
                rating,
                extracted_details:response:foods_mentioned::STRING as foods_mentioned,
                extracted_details:response:customer_favorite::STRING as customer_favorite,
                extracted_details:response:customer_complaint::STRING as customer_complaint,
                review_text
            FROM extracted_data
            ORDER BY rating DESC
            """
            result, error = execute_query(query)
            if result:
                st.success(f"**✅ Información extraída de {len(result)} reseñas:**")
                
                # Crear dataframe de visualización
                display_df = []
                for row in result:
                    display_df.append({
                        'ID Reseña': row['REVIEW_ID'],
                        'Cliente': row['CUSTOMER_NAME'],
                        'Food Truck': row['FOOD_TRUCK_NAME'],
                        'Calificación': '⭐' * row['RATING'],
                        'Elementos Comida': row['FOODS_MENTIONED'] or 'N/A',
                        'Le Gustó Más': row['CUSTOMER_FAVORITE'] or 'N/A',
                        'Quejas': row['CUSTOMER_COMPLAINT'] or 'N/A',
                        'Reseña': row['REVIEW_TEXT']
                    })
                
                st.dataframe(display_df, use_container_width=True)
                st.code(query, language="sql")
            elif error:
                st.error(f"Error: {error}")
    
    st.markdown("---")
    
    # Ejemplo 2: Extraer Datos Estructurados de Tickets de Soporte
    show_example_card(
        "Extraer Detalles de Problemas de Todos los Tickets de Soporte",
        "Analiza todos los tickets de soporte para extraer información clave",
        2
    )
    
    if st.button("Analizar Todos los Tickets", key="extract_tickets"):
        with st.spinner("Extrayendo detalles de todos los tickets de soporte..."):
            query = """
            WITH extracted_data AS (
                SELECT 
                    ticket_id,
                    customer_name,
                    urgency,
                    status,
                    issue_description,
                    AI_EXTRACT(issue_description, 
                        {
                            'issue_type': '¿Qué tipo de problema es este? (Calidad de Comida, Servicio, Pago, Ubicación, Sugerencia, Cumplido)',
                            'requires_refund': '¿El cliente quiere un reembolso? (Sí/No)',
                            'food_item_mentioned': '¿Qué elemento de comida se menciona, si hay alguno?'
                        }
                    ) as extracted_details
                FROM AISQL_PLAYGROUND_ES.DEMO.SUPPORT_TICKETS
            )
            SELECT 
                ticket_id,
                customer_name,
                urgency,
                status,
                extracted_details:response:issue_type::STRING as issue_type,
                extracted_details:response:requires_refund::STRING as requires_refund,
                extracted_details:response:food_item_mentioned::STRING as food_item_mentioned,
                issue_description
            FROM extracted_data
            ORDER BY 
                ticket_id;
            """
            result, error = execute_query(query)
            if result:
                st.success(f"**✅ Información extraída de {len(result)} tickets de soporte:**")
                
                # Mostrar estadísticas resumidas
                col1, col2, col3 = st.columns(3)
                with col1:
                    refund_count = sum(1 for row in result if row['REQUIRES_REFUND'] and 'sí' in row['REQUIRES_REFUND'].lower())
                    st.metric("Solicitudes de Reembolso", refund_count)
                with col2:
                    high_urgency = sum(1 for row in result if row['URGENCY'] == 'Alta')
                    st.metric("Urgencia Alta", high_urgency)
                with col3:
                    open_tickets = sum(1 for row in result if row['STATUS'] == 'Abierto')
                    st.metric("Tickets Abiertos", open_tickets)
                
                # Crear dataframe de visualización
                display_df = []
                for row in result:
                    display_df.append({
                        'ID Ticket': row['TICKET_ID'],
                        'Cliente': row['CUSTOMER_NAME'],
                        'Urgencia': row['URGENCY'],
                        'Estado': row['STATUS'],
                        'Tipo Problema': row['ISSUE_TYPE'] or 'N/A',
                        '¿Reembolso?': row['REQUIRES_REFUND'] or 'N/A',
                        'Elemento Comida': row['FOOD_ITEM_MENTIONED'] or 'N/A',
                        'Descripción': row['ISSUE_DESCRIPTION']
                    })
                
                st.dataframe(display_df, use_container_width=True)
                st.code(query, language="sql")
            elif error:
                st.error(f"Error: {error}")
    
    st.markdown("---")
    
    # Ejemplo 3: Extracción Personalizada
    show_example_card(
        "Consulta de Extracción Personalizada",
        "Define tus propias preguntas de extracción",
        3
    )
    
    custom_text = st.text_area("Ingresa texto para analizar:", 
        "Visité el camión Guac n Roll ayer y pedí 3 tacos de carne asada por $13.50. ¡La comida estuvo increíble pero tuve que esperar 25 minutos!")
    
    questions = st.text_area("Ingresa preguntas (una por línea):", 
        "¿Qué elementos de comida se pidieron?\n¿Cuál fue el costo total?\n¿Cuánto tiempo fue la espera?\n¿Cuál fue el sentimiento?")
    
    if st.button("Extraer Información", key="custom_extract"):
        with st.spinner("Extrayendo..."):
            escaped_text = escape_sql_string(custom_text)
            question_list = [q.strip() for q in questions.split('\n') if q.strip()]
            question_array = str(question_list).replace("[", '').replace("]", '')
            query = f"""
            SELECT AI_EXTRACT(
                '{escaped_text}',
                ARRAY_CONSTRUCT({question_array})
            ) as extraction_result
            """
            result, error = execute_query(query)
            if result:
                st.json(json.loads(result[0]['EXTRACTION_RESULT']))
                st.code(query, language="sql")
            elif error:
                st.error(f"Error: {error}")
    
    st.markdown("---")
    
    # Ejemplo 4: Extraer de Factura de Proveedor Individual
    show_example_card(
        "Extraer Datos Estructurados de una Factura de Proveedor PDF",
        "Usa AI_EXTRACT para extraer campos clave de factura de un documento PDF",
        4
    )
    
    # Verificar si hay facturas en el stage
    stage_check_query = """
        SELECT COUNT(*) as cnt 
        FROM DIRECTORY(@AISQL_PLAYGROUND_ES.DEMO.SUPPLIER_DOCUMENTS_STAGE) 
        WHERE RELATIVE_PATH LIKE '%supplier_invoice%'
    """
    stage_result, _ = execute_query(stage_check_query)
    invoice_count = stage_result[0]['CNT'] if stage_result else 0
    
    if invoice_count == 0:
        st.warning("""
        ⚠️ **No se encontraron facturas de proveedores en SUPPLIER_DOCUMENTS_STAGE**
        
        Para usar esta demo:
        1. Genera facturas: `python generate_supplier_invoices.py`
        2. Sube a Snowflake: `PUT file://supplier_invoice_*.pdf @SUPPLIER_DOCUMENTS_STAGE AUTO_COMPRESS=FALSE;`
        """)
    else:
        st.success(f"✅ Se encontraron {invoice_count} factura(s) de proveedor en el stage")
        
        # Obtener lista de facturas disponibles
        invoices_query = """
            SELECT RELATIVE_PATH 
            FROM DIRECTORY(@AISQL_PLAYGROUND_ES.DEMO.SUPPLIER_DOCUMENTS_STAGE) 
            WHERE RELATIVE_PATH LIKE '%supplier_invoice%'
            ORDER BY RELATIVE_PATH
        """
        invoices_result, _ = execute_query(invoices_query)
        
        if invoices_result:
            invoice_files = [row['RELATIVE_PATH'] for row in invoices_result]
            
            selected_invoice = st.selectbox(
                "Selecciona una factura para extraer:",
                invoice_files,
                index=0,
                key="single_invoice_select"
            )
            
            if st.button("🔍 Extraer Datos de Factura", key="extract_single_invoice"):
                with st.spinner("Extrayendo datos de factura PDF..."):
                    query = f"""
                    WITH extracted_json AS (
                        SELECT 
                            '{selected_invoice}' as file_name,
                            BUILD_SCOPED_FILE_URL(@AISQL_PLAYGROUND_ES.DEMO.SUPPLIER_DOCUMENTS_STAGE, '{selected_invoice}') as file_url,
                            AI_EXTRACT(
                                file => TO_FILE('@AISQL_PLAYGROUND_ES.DEMO.SUPPLIER_DOCUMENTS_STAGE', '{selected_invoice}'),
                                responseFormat => {{
                                    'invoice_number': 'El número de factura (ej., INV-1001)',
                                    'invoice_date': 'La fecha de factura en formato YYYY-MM-DD',
                                    'supplier_name': 'El nombre de la empresa proveedora/vendedora',
                                    'supplier_address': 'La dirección completa del proveedor',
                                    'supplier_phone': 'El número de teléfono del proveedor',
                                    'customer_name': 'El nombre de la empresa cliente (debería ser Guac n Roll)',
                                    'customer_address': 'La dirección completa del cliente',
                                    'customer_phone': 'El número de teléfono del cliente',
                                    'subtotal': 'El monto subtotal antes de impuestos como número',
                                    'tax_amount': 'El monto de impuestos como número',
                                    'total_amount': 'El monto total de la factura como número',
                                    'payment_terms': 'Los términos de pago (ej., Net 30 Days)',
                                    'item_count': 'El número de elementos de línea en la factura'
                                }}
                            ) AS extracted_json
                    )
                    SELECT
                        file_name,
                        file_url,
                        extracted_json:response:invoice_number::string as invoice_number,
                        extracted_json:response:invoice_date::date as invoice_date,
                        extracted_json:response:supplier_name::string as supplier_name,
                        extracted_json:response:supplier_address::string as supplier_address,
                        extracted_json:response:supplier_phone::string as supplier_phone,
                        extracted_json:response:customer_name::string as customer_name,
                        extracted_json:response:customer_address::string as customer_address,
                        extracted_json:response:customer_phone::string as customer_phone,
                        REPLACE(extracted_json:response:subtotal, '$', '')::float as subtotal,
                        REPLACE(extracted_json:response:tax_amount, '$', '')::float as tax_amount,
                        REPLACE(extracted_json:response:total_amount, '$', '')::float as total_amount,
                        extracted_json:response:payment_terms::string as payment_terms,
                        extracted_json:response:item_count::integer as item_count,
                        extracted_json as raw_json
                    FROM extracted_json
                    """
                    
                    result, error = execute_query(query)
                    if result and not error:
                        row = result[0]
                        
                        st.success("✅ ¡Datos extraídos y analizados exitosamente!")
                        
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.markdown("#### 📋 JSON Extraído")
                            st.json(row['RAW_JSON'])
                        
                        with col2:
                            st.markdown("#### 📄 Factura PDF")
                            # Mostrar PDF
                            try:
                                # Inicializar estado de sesión para visualización de PDF
                                if 'pdf_page' not in st.session_state:
                                    st.session_state['pdf_page'] = 0
                                
                                if 'pdf_url' not in st.session_state:
                                    st.session_state['pdf_url'] = selected_invoice
                                
                                if 'pdf_doc' not in st.session_state or st.session_state['pdf_url'] != selected_invoice:
                                    pdf_stream = session.file.get_stream(f"@AISQL_PLAYGROUND_ES.DEMO.SUPPLIER_DOCUMENTS_STAGE/{selected_invoice}", decompress=False)
                                    pdf = pdfium.PdfDocument(pdf_stream)
                                    st.session_state['pdf_doc'] = pdf
                                    st.session_state['pdf_url'] = selected_invoice
                                    st.session_state['pdf_page'] = 0
                                
                                # Mostrar página actual
                                display_pdf_page()
                                
                                # Botón de descarga
                                pdf_stream_download = session.file.get_stream(f"@AISQL_PLAYGROUND_ES.DEMO.SUPPLIER_DOCUMENTS_STAGE/{selected_invoice}", decompress=False)
                                pdf_binary_data = pdf_stream_download.read()
                                st.download_button(
                                    label="📥 Descargar Factura PDF",
                                    data=pdf_binary_data,
                                    file_name=selected_invoice,
                                    mime="application/pdf",
                                    use_container_width=True
                                )
                                
                            except Exception as e:
                                st.error(f"Error recuperando PDF del stage de Snowflake: {e}")
                                st.info("Por favor asegúrate de que la ruta del stage y el nombre del archivo sean correctos y tengas los permisos necesarios.")
                        
                        with st.expander("🔍 Ver Consulta SQL"):
                            st.code(query, language="sql")
                    elif error:
                        st.error(f"Error: {error}")
    
    st.markdown("---")
    
    # Ejemplo 5: Extraer Todas las Facturas y Cargar en Tabla
    show_example_card(
        "Extraer por Lotes Todas las Facturas de Proveedores y Cargar en Tabla",
        "Procesa todas las facturas a la vez, extrae datos estructurados e inserta en tabla SUPPLIER_INVOICE_DETAILS",
        5
    )
    
    if invoice_count == 0:
        st.warning("⚠️ No se encontraron facturas de proveedores. Por favor sube facturas al stage primero.")
    else:
        st.info(f"📄 **Listo para procesar {invoice_count} factura(s)**")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🔍 Extraer Todas las Facturas", key="extract_all_invoices"):
                with st.spinner(f"Extrayendo datos de {invoice_count} factura(s)..."):
                    query = """
                    WITH extracted_json AS (
                        SELECT 
                            RELATIVE_PATH as file_name,
                            BUILD_SCOPED_FILE_URL(@AISQL_PLAYGROUND_ES.DEMO.SUPPLIER_DOCUMENTS_STAGE, RELATIVE_PATH) as file_url,
                            AI_EXTRACT(
                                file => TO_FILE('@AISQL_PLAYGROUND_ES.DEMO.SUPPLIER_DOCUMENTS_STAGE', RELATIVE_PATH),
                                responseFormat => {
                                    'invoice_number': 'El número de factura (ej., INV-1001)',
                                    'invoice_date': 'La fecha de factura en formato YYYY-MM-DD',
                                    'supplier_name': 'El nombre de la empresa proveedora/vendedora',
                                    'supplier_address': 'La dirección completa del proveedor',
                                    'supplier_phone': 'El número de teléfono del proveedor',
                                    'customer_name': 'El nombre de la empresa cliente (debería ser Guac n Roll)',
                                    'customer_address': 'La dirección completa del cliente',
                                    'customer_phone': 'El número de teléfono del cliente',
                                    'subtotal': 'El monto subtotal antes de impuestos como número',
                                    'tax_amount': 'El monto de impuestos como número',
                                    'total_amount': 'El monto total de la factura como número',
                                    'payment_terms': 'Los términos de pago (ej., Net 30 Days)',
                                    'item_count': 'El número de elementos de línea en la factura'
                                }
                            ) AS extracted_json
                        FROM DIRECTORY(@AISQL_PLAYGROUND_ES.DEMO.SUPPLIER_DOCUMENTS_STAGE)
                        WHERE RELATIVE_PATH LIKE '%supplier_invoice%'
                    )
                    SELECT
                        file_name,
                        file_url,
                        extracted_json:response:invoice_number::string as invoice_number,
                        extracted_json:response:invoice_date::date as invoice_date,
                        extracted_json:response:supplier_name::string as supplier_name,
                        extracted_json:response:supplier_address::string as supplier_address,
                        extracted_json:response:supplier_phone::string as supplier_phone,
                        extracted_json:response:customer_name::string as customer_name,
                        extracted_json:response:customer_address::string as customer_address,
                        extracted_json:response:customer_phone::string as customer_phone,
                        REPLACE(extracted_json:response:subtotal, '$', '')::float as subtotal,
                        REPLACE(extracted_json:response:tax_amount, '$', '')::float as tax_amount,
                        REPLACE(extracted_json:response:total_amount, '$', '')::float as total_amount,
                        extracted_json:response:payment_terms::string as payment_terms,
                        extracted_json:response:item_count::integer as item_count,
                        extracted_json as raw_json
                    FROM extracted_json
                    ORDER BY file_name
                    """
                    
                    result, error = execute_query(query)
                    if result and not error:
                        st.success(f"**✅ Extraídos y analizados datos de {len(result)} factura(s)!**")
                        
                        # Mostrar los datos extraídos
                        display_data = []
                        for row in result:
                            display_data.append({
                                'Archivo': row['FILE_NAME'],
                                'Factura #': row['INVOICE_NUMBER'] or 'N/A',
                                'Fecha': row['INVOICE_DATE'] or 'N/A',
                                'Proveedor': row['SUPPLIER_NAME'] or 'N/A',
                                'Subtotal': f"${row['SUBTOTAL']:.2f}" if row['SUBTOTAL'] else 'N/A',
                                'Impuestos': f"${row['TAX_AMOUNT']:.2f}" if row['TAX_AMOUNT'] else 'N/A',
                                'Total': f"${row['TOTAL_AMOUNT']:.2f}" if row['TOTAL_AMOUNT'] else 'N/A',
                                'Elementos': row['ITEM_COUNT'] or 'N/A'
                            })
                        
                        st.dataframe(display_data, use_container_width=True)
                        
                        with st.expander("🔍 Ver Consulta SQL"):
                            st.code(query, language="sql")
                    elif error:
                        st.error(f"Error: {error}")
        
        with col2:
            if st.button("💾 Extraer y Cargar en Tabla", key="load_invoices_table"):
                with st.spinner("Extrayendo y cargando datos en tabla SUPPLIER_INVOICE_DETAILS..."):
                    # Truncar tabla primero
                    truncate_query = "TRUNCATE TABLE AISQL_PLAYGROUND_ES.DEMO.SUPPLIER_INVOICE_DETAILS"
                    session.sql(truncate_query).collect()
                    st.info("🗑️ Tabla truncada, ahora extrayendo y cargando...")
                    
                    # Extraer e insertar
                    insert_query = """
                    INSERT INTO AISQL_PLAYGROUND_ES.DEMO.SUPPLIER_INVOICE_DETAILS (
                        file_name,
                        file_url,
                        invoice_number,
                        invoice_date,
                        supplier_name,
                        supplier_address,
                        supplier_phone,
                        customer_name,
                        customer_address,
                        customer_phone,
                        subtotal,
                        tax_amount,
                        total_amount,
                        payment_terms,
                        item_count,
                        extraction_date,
                        raw_json
                    )
                    WITH extracted_json AS (
                        SELECT 
                            RELATIVE_PATH as file_name,
                            BUILD_SCOPED_FILE_URL(@AISQL_PLAYGROUND_ES.DEMO.SUPPLIER_DOCUMENTS_STAGE, RELATIVE_PATH) as file_url,
                            AI_EXTRACT(
                                file => TO_FILE('@AISQL_PLAYGROUND_ES.DEMO.SUPPLIER_DOCUMENTS_STAGE', RELATIVE_PATH),
                                responseFormat => {
                                    'invoice_number': 'El número de factura (ej., INV-1001)',
                                    'invoice_date': 'La fecha de factura en formato YYYY-MM-DD',
                                    'supplier_name': 'El nombre de la empresa proveedora/vendedora',
                                    'supplier_address': 'La dirección completa del proveedor',
                                    'supplier_phone': 'El número de teléfono del proveedor',
                                    'customer_name': 'El nombre de la empresa cliente (debería ser Guac n Roll)',
                                    'customer_address': 'La dirección completa del cliente',
                                    'customer_phone': 'El número de teléfono del cliente',
                                    'subtotal': 'El monto subtotal antes de impuestos como número',
                                    'tax_amount': 'El monto de impuestos como número',
                                    'total_amount': 'El monto total de la factura como número',
                                    'payment_terms': 'Los términos de pago (ej., Net 30 Days)',
                                    'item_count': 'El número de elementos de línea en la factura'
                                }
                            ) AS extracted_json
                        FROM DIRECTORY(@AISQL_PLAYGROUND_ES.DEMO.SUPPLIER_DOCUMENTS_STAGE)
                        WHERE RELATIVE_PATH LIKE '%supplier_invoice%'
                    )
                    SELECT
                        file_name,
                        file_url,
                        extracted_json:response:invoice_number::string,
                        extracted_json:response:invoice_date::date,
                        extracted_json:response:supplier_name::string,
                        extracted_json:response:supplier_address::string,
                        extracted_json:response:supplier_phone::string,
                        extracted_json:response:customer_name::string,
                        extracted_json:response:customer_address::string,
                        extracted_json:response:customer_phone::string,
                        REPLACE(extracted_json:response:subtotal, '$', '')::float,
                        REPLACE(extracted_json:response:tax_amount, '$', '')::float,
                        REPLACE(extracted_json:response:total_amount, '$', '')::float,
                        extracted_json:response:payment_terms::string,
                        extracted_json:response:item_count::integer,
                        CURRENT_DATE as extraction_date,
                        extracted_json as raw_json
                    FROM extracted_json
                    """
                    
                    try:
                        result, error = execute_query(insert_query)
                        
                        if not error:
                            st.success("✅ ¡Datos cargados exitosamente en tabla SUPPLIER_INVOICE_DETAILS!")
                            
                            # Mostrar conteo de filas
                            count_query = "SELECT COUNT(*) as cnt FROM AISQL_PLAYGROUND_ES.DEMO.SUPPLIER_INVOICE_DETAILS"
                            count_result, _ = execute_query(count_query)
                            if count_result:
                                st.info(f"📊 Total de registros cargados: {count_result[0]['CNT']}")
                            
                            # Mostrar datos cargados
                            display_query = """
                            SELECT 
                                invoice_number,
                                invoice_date,
                                supplier_name,
                                subtotal,
                                tax_amount,
                                total_amount,
                                payment_terms,
                                item_count
                            FROM AISQL_PLAYGROUND_ES.DEMO.SUPPLIER_INVOICE_DETAILS
                            ORDER BY invoice_date DESC
                            """
                            display_result, _ = execute_query(display_query)
                            if display_result:
                                st.markdown("**📊 Datos de Facturas Cargados:**")
                                st.dataframe(display_result, use_container_width=True)
                            
                            with st.expander("🔍 Ver Consulta SQL INSERT"):
                                st.code(insert_query, language="sql")
                        else:
                            st.error(f"Error cargando datos: {error}")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
    
    st.markdown("---")
    
    # Ejemplo 6: Análisis de Datos de Facturas Extraídos
    show_example_card(
        "Analizar Datos de Facturas Extraídos",
        "Consulta la tabla SUPPLIER_INVOICE_DETAILS para obtener insights de negocio",
        6
    )
    
    # Verificar si la tabla tiene datos
    count_query = "SELECT COUNT(*) as cnt FROM AISQL_PLAYGROUND_ES.DEMO.SUPPLIER_INVOICE_DETAILS"
    count_result, _ = execute_query(count_query)
    table_count = count_result[0]['CNT'] if count_result else 0
    
    if table_count == 0:
        st.warning("⚠️ No hay datos en tabla SUPPLIER_INVOICE_DETAILS. Por favor ejecuta el Ejemplo 5 para extraer y cargar facturas primero.")
    else:
        st.success(f"✅ Analizando {table_count} factura(s) de la base de datos")
        
        # Tendencias de Gastos Mensuales
        st.markdown("#### 📅 Tendencias de Gastos Mensuales")
        query2 = """
        SELECT 
            DATE_TRUNC('MONTH', invoice_date) as invoice_month,
            COUNT(*) as invoice_count,
            SUM(total_amount) as total_spent,
            AVG(total_amount) as avg_invoice,
            SUM(tax_amount) as total_tax
        FROM AISQL_PLAYGROUND_ES.DEMO.SUPPLIER_INVOICE_DETAILS
        GROUP BY DATE_TRUNC('MONTH', invoice_date)
        ORDER BY invoice_month DESC
        """
        result2, _ = execute_query(query2)
        if result2:
            st.dataframe(result2, use_container_width=True)
            with st.expander("🔍 Ver SQL"):
                st.code(query2, language="sql")
        
        st.markdown("---")
        st.markdown("#### 📋 Todos los Detalles de Facturas")
        query4 = """
        SELECT 
            invoice_number,
            invoice_date,
            supplier_name,
            supplier_phone,
            subtotal,
            tax_amount,
            total_amount,
            payment_terms,
            item_count,
            extraction_date
        FROM AISQL_PLAYGROUND_ES.DEMO.SUPPLIER_INVOICE_DETAILS
        ORDER BY invoice_date DESC
        """
        result4, _ = execute_query(query4)
        if result4:
            st.dataframe(result4, use_container_width=True)
            with st.expander("🔍 Ver SQL"):
                st.code(query4, language="sql")

# =============================================================================
# PÁGINA: AI_CLASSIFY
# =============================================================================

def page_ai_classify():
    show_header()
    
    st.title("🏷️ AI_CLASSIFY")
    
    multimodal_badge = show_multimodal_support(text=True, images=True, documents=False, audio=False)
    st.markdown(f"""
    **AI_CLASSIFY** clasifica texto e imágenes en categorías definidas por el usuario.
    Ideal para categorización de contenido, enrutamiento de tickets y moderación de contenido.
    
    {multimodal_badge} &nbsp;&nbsp; 📖 [Ver Documentación](https://docs.snowflake.com/en/sql-reference/functions/ai_classify)
    
    **💰 Costo (Tabla 6a):**
    - **Texto:** 1.39 créditos por 1M tokens
    - **Imágenes:** 1.20 créditos por 1K imágenes (estimado)
    
    [Ver detalles de precios](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf)
    """, unsafe_allow_html=True)
    
    # Ejemplo 1: Clasificar Tickets de Soporte
    show_example_card(
        "Auto-Clasificar Tickets de Soporte",
        "Categoriza automáticamente tickets de soporte para enrutamiento",
        1
    )
    
    if st.button("Clasificar Tickets", key="classify_tickets"):
        with st.spinner("Clasificando..."):
            query = """
                WITH classified_data
                AS
                (
                    SELECT 
                        ticket_id,
                        customer_name,
                        issue_description,
                        urgency,
                        AI_CLASSIFY(
                            issue_description,
                            [
                                {'label': 'Problema de Calidad de Comida', 'description': 'Quejas sobre sabor de comida, temperatura, frescura u objetos extraños'},
                                {'label': 'Problema de Servicio', 'description': 'Problemas con personal, tiempos de espera o precisión de pedidos'},
                                {'label': 'Problema de Pago', 'description': 'Errores de facturación, sobrecargos o problemas del sistema de pago'},
                                {'label': 'Problema de Ubicación', 'description': 'Camión no en ubicación esperada o información de ubicación incorrecta'},
                                {'label': 'Comentario Positivo', 'description': 'Cumplidos, elogios o experiencias positivas'},
                                {'label': 'Solicitud de Funcionalidad', 'description': 'Sugerencias para nuevos elementos del menú, servicios o mejoras'}
                            ]
                        ) as classification_json
                    FROM AISQL_PLAYGROUND_ES.DEMO.SUPPORT_TICKETS
                )
                    SELECT 
                        ticket_id,
                        customer_name,
                        issue_description,
                        urgency,
                        REPLACE(REPLACE(classification_json:labels::string, '["', ''), '"]', '') as ticket_classification
                    FROM classified_data;
            """
            result, error = execute_query(query)
            if result:
                st.dataframe(result, use_container_width=True)
                st.code(query, language="sql")
    
    st.markdown("---")
    
    # Ejemplo 2: Clasificar Reseñas por Tema
    show_example_card(
        "Clasificar Todas las Reseñas por Tema",
        "Categoriza todas las reseñas para entender de qué hablan más los clientes",
        2
    )
    
    if st.button("Clasificar Todas las Reseñas", key="classify_reviews"):
        with st.spinner("Clasificando todas las reseñas..."):
            query = """
            WITH classified_reviews AS (
                SELECT 
                    review_id,
                    food_truck_name,
                    customer_name,
                    review_text,
                    rating,
                    AI_CLASSIFY(
                        review_text,
                        [
                            {'label': 'Calidad de Comida'},
                            {'label': 'Tamaño de Porción'},
                            {'label': 'Servicio al Cliente'},
                            {'label': 'Relación Calidad-Precio'},
                            {'label': 'Tiempo de Espera'},
                            {'label': 'Ambiente'}
                        ],
                        {'output_mode': 'multi'}
                    ) as topics_json
                FROM AISQL_PLAYGROUND_ES.DEMO.CUSTOMER_REVIEWS
            )
            SELECT 
                review_id,
                food_truck_name,
                customer_name,
                rating,
                REPLACE(REPLACE(REPLACE(topics_json:labels::string, '[', ''), ']', ''), '"', '') as topics,
                review_text
            FROM classified_reviews
            ORDER BY rating DESC
            """
            result, error = execute_query(query)
            if result:
                st.success(f"**✅ Clasificadas {len(result)} reseñas:**")
                
                # Crear dataframe de visualización
                display_df = []
                for row in result:
                    display_df.append({
                        'ID Reseña': row['REVIEW_ID'],
                        'Cliente': row['CUSTOMER_NAME'],
                        'Food Truck': row['FOOD_TRUCK_NAME'],
                        'Calificación': '⭐' * row['RATING'],
                        'Temas': row['TOPICS'] or 'N/A',
                        'Reseña': row['REVIEW_TEXT']
                    })
                
                st.dataframe(display_df, use_container_width=True)
                st.code(query, language="sql")
    
    st.markdown("---")
    
    # Ejemplo 3: Clasificación Personalizada de Texto
    show_example_card(
        "Clasificación Personalizada de Texto",
        "Prueba clasificar tu propio texto con categorías personalizadas",
        4
    )
    
    custom_text = st.text_area("Ingresa texto para clasificar:", 
        "¡La hamburguesa vegana estuvo increíblemente deliciosa! No puedo creer que no sea carne real. ¡La mejor opción a base de plantas en la ciudad!")
    
    categories = st.text_area("Ingresa categorías (una por línea):", 
        "Muy Positivo\nPositivo\nNeutral\nNegativo\nMuy Negativo")
    
    if st.button("Clasificar Texto", key="custom_classify"):
        with st.spinner("Clasificando..."):
            escaped_text = escape_sql_string(custom_text)
            cat_list = [{"label": c.strip()} for c in categories.split('\n') if c.strip()]
            query = f"""
            SELECT AI_CLASSIFY(
                '{escaped_text}',
                {str(cat_list)}
            ) as classification
            """
            result, error = execute_query(query)
            if result:
                st.success("**Resultado de Clasificación:**")
                st.json(json.loads(result[0]['CLASSIFICATION']))
                st.code(query, language="sql")

# =============================================================================
# PÁGINA: AI_FILTER
# =============================================================================

def page_ai_filter():
    show_header()
    
    st.title("🎯 AI_FILTER")
    
    multimodal_badge = show_multimodal_support(text=True, images=True, documents=False, audio=False)
    st.markdown(f"""
    **AI_FILTER** devuelve Verdadero/Falso para preguntas de sí o no sobre texto o imágenes.
    Perfecto para filtrar datos en cláusulas WHERE usando lenguaje natural.
    
    {multimodal_badge} &nbsp;&nbsp; 📖 [Ver Documentación](https://docs.snowflake.com/en/sql-reference/functions/ai_filter)
    
    **💰 Costo (Tabla 6a):**
    - **Texto:** 1.39 créditos por 1M tokens
    - **Imágenes:** 1.20 créditos por 1K imágenes (estimado)
    
    [Ver detalles de precios](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf)
    """, unsafe_allow_html=True)
    
    # Ejemplo 1: Filtrar Reseñas
    show_example_card(
        "Filtrar Reseñas Usando Lenguaje Natural",
        "Usa AI_FILTER para encontrar reseñas que coincidan con criterios específicos",
        1
    )
    
    # Opciones de filtro predefinidas que devolverán resultados
    review_filters = [
        "¿Esta reseña menciona elementos de comida específicos?",
        "¿Esta reseña expresa sentimiento positivo?",
        "¿Esta reseña menciona que el cliente volverá o recomendará?",
        "¿Esta reseña menciona precio o valor?",
        "¿Esta reseña menciona tiempo de espera o velocidad del servicio?"
    ]
    
    filter_question = st.selectbox("Selecciona pregunta de filtro:", review_filters)
    
    if st.button("Aplicar Filtro", key="filter_reviews"):
        with st.spinner("Filtrando reseñas..."):
            escaped_question = escape_sql_string(filter_question)
            query = f"""
            SELECT 
                review_id,
                customer_name,
                food_truck_name,
                rating,
                review_text
            FROM AISQL_PLAYGROUND_ES.DEMO.CUSTOMER_REVIEWS
            WHERE AI_FILTER(CONCAT('{escaped_question}', review_text)) = TRUE
            """
            result, error = execute_query(query)
            if result and len(result) > 0:
                st.success(f"**✅ Encontradas {len(result)} reseñas coincidentes:**")
                
                # Crear dataframe de visualización
                display_df = []
                for row in result:
                    display_df.append({
                        'ID Reseña': row['REVIEW_ID'],
                        'Cliente': row['CUSTOMER_NAME'],
                        'Food Truck': row['FOOD_TRUCK_NAME'],
                        'Calificación': '⭐' * row['RATING'],
                        'Reseña': row['REVIEW_TEXT']
                    })
                
                st.dataframe(display_df, use_container_width=True)
                st.code(query, language="sql")
            else:
                st.info("No se encontraron reseñas que coincidan con los criterios del filtro")
    
    st.markdown("---")
    
    # Ejemplo 2: Filtrar Tickets de Soporte
    show_example_card(
        "Filtrar Tickets de Soporte Usando Lenguaje Natural",
        "Encuentra tickets que coincidan con criterios o problemas específicos",
        2
    )
    
    ticket_filters = [
        "¿Esto menciona comida o elementos del menú?",
        "¿Esto menciona pago o precios?",
        "¿Es esto una queja o comentario negativo?",
        "¿Esto menciona ubicación o encontrar el camión?",
        "¿Es esto un comentario positivo o cumplido?"
    ]
    
    selected_filter = st.selectbox("Selecciona filtro:", ticket_filters)
    
    if st.button("Filtrar Tickets", key="filter_tickets"):
        with st.spinner("Filtrando tickets..."):
            escaped_filter = escape_sql_string(selected_filter)
            query = f"""
            SELECT 
                ticket_id,
                customer_name,
                urgency,
                status,
                issue_description
            FROM AISQL_PLAYGROUND_ES.DEMO.SUPPORT_TICKETS
            WHERE AI_FILTER(CONCAT('{escaped_filter}', issue_description)) = TRUE
            """
            result, error = execute_query(query)
            if result and len(result) > 0:
                st.success(f"**✅ Encontrados {len(result)} tickets coincidentes:**")
                
                # Crear dataframe de visualización
                display_df = []
                for row in result:
                    display_df.append({
                        'ID Ticket': row['TICKET_ID'],
                        'Cliente': row['CUSTOMER_NAME'],
                        'Urgencia': row['URGENCY'],
                        'Estado': row['STATUS'],
                        'Problema': row['ISSUE_DESCRIPTION']
                    })
                
                st.dataframe(display_df, use_container_width=True)
                st.code(query, language="sql")
            else:
                st.info("No se encontraron tickets que coincidan con los criterios del filtro")

# =============================================================================
# PÁGINA: AI_SIMILARITY
# =============================================================================

def page_ai_similarity():
    show_header()
    
    st.title("🔗 AI_SIMILARITY")
    
    multimodal_badge = show_multimodal_support(text=True, images=True, documents=False, audio=False)
    st.markdown(f"""
    **AI_SIMILARITY** calcula la similitud del coseno entre dos textos e imágenes sin crear explícitamente embeddings.
    Perfecto para encontrar contenido similar, detección de duplicados y sistemas de recomendación.
    
    {multimodal_badge} &nbsp;&nbsp; 📖 [Ver Documentación](https://docs.snowflake.com/en/sql-reference/functions/ai_similarity)
    
    **💰 Costo:** Calculado a partir de embeddings (ver precios de AI_EMBED en Tabla 6a)
    - Modelos de embedding de texto: 0.03-0.07 créditos por 1M tokens
    - Embeddings de imágenes: 0.004 créditos por imagen
    
    [Ver detalles de precios](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf)
    """, unsafe_allow_html=True)
    
    # Ejemplo 1: Encontrar Reseñas Similares
    show_example_card(
        "Encontrar Reseñas de Clientes Similares",
        "Compara reseñas para encontrar experiencias similares de clientes en todo el conjunto de datos",
        1
    )
    
    result, _ = execute_query("SELECT review_id, customer_name, review_text FROM AISQL_PLAYGROUND_ES.DEMO.CUSTOMER_REVIEWS")
    if result:
        review_options = {f"{r['CUSTOMER_NAME']}: {r['REVIEW_TEXT'][:50]}...": r['REVIEW_TEXT'] 
                         for r in result}
        selected_review = st.selectbox("Selecciona una reseña de referencia:", list(review_options.keys()))
        
        if st.button("Encontrar Reseñas Similares", key="similar_reviews"):
            with st.spinner("Calculando similitud en todas las reseñas..."):
                reference_text = escape_sql_string(review_options[selected_review])
                query = f"""
                SELECT 
                    review_id,
                    customer_name,
                    food_truck_name,
                    rating,
                    AI_SIMILARITY(
                        '{reference_text}',
                        review_text
                    ) as similarity_score,
                    review_text
                FROM AISQL_PLAYGROUND_ES.DEMO.CUSTOMER_REVIEWS
                WHERE review_text != '{reference_text}'
                ORDER BY similarity_score DESC
                """
                result, error = execute_query(query)
                if result:
                    st.success(f"**✅ Encontradas {len(result)} reseñas ordenadas por similitud:**")
                    
                    st.markdown("**📝 Reseña de Referencia:**")
                    st.info(review_options[selected_review])
                    
                    st.markdown("**🔍 Reseñas Similares:**")
                    
                    # Crear dataframe de visualización
                    display_df = []
                    for row in result:
                        display_df.append({
                            'ID Reseña': row['REVIEW_ID'],
                            'Similitud': f"{row['SIMILARITY_SCORE']:.4f}",
                            'Cliente': row['CUSTOMER_NAME'],
                            'Food Truck': row['FOOD_TRUCK_NAME'],
                            'Calificación': '⭐' * row['RATING'],
                            'Reseña': row['REVIEW_TEXT']
                        })
                    
                    st.dataframe(display_df, use_container_width=True)
                    st.code(query, language="sql")
    
    st.markdown("---")
    
    # Ejemplo 2: Detección de Tickets Duplicados
    show_example_card(
        "Detectar Tickets de Soporte Duplicados",
        "Encuentra tickets de soporte similares para identificar problemas recurrentes en todo el conjunto de datos",
        2
    )
    
    if st.button("Encontrar Tickets Similares", key="similar_tickets"):
        with st.spinner("Analizando todos los pares de tickets..."):
            query = """
            WITH ticket_pairs AS (
                SELECT 
                    t1.ticket_id as ticket1_id,
                    t1.customer_name as customer1,
                    t1.issue_description as issue1,
                    t2.ticket_id as ticket2_id,
                    t2.customer_name as customer2,
                    t2.issue_description as issue2,
                    AI_SIMILARITY(t1.issue_description, t2.issue_description) as similarity
                FROM AISQL_PLAYGROUND_ES.DEMO.SUPPORT_TICKETS t1
                JOIN AISQL_PLAYGROUND_ES.DEMO.SUPPORT_TICKETS t2 
                    ON t1.ticket_id < t2.ticket_id
            )
            SELECT 
                ticket1_id,
                customer1,
                ticket2_id,
                customer2,
                similarity,
                issue1,
                issue2
            FROM ticket_pairs
            WHERE similarity > 0.7
            ORDER BY similarity DESC
            """
            result, error = execute_query(query)
            if result:
                st.success(f"**✅ Encontrados {len(result)} pares de tickets similares:**")
                for row in result:
                    with st.expander(f"Similitud: {row['SIMILARITY']:.3f} - Ticket #{row['TICKET1_ID']} & #{row['TICKET2_ID']}"):
                        col1, col2 = st.columns(2)
                        with col1:
                            st.markdown(f"**Ticket #{row['TICKET1_ID']} ({row['CUSTOMER1']})**")
                            st.write(row['ISSUE1'])
                        with col2:
                            st.markdown(f"**Ticket #{row['TICKET2_ID']} ({row['CUSTOMER2']})**")
                            st.write(row['ISSUE2'])
                st.code(query, language="sql")
    
    st.markdown("---")
    
    # Ejemplo 3: Comparación de Similitud de Texto Personalizada
    show_example_card(
        "Comparación de Similitud de Texto Personalizada",
        "Compara cualquier dos textos para ver qué tan similares son",
        4
    )
    
    text1 = st.text_area("Texto 1:", "La comida estaba fría y sabía terrible. Muy decepcionado.")
    text2 = st.text_area("Texto 2:", "Mi comida llegó fría y el sabor era horrible. No volveré.")
    
    if st.button("Calcular Similitud", key="custom_similarity"):
        with st.spinner("Calculando..."):
            escaped_text1 = escape_sql_string(text1)
            escaped_text2 = escape_sql_string(text2)
            query = f"""
            SELECT AI_SIMILARITY(
                '{escaped_text1}',
                '{escaped_text2}'
            ) as similarity_score
            """
            result, error = execute_query(query)
            if result:
                score = result[0]['SIMILARITY_SCORE']
                st.metric("Puntuación de Similitud", f"{score:.4f}")
                
                if score > 0.8:
                    st.success("✅ ¡Textos muy similares!")
                elif score > 0.6:
                    st.info("🔵 Textos moderadamente similares")
                elif score > 0.4:
                    st.warning("⚠️ Textos algo similares")
                else:
                    st.error("❌ Textos muy diferentes")
                
                st.code(query, language="sql")

# =============================================================================
# PÁGINA: AI_REDACT
# =============================================================================

def page_ai_redact():
    show_header()
    
    st.title("🔒 AI_REDACT")
    
    multimodal_badge = show_multimodal_support(text=True, images=False, documents=False, audio=False)
    st.markdown(f"""
    **AI_REDACT** oculta información de identificación personal (PII) de datos de texto no estructurados.
    Perfecto para cumplimiento de privacidad de datos, anonimización e intercambio seguro de datos.
    
    {multimodal_badge} &nbsp;&nbsp; 📖 [Ver Documentación](https://docs.snowflake.com/en/sql-reference/functions/ai_redact)
    
    **💰 Costo:** 0.63 créditos por 1M tokens (Tabla 6a)  
    [Ver detalles de precios](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf)
    
    **🔍 Categorías de PII Detectadas:**  
    NAME, EMAIL, PHONE_NUMBER, SSN, CREDIT_CARD, ADDRESS, DATE_OF_BIRTH, IP_ADDRESS y más
    """, unsafe_allow_html=True)
    
    # Ejemplo 1: Ocultar Todo el PII de Tickets de Soporte
    show_example_card(
        "Ocultar Todo el PII de Tickets de Soporte",
        "Oculta automáticamente todos los tipos de PII de comunicaciones de soporte al cliente",
        1
    )
    
    if st.button("Ocultar Todo el PII", key="redact_all"):
        with st.spinner("Ocultando PII de tickets de soporte..."):
            query = """
            SELECT 
                ticket_id,
                customer_name,
                food_truck_name,
                issue_description as original_text,
                AI_REDACT(issue_description) as redacted_text
            FROM AISQL_PLAYGROUND_ES.DEMO.SUPPORT_TICKETS_PII
            LIMIT 5
            """
            result, error = execute_query(query)
            if result:
                st.success(f"**✅ PII ocultado de {len(result)} tickets de soporte:**")
                
                for row in result:
                    with st.expander(f"Ticket #{row['TICKET_ID']} - {row['CUSTOMER_NAME']} ({row['FOOD_TRUCK_NAME']})"):
                        col1, col2 = st.columns(2)
                        with col1:
                            st.markdown("**Original:**")
                            st.write(row['ORIGINAL_TEXT'])
                        with col2:
                            st.markdown("**Ocultado:**")
                            st.write(row['REDACTED_TEXT'])
                
                st.code(query, language="sql")
            elif error:
                st.error(f"Error: {error}")
    
    st.markdown("---")
    
    # Ejemplo 2: Ocultar Categorías Específicas de PII
    show_example_card(
        "Ocultar Categorías Específicas de PII",
        "Elige qué tipos de PII ocultar usando filtros de categoría",
        2
    )
    
    st.info("Selecciona categorías específicas de PII para ocultar mientras preservas otra información")
    
    # Multi-selección para categorías de PII
    pii_categories = st.multiselect(
        "Selecciona categorías de PII para ocultar:",
        ["NAME", "EMAIL", "PHONE_NUMBER", "SSN", "CREDIT_CARD", "ADDRESS", "DATE_OF_BIRTH", "IP_ADDRESS"],
        default=["NAME", "EMAIL"],
        key="pii_categories"
    )
    
    if st.button("Ocultar Categorías Seleccionadas", key="redact_specific"):
        if pii_categories:
            with st.spinner(f"Ocultando {', '.join(pii_categories)} de tickets de soporte..."):
                categories_array = str(pii_categories).replace("'", "''")
                query = f"""
                SELECT 
                    ticket_id,
                    customer_name,
                    food_truck_name,
                    issue_description as original_text,
                    AI_REDACT(issue_description, {pii_categories}) as redacted_text
                FROM AISQL_PLAYGROUND_ES.DEMO.SUPPORT_TICKETS_PII
                LIMIT 5
                """
                result, error = execute_query(query)
                if result:
                    st.success(f"**✅ Ocultado {', '.join(pii_categories)} de {len(result)} tickets de soporte:**")
                    
                    for row in result:
                        with st.expander(f"Ticket #{row['TICKET_ID']} - {row['CUSTOMER_NAME']} ({row['FOOD_TRUCK_NAME']})"):
                            col1, col2 = st.columns(2)
                            with col1:
                                st.markdown("**Original:**")
                                st.write(row['ORIGINAL_TEXT'])
                            with col2:
                                st.markdown("**Ocultado:**")
                                st.write(row['REDACTED_TEXT'])
                    
                    st.code(query, language="sql")
                elif error:
                    st.error(f"Error: {error}")
        else:
            st.warning("Por favor selecciona al menos una categoría de PII para ocultar")
    
    st.markdown("---")
    
    # Ejemplo 3: Ocultación de Texto Personalizada
    show_example_card(
        "Ocultación de Texto Personalizada",
        "Prueba AI_REDACT en tu propio texto",
        3
    )
    
    custom_text = st.text_area(
        "Ingresa texto que contenga PII:",
        "Mi nombre es Juan Pérez y mi email es juan.perez@ejemplo.com. Puedes contactarme al 555-123-4567. Mi SSN es 123-45-6789.",
        height=100
    )
    
    col1, col2 = st.columns(2)
    with col1:
        redact_all_categories = st.checkbox("Ocultar todas las categorías de PII", value=True, key="custom_redact_all")
    
    with col2:
        if not redact_all_categories:
            custom_categories = st.multiselect(
                "Selecciona categorías:",
                ["NAME", "EMAIL", "PHONE_NUMBER", "SSN"],
                default=["EMAIL"],
                key="custom_categories"
            )
    
    if st.button("Ocultar Texto Personalizado", key="redact_custom"):
        with st.spinner("Ocultando PII..."):
            escaped_text = escape_sql_string(custom_text)
            
            if redact_all_categories:
                query = f"""
                SELECT AI_REDACT('{escaped_text}') as redacted_text
                """
            else:
                query = f"""
                SELECT AI_REDACT('{escaped_text}', {custom_categories}) as redacted_text
                """
            
            result, error = execute_query(query)
            if result:
                st.success("**Resultado de Ocultación:**")
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("**Original:**")
                    st.info(custom_text)
                with col2:
                    st.markdown("**Ocultado:**")
                    st.success(result[0]['REDACTED_TEXT'])
                
                st.code(query, language="sql")
            elif error:
                st.error(f"Error: {error}")

# =============================================================================
# PÁGINA: AI_TRANSCRIBE
# =============================================================================

def page_ai_transcribe():
    show_header()
    
    st.title("🎙️ AI_TRANSCRIBE")
    
    multimodal_badge = show_multimodal_support(text=False, images=False, documents=False, audio=True)
    st.markdown(f"""
    **AI_TRANSCRIBE** convierte archivos de audio y video a texto con marcas de tiempo e identificación de hablantes.
    Soporta 31 idiomas y procesa archivos de hasta 2 horas de duración.
    
    {multimodal_badge} &nbsp;&nbsp; 📖 [Ver Documentación](https://docs.snowflake.com/en/sql-reference/functions/ai_transcribe)
    
    **💰 Costo:** 1.30 créditos por 1M tokens (Tabla 6a)  
    [Ver detalles de precios](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf)
    """, unsafe_allow_html=True)
    
    # Archivos de audio disponibles
    audio_files = {
        "call_001_order_issue.wav": "Problema de Pedido - Cliente reportando artículos incorrectos recibidos",
        "call_002_food_quality.wav": "Calidad de Comida - Queja sobre comida fría",
        "call_003_allergy_concern.wav": "Preocupación por Alergia - Cliente preguntando sobre ingredientes",
        "call_004_location_issue.wav": "Problema de Ubicación - Camión no en ubicación esperada",
        "call_005_payment_error.wav": "Error de Pago - Problema de facturación",
        "call_006_positive_feedback.wav": "Comentario Positivo - Cumplido del cliente",
        "call_007_catering_inquiry.wav": "Consulta de Catering - Pregunta sobre catering para eventos",
        "call_008_delivery_delay.wav": "Retraso en Entrega - Pedido llegando tarde",
        "call_009_menu_question.wav": "Pregunta sobre Menú - Preguntando sobre ingredientes",
        "call_010_refund_request.wav": "Solicitud de Reembolso - Cliente solicitando reembolso"
    }
    
    # Ejemplo 1: Transcripción Básica con Reproductor de Audio
    show_example_card(
        "Transcribir Llamada de Servicio al Cliente",
        "Convierte audio a texto y escucha la grabación",
        1
    )
    
    selected_audio = st.selectbox(
        "Selecciona una grabación de llamada:",
        list(audio_files.keys()),
        format_func=lambda x: f"{x.replace('.wav', '').replace('call_', 'Llamada #').replace('_', ' ').title()} - {audio_files[x]}"
    )
    
    st.info(f"📞 **Llamada Seleccionada:** {audio_files[selected_audio]}")
    
    # Reproductor de audio - obtener URL con alcance del stage
    try:
        # Obtener URL presignada para reproducción de audio
        url_query = f"SELECT GET_PRESIGNED_URL(@AISQL_PLAYGROUND_ES.DEMO.AUDIO_STAGE, '{selected_audio}', 3600) as audio_url"
        url_result, _ = execute_query(url_query)
        if url_result and url_result[0]['AUDIO_URL']:
            audio_url = url_result[0]['AUDIO_URL']
            st.audio(audio_url, format='audio/wav')
        else:
            st.caption("🎵 Reproductor de audio no disponible - usando BUILD_SCOPED_FILE_URL como respaldo")
            # Respaldo a URL de archivo con alcance
            scoped_query = f"SELECT BUILD_SCOPED_FILE_URL(@AISQL_PLAYGROUND_ES.DEMO.AUDIO_STAGE, '{selected_audio}') as audio_url"
            scoped_result, _ = execute_query(scoped_query)
            if scoped_result and scoped_result[0]['AUDIO_URL']:
                st.audio(scoped_result[0]['AUDIO_URL'], format='audio/wav')
    except Exception as e:
        st.caption(f"🎵 Reproductor de audio no disponible: {str(e)}")
    
    if st.button("Transcribir Audio", key="transcribe_basic"):
        with st.spinner("Transcribiendo audio..."):
            query = f"""
            SELECT 
                '{selected_audio}' as audio_file,
                AI_TRANSCRIBE(
                    TO_FILE('@AISQL_PLAYGROUND_ES.DEMO.AUDIO_STAGE/{selected_audio}')
                ) as transcription
            """
            result, error = execute_query(query)
            if result:
                transcription_data = json.loads(result[0]['TRANSCRIPTION'])
                st.success("**¡Transcripción Completa!**")
                st.markdown("**Texto Transcrito:**")
                st.info(transcription_data.get('text', 'No se encontró texto'))
                st.caption(f"Idioma: {transcription_data.get('language', 'desconocido').upper()}")
                st.code(query, language="sql")
            elif error:
                st.error(f"Error: {error}")
    
    st.markdown("---")
    
    # Ejemplo 2: Transcripción + Análisis de Sentimiento
    show_example_card(
        "Post-Procesamiento: Transcribir + Análisis de Sentimiento",
        "Transcribe llamada e inmediatamente analiza el sentimiento",
        2
    )
    
    selected_audio2 = st.selectbox(
        "Selecciona una grabación de llamada:",
        list(audio_files.keys()),
        format_func=lambda x: f"{x.replace('.wav', '').replace('call_', 'Llamada #').replace('_', ' ').title()}",
        key="audio2"
    )
    
    if st.button("Transcribir y Analizar Sentimiento", key="transcribe_sentiment"):
        with st.spinner("Transcribiendo y analizando..."):
            query = f"""
            WITH transcription AS (
                SELECT 
                    '{selected_audio2}' as audio_file,
                    AI_TRANSCRIBE(
                        TO_FILE('@AISQL_PLAYGROUND_ES.DEMO.AUDIO_STAGE/{selected_audio2}')
                    ) as transcript_json
            )
            SELECT 
                audio_file,
                transcript_json:text::STRING as transcribed_text,
                SNOWFLAKE.CORTEX.SENTIMENT(transcript_json:text::STRING) as sentiment_score,
                CASE 
                    WHEN SNOWFLAKE.CORTEX.SENTIMENT(transcript_json:text::STRING) > 0.3 THEN 'Positivo 😊'
                    WHEN SNOWFLAKE.CORTEX.SENTIMENT(transcript_json:text::STRING) < -0.3 THEN 'Negativo 😟'
                    ELSE 'Neutral 😐'
                END as sentiment_category
            FROM transcription
            """
            result, error = execute_query(query)
            if result:
                st.success("**Análisis Completo!**")
                st.markdown("**Texto Transcrito:**")
                st.write(result[0]['TRANSCRIBED_TEXT'])
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Puntuación de Sentimiento", f"{result[0]['SENTIMENT_SCORE']:.3f}")
                with col2:
                    st.metric("Categoría", result[0]['SENTIMENT_CATEGORY'])
                
                st.code(query, language="sql")
            elif error:
                st.error(f"Error: {error}")
    
    st.markdown("---")
    
    # Ejemplo 3: Transcripción + Generar Respuesta
    show_example_card(
        "Post-Procesamiento: Transcribir + Generar Respuesta",
        "Transcribe llamada y genera una respuesta apropiada de servicio al cliente",
        3
    )
    
    col1, col2 = st.columns([3, 1])
    with col1:
        selected_audio3 = st.selectbox(
            "Selecciona una grabación de llamada:",
            list(audio_files.keys()),
            format_func=lambda x: f"{x.replace('.wav', '').replace('call_', 'Llamada #').replace('_', ' ').title()}",
            key="audio3"
        )
    with col2:
        model_ex3_transcribe = st.selectbox("Modelo:", AI_COMPLETE_MODELS, key="model_transcribe_ex3")
    
    if st.button("Transcribir y Generar Respuesta", key="transcribe_complete"):
        with st.spinner("Transcribiendo y generando respuesta..."):
            query = f"""
            WITH transcription AS (
                SELECT 
                    AI_TRANSCRIBE(
                        TO_FILE('@AISQL_PLAYGROUND_ES.DEMO.AUDIO_STAGE/{selected_audio3}')
                    ) as transcript_json
            )
            SELECT 
                transcript_json:text::STRING as transcribed_text,
                AI_COMPLETE(
                    '{model_ex3_transcribe}',
                    'Eres un agente de servicio al cliente profesional para los food trucks Tasty Bytes. Basándote en esta llamada de cliente transcrita, escribe una respuesta breve y empática que aborde sus preocupaciones: ' || transcript_json:text::STRING,
                    {{'temperature': 0.5}}
                ) as suggested_response
            FROM transcription
            """
            result, error = execute_query(query)
            if result:
                st.success("**Respuesta Generada!**")
                
                with st.expander("📝 Ver Transcripción"):
                    st.write(result[0]['TRANSCRIBED_TEXT'])
                
                st.markdown("**💬 Respuesta Sugerida:**")
                st.markdown(result[0]['SUGGESTED_RESPONSE'])
                
                st.code(query, language="sql")
            elif error:
                st.error(f"Error: {error}")
    
    st.markdown("---")
    
    # Ejemplo 4: Dashboard Completo de Análisis de Llamadas
    show_example_card(
        "Dashboard Completo de Análisis de Llamadas",
        "Procesa llamadas por lotes con transcripción, resumen, sentimiento y recomendaciones de acción",
        4
    )
    
    st.info("🎯 **Pipeline de Análisis Completo:** Transcribir → Resumir → Analizar Sentimiento → Generar Acciones")
    
    # Seleccionar número de llamadas y modelo
    col1, col2 = st.columns([3, 1])
    with col1:
        num_calls = st.slider("Número de llamadas a analizar:", min_value=2, max_value=5, value=3, key="num_calls_dashboard")
    with col2:
        model_ex4_transcribe = st.selectbox("Modelo:", AI_COMPLETE_MODELS, key="model_transcribe_ex4")
    
    if st.button("Analizar Llamadas", key="analyze_calls_dashboard"):
        with st.spinner(f"Procesando {num_calls} grabaciones de llamadas..."):
            # Obtener subconjunto de archivos de audio
            audio_list = list(audio_files.keys())[:num_calls]
            
            # Construir consulta de análisis completa
            union_parts = []
            for audio in audio_list:
                union_parts.append(f"""
                SELECT 
                    '{audio}' as filename,
                    '{audio_files[audio]}' as call_type,
                    AI_TRANSCRIBE(TO_FILE('@AISQL_PLAYGROUND_ES.DEMO.AUDIO_STAGE/{audio}')) as transcript_json
                """)
            
            union_query = " UNION ALL ".join(union_parts)
            
            query = f"""
            WITH transcriptions AS (
                {union_query}
            ),
            analyzed_calls AS (
                SELECT 
                    filename,
                    call_type,
                    transcript_json:text::STRING as transcribed_text,
                    AI_COMPLETE(
                        '{model_ex4_transcribe}',
                        'Resume esta llamada de servicio al cliente en 2-3 oraciones: ' || transcript_json:text::STRING,
                        {{'temperature': 0.3}}
                    ) as call_summary,
                    SNOWFLAKE.CORTEX.SENTIMENT(transcript_json:text::STRING) as sentiment_score,
                    CASE 
                        WHEN SNOWFLAKE.CORTEX.SENTIMENT(transcript_json:text::STRING) > 0.3 THEN 'Positivo 😊'
                        WHEN SNOWFLAKE.CORTEX.SENTIMENT(transcript_json:text::STRING) < -0.3 THEN 'Negativo 😟'
                        ELSE 'Neutral 😐'
                    END as sentiment_category,
                    AI_COMPLETE(
                        '{model_ex4_transcribe}',
                        'Basándote en esta transcripción de llamada de servicio al cliente, proporciona exactamente 3 acciones recomendadas específicas. Formato como lista numerada (1., 2., 3.): ' || transcript_json:text::STRING,
                        {{'temperature': 0.4}}
                    ) as recommended_actions
                FROM transcriptions
            )
            SELECT * FROM analyzed_calls
            """
            
            result, error = execute_query(query)
            
            if result:
                st.success(f"**✅ Analizadas exitosamente {len(result)} llamadas!**")
                
                # Mostrar cada llamada en un diseño de tarjeta
                for idx, row in enumerate(result, 1):
                    st.markdown(f"### 📞 Llamada {idx}: {row['CALL_TYPE']}")
                    
                    # Crear diseño de 3 columnas para métricas clave
                    col1, col2, col3 = st.columns([2, 1, 1])
                    with col1:
                        st.markdown(f"**📁 Archivo:** `{row['FILENAME']}`")
                    with col2:
                        st.metric("Puntuación de Sentimiento", f"{row['SENTIMENT_SCORE']:.3f}")
                    with col3:
                        st.markdown(f"**Categoría:** {row['SENTIMENT_CATEGORY']}")
                    
                    # Secciones expandibles para información detallada
                    with st.expander("📝 Texto Transcrito", expanded=False):
                        st.text_area("Transcripción Completa", row['TRANSCRIBED_TEXT'], height=150, key=f"trans_{idx}")
                    
                    # Resumen en una caja con color
                    st.markdown(f"""
                    <div style='background: linear-gradient(135deg, #F0F9FF 0%, #E0F2FE 100%); 
                                padding: 15px; 
                                border-radius: 8px; 
                                border-left: 4px solid {SNOWFLAKE_BLUE};
                                margin: 10px 0;'>
                        <strong>📋 Resumen:</strong><br/>
                        {row['CALL_SUMMARY']}
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Acciones recomendadas en una caja estilizada
                    st.markdown(f"""
                    <div style='background: linear-gradient(135deg, #F0FDF4 0%, #DCFCE7 100%); 
                                padding: 15px; 
                                border-radius: 8px; 
                                border-left: 4px solid #22C55E;
                                margin: 10px 0;'>
                        <strong>✅ Acciones Recomendadas:</strong><br/>
                        {row['RECOMMENDED_ACTIONS'].replace(chr(10), '<br/>')}
                    </div>
                    """, unsafe_allow_html=True)
                    
                    if idx < len(result):
                        st.markdown("---")
                
                # Mostrar la consulta
                with st.expander("🔍 Ver Consulta SQL"):
                    st.code(query, language="sql")
                    
            elif error:
                st.error(f"Error: {error}")
    
    st.markdown("---")
    
    st.markdown("""
    ### 🎯 Capacidades Clave Demostradas
    
    1. **Transcripción Básica**: Convierte audio a texto con detección de idioma
    2. **Análisis de Sentimiento**: Entiende emociones del cliente a partir de llamadas transcritas
    3. **Generación de Respuesta con IA**: Auto-genera respuestas de servicio al cliente
    4. **Procesamiento por Lotes**: Transcribe y resume múltiples llamadas a la vez
    
    ### 📋 Formatos Soportados
    - **Audio**: FLAC, MP3, MP4, OGG, WAV, WEBM
    - **Video**: MKV, MP4, OGV, WEBM
    - **Duración Máxima**: 120 minutos (60 min con marcas de tiempo)
    - **Tamaño Máximo de Archivo**: 700 MB
    - **Idiomas**: 31 idiomas con auto-detección
    """)

# =============================================================================
# PÁGINA: AI_PARSE_DOCUMENT
# =============================================================================

def page_ai_parse_document():
    show_header()
    
    st.title("📄 AI_PARSE_DOCUMENT")
    
    multimodal_badge = show_multimodal_support(text=False, images=False, documents=True, audio=False)
    st.markdown(f"""
    **AI_PARSE_DOCUMENT** extrae texto y diseño de documentos con alta fidelidad.
    Soporta modo OCR (solo texto) y modo LAYOUT (preserva tablas y estructura).
    
    {multimodal_badge} &nbsp;&nbsp; 📖 [Ver Documentación](https://docs.snowflake.com/en/sql-reference/functions/ai_parse_document)
    
    **💰 Costo (por 1,000 páginas - Tabla 6e):**
    - **Modo OCR:** 0.5 créditos por 1K páginas
    - **Modo Layout:** 3.33 créditos por 1K páginas
    
    [Ver detalles de precios](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf)
    """, unsafe_allow_html=True)
    
    # Obtener documentos PDF disponibles del stage
    docs_query = "SELECT RELATIVE_PATH FROM DIRECTORY(@AISQL_PLAYGROUND_ES.DEMO.DOCUMENT_STAGE) ORDER BY RELATIVE_PATH"
    docs_result, docs_error = execute_query(docs_query)
    
    if docs_error or not docs_result:
        st.warning("⚠️ No se encontraron documentos en DOCUMENT_STAGE. Por favor carga documentos PDF al stage primero.")
        st.info("""
        **Para cargar documentos:**
        1. Usa `PUT file://tu_documento.pdf @AISQL_PLAYGROUND_ES.DEMO.DOCUMENT_STAGE AUTO_COMPRESS=FALSE;`
        2. O carga vía interfaz de Snowsight al DOCUMENT_STAGE
        """)
        return
    
    available_docs = [row['RELATIVE_PATH'] for row in docs_result]
    doc_count = len(available_docs)
    
    st.success(f"✅ Encontrados {doc_count} documento(s) en DOCUMENT_STAGE")
    
    # Mostrar documentos disponibles
    with st.expander("📄 Documentos Disponibles", expanded=False):
        for doc in available_docs:
            st.markdown(f"- {doc}")
    
    # Ejemplo 1: Analizar y Almacenar Documentos
    show_example_card(
        "Analizar Documentos y Almacenar Texto Crudo",
        "Extrae texto de todos los PDFs y almacena en tabla PARSE_DOC_RAW_TEXT",
        1
    )
    
    st.info("📄 **Paso 1:** Analiza documentos y extrae texto crudo para procesamiento posterior")
    
    parse_mode = st.radio(
        "Selecciona modo de análisis:",
        ["OCR", "LAYOUT"],
        format_func=lambda x: f"Modo {x} - {'Rápido, solo texto plano' if x == 'OCR' else 'Preserva tablas y formato (recomendado)'}",
        key="parse_mode",
        horizontal=True
    )
    
    st.caption(f"**{'⚡ Modo OCR:' if parse_mode == 'OCR' else '📋 Modo LAYOUT:'}** "
               f"{'Procesamiento más rápido, extrae solo texto plano (0.5 créditos/1K páginas)' if parse_mode == 'OCR' else 'Preserva estructura del documento, tablas y formato - procesamiento más lento pero más preciso (3.33 créditos/1K páginas)'}")
    
    if st.button("Analizar Todos los Documentos", key="parse_all_docs"):
        with st.spinner(f"Analizando {doc_count} documento(s) en modo {parse_mode}..."):
            # Primero, truncar tabla existente
            truncate_query = "TRUNCATE TABLE AISQL_PLAYGROUND_ES.DEMO.PARSE_DOC_RAW_TEXT;"
            execute_query(truncate_query)
            
            total_docs = 0
            for doc_file in available_docs:
                st.write(f"Procesando **{doc_file}**...")
                
                # Analizar y almacenar texto crudo
                parse_query = f"""
                INSERT INTO AISQL_PLAYGROUND_ES.DEMO.PARSE_DOC_RAW_TEXT (file_name, file_url, raw_text)
                SELECT 
                    '{doc_file}' as file_name,
                    TO_VARCHAR(GET_PRESIGNED_URL(@AISQL_PLAYGROUND_ES.DEMO.DOCUMENT_STAGE, '{doc_file}', 3600)) as file_url,
                    parsed_json:content::STRING as raw_text
                FROM (
                    SELECT 
                        AI_PARSE_DOCUMENT(
                            TO_FILE('@AISQL_PLAYGROUND_ES.DEMO.DOCUMENT_STAGE/{doc_file}'),
                            {{'mode': '{parse_mode}'}}
                        ) as parsed_json
                )
                """
                result, error = execute_query(parse_query)
                
                if not error:
                    total_docs += 1
                    st.success(f"✅ {doc_file} analizado exitosamente")
                else:
                    st.error(f"❌ Error analizando {doc_file}: {error}")
            
            if total_docs > 0:
                st.success(f"**🎉 ¡Analizados exitosamente {total_docs} documento(s) en modo {parse_mode}!**")
                
                # Mostrar datos de muestra de la tabla
                sample_query = """
                SELECT 
                    file_name, 
                    LEFT(raw_text, 500) as preview,
                    LENGTH(raw_text) as text_length,
                    parsed_date
                FROM AISQL_PLAYGROUND_ES.DEMO.PARSE_DOC_RAW_TEXT
                ORDER BY parsed_date DESC
                """
                sample_result, _ = execute_query(sample_query)
                if sample_result:
                    st.markdown("**📊 Documentos Almacenados:**")
                    for row in sample_result:
                        with st.expander(f"📄 {row['FILE_NAME']} ({row['TEXT_LENGTH']:,} caracteres)"):
                            st.caption(f"Analizado: {row['PARSED_DATE']}")
                            st.text_area("Vista Previa del Texto", row['PREVIEW'] + "...", height=150, key=f"preview_{row['FILE_NAME']}")
    
    st.markdown("---")
    
    # Ejemplo 2: Fragmentar Documentos Analizados
    show_example_card(
        "Fragmentar Documentos Analizados",
        "Divide documentos en fragmentos y almacena en tabla PARSE_DOC_CHUNKED_TEXT",
        2
    )
    
    st.info("✂️ **Paso 2:** Fragmenta documentos analizados para búsqueda semántica y RAG usando SPLIT_TEXT_RECURSIVE_CHARACTER")
    
    # Verificar si hay documentos en PARSE_DOC_RAW_TEXT
    check_raw_query = "SELECT COUNT(*) as cnt FROM AISQL_PLAYGROUND_ES.DEMO.PARSE_DOC_RAW_TEXT"
    check_raw_result, _ = execute_query(check_raw_query)
    raw_doc_count = check_raw_result[0]['CNT'] if check_raw_result else 0
    
    if raw_doc_count == 0:
        st.warning("⚠️ No hay documentos en PARSE_DOC_RAW_TEXT. Por favor ejecuta el Ejemplo 1 primero para analizar documentos.")
    else:
        st.success(f"✅ Encontrados {raw_doc_count} documento(s) en PARSE_DOC_RAW_TEXT listos para fragmentar")
        
        col1, col2 = st.columns(2)
        with col1:
            chunk_size = st.number_input(
                "Tamaño de fragmento (caracteres):",
                min_value=100,
                max_value=5000,
                value=1512,
                step=100,
                key="chunk_size"
            )
            st.caption("Fragmentos más grandes retienen más contexto pero pueden ser menos precisos")
        
        with col2:
            chunk_overlap = st.number_input(
                "Solapamiento de fragmentos:",
                min_value=0,
                max_value=500,
                value=200,
                step=50,
                key="chunk_overlap"
            )
            st.caption("El solapamiento ayuda a mantener contexto entre fragmentos")
        
        if st.button("Fragmentar Documentos", key="chunk_documents"):
            with st.spinner("Fragmentando documentos..."):
                # Primero verificar si hay documentos para fragmentar
                check_query = "SELECT COUNT(*) as doc_count FROM AISQL_PLAYGROUND_ES.DEMO.PARSE_DOC_RAW_TEXT"
                check_result, _ = execute_query(check_query)
                
                if check_result and check_result[0]['DOC_COUNT'] == 0:
                    st.warning("⚠️ No se encontraron documentos en PARSE_DOC_RAW_TEXT. Por favor ejecuta el Ejemplo 1 primero para analizar documentos.")
                else:
                    # Truncar tabla de fragmentos
                    truncate_query = "TRUNCATE TABLE AISQL_PLAYGROUND_ES.DEMO.PARSE_DOC_CHUNKED_TEXT;"
                    execute_query(truncate_query)
                    
                    # Fragmentar documentos usando SPLIT_TEXT_RECURSIVE_CHARACTER (código original)
                    chunk_query = f"""
                    INSERT INTO AISQL_PLAYGROUND_ES.DEMO.PARSE_DOC_CHUNKED_TEXT (file_name, file_url, chunk_index, chunk_text, chunk_length)
                    SELECT 
                        file_name,
                        file_url,
                        c.INDEX as chunk_index,
                        c.VALUE as chunk_text,
                        LENGTH(c.VALUE) as chunk_length
                    FROM AISQL_PLAYGROUND_ES.DEMO.PARSE_DOC_RAW_TEXT,
                        LATERAL FLATTEN(input => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
                            raw_text,
                            'markdown',
                            {chunk_size},
                            {chunk_overlap}
                        )) as c
                    """
                    
                    result, error = execute_query(chunk_query)
                    
                    if not error:
                        # Contar fragmentos creados por documento
                        count_query = """
                        SELECT 
                            file_name,
                            COUNT(*) as chunk_count,
                            AVG(chunk_length) as avg_chunk_size
                        FROM AISQL_PLAYGROUND_ES.DEMO.PARSE_DOC_CHUNKED_TEXT
                        GROUP BY file_name
                        ORDER BY file_name
                        """
                        count_result, _ = execute_query(count_query)
                        
                        if count_result:
                            total_chunks = sum(row['CHUNK_COUNT'] for row in count_result)
                            st.success(f"**🎉 Fragmentación completada exitosamente! Se crearon {total_chunks} fragmentos.**")
                            
                            # Mostrar resumen por documento
                            st.markdown("**📊 Resumen de Fragmentación:**")
                            for row in count_result:
                                st.markdown(f"- **{row['FILE_NAME']}**: {row['CHUNK_COUNT']} fragmentos (tamaño promedio: {int(row['AVG_CHUNK_SIZE'])} caracteres)")
                            
                            # Mostrar todos los fragmentos en una tabla
                            sample_query = """
                            SELECT 
                                file_name,
                                chunk_index,
                                LEFT(chunk_text, 200) || '...' as chunk_preview,
                                chunk_length
                            FROM AISQL_PLAYGROUND_ES.DEMO.PARSE_DOC_CHUNKED_TEXT
                            ORDER BY file_name, chunk_index
                            """
                            sample_result, _ = execute_query(sample_query)
                            if sample_result:
                                st.markdown("**📋 Todos los Fragmentos de Documentos:**")
                                st.dataframe(
                                    sample_result,
                                    use_container_width=True,
                                    column_config={
                                        "FILE_NAME": st.column_config.TextColumn("Nombre Archivo", width="medium"),
                                        "CHUNK_INDEX": st.column_config.NumberColumn("Fragmento #", width="small"),
                                        "CHUNK_PREVIEW": st.column_config.TextColumn("Vista Previa", width="large"),
                                        "CHUNK_LENGTH": st.column_config.NumberColumn("Longitud (caracteres)", width="small")
                                    }
                                )
                            
                            with st.expander("🔍 Ver Consulta SQL"):
                                st.code(chunk_query, language="sql")
                    else:
                        st.error(f"Error fragmentando documentos: {error}")
    
    st.markdown("---")
    
    # Ejemplo 3: Crear Servicio de Búsqueda Cortex
    show_example_card(
        "Crear Servicio de Búsqueda Cortex",
        "Construye un índice de búsqueda semántica sobre fragmentos de documentos",
        3
    )
    
    st.info("🔍 **Paso 3:** Crea un Servicio de Búsqueda Cortex sobre documentos fragmentados (requiere que el Ejemplo 2 esté completado)")
    
    st.markdown("""
    Este servicio de búsqueda:
    - Genera automáticamente embeddings para todos los fragmentos de documentos
    - Habilita búsqueda semántica (basada en significado), no solo coincidencia de palabras clave
    - Se actualiza con un retraso de 9999 días (efectivamente actualización manual para esta demo)
    - Usa la tabla `PARSE_DOC_CHUNKED_TEXT` como fuente de datos
    """)
    
    if st.button("Crear Servicio de Búsqueda", key="create_search_service"):
        with st.spinner("Creando Servicio de Búsqueda Cortex..."):
            # Primero verificar si hay fragmentos para indexar
            check_query = "SELECT COUNT(*) as chunk_count FROM AISQL_PLAYGROUND_ES.DEMO.PARSE_DOC_CHUNKED_TEXT"
            check_result, _ = execute_query(check_query)
            
            if check_result and check_result[0]['CHUNK_COUNT'] == 0:
                st.warning("⚠️ No se encontraron fragmentos de documentos en la tabla PARSE_DOC_CHUNKED_TEXT. Por favor ejecuta los Ejemplos 1 y 2 primero!")
            else:
                # Eliminar servicio existente si existe
                drop_query = """
                DROP CORTEX SEARCH SERVICE IF EXISTS AISQL_PLAYGROUND_ES.DEMO.DOCUMENT_SEARCH_SERVICE
                """
                execute_query(drop_query)
                
                # Crear el servicio de búsqueda (código original)
                create_service_query = """
                CREATE CORTEX SEARCH SERVICE AISQL_PLAYGROUND_ES.DEMO.DOCUMENT_SEARCH_SERVICE
                ON chunk_text
                ATTRIBUTES file_name, chunk_index
                WAREHOUSE = CORTEX_SEARCH_WH
                TARGET_LAG = '9999 days'
                AS (
                    SELECT 
                        chunk_text,
                        file_name,
                        chunk_index
                    FROM AISQL_PLAYGROUND_ES.DEMO.PARSE_DOC_CHUNKED_TEXT
                )
                """
                result, error = execute_query(create_service_query)
                
                if not error:
                    st.success("**✅ Servicio de Búsqueda Cortex creado exitosamente!**")
                    st.markdown("""
                    **Detalles del Servicio:**
                    - **Nombre:** `AISQL_PLAYGROUND_ES.DEMO.DOCUMENT_SEARCH_SERVICE`
                    - **Columna de Búsqueda:** `chunk_text`
                    - **Atributos:** `file_name`, `chunk_index`
                    - **Warehouse:** `CORTEX_SEARCH_WH`
                    - **Target Lag:** 9999 días (actualización manual)
                    """)
                    
                    # Probar el servicio de búsqueda
                    st.markdown("**Prueba de Búsqueda:**")
                    test_query = """
                    SELECT 
                        file_name,
                        chunk_index,
                        LEFT(chunk_text, 200) as preview
                    FROM TABLE(
                        AISQL_PLAYGROUND_ES.DEMO.DOCUMENT_SEARCH_SERVICE!SEARCH(
                            'crecimiento de ingresos',
                            {'limit': 3}
                        )
                    )
                    """
                    test_result, test_error = execute_query(test_query)
                    if test_result:
                        st.success(f"Se encontraron {len(test_result)} fragmentos relevantes para 'crecimiento de ingresos'")
                        for idx, row in enumerate(test_result, 1):
                            st.caption(f"**{idx}. {row['FILE_NAME']}** (Fragmento {row['CHUNK_INDEX']})")
                            st.text(row['PREVIEW'] + "...")
                    
                    with st.expander("🔍 Ver SQL"):
                        st.code(create_service_query, language="sql")
                else:
                    st.error(f"Error creando servicio de búsqueda: {error}")
    
    st.markdown("---")
    
    # Sección de Next Steps
    st.markdown("""
    ### 🚀 Próximos Pasos: Construir un Agente Inteligente
    
    Ahora que tienes un Servicio de Búsqueda Cortex, puedes crear un Cortex Agent que lo referencie para construir experiencias de IA conversacional poderosas!
    
    **Qué hacer a continuación:**
    
    1. Navega a Snowsight → AI & ML → Cortex Agents
    2. Crea un nuevo Cortex Agent y agrega tu DOCUMENT_SEARCH_SERVICE como herramienta
    3. Prueba tu agente haciendo preguntas en lenguaje natural en Snowflake Intelligence
    
    Tu agente podrá:
    
    ✅ Responder preguntas "¿Qué?" (ej., "¿Cuáles fueron las cifras de ingresos del Q2?")
    ✅ Responder preguntas "¿Por qué?" (ej., "¿Por qué crecieron los ingresos en Q2?")
    ✅ Buscar automáticamente en todos tus documentos analizados
    ✅ Proporcionar citas y referencias de fuentes
    
    📖 **Aprende Más:** Lee sobre cómo Snowflake Intelligence sobresale respondiendo preguntas "¿Por qué?" en esta publicación de blog:
    
    [Snowflake Intelligence: Tell Me Why!](https://www.snowflake.com/blog/snowflake-intelligence-tell-me-why/)
    """)

# =============================================================================
# PÁGINA: AI_SUMMARIZE_AGG
# =============================================================================

def page_ai_summarize_agg():
    show_header()
    
    st.title("📝 AI_SUMMARIZE_AGG")
    
    multimodal_badge = show_multimodal_support(text=True, images=False, documents=False, audio=False)
    st.markdown(f"""
    **AI_SUMMARIZE_AGG** es una función agregada que resume múltiples filas de texto.
    A diferencia de los LLMs regulares, no está limitada por ventanas de contexto - ¡perfecta para grandes conjuntos de datos!
    
    {multimodal_badge} &nbsp;&nbsp; 📖 [Ver Documentación](https://docs.snowflake.com/en/sql-reference/functions/ai_summarize_agg)
    
    **💰 Costo:** 1.60 créditos por 1M tokens (Tabla 6a)  
    [Ver detalles de precios](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf)
    """, unsafe_allow_html=True)
    
    # Ejemplo 1: Resumir Todas las Reseñas para un Food Truck
    show_example_card(
        "Resumir Todas las Reseñas para un Food Truck",
        "Obtén un resumen completo de todos los comentarios de clientes",
        1
    )
    
    result, _ = execute_query("SELECT DISTINCT food_truck_name FROM AISQL_PLAYGROUND_ES.DEMO.CUSTOMER_REVIEWS ORDER BY food_truck_name")
    if result:
        truck_options = [r['FOOD_TRUCK_NAME'] for r in result]
        selected_truck = st.selectbox("Selecciona food truck:", truck_options)
        
        if st.button("Resumir Reseñas", key="summarize_truck"):
            with st.spinner("Resumiendo todas las reseñas..."):
                query = f"""
                SELECT 
                    '{selected_truck}' as food_truck,
                    COUNT(*) as total_reviews,
                    AVG(rating) as avg_rating,
                    AI_SUMMARIZE_AGG(review_text) as review_summary
                FROM AISQL_PLAYGROUND_ES.DEMO.CUSTOMER_REVIEWS
                WHERE food_truck_name = '{selected_truck}'
                GROUP BY food_truck_name
                """
                result, error = execute_query(query)
                if result:
                    st.markdown(f"### {result[0]['FOOD_TRUCK']}")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Total de Reseñas", result[0]['TOTAL_REVIEWS'])
                    with col2:
                        st.metric("Calificación Promedio", f"{'⭐' * int(result[0]['AVG_RATING'])} ({result[0]['AVG_RATING']:.1f}/5)")
                    
                    st.markdown("**Resumen de Todas las Reseñas:**")
                    st.markdown(f"_{result[0]['REVIEW_SUMMARY']}_")
                    st.code(query, language="sql")
    
    st.markdown("---")
    
    # Ejemplo 2: Resumir Tickets de Soporte por Estado
    show_example_card(
        "Resumir Tickets de Soporte Abiertos",
        "Obtén insights sobre problemas comunes de tickets de soporte",
        2
    )
    
    ticket_status = st.selectbox("Selecciona estado de ticket:", ["Abierto", "En Progreso", "Cerrado"])
    
    if st.button("Resumir Tickets", key="summarize_tickets"):
        with st.spinner("Resumiendo tickets..."):
            query = f"""
            SELECT 
                status,
                COUNT(*) as ticket_count,
                AI_SUMMARIZE_AGG(issue_description) as issues_summary
            FROM AISQL_PLAYGROUND_ES.DEMO.SUPPORT_TICKETS
            WHERE status = '{ticket_status}'
            GROUP BY status
            """
            result, error = execute_query(query)
            if result and len(result) > 0:
                st.markdown(f"### Tickets {result[0]['STATUS']}")
                st.metric("Total de Tickets", result[0]['TICKET_COUNT'])
                st.markdown("**Resumen de Problemas:**")
                st.markdown(f"_{result[0]['ISSUES_SUMMARY']}_")
                st.code(query, language="sql")
            else:
                st.warning(f"No se encontraron tickets {ticket_status}")
    
    st.markdown("---")
    
    # Ejemplo 3: Tendencias Mensuales de Reseñas
    show_example_card(
        "Tendencias Mensuales de Reseñas",
        "Resume reseñas por mes para identificar tendencias",
        3
    )
    
    if st.button("Generar Resúmenes Mensuales", key="monthly_summaries"):
        with st.spinner("Generando resúmenes mensuales..."):
            query = """
            SELECT 
                DATE_TRUNC('MONTH', review_date) as month,
                COUNT(*) as review_count,
                AVG(rating) as avg_rating,
                AI_SUMMARIZE_AGG(review_text) as monthly_summary
            FROM AISQL_PLAYGROUND_ES.DEMO.CUSTOMER_REVIEWS
            GROUP BY DATE_TRUNC('MONTH', review_date)
            ORDER BY month DESC
            LIMIT 3
            """
            result, error = execute_query(query)
            if result:
                for row in result:
                    # Formatear mes (row['MONTH'] es un objeto date de Snowflake)
                    month_str = str(row['MONTH'])[:7]  # Obtener YYYY-MM
                    month_name = {
                        '01': 'Enero', '02': 'Febrero', '03': 'Marzo', '04': 'Abril',
                        '05': 'Mayo', '06': 'Junio', '07': 'Julio', '08': 'Agosto',
                        '09': 'Septiembre', '10': 'Octubre', '11': 'Noviembre', '12': 'Diciembre'
                    }
                    month_display = f"{month_name.get(month_str[5:7], month_str[5:7])} {month_str[:4]}"
                    with st.expander(f"{month_display} - {row['REVIEW_COUNT']} reseñas (Promedio: {'⭐' * int(row['AVG_RATING'])})"):
                        st.markdown(row['MONTHLY_SUMMARY'])
                st.code(query, language="sql")

# =============================================================================
# PÁGINA: AI_AGG
# =============================================================================

def page_ai_agg():
    show_header()
    
    st.title("📊 AI_AGG")
    
    multimodal_badge = show_multimodal_support(text=True, images=False, documents=False, audio=False)
    st.markdown(f"""
    **AI_AGG** es como AI_SUMMARIZE_AGG pero con instrucciones personalizadas.
    Agrega y analiza datos de texto con tus propios prompts específicos - ¡no limitado por ventanas de contexto!
    
    {multimodal_badge} &nbsp;&nbsp; 📖 [Ver Documentación](https://docs.snowflake.com/en/sql-reference/functions/ai_agg)
    
    **💰 Costo:** 1.60 créditos por 1M tokens (Tabla 6a)  
    [Ver detalles de precios](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf)
    """, unsafe_allow_html=True)
    
    # Ejemplo 1: Extraer Quejas Comunes
    show_example_card(
        "Identificar Quejas Comunes en Reseñas",
        "Usa un prompt personalizado para encontrar patrones específicos",
        1
    )
    
    if st.button("Encontrar Quejas Comunes", key="find_complaints"):
        with st.spinner("Analizando reseñas..."):
            query = """
            SELECT 
                AI_AGG(
                    review_text,
                    'Identifica las 5 quejas o problemas más comunes mencionados en estas reseñas. 
                    Para cada problema, proporciona una breve descripción y estima cuántas reseñas lo mencionan.'
                ) as common_complaints
            FROM AISQL_PLAYGROUND_ES.DEMO.CUSTOMER_REVIEWS
            WHERE rating <= 3
            """
            result, error = execute_query(query)
            if result:
                st.markdown("**Análisis de Quejas Comunes:**")
                st.markdown(result[0]['COMMON_COMPLAINTS'])
                st.code(query, language="sql")
    
    st.markdown("---")
    
    # Ejemplo 2: Extraer Elementos Populares del Menú
    show_example_card(
        "Identificar Elementos del Menú Más Elogiados",
        "Encuentra qué elementos del menú aman más los clientes",
        2
    )
    
    if st.button("Encontrar Elementos Populares", key="popular_items"):
        with st.spinner("Analizando reseñas positivas..."):
            query = """
            SELECT 
                food_truck_name,
                AI_AGG(
                    review_text,
                    'Lista los elementos específicos del menú que los clientes mencionaron positivamente. 
                    Para cada elemento, explica qué les gustó a los clientes.'
                ) as popular_items
            FROM AISQL_PLAYGROUND_ES.DEMO.CUSTOMER_REVIEWS
            WHERE rating >= 4
            GROUP BY food_truck_name
            LIMIT 5
            """
            result, error = execute_query(query)
            if result:
                for row in result:
                    with st.expander(f"🍽️ {row['FOOD_TRUCK_NAME']}"):
                        st.markdown(row['POPULAR_ITEMS'])
                st.code(query, language="sql")
    
    st.markdown("---")
    
    # Ejemplo 3: Prompt de Análisis Personalizado
    show_example_card(
        "Análisis Personalizado con Tu Propio Prompt",
        "Prueba tu propio análisis de agregación personalizado",
        3
    )
    
    custom_instruction = st.text_area(
        "Ingresa tu instrucción de análisis personalizada:",
        "Analiza la calidad del servicio al cliente basándote en estos tickets de soporte. "
        "Identifica áreas donde sobresalimos y áreas que necesitan mejora. "
        "Proporciona recomendaciones específicas."
    )
    
    data_source = st.radio("Analizar:", ["Reseñas de Clientes", "Tickets de Soporte"])
    
    if st.button("Ejecutar Análisis Personalizado", key="custom_agg"):
        with st.spinner("Ejecutando análisis personalizado..."):
            escaped_instruction = escape_sql_string(custom_instruction)
            if data_source == "Reseñas de Clientes":
                query = f"""
                SELECT AI_AGG(
                    review_text,
                    '{escaped_instruction}'
                ) as analysis_result
                FROM AISQL_PLAYGROUND_ES.DEMO.CUSTOMER_REVIEWS
                """
            else:
                query = f"""
                SELECT AI_AGG(
                    issue_description,
                    '{escaped_instruction}'
                ) as analysis_result
                FROM AISQL_PLAYGROUND_ES.DEMO.SUPPORT_TICKETS
                """
            
            result, error = execute_query(query)
            if result:
                st.markdown("**Resultado del Análisis:**")
                st.markdown(result[0]['ANALYSIS_RESULT'])
                st.code(query, language="sql")
            elif error:
                st.error(f"Error: {error}")
    
    st.markdown("---")
    
    st.markdown("""
    ### 💡 AI_AGG vs AI_SUMMARIZE_AGG
    
    | Característica | AI_SUMMARIZE_AGG | AI_AGG |
    |---------|------------------|--------|
    | Propósito | Resumen general | Análisis personalizado |
    | Prompt | Fijo (resumir) | Instrucción personalizada |
    | Caso de Uso | Resúmenes rápidos | Insights específicos |
    | Flexibilidad | Baja | Alta |
    
    **Usa AI_AGG cuando necesites:**
    - Insights específicos (no solo resúmenes)
    - Análisis personalizado con tus propias instrucciones
    - Extracción de patrones particulares
    - Análisis comparativo
    - Recomendaciones accionables
    """)

# =============================================================================
# ENRUTAMIENTO PRINCIPAL DE LA APLICACIÓN
# =============================================================================

def main():
    # Enrutar a la página apropiada
    if current_page == "home":
        page_home()
    elif current_page == "ai_complete":
        page_ai_complete()
    elif current_page == "ai_translate":
        page_ai_translate()
    elif current_page == "ai_sentiment":
        page_ai_sentiment()
    elif current_page == "ai_extract":
        page_ai_extract()
    elif current_page == "ai_classify":
        page_ai_classify()
    elif current_page == "ai_filter":
        page_ai_filter()
    elif current_page == "ai_similarity":
        page_ai_similarity()
    elif current_page == "ai_redact":
        page_ai_redact()
    elif current_page == "ai_transcribe":
        page_ai_transcribe()
    elif current_page == "ai_parse_document":
        page_ai_parse_document()
    elif current_page == "ai_summarize_agg":
        page_ai_summarize_agg()
    elif current_page == "ai_agg":
        page_ai_agg()
    
    # Pie de página
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; padding: 30px; background: linear-gradient(135deg, #F8FAFC 0%, #E8EEF2 100%); border-radius: 12px; margin-top: 40px;'>
        <p style='font-size: 18px; font-weight: 600; color: #1E293B; margin-bottom: 8px;'>
            ❄️ Snowflake Cortex AISQL Playground ❄️
        </p>
        <p style='color: #64748b; font-size: 14px; margin-bottom: 0;'>
            Construido con Snowflake Cortex AI | Impulsado por Streamlit en Snowflake
        </p>
        <p style='color: #64748b; font-size: 12px; margin-top: 8px;'>
            🍔 Datos Demo de Tasty Bytes | 12 Funciones AISQL Mostradas
        </p>
    </div>
    """, unsafe_allow_html=True)

# Ejecutar la aplicación
if __name__ == "__main__":
    main()

