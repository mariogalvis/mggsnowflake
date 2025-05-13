
from snowflake.snowpark.context import get_active_session
import streamlit as st
import ast
session = get_active_session()

prompt = """You are a customer support representative at a telecommunications company. 
Suddenly there is a spike in customer support tickets. 
You need to understand and analyze the support requests from customers.
Based on the root cause of the main issue in the support request, craft a response to resolve the customer issue.
Write a text message under 25 words, if the contact_preference field is text message.
Write an email in maximum of 100 words if the contact_preference field is email. 
Focus on alleviating the customer issue and improving customer satisfaction in your response.
Strictly follow the word count limit for the response. 
Write only email or text message response based on the contact_preference for every customer. 
Do not generate both email and text message response.
"""
#Always answer in Spanish.


prompt1 = """
Please write an email or text promoting a new plan that will save customers total costs. 
Also resolve the customer issue based on the ticket category. 
If the contact_preference is text message, write text message response in less than 25 words. 
If the contact_preference is email, write email response in maximum 100 words.
Write only email or text message response based on the contact_preference for every customer.
"""

ticket_categories = ['Roaming fees', 'Slow data speed', 'Lost phone', 'Add new line', 'Closing account']
st.image('https://mgg.com.co/wp-content/uploads/images/logo.png');

st.subheader("Generación automática de correos/SMS personalizados (Destilación)")

with st.container():
    with st.expander("Ingrese la solicitud del cliente y seleccione LLM", expanded=True):
        #customer_request = st.text_area('Solicitud',"""Viajé a Lima durante dos semanas y mantuve mi uso de datos al mínimo. Sin embargo, se me cobraron $90 en tarifas internacionales. Estos cargos no me fueron comunicados, y solicito un desglose detallado y un reembolso. Gracias.""")
        #Desde hace varios días, la velocidad de mi conexión a Internet ha sido extremadamente lenta, incluso cuando solo tengo un dispositivo conectado. No puedo ver videos en streaming ni hacer videollamadas sin interrupciones. He reiniciado el router varias veces, pero el problema persiste. Solicito una solución urgente o una revisión de mi servicio contratado.
        #ticket_categories = ['Roaming', 'Lentitud de Datos', 'Celular Perdido', 'Nueva Línea', 'Cerrar Cuenta']

        customer_request = st.text_area('Solicitud', """Realicé un pago con mi tarjeta de crédito, pero la transacción fue rechazada sin razón aparente. Me gustaría entender el motivo y si hay algún bloqueo en mi cuenta. Agradecería una pronta solución.""")
        #Revisando los movimientos de mi cuenta, noté un cargo de 450.000 pesos que no reconozco y que nunca autoricé. Intenté comunicarme con el banco, pero no he recibido una respuesta clara sobre el proceso de disputa. Solicito una investigación urgente y el reembolso del monto debitado sin mi autorización.
        ticket_categories = ['Problema con Tarjeta', 'Fraude o Transacción No Reconocida', 'Solicitud de Crédito', 'Acceso a Banca Digital', 'Cierre de Cuenta']
        
        #customer_request = st.text_area('Solicitud', """Tuve una emergencia médica y acudí a un hospital dentro de la red de cobertura. Sin embargo, al momento del pago, me informaron que mi póliza no cubría ciertos procedimientos, a pesar de que en el contrato se indicaba lo contrario. Solicito una aclaración y el reembolso de los costos adicionales que tuve que asumir.""")
        #La semana pasada tuve una emergencia médica y llamé a la línea de asistencia de mi seguro para solicitar una ambulancia. Me dijeron que llegaría en 20 minutos, pero tardó más de una hora en llegar. Como consecuencia, tuve que buscar otro medio de transporte para llegar al hospital. Solicito una explicación sobre lo ocurrido y qué medidas tomarán para evitar estas demoras en el futuro.
        #ticket_categories = ['Reclamación de Seguro', 'Estado de Póliza', 'Cobertura de Beneficios', 'Cambio de Plan', 'Atención Médica y Asistencia']
         
        with st.container():
            left_col, right_col = st.columns(2)
            with left_col:
                selected_preference = st.selectbox('Seleccione preferencia de contacto', ('Text message', 'Email'), index=1)
            with right_col:
                selected_llm = st.selectbox('Seleccione LLM',('llama3-8b', 'mistral-7b', 'mistral-large', 'SUPPORT_MESSAGES_FINETUNED_MISTRAL_7B',), index=1)

with st.container():
    _,mid_col,_ = st.columns([.4,.3,.3])
    with mid_col:
        generate_template = st.button('Generar Mensaje ⚡',type="primary")

with st.container():
    if generate_template:
        category_sql = f"""
        select snowflake.cortex.classify_text('{customer_request}', {ticket_categories}) as ticket_category
        """
        df_category = session.sql(category_sql).to_pandas().iloc[0]['TICKET_CATEGORY']
        df_category_dict = ast.literal_eval(df_category)
        st.subheader("Categoría de Solicitud")
        st.write(df_category_dict['label'])

        message_sql = f"""
        select snowflake.cortex.translate(snowflake.cortex.complete('{selected_llm}',concat('{prompt}', '{customer_request}', '{selected_preference}')),'en','es') as custom_message
        """
        df_message = session.sql(message_sql).to_pandas().iloc[0]['CUSTOM_MESSAGE']
        st.subheader(selected_preference)
        st.write(df_message)