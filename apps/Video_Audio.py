import streamlit as st

st.image('https://mgg.com.co/wp-content/uploads/images/logo.png');

st.title("🎯 Algunos casos de uso con análisis de imágenes, video y audio ...")

st.markdown("""
1. 🏦 Verificación de identidad por rostro y voz para onboarding digital  
2. 🛡️ Detección de fraude en declaraciones o reclamaciones de seguros  
3. 📞 Análisis emocional en llamadas y videollamadas de atención al cliente  
4. 📋 Validación visual y verbal de formularios y contratos en procesos remotos  
5. 🎧 Supervisión de cumplimiento normativo en centros de contacto  
6. 📹 Monitoreo de sucursales o puntos físicos con reconocimiento facial y voz  
7. 🧠 Entrenamiento automatizado para asesores y agentes con análisis de desempeño  
8. 🕵️‍♂️ Detección de comportamiento sospechoso en cajeros o oficinas  
9. 🌐 Mejora de experiencia en kioscos y apps con análisis multimodal  
10. 📊 Análisis de focus groups en video para testing de productos o campañas
""")

enable = st.checkbox("Enable camera")
picture = st.camera_input("Take a picture", disabled=not enable)

if picture:
    st.image(picture)


audio_value = st.experimental_audio_input("Record a voice message")

if audio_value:
    st.audio(audio_value)

