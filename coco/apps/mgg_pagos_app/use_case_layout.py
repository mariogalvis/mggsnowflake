import streamlit as st


def render_use_case(title, description, resuelve, como_funciona, valor_negocio):
    st.header(title)
    st.caption(description)

    col1, col2, col3 = st.columns(3)

    with col1:
        with st.container(border=True, height="stretch"):
            st.markdown("**:material/lightbulb: Que resuelve**")
            st.markdown(resuelve)

    with col2:
        with st.container(border=True, height="stretch"):
            st.markdown("**:material/settings: Como funciona**")
            st.markdown(como_funciona)

    with col3:
        with st.container(border=True, height="stretch"):
            st.markdown("**:material/trending_up: Valor de negocio**")
            st.markdown(valor_negocio)
