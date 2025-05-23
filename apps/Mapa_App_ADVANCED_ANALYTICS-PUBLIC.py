from snowflake.snowpark.context import get_active_session
from typing import Tuple
import branca.colormap as cm
import pandas as pd
import pydeck as pdk
import streamlit as st

@st.cache_data
def get_dataframe_from_raw_sql(query: str) -> pd.DataFrame:
    session = get_active_session()
    pandas_df = session.sql(query).to_pandas()
    return pandas_df

def get_h3_df_orders_quantiles(resolution: float, type_of_location: str) -> pd.DataFrame:
    df = get_dataframe_from_raw_sql(
        f"""SELECT
        H3_POINT_TO_CELL_STRING(to_geography({ type_of_location }), { resolution }) AS h3,
        round(count(*),2) as count
        FROM ADVANCED_ANALYTICS.PUBLIC.ORDERS_REVIEWS_SENTIMENT_ANALYSIS
        GROUP BY 1""")

    quantiles = get_quantile_in_column(df, "COUNT")
    return df, quantiles

def get_h3_df_sentiment_quantiles(
    resolution: float, type_of_sentiment: str, type_of_location: str
) -> Tuple[pd.DataFrame, pd.core.series.Series]:
    df = get_dataframe_from_raw_sql(
        f""" SELECT 
        H3_POINT_TO_CELL_STRING(TO_GEOGRAPHY({ type_of_location }),{ resolution }) AS h3,
        round(AVG({ type_of_sentiment }),2) AS count
        FROM ADVANCED_ANALYTICS.PUBLIC.ORDERS_REVIEWS_SENTIMENT_ANALYSIS
        WHERE { type_of_sentiment } IS NOT NULL 
        GROUP BY 1""")

    quantiles = get_quantile_in_column(df, "COUNT")
    df = df[(df["COUNT"] >= values[0]) & (df["COUNT"] <= values[1])]
    return df, quantiles

def get_h3_layer(layer_dataframe: pd.DataFrame, elevation_3d: bool = False,) -> pdk.Layer:
    highest_count_df = 0 if layer_dataframe is None else layer_dataframe["COUNT"].max()
    return pdk.Layer(
        "H3HexagonLayer",
        layer_dataframe,
        get_hexagon="H3",
        get_fill_color="COLOR",
        get_line_color="COLOR",
        auto_highlight=True,
        get_elevation=f"COUNT/{highest_count_df}",
        elevation_scale=10000 if elevation_3d else 0,
        elevation_range=[0, 300],
        pickable=True,
        opacity=0.5,
        extruded=True)

def get_quantile_in_column(
    quantile_dataframe: pd.DataFrame, column_name: str
) -> pd.core.series.Series:
    return quantile_dataframe[column_name].quantile([0, 0.25, 0.5, 0.75, 1])

def render_pydeck_chart(
    chart_quantiles: pd.core.series.Series, 
    chart_dataframe: pd.DataFrame, 
    elevation_3d: bool = False):
    colors = ["gray", "blue", "green", "yellow", "orange", "red"]
    color_map = cm.LinearColormap(
        colors,
        vmin=chart_quantiles.min(),
        vmax=chart_quantiles.max(),
        index=chart_quantiles)
    chart_dataframe["COLOR"] = chart_dataframe["COUNT"].apply(color_map.rgb_bytes_tuple)
    st.pydeck_chart(
        pdk.Deck(
            map_provider="mapbox",
            map_style="light",
            initial_view_state=pdk.ViewState(
                latitude=37.633,
                longitude=-122.284,
                zoom=7,
                pitch=50 if elevation_3d else 0,
                height=430),
            tooltip={"html": "<b>Value:</b> {COUNT}",
            "style": {"color": "white"}},
            layers=get_h3_layer(chart_dataframe, elevation_3d)))

st.set_page_config(layout="centered", initial_sidebar_state="expanded")
st.image('https://mgg.com.co/wp-content/uploads/images/logo.png');
st.title("Opiniones sobre la Experiencia del Servicio")

with st.sidebar:
    h3_resolution = st.slider("Granularidad", min_value=6, max_value=9, value=7)
    type_of_locations = st.selectbox("Dimensiones", ("DELIVERY_LOCATION", "RESTAURANT_LOCATION"), index=0)
    type_of_data = st.selectbox(
        "Indicador",("ORDERS","SENTIMENT_SCORE","COST_SCORE","FOOD_QUALITY_SCORE","DELIVERY_TIME_SCORE"), index=0)
    if type_of_data != "ORDERS":
        values = st.slider("Select a range for score values", 0.0, 5.0, (0.0, 5.0))
        chckbox_3d_value = False
    else:
        chckbox_3d_value = st.checkbox("3D", key="chkbx_forecast", help="Renders H3 Hexagons in 3D")

if type_of_data != "ORDERS":
    df, quantiles = get_h3_df_sentiment_quantiles(h3_resolution, type_of_data, type_of_locations)

if type_of_data == "ORDERS":
    df, quantiles = get_h3_df_orders_quantiles(h3_resolution, type_of_locations)

st.image("https://sfquickstarts.s3.us-west-1.amazonaws.com/hol_geo_spatial_ml_using_snowflake_cortex/gradient.png")

render_pydeck_chart(quantiles, df, chckbox_3d_value)