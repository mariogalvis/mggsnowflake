# BEFORE YOU BEGIN 
# Add the following packages to the Packages dropdown at the top of the UI:
# plotly, matplotlib, pydeck, snowflake-ml-python, nbformat

# Import Python packages
import streamlit as st
import pydeck as pdk
import pandas as pd
import altair as alt
import matplotlib.pyplot as plt
import streamlit as st
from snowflake.ml.feature_store import (
FeatureStore,
FeatureView,
CreationMode)
from datetime import datetime, timedelta

from snowflake.snowpark.context import get_active_session

# Write directly to the app
st.image('https://mgg.com.co/wp-content/uploads/images/logo.png');

st.title("Detección de Fraude")

st.write(
    """Transacciones actuales
    """
)

# Get the current credentials
session = get_active_session()

session.sql("USE ROLE SYSADMIN")

#  Create an example dataframe
#  Note: this is just some dummy data, but you can easily connect to your Snowflake data
#  It is also possible to query data using raw SQL using session.sql() e.g. session.sql("select * from table")
queried_data = session.sql(
   "select * from CC_FINS_DB.ANALYTICS.CC_APP_TBL"
).to_pandas()
queried_data['TRANSACTION_DATE'] = pd.to_datetime(queried_data['TRANSACTION_DATE'], format='%m/%d/%y %H:%M')

# Get yesterday's date
yesterday = datetime.now() - timedelta(days=1)
yesterday_date = yesterday.date()

# Update the TRANSACTION_DATE column to have yesterday's date but keep the original time
queried_data['TRANSACTION_DATE'] = queried_data['TRANSACTION_DATE'].apply(lambda dt: dt.replace(year=yesterday_date.year, month=yesterday_date.month, day=yesterday_date.day))

# If you need the TRANSACTION_DATE column back in the original string format
queried_data['TRANSACTION_DATE'] = queried_data['TRANSACTION_DATE'].dt.strftime('%m/%d/%y %H:%M')

st.write(queried_data, use_container_width=True)


def get_custfeatures():
    stmt = f'''WITH 
    weekly_spending AS (
        SELECT
            USER_ID,
            DATE_TRUNC('week',TO_TIMESTAMP(TRANSACTION_DATE,'MM/DD/YY HH24:MI')) AS week,
            SUM(TRANSACTION_AMOUNT) AS total_spent_wk
        FROM
            CC_FINS_DB.ANALYTICS.CC_APP_TBL
        GROUP BY
            USER_ID,
            DATE_TRUNC('week',TO_TIMESTAMP(TRANSACTION_DATE,'MM/DD/YY HH24:MI'))
    ),
    mean_weekly_spending AS (
        SELECT
            USER_ID,
            AVG(total_spent_wk) AS mean_weekly_spent
        FROM
            weekly_spending
        GROUP BY
            USER_ID
    ),
    monthly_spending AS (
        SELECT
            USER_ID,
            DATE_TRUNC('month',TO_TIMESTAMP(TRANSACTION_DATE,'MM/DD/YY HH24:MI')) AS month,
            SUM(TRANSACTION_AMOUNT) AS total_spent
        FROM
            CC_FINS_DB.ANALYTICS.CC_APP_TBL
        GROUP BY
            USER_ID,
            DATE_TRUNC('month',TO_TIMESTAMP(TRANSACTION_DATE,'MM/DD/YY HH24:MI'))
    ),
    mean_monthly_spending AS (
        SELECT
            USER_ID,
            AVG(total_spent) AS mean_monthly_spent
        FROM
            monthly_spending
        GROUP BY
            USER_ID
    ),
    yearly_spending AS (
        SELECT
            USER_ID,
            DATE_TRUNC('year',TO_TIMESTAMP(TRANSACTION_DATE,'MM/DD/YY HH24:MI')) AS month,
            SUM(TRANSACTION_AMOUNT) AS total_spent_yr
        FROM
            CC_FINS_DB.ANALYTICS.CC_APP_TBL
        GROUP BY
            USER_ID,
            DATE_TRUNC('year',TO_TIMESTAMP(TRANSACTION_DATE,'MM/DD/YY HH24:MI'))
    ),
    mean_yearly_spending AS (
        SELECT
            USER_ID,
            AVG(total_spent_yr) AS mean_yearly_spent
        FROM
            yearly_spending
        GROUP BY
            USER_ID
    ),
    features AS (
        SELECT
            USER_ID,
            COUNT(TRANSACTION_ID) AS total_transactions,
            AVG(TRANSACTION_AMOUNT) AS avg_per_transaction_amount,
            STDDEV(TRANSACTION_AMOUNT) AS stddev_transaction_amount,
            COUNT(DISTINCT MERCHANT) AS num_unique_merchants,
            round(COUNT(TRANSACTION_ID) / COUNT(DISTINCT DATE_TRUNC('month',TO_TIMESTAMP(TRANSACTION_DATE,'MM/DD/YY HH24:MI')),0)) AS transactions_per_month
        FROM
            CC_FINS_DB.ANALYTICS.CC_APP_TBL
        GROUP BY
            USER_ID
    )
    SELECT
        DISTINCT cc.USER_ID,total_transactions,avg_per_transaction_amount,stddev_transaction_amount,num_unique_merchants,
        ROUND(mws.mean_weekly_spent,2) as mean_weekly_spent,
        ROUND(mms.mean_monthly_spent,2) as mean_monthly_spent,
        ROUND(mys.mean_yearly_spent,2) as mean_yearly_spent
    FROM
        CC_FINS_DB.ANALYTICS.CC_APP_TBL cc
        JOIN features fs ON cc.USER_ID = fs.User_ID
        JOIN mean_monthly_spending mms ON cc.USER_ID = mms.User_ID
        JOIN mean_yearly_spending mys ON cc.USER_ID = mys.User_ID
        JOIN mean_weekly_spending mws ON cc.USER_ID = mws.User_ID'''
    custdf=session.sql(stmt)
    return custdf

def get_transactions():
    stmt1=f'''WITH cumulative_behavior AS (
    SELECT
        USER_ID,
        SESSION_ID,
        TRANSACTION_DATE,
        SUM(CLICKS) OVER (PARTITION BY USER_ID ORDER BY TRANSACTION_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_clicks,
        SUM(LOGIN_PER_HOUR) OVER (PARTITION BY USER_ID ORDER BY TRANSACTION_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_logins_per_hour
    FROM CC_FINS_DB.ANALYTICS.CC_APP_TBL
),
unique_transactions AS (
    SELECT DISTINCT
        USER_ID,
        SESSION_ID,
        TRANSACTION_DATE,
        TRANSACTION_ID,
        TIME_ELAPSED,
        CLICKS,
        LOCATION,
        LATITUDE,
        LONGITUDE
    FROM CC_FINS_DB.ANALYTICS.CC_APP_TBL
)
SELECT
    ut.SESSION_ID,
    ut.TRANSACTION_DATE,
    ut.TIME_ELAPSED,
    ut.CLICKS,
    ut.TRANSACTION_ID,
    ut.LOCATION,
    ut.LATITUDE,
    ut.LONGITUDE,
    cb.cumulative_clicks,
    cb.cumulative_logins_per_hour
FROM
    unique_transactions ut
    JOIN cumulative_behavior cb ON ut.USER_ID = cb.USER_ID 
    AND ut.SESSION_ID = cb.SESSION_ID
    AND ut.TRANSACTION_DATE = cb.TRANSACTION_DATE
ORDER BY
    ut.TRANSACTION_ID'''
    transdf=session.sql(stmt1)
    return transdf

def get_spinedf():
    spinedf = session.sql("SELECT distinct User_ID, Transaction_ID FROM CC_FINS_DB.ANALYTICS.CC_APP_TBL")
    spinedf=spinedf.to_pandas()
    
    transdf= get_transactions()
    transdf=transdf.to_pandas()
    
    custdf=get_custfeatures()
    custdf=custdf.to_pandas()
    df_merged = pd.merge(spinedf, custdf, on='USER_ID', how='inner')

    final_df = pd.merge(df_merged, transdf, on='TRANSACTION_ID', how='inner')
    final_sdf = session.create_dataframe(final_df)
    
    return final_sdf

def get_predictions():
    
    session.sql("USE ROLE SYSADMIN")
    session.sql("USE DATABASE CC_FINS_DB")
    session.sql("USE SCHEMA ANALYTICS")
    final_df = get_spinedf()
    
    sd = final_df.write.mode("overwrite").save_as_table("CC_FINS_DB.ANALYTICS.inference_fd_table")
    #view = session.sql("CREATE OR REPLACE VIEW fraud_classification_input_view AS SELECT * FROM CC_FINS_DB.ANALYTICS.inference_fd_table ").collect()
    # Get predictions    
    result = session.sql("CREATE OR REPLACE TABLE CC_FINS_DB.ANALYTICS.predictions_creditcardfd AS SELECT *,CC_FINS_DB.ANALYTICS.fraud_classification_model!PREDICT(INPUT_DATA => object_construct(*)) as predictions from CC_FINS_DB.ANALYTICS.inference_fd_table").collect()
    prediction_df = session.sql("SELECT * EXCLUDE PREDICTIONS,predictions:class::STRING AS CLASS,round(predictions['probability'][class], 3) as probability FROM CC_FINS_DB.ANALYTICS.predictions_creditcardfd")
    return prediction_df

    

if st.button('Predecir transacciones fraudulentas'):
    with st.spinner("Obteniendo predicciones..."):
        view_name= "fraud_classification_val_view"
        predictions_df = get_predictions()
        
        predictions=predictions_df.to_pandas()
        
        predictions["CLASS"] = pd.to_numeric(predictions["CLASS"], errors='coerce')
        
        # Update CLASS for Moscow location to 1 (fraud) if models does not perform well
        #predictions.loc[predictions['LOCATION'] == 'Moscow', 'CLASS'] = 1
        
        # Define colors based on CLASS
        predictions['COLOR'] = predictions["CLASS"].apply(lambda x: [255, 0, 0, 255] if x == 1 else [0, 128, 0, 255])
        predictions['RESULT'] = predictions["CLASS"].apply(lambda x: "FRAUDLENT" if x == 1 else "NORMAL")
        
        st.title('Mapa - Localización de Transacciones')

        # Map Layer
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=predictions,
            get_position=["LONGITUDE", "LATITUDE"],
            opacity=0.8,
            filled=True,
            elevation_range=[0, 1000],
            extruded=True,
            coverage=1,
            get_fill_color="COLOR",
            get_radius=8000,
            pickable=True,
            radius_min_pixels=5,
            radius_scale=5,
            stroked=True,
            line_width_min_pixels=1,
            line_color=[50, 0, 0, 50],  # Light red border
        )
        # Tooltips
        tooltip = {
            "html": "<b>Transaction:</b> {TRANSACTION_ID}<br><b>Location:</b> {LATITUDE}, {LONGITUDE}<br><b>Fraud Status:</b> {RESULT}<br><b>Total Transactions:</b> {TOTAL_TRANSACTIONS}",
            "style": {"color": "white", "backgroundColor": "rgba(0, 0, 0, 0.7)", "padding": "5px"}
        }
        
        # View State
        view_state = pdk.ViewState(
            latitude=predictions['LATITUDE'].mean(),
            longitude=predictions['LONGITUDE'].mean(),
            zoom=4,
            pitch=0
        )
        
        # Deck
        deck = pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            map_style="mapbox://styles/mapbox/light-v9",
            tooltip=tooltip
        )
        
        # Display the map
        st.pydeck_chart(deck)
        
        result = predictions_df.to_pandas()
        result['TRANSACTION_DATE'] = pd.to_datetime(result['TRANSACTION_DATE'], format='%m/%d/%y %H:%M')

        # Get yesterday's date
        yesterday = datetime.now() - timedelta(days=1)
        yesterday_date = yesterday.date()
        
        # Update the TRANSACTION_DATE column to have yesterday's date
        result['TRANSACTION_DATE'] = result['TRANSACTION_DATE'].apply(lambda dt: dt.replace(year=yesterday_date.year, month=yesterday_date.month, day=yesterday_date.day))
        
        # TRANSACTION_DATE column back in the original string format
        result['TRANSACTION_DATE'] = result['TRANSACTION_DATE'].dt.strftime('%m/%d/%y %H:%M')
        st.write(result)
        
        raw_data = session.sql("select * from CC_FINS_DB.ANALYTICS.CC_APP_TBL")
        raw_datapd=raw_data.to_pandas()

        
        
        df_columns = ['USER_ID', 'TRANSACTION_ID', 'MERCHANT', 'TRANSACTION_AMOUNT', 'TIME_ELAPSED', 'CLICKS']
        df_columns1 = ['TRANSACTION_ID', 'CLASS']
        
        # Select the necessary columns from the DataFrames
        predictions_selected = predictions[df_columns1]
        raw_datapd_selected = raw_datapd[df_columns]
        
        # Perform the merge operation
        predictionsmerged = pd.merge(predictions_selected, raw_datapd_selected, on='TRANSACTION_ID', how='inner')

        
        # Define colors for visualizations
        def get_color_palette():
            return {
                'FRAUDULENT': '#FF0000',  # Red
                'NORMAL': '#008000'       # Green
            }
        
        # Add result classification for tooltip
        predictions['RESULT'] = predictions['CLASS'].apply(lambda x: "FRAUDULENT" if x == 1 else "NORMAL")

        
        # Chart for Fraudulent and Normal Transactions by Merchant Category
        chart_fraud_by_merchant = alt.Chart(predictionsmerged).mark_bar(size=10).encode(
            x=alt.X('MERCHANT:N', title='Comercio'),
            y=alt.Y('count():Q', title='Numero de Transacciones'),
            color=alt.Color('CLASS:N', scale=alt.Scale(domain=[0, 1], range=['green', 'red']), legend=alt.Legend(title='Tipo Transacción')),
            tooltip=['MERCHANT:N', 'count():Q', 'CLASS:N']
        ).properties(title='Trans. Fraudulentas y Normales por Canal')

        #Cumulative Logins 
        TRANSACTION_AMOUNT_histogram = alt.Chart(predictionsmerged).mark_bar(size=10).encode(
        alt.X('TRANSACTION_AMOUNT:Q', bin=alt.Bin(maxbins=30), title='Monto transacción'),
        alt.Y('count():Q', title='Numero de Transacciones'),
        color=alt.Color('CLASS:N', scale=alt.Scale(domain=[0, 1], range=['green', 'red']), legend=alt.Legend(title='Tipo Transacción')),
        tooltip=['TRANSACTION_AMOUNT:Q', 'count():Q', 'CLASS:N']
    ).properties(
        title='Distribución de Transa. x Monto',
        width=600
    )

        
        # Time Elapsed Distribution for Fraudulent and Normal Transactions
        time_elapsed_chart = (
            alt.Chart(predictions)
            .mark_bar()
            .encode(
                x=alt.X('TIME_ELAPSED:Q', title='Tiempo Transcurrido'),
                y=alt.Y('count():Q', title='Numero de Transacciones'),
                color=alt.Color('CLASS:N', scale=alt.Scale(domain=[0, 1], range=['green', 'red']), legend=alt.Legend(title='Tipo Transacción')),
                tooltip=['TIME_ELAPSED:Q', 'count():Q', 'CLASS:N']
            )
            .properties(title='Distribución Tiempo Transcurrido para las Transacciones')
        )
        
        # Clicks Distribution for Fraudulent and Normal Transactions
        clicks_chart = (
            alt.Chart(predictions)
            .mark_bar()
            .encode(
                x=alt.X('CLICKS:Q', title='Clicks'),
                y=alt.Y('count():Q', title='Numero de Transacciones'),
                color=alt.Color('CLASS:N', scale=alt.Scale(domain=[0, 1], range=['green', 'red']), legend=alt.Legend(title='Tipo Transacción')),
                tooltip=['CLICKS:Q', 'count():Q', 'CLASS:N']
            )
            .properties(title='Distribución de Clics por Transacciones')
        )
        
        # Display the charts in Streamlit side by side
        st.title('Visualización Detección de fraude')

        # Layout
        ##col1, col2 = st.columns(2)
        ##with col1:
        
        
        ##with col2:
        st.altair_chart(TRANSACTION_AMOUNT_histogram, use_container_width=True)

        st.altair_chart(chart_fraud_by_merchant, use_container_width=True)

        ##col4, col5 = st.columns(2)
    
        ##with col4:
        st.altair_chart(time_elapsed_chart, use_container_width=True)
        
        ##with col5:
        st.altair_chart(clicks_chart, use_container_width=True)
               
