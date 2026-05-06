FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    streamlit==1.44.1 \
    snowflake-connector-python[pandas]==3.13.1 \
    snowflake-snowpark-python==1.25.0 \
    plotly==5.24.1 \
    pydeck==0.9.1 \
    pandas==2.2.3

COPY . /app/

EXPOSE 8080

CMD ["streamlit", "run", "streamlit_app.py", \
     "--server.port=8080", \
     "--server.address=0.0.0.0", \
     "--server.headless=true", \
     "--browser.gatherUsageStats=false"]
