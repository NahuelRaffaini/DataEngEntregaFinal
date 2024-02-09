FROM apache/airflow:2.1.0
WORKDIR /opt/airflow
COPY requirements.txt .
COPY scriptFinal.py ./dags/
COPY cryptoDag.py ./dags/
COPY crypto.env .
COPY configAlertas.json .
RUN pip install --no-cache-dir -r requirements.txt
