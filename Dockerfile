FROM python:3.10.2

ENV AIRFLOW_HOME=/usr/local/airflow

RUN mkdir -p $AIRFLOW_HOME/dags $AIRFLOW_HOME/logs

COPY requirements.txt /tmp/

RUN pip install --no-cache-dir -r /tmp/requirements.txt
RUN pip install apache-airflow==2.1.0

COPY scriptPre3.py $AIRFLOW_HOME/dags/
COPY api_key.txt $AIRFLOW_HOME/
COPY psw.txt $AIRFLOW_HOME/

CMD ["airflow", "webserver"]
