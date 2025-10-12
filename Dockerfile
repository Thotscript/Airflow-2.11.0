FROM apache/airflow:2.11.0-python3.12

USER airflow

# Use o constraints oficial do Airflow para a mesma versão/Python
ARG AIRFLOW_VERSION=2.11.0
ARG PYTHON_VERSION=3.12
ARG CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

COPY requirements.txt /requirements.txt

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt -c ${CONSTRAINT_URL}
