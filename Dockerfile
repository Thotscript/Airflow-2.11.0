FROM apache/airflow:2.11.0-python3.12

COPY requirements.txt /requirements.txt

USER airflow

# Atualiza pip primeiro
RUN pip install --upgrade pip

# Instala as dependências com flags para resolver conflitos
RUN pip install --no-cache-dir --no-deps -r /requirements.txt || \
    pip install --no-cache-dir --force-reinstall --no-deps -r /requirements.txt

# Reinstala dependências que podem ter sido quebradas
RUN pip install --no-cache-dir --upgrade \
    apache-airflow==2.11.0 \
    apache-airflow-providers-common-sql \
    apache-airflow-providers-http

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl unzip fonts-liberation \
 && rm -rf /var/lib/apt/lists/*

USER airflow