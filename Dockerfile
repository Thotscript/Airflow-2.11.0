FROM apache/airflow:2.11.0-python3.12

ARG AIRFLOW_VERSION=2.11.0
ARG PYTHON_VERSION=3.12
ARG CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

# Playwright: coloca os browsers num caminho estável (evita /root/.cache)
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

USER root

COPY requirements.txt /requirements.txt

# 1) Instala suas dependências do requirements usando constraints do Airflow
# 2) Instala Playwright
# 3) Baixa Chromium + dependências do SO
# 4) Ajusta permissões do diretório de browsers pro usuário airflow
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt -c ${CONSTRAINT_URL} && \
    pip install --no-cache-dir playwright && \
    python -m playwright install --with-deps chromium && \
    mkdir -p /ms-playwright && \
    chown -R airflow:0 /ms-playwright && \
    chmod -R g+rwX /ms-playwright

USER airflow
