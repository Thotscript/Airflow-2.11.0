FROM apache/airflow:2.11.0-python3.12

ARG AIRFLOW_VERSION=2.11.0
ARG PYTHON_VERSION=3.12
ARG CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

# Define caminho estável para os browsers do Playwright
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# Copia os requirements primeiro (melhor cache)
COPY requirements.txt /requirements.txt

# Instala pacotes Python como o usuário airflow
USER airflow
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt -c ${CONSTRAINT_URL} && \
    pip install --no-cache-dir playwright

# Agora instala os browsers e dependências do sistema como root
USER root
RUN python -m playwright install --with-deps chromium && \
    mkdir -p /ms-playwright && \
    chown -R airflow:0 /ms-playwright && \
    chmod -R g+rwX /ms-playwright

USER airflow
