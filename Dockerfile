FROM apache/airflow:2.11.0-python3.12

ARG AIRFLOW_VERSION=2.11.0
ARG PYTHON_VERSION=3.12
ARG CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

# ---- Playwright paths (evita cair em /root/.cache) ----
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
ENV XDG_CACHE_HOME=/home/airflow/.cache

COPY requirements.txt /requirements.txt

# 1) Como root: cria pasta dos browsers e dá permissão pro airflow
USER root
RUN mkdir -p /ms-playwright && \
    chown -R airflow:0 /ms-playwright && \
    chmod -R g+rwX /ms-playwright && \
    mkdir -p /home/airflow/.cache && \
    chown -R airflow:0 /home/airflow/.cache && \
    chmod -R g+rwX /home/airflow/.cache

# 2) Como airflow: instala libs python (incluindo playwright)
USER airflow
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt -c ${CONSTRAINT_URL} && \
    pip install --no-cache-dir playwright

# 3) Como root: instala deps do SO necessárias pro Chromium
#    (isso é o que o --with-deps faria, mas fazemos explícito aqui)
USER root
RUN python -m playwright install-deps chromium

# 4) Como airflow: instala o browser Chromium no caminho definido (PLAYWRIGHT_BROWSERS_PATH)
USER airflow
RUN python -m playwright install chromium

# Volta ao padrão
USER airflow
