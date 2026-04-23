# Imagen base de Airflow (incluye Python 3.11, Airflow 2.9.x)
FROM apache/airflow:2.9.3-python3.11

USER root

# --- Java para PySpark + utilidades ---
RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless \
      curl \
      procps \
    && JAVA_BIN="$(readlink -f "$(command -v java)")" \
    && JAVA_HOME_DIR="$(dirname "$(dirname "$JAVA_BIN")")" \
    && ln -sfn "$JAVA_HOME_DIR" /usr/lib/jvm/default-java \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# --- Driver JDBC Postgres para Spark ---
ENV POSTGRES_JDBC_VERSION=42.7.3
RUN mkdir -p /opt/spark/jars && \
    curl -fsSL -o /opt/spark/jars/postgresql.jar \
      "https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar"

# --- App ---
WORKDIR /opt/airflow/app
COPY requirements.txt /opt/airflow/app/requirements.txt

USER airflow
RUN pip install --no-cache-dir -r /opt/airflow/app/requirements.txt

# Copiamos el código fuente
COPY --chown=airflow:root src /opt/airflow/app

# PYTHONPATH para que `python -m etl.run_sync` funcione
ENV PYTHONPATH=/opt/airflow/app
