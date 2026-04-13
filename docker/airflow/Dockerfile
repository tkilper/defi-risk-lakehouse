# =============================================================================
# Custom Airflow image
# Base: apache/airflow:2.9.3-python3.11
# Adds: OpenJDK 17 (required for PySpark), extra pip packages
# =============================================================================
FROM apache/airflow:2.9.3-python3.11

USER root

# Install OpenJDK 17 — required by PySpark's JVM bridge
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      openjdk-17-jdk-headless \
      procps \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Switch back to airflow user for pip installs
USER airflow

COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt" \
    -r /tmp/requirements-airflow.txt

# Install dbt in an isolated virtualenv to avoid the protobuf conflict:
# dbt-core>=1.9 requires protobuf>=5.0, but Airflow 2.9.3 pins protobuf==4.25.3.
# The venv keeps the two dependency trees completely separate.
RUN python -m venv /opt/airflow/dbt-venv \
 && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir \
    dbt-core==1.9.0 \
    dbt-trino==1.9.0

# Copy project source so it's available to Airflow tasks at runtime.
# The dags/ directory is bind-mounted at runtime; everything else is baked in.
COPY --chown=airflow:root ingestion/   /opt/airflow/ingestion/
COPY --chown=airflow:root spark/       /opt/airflow/spark/
COPY --chown=airflow:root dbt/         /opt/airflow/dbt/
COPY --chown=airflow:root scripts/     /opt/airflow/scripts/
