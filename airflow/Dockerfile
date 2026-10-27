FROM apache/airflow:3.3.1

# Java 17 pour Spark (en root)
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# PySpark et dbt dans un venv séparé (évite les conflits avec Airflow)
USER airflow
RUN python -m venv /home/airflow/rdw-venv \
 && /home/airflow/rdw-venv/bin/pip install --no-cache-dir \
      pyspark==4.2.0 dbt-core==1.12.5 dbt-postgres==1.11.0
