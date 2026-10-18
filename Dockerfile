FROM eclipse-temurin:11-jre-jammy AS java

FROM apache/airflow:3.2.1-python3.11

COPY --from=java /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

RUN mkdir -p /opt/airflow/jars && \
    curl -fsSL --retry 3 https://jdbc.postgresql.org/download/postgresql-42.7.14.jar \
    -o /opt/airflow/jars/postgresql.jar

USER root
RUN apt-get update && apt-get install -y --no-install-recommends procps && \
    rm -rf /var/lib/apt/lists/*
USER airflow
