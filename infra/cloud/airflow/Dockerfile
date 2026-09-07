# Image Airflow personnalisée (remplace le raccourci _PIP_ADDITIONAL_REQUIREMENTS) : PySpark
# (Bloc 1, partie 4.1 — "pour les fichiers volumineux [...] nous utilisons PySpark") a besoin
# d'une JVM, absente de l'image officielle et impossible à ajouter via _PIP_ADDITIONAL_REQUIREMENTS
# (installe des paquets Python, pas des paquets système).
FROM apache/airflow:2.10.5-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
