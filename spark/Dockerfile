# Utiliser l'image Spark officielle
FROM apache/spark:3.5.0

USER root

# Installer Python et les dépendances
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Installer les packages Python
RUN pip3 install pyspark==3.5.0 kafka-python pandas numpy

# Créer le répertoire pour les scripts
RUN mkdir -p /opt/spark/scripts

# Copier les scripts
COPY scripts/ /opt/spark/scripts/

# Définir les permissions
RUN chown -R spark:spark /opt/spark

USER spark

WORKDIR /opt/spark