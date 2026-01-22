FROM apache/airflow:2.7.1

# On repasse en utilisateur root pour installer des dépendances si nécessaire
USER root

# Installation des librairies Python spécifiques à votre projet
# On installe kafka-python et hdfs pour le DAG d'alertes
USER airflow
RUN pip install --no-cache-dir kafka-python hdfs