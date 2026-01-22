FROM jupyter/minimal-notebook:latest

USER root
# Correction : default-jre au lieu de default-jr
RUN apt-get update && apt-get install -y default-jre && apt-get clean

USER jovyan
# Installation des librairies Python
RUN pip install --no-cache-dir kafka-python requests hdfs pyspark==3.5.0