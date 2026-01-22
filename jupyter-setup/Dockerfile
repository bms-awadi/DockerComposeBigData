FROM jupyter/minimal-notebook:latest

USER root
RUN apt-get update && apt-get install -y default-jre && apt-get clean

USER ${NB_UID}
RUN pip install --no-cache-dir kafka-python requests hdfs pyspark==3.5.0 pandas