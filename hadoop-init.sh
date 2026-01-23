#!/bin/bash
/run.sh &
sleep 30
hdfs dfs -mkdir -p /user
hdfs dfs -mkdir -p /user/jovyan
hdfs dfs -mkdir -p /user/jovyan/weather
hdfs dfs -mkdir -p /user/jovyan/alerts
hdfs dfs -chown -R jovyan:supergroup /user/jovyan
hdfs dfs -chmod -R 755 /user/jovyan
echo "HDFS ready for jovyan"
tail -f /dev/null