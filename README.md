# Projet Big Data - Architecture Kafka, Spark, HDFS, Airflow

## Architecture
```
API Météo → Kafka → Spark (agrégation) → HDFS
                 ↓
              Airflow (alertes)
```

## Stack technique

- **Jupyter**: Environnement de développement
- **Kafka**: Streaming de données météo
- **Spark**: Agrégation sur fenêtre temporelle
- **HDFS**: Stockage distribué
- **Airflow**: Orchestration et traitement des alertes

---

## Démarrage rapide
```bash
# Tout démarrer
make all-up

# Tout arrêter
make all-down

# Nettoyer volumes
make clean
```

---

## Services et ports

| Service | URL | Credentials |
|---------|-----|-------------|
| Jupyter | http://localhost:8888 | - |
| HDFS UI | http://localhost:9870 | - |
| Spark UI | http://localhost:8181 | - |
| Airflow | http://localhost:8082 | admin/admin |

---

## Scripts

### `notebooks/weather_producer.py`
**Rôle**: Producer Kafka qui récupère les données météo de Berlin toutes les 30 secondes.

**Actions**:
- Appelle l'API Open-Meteo
- Convertit température Celsius → Fahrenheit
- Détecte alertes vent fort (> 10 km/h)
- Envoie dans Kafka topic `weather_transformed`

**Exécution**:
```bash
docker exec -it jupyter_notebook python /home/jovyan/work/weather_producer.py
```

---

### `notebooks/consumer_test.py`
**Rôle**: Consumer Kafka de test pour visualiser les messages.

**Actions**:
- Lit les messages du topic `weather_transformed`
- Affiche dans la console

**Exécution**:
```bash
docker exec -it jupyter_notebook python /home/jovyan/work/consumer_test.py
```

---

### `notebooks/spark_processor.py`
**Rôle**: Job Spark qui agrège les données météo et les sauvegarde dans HDFS.

**Actions**:
- Lit les messages Kafka
- Agrège par fenêtre temporelle de 1 minute:
  - Température moyenne
  - Nombre d'alertes vent fort
- Convertit en CSV
- Sauvegarde dans HDFS: `/user/jovyan/weather/aggregated_data.csv`

**Exécution**:
```bash
docker exec -it jupyter_notebook python /home/jovyan/work/spark_processor.py
```

---

### `dags/weather_alerts_dag.py`
**Rôle**: DAG Airflow qui filtre et sauvegarde les alertes météo.

**Actions**:
- **Tâche 1** (`filter_alerts`): Lit Kafka et filtre uniquement `high_wind_alert: True`
- **Tâche 2** (`save_to_hdfs`): Sauvegarde les alertes en JSON dans `/user/jovyan/alerts/alerts_TIMESTAMP.json`
- **Tâche 3** (`verify_hdfs`): Vérifie que le fichier a été créé

**Déclenchement**: Automatique toutes les 5 minutes ou manuel via UI Airflow

---

### `hadoop-init.sh`
**Rôle**: Script d'initialisation HDFS au démarrage du namenode.

**Actions**:
- Démarre le namenode
- Crée les dossiers `/user/jovyan/weather` et `/user/jovyan/alerts`
- Configure les permissions pour l'utilisateur `jovyan`

---

## Vérifications

### Kafka
```bash
# Voir les messages
docker exec -it jupyter_notebook python /home/jovyan/work/consumer_test.py
```

### HDFS
```bash
# Lister les fichiers
docker exec -it namenode hdfs dfs -ls /user/jovyan/weather/
docker exec -it namenode hdfs dfs -ls /user/jovyan/alerts/

# Lire un fichier
docker exec -it namenode hdfs dfs -cat /user/jovyan/weather/aggregated_data.csv
```

### Airflow
- Accéder à http://localhost:8082
- Activer le DAG `weather_alert_kafka_to_hdfs`
- Consulter les logs des tâches

---

## Variables d'environnement (`.env`)
```env
JUPYTER_USER=jovyan
NETWORK_NAME=bigdata_network
KAFKA_BROKER=kafka:9092
KAFKA_TOPIC=weather_transformed
HDFS_URL=http://namenode:9870
HDFS_USER=jovyan
HDFS_DIR=/user/jovyan/weather
SPARK_MASTER=spark://spark-master:7077
```

---

## Flux de données

1. **Producer** → Récupère météo Berlin → Kafka
2. **Spark** → Lit Kafka → Agrège → Sauvegarde CSV dans HDFS
3. **Airflow** → Lit Kafka → Filtre alertes → Sauvegarde JSON dans HDFS

---

## Branches Git

- `etape1`: Jupyter
- `etape2`: Kafka + Producer
- `etape3`: Spark + Agrégation
- `etape4`: HDFS + Sauvegarde
- `etape5`: Airflow + DAG
- `main`: Projet complet

---
