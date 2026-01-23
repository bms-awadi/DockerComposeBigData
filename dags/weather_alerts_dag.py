from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from hdfs import InsecureClient
import json
import traceback
import os

# Configuration depuis variables d'environnement
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather_transformed")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "jovyan")
HDFS_DIR = "/user/jovyan/alerts"


def read_and_filter_alerts():
    """
    Lit les messages depuis Kafka et filtre uniquement les alertes de vent fort.
    Retourne une liste d'alertes a sauvegarder.
    """
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=10000,
    )

    alerts = []

    try:
        for msg in consumer:
            record = msg.value

            # Filtrer uniquement les messages avec alerte de vent fort
            if record.get("high_wind_alert", False):
                alerts.append(record)
                print(f"Alerte detectee: {record}")

        if not alerts:
            print("Aucune alerte trouvee dans les messages Kafka.")

    except Exception as e:
        print(f"Erreur lors de la lecture Kafka: {e}")
        traceback.print_exc()

    finally:
        consumer.close()

    return alerts


def save_alerts_to_hdfs(**context):
    """
    Sauvegarde les alertes dans HDFS avec un nom de fichier horodate.
    """
    ti = context["ti"]
    alerts = ti.xcom_pull(task_ids="filter_alerts")

    if not alerts:
        print("Aucune alerte a sauvegarder.")
        return

    # Creer le contenu JSON avec toutes les alertes
    json_data = json.dumps(
        {
            "timestamp": datetime.utcnow().isoformat(),
            "alert_count": len(alerts),
            "alerts": alerts,
        },
        indent=2,
    )

    # Creer un nom de fichier horodate
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename = f"alerts_{ts}.json"
    hdfs_path = f"{HDFS_DIR}/{filename}"

    try:
        # Creer le client HDFS
        client = InsecureClient(HDFS_URL, user=HDFS_USER)

        # Creer le repertoire HDFS s'il n'existe pas
        print(f"Creation du repertoire: {HDFS_DIR}")
        try:
            client.makedirs(HDFS_DIR)
        except:
            print(f"Repertoire existe deja: {HDFS_DIR}")

        # Ecrire directement dans HDFS
        print(f"Ecriture dans HDFS: {hdfs_path}")
        with client.write(hdfs_path, overwrite=False, encoding="utf-8") as writer:
            writer.write(json_data)

        print(f"Upload reussi: {hdfs_path}")
        print(f"Nombre d'alertes sauvegardees: {len(alerts)}")

    except Exception as e:
        print("ERREUR HDFS:")
        traceback.print_exc()
        raise


def verify_hdfs_upload(**context):
    """
    Verifie que le fichier a bien ete cree dans HDFS.
    """
    try:
        client = InsecureClient(HDFS_URL, user=HDFS_USER)
        files = client.list(HDFS_DIR)
        print(f"Fichiers dans {HDFS_DIR}:")
        for f in files:
            print(f"  - {f}")

        if files:
            # Lire le dernier fichier cree
            latest_file = sorted(files)[-1]
            hdfs_path = f"{HDFS_DIR}/{latest_file}"

            with client.read(hdfs_path, encoding="utf-8") as reader:
                content = reader.read()
                data = json.loads(content)
                print(f"Contenu du dernier fichier ({latest_file}):")
                print(f"  - Nombre d'alertes: {data.get('alert_count', 0)}")
                print(f"  - Timestamp: {data.get('timestamp', 'N/A')}")
        else:
            print("Aucun fichier trouve dans HDFS.")

    except Exception as e:
        print(f"Erreur lors de la verification: {e}")
        traceback.print_exc()


# Definition du DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weather_alert_kafka_to_hdfs",
    default_args=default_args,
    description="Lit les alertes meteo depuis Kafka et les sauvegarde dans HDFS",
    start_date=datetime(2024, 1, 1),
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    tags=["weather", "kafka", "hdfs"],
) as dag:

    # Tache 1: Lire et filtrer les alertes depuis Kafka
    filter_alerts_task = PythonOperator(
        task_id="filter_alerts",
        python_callable=read_and_filter_alerts,
    )

    # Tache 2: Sauvegarder les alertes dans HDFS
    save_to_hdfs_task = PythonOperator(
        task_id="save_to_hdfs",
        python_callable=save_alerts_to_hdfs,
        provide_context=True,
    )

    # Tache 3: Verifier l'upload dans HDFS
    verify_task = PythonOperator(
        task_id="verify_hdfs",
        python_callable=verify_hdfs_upload,
        provide_context=True,
    )

    # Definir l'ordre d'execution des taches
    filter_alerts_task >> save_to_hdfs_task >> verify_task
