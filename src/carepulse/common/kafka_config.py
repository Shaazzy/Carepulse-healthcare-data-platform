
import os

from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

VITALS_TOPIC = os.getenv(
    "KAFKA_VITALS_TOPIC",
    "iot_vitals"
)

GLUCOSE_TOPIC = os.getenv(
    "KAFKA_GLUCOSE_TOPIC",
    "iot_glucose"
)

PATIENT_IDS = [101, 102, 103, 104, 105]


def get_producer_config(client_id: str) -> dict:
    return {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": client_id,
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "snappy",
        "message.timeout.ms": 30000
    }
