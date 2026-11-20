
import json
import logging
import random
import time
import uuid

from datetime import datetime, timezone

from confluent_kafka import Producer

from carepulse.common.kafka_config import (
    PATIENT_IDS,
    VITALS_TOPIC,
    get_producer_config
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger(__name__)


def generate_vitals_data() -> dict:
    patient_id = random.choice(PATIENT_IDS)

    systolic = random.randint(90, 140)
    diastolic = random.randint(60, 90)

    return {
        "EventID": str(uuid.uuid4()),
        "PatientID": patient_id,
        "DeviceID": f"VITAL-{patient_id}",
        "HeartRate": random.randint(60, 100),
        "SystolicBP": systolic,
        "DiastolicBP": diastolic,
        "OxygenSaturation": round(
            random.uniform(95, 100), 2
        ),
        "Timestamp": datetime.now(
            timezone.utc
        ).isoformat()
    }


def delivery_callback(err, msg):
    if err is not None:
        logger.error("Kafka delivery failed: %s", err)
    else:
        logger.info(
            "Delivered topic=%s partition=%s offset=%s",
            msg.topic(),
            msg.partition(),
            msg.offset()
        )


def main():
    producer = Producer(
        get_producer_config("carepulse-vitals-producer")
    )

    logger.info("Starting CarePulse vitals producer")

    try:
        while True:
            event = generate_vitals_data()

            producer.produce(
                topic=VITALS_TOPIC,
                key=str(event["PatientID"]),
                value=json.dumps(event).encode("utf-8"),
                on_delivery=delivery_callback
            )

            producer.poll(0)

            logger.info(
                "Published patient vitals: %s",
                event
            )

            time.sleep(2)

    except KeyboardInterrupt:
        logger.info("Stopping vitals producer")

    finally:
        remaining = producer.flush(30)
        if remaining:
            logger.error(
                "%s messages were not delivered",
                remaining
            )


if __name__ == "__main__":
    main()
