
import json
import logging

from confluent_kafka import Consumer, KafkaException

from carepulse.common.kafka_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    VITALS_TOPIC,
    GLUCOSE_TOPIC
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger(__name__)


def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "carepulse-monitoring-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True
    })

    consumer.subscribe([
        VITALS_TOPIC,
        GLUCOSE_TOPIC
    ])

    logger.info("CarePulse Kafka consumer started")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            try:
                event = json.loads(
                    msg.value().decode("utf-8")
                )
            except (UnicodeDecodeError, json.JSONDecodeError):
                logger.exception(
                    "Invalid JSON topic=%s partition=%s offset=%s",
                    msg.topic(),
                    msg.partition(),
                    msg.offset()
                )
                continue

            logger.info(
                "Received topic=%s partition=%s offset=%s "
                "patient=%s event=%s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
                event.get("PatientID"),
                event.get("EventID")
            )

    except KeyboardInterrupt:
        logger.info("Stopping consumer")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
