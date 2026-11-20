
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

from azure.identity import AzureCliCredential
from azure.storage.filedatalake import DataLakeServiceClient
from confluent_kafka import Consumer, KafkaException, TopicPartition
from dotenv import load_dotenv

from carepulse.common.kafka_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    VITALS_TOPIC,
    GLUCOSE_TOPIC,
)

load_dotenv()

STORAGE_ACCOUNT = os.environ["AZURE_STORAGE_ACCOUNT"]
FILESYSTEM = os.environ["ADLS_FILESYSTEM"]

BATCH_SIZE = int(os.getenv("KAFKA_ADLS_BATCH_SIZE", "5"))
FLUSH_SECONDS = int(os.getenv("KAFKA_ADLS_FLUSH_SECONDS", "10"))

if BATCH_SIZE < 1 or FLUSH_SECONDS < 1:
    raise ValueError("Batch size and flush interval must be positive")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

# Suppress verbose Azure SDK HTTP request logs
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING
)

# Suppress Kafka client informational logs
logging.getLogger("confluent_kafka").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


def create_adls_client(credential):
    service = DataLakeServiceClient(
        account_url=(
            f"https://{STORAGE_ACCOUNT}.dfs.core.windows.net"
        ),
        credential=credential,
    )
    return service.get_file_system_client(FILESYSTEM)


def create_kafka_consumer():
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "carepulse-adls-historical-v1",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    })


def create_record(msg):
    payload = json.loads(msg.value().decode("utf-8"))

    return {
        "kafka_topic": msg.topic(),
        "kafka_partition": msg.partition(),
        "kafka_offset": msg.offset(),
        "ingestion_timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
        "payload": payload,
    }


def upload_batch(lake, topic, partition, records):
    first_offset = records[0]["kafka_offset"]
    last_offset = records[-1]["kafka_offset"]

    path = (
        f"raw/{topic}/"
        f"partition={partition}/"
        f"offset_{first_offset}_{last_offset}.jsonl"
    )

    content = "\n".join(
        json.dumps(record, separators=(",", ":"))
        for record in records
    ) + "\n"

    file_client = lake.get_file_client(path)

    file_client.upload_data(
        content.encode("utf-8"),
        overwrite=True,
    )

    logger.info(
        "Uploaded %s records to %s",
        len(records),
        path,
    )

    return last_offset + 1


def main():
    credential = AzureCliCredential()
    consumer = None

    buffers = defaultdict(list)
    last_flush = time.monotonic()

    try:
        lake = create_adls_client(credential)
        lake.get_file_system_properties()

        logger.info(
            "Connected to ADLS Gen2 account=%s filesystem=%s",
            STORAGE_ACCOUNT,
            FILESYSTEM,
        )

        consumer = create_kafka_consumer()

        def flush_partition(topic, partition):
            key = (topic, partition)
            records = buffers[key]

            if not records:
                return

            next_offset = upload_batch(
                lake,
                topic,
                partition,
                records,
            )

            consumer.commit(
                offsets=[
                    TopicPartition(
                        topic,
                        partition,
                        next_offset,
                    )
                ],
                asynchronous=False,
            )

            logger.info(
                "Committed topic=%s partition=%s next_offset=%s",
                topic,
                partition,
                next_offset,
            )

            buffers[key].clear()

        def flush_all():
            for topic, partition in list(buffers):
                flush_partition(topic, partition)

        consumer.subscribe([
            VITALS_TOPIC,
            GLUCOSE_TOPIC,
        ])

        logger.info("CarePulse Kafka-to-ADLS consumer started")

        while True:
            msg = consumer.poll(1.0)

            if msg is not None:
                if msg.error():
                    raise KafkaException(msg.error())

                record = create_record(msg)
                key = (msg.topic(), msg.partition())
                buffers[key].append(record)

                if len(buffers[key]) >= BATCH_SIZE:
                    flush_partition(*key)

            if time.monotonic() - last_flush >= FLUSH_SECONDS:
                flush_all()
                last_flush = time.monotonic()

    except KeyboardInterrupt:
        logger.info("Shutdown requested")

    finally:
        if consumer is not None:
            try:
                # Flush remaining records before closing.
                for topic, partition in list(buffers):
                    if buffers[(topic, partition)]:
                        next_offset = upload_batch(
                            lake,
                            topic,
                            partition,
                            buffers[(topic, partition)],
                        )
                        consumer.commit(
                            offsets=[
                                TopicPartition(
                                    topic,
                                    partition,
                                    next_offset,
                                )
                            ],
                            asynchronous=False,
                        )
            finally:
                consumer.close()

        credential.close()


if __name__ == "__main__":
    main()
