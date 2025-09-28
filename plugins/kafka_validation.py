from airflow.models import Variable
from kafka import KafkaConsumer
from datetime import datetime

TOPIC = "intraday_sales"
BROKER = "broker:9092"   # change if needed
EXPECTED_RATE = 1000     # rows per minute

def check_kafka_lag(**context):
    """
    Checks if Kafka offsets are increasing as expected.
    Raises an error if no new messages or fewer than expected.
    """

    consumer = KafkaConsumer(
        bootstrap_servers=BROKER,
        group_id="airflow-monitor",
        enable_auto_commit=False
    )

    # get latest offsets per partition
    partitions = consumer.partitions_for_topic(TOPIC)
    end_offsets = consumer.end_offsets([p for p in partitions])

    latest_offset = sum(end_offsets.values())
    last_offset = int(Variable.get("last_kafka_offset", 0))

    # push new offset so it can be stored
    context['ti'].xcom_push(key="current_offset", value=latest_offset)

    # calculate how many new rows since last check
    new_rows = latest_offset - last_offset

    # check rows against expectation
    interval_minutes = 5   # since DAG runs every 5 min
    expected_rows = EXPECTED_RATE * interval_minutes

    if new_rows < expected_rows:
        raise ValueError(
            f"⚠️ Kafka issue detected at {datetime.now()}:\n"
            f"New rows in last {interval_minutes} min: {new_rows}, "
            f"expected at least {expected_rows}.\n"
            f"Last offset: {last_offset}, Current offset: {latest_offset}"
        )

    print(f"✅ Kafka healthy: {new_rows} rows in last {interval_minutes} min")

def store_last_offset(**context):
    """
    Stores the latest offset into Airflow Variable
    so it persists between DAG runs.
    """
    current_offset = context['ti'].xcom_pull(
        task_ids="check_kafka_lag", key="current_offset"
    )
    if current_offset is not None:
        Variable.set("last_kafka_offset", current_offset)
        print(f"Stored offset: {current_offset}")
