import json

from data_model import PackageObj, generate_sample
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import os
import csv



# Format <pod name>.<service name>:<port>
KAFKA_BOOTSTRAP: list[str] = ["kafka:9092"]

DEFAULT_ENCODING: str = "utf-8"
DEFAULT_CONSUMER: str = "DEFAULT_CONSUMER"


def get_producer() -> KafkaProducer:
    return KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)


def get_consumer(topic: str, group_id: str = None) -> KafkaConsumer:
    if group_id is None:
        group_id = DEFAULT_CONSUMER
    return KafkaConsumer(topic, bootstrap_servers=KAFKA_BOOTSTRAP, group_id=group_id)


def send_msg(value, key: str, topic: str, producer: KafkaProducer) -> None:
    producer.send(
        topic=topic,
        key=key.encode(DEFAULT_ENCODING),
        value=json.dumps(value).encode(DEFAULT_ENCODING),
    )


def produce_msg(sensor_id: int, topic: str, producer: KafkaProducer) -> None:
    key, value = generate_sample(sensor_id=sensor_id)
    print(f"Produced: {value}")
    send_msg(key=str(key), value=value, topic=topic, producer=producer)


def recive_msg_with_logging(consumer, base_dir="logs"):
    """Consumes messages, detects anomalies, and logs them."""
    try:
        os.makedirs(base_dir, exist_ok=True)  # Ensure the directory exists
        for msg in consumer:
            package = PackageObj(**json.loads(msg.value.decode('utf-8')))
            consumed_at = datetime.utcnow()
            time_diff = (consumed_at - package.created_at).total_seconds()
            
            anomalies = detect_anomalies(package)
            if anomalies:
                # Serialize the package object to JSON format with custom datetime handling
                send_msg(
                    key=str(package.payload.sensor_id),
                    value=json.loads(json.dumps(package, default=serialize_non_json)),
                    topic="ALERT",
                    producer=get_producer()
                )
            
            # Log normal message data
            data_to_save = [{
                'Sensor ID': package.payload.sensor_id,
                'Correlation ID': package.correlation_id,
                'Created At': package.created_at,
                'Consumed At': consumed_at,
                'Time Difference (seconds)': time_diff
            }]
            topic_file = os.path.join(base_dir, "sensor_monitoring.csv")
            save_to_csv(topic_file, data_to_save)
        
    except Exception as e:
        print(f"Error consuming messages: {e}")


def serialize_non_json(obj):
    """Custom serialization for non-JSON-serializable objects."""
    if isinstance(obj, datetime):
        return obj.isoformat()  # Convert datetime to ISO 8601 string
    if hasattr(obj, '__dict__'):
        return obj.__dict__  # Use object's __dict__ if available
    return str(obj)  # Fallback to string representation

        
def detect_anomalies(package: PackageObj):
    """Tjekker for afvigelser i sensorens målinger."""
    anomalies = []
    if not (980 <= package.payload.pressure <= 990):  # Normalt trykområde
        anomalies.append(f"Trykafvigelse: {package.payload.pressure}")
    if not (20 <= package.payload.temperature <= 21):  # Normal temperatur
        anomalies.append(f"Temperaturafvigelse: {package.payload.temperature}")
    return anomalies


from datetime import datetime

def recive_msg(consumer, base_dir="logs"):
    """Consumes messages and saves them to topic-specific CSV files."""
    try:
        os.makedirs(base_dir, exist_ok=True)  # Ensure the directory exists
        for msg in consumer:
            # Deserialize the message
            package_data = json.loads(msg.value.decode('utf-8'))

            # Ensure `created_at` is a datetime object
            if isinstance(package_data['created_at'], str):
                package_data['created_at'] = datetime.fromisoformat(package_data['created_at'])
            elif isinstance(package_data['created_at'], (int, float)):
                # Convert Unix timestamp to datetime
                package_data['created_at'] = datetime.utcfromtimestamp(package_data['created_at'])
            elif not isinstance(package_data['created_at'], datetime):
                raise ValueError(f"Unexpected type for 'created_at': {type(package_data['created_at'])}")

            package = PackageObj(**package_data)

            consumed_at = datetime.utcnow()
            time_diff = (consumed_at - package.created_at).total_seconds()

            data_to_save = [{
                'Sensor ID': package.payload.sensor_id,
                'Correlation ID': package.correlation_id,
                'Created At': package.created_at,
                'Consumed At': consumed_at,
                'Time Difference (seconds)': time_diff
            }]

            # Create file path for the topic
            topic_file = os.path.join(base_dir, "alert.csv")
            save_to_csv(topic_file, data_to_save)

    except Exception as e:
        print(f"Error consuming messages: {e}")



def save_to_csv(file_path: str, data: list[dict]):
    """Save data to a CSV file."""
    file_exists = os.path.exists(file_path)
    with open(file_path, mode='a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        if not file_exists:  # Write header only if the file doesn't exist
            writer.writeheader()
        writer.writerows(data)