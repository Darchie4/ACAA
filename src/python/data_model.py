from dataclasses import dataclass, field, asdict
from datetime import datetime
from uuid import uuid4
import random
import json

VALID_SENSOR_IDS: list[int] = [1, 2, 3, 4, 5, 6]
VALID_TEMPORAL_ASPECTS: list[str] = ["real_time", "edge_prediction"]
VALID_RANGE: tuple[int] = (-10, 10)


def get_uuid():
    return str(uuid4())

def clean_dimensions(dimensions):
    """Cleans and parses the dimensions field."""
    if isinstance(dimensions, str):
        try:
            # Remove surrounding quotes if they exist and parse JSON
            dimensions = json.loads(dimensions.strip('"'))
        except json.JSONDecodeError:
            print(f"Failed to parse dimensions: {dimensions}")
            return None
    return dimensions

@dataclass
class SensorObj:
    sensor_id: str
    pressure: float
    temperature: float
    dimensions: dict  # Stored as a dictionary

    def to_dict(self) -> dict:
        return {
            "sensor_id": self.sensor_id,
            "pressure": self.pressure,
            "temperature": self.temperature,
            "dimensions": json.dumps(self.dimensions),  # Serialize dimensions as JSON
        }

@dataclass
class PackageObj:
    payload: SensorObj
    correlation_id: str = field(default_factory=get_uuid)
    created_at: datetime = field(default_factory=datetime.utcnow)
    schema_version: int = field(default=1)

    def __post_init__(self):
        # Ensure payload is a SensorObj instance
        if isinstance(self.payload, dict):
            self.payload = SensorObj(**self.payload)
        elif isinstance(self.payload, str):  # If payload is a JSON string
            self.payload = self.str_to_sensor_obj(self.payload)

        # Ensure created_at is a datetime object
        if isinstance(self.created_at, (float, int)):  # If it's a UNIX timestamp
            self.created_at = datetime.fromtimestamp(self.created_at)

    def str_to_sensor_obj(self, x: str) -> SensorObj:
        return SensorObj(**json.loads(x))

    def to_dict(self):
        return {
            "payload": self.payload.to_dict(),
            "correlation_id": self.correlation_id,
            "created_at": self.created_at.timestamp(),
            "schema_version": self.schema_version,
        }


def get_sensor_sample(
        sensor_id: int = None,
        pressure: float = None,
        temperature: float = None,
        dimensions: dict[str, float] = None,
) -> SensorObj:
    if sensor_id is None:
        sensor_id = random.choice(VALID_SENSOR_IDS)

    # Introduce anomalies
    if pressure is None:
        if random.random() < 0.90:  # 10% chance of anomaly
            pressure = random.uniform(950.1, 979.9)  # Anomalous pressure range
        else:
            pressure = random.uniform(980.0, 990)  # Normal pressure range

    if temperature is None:
        if random.random() < 0.90:  # 10% chance of anomaly
            temperature = random.uniform(21.1, 22.0)  # Anomalous temperature range
        else:
            temperature = random.uniform(20.0, 21.0)  # Normal temperature range

    if dimensions is None:
        dimensions = {"length": random.uniform(9.95, 10.0), "width": random.uniform(0.95, 1.00)}

    return SensorObj(
        sensor_id=sensor_id,
        pressure=pressure,
        temperature=temperature,
        dimensions=dimensions,
    )


def generate_sample(sensor_id: int) -> tuple[int, dict]:
    po = PackageObj(payload=get_sensor_sample(sensor_id=sensor_id))
    return sensor_id, po.to_dict()
