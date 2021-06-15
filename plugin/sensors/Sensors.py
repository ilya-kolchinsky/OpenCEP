from datetime import datetime, timedelta
from typing import Any, Dict, Optional
import random

from base.DataFormatter import DataFormatter, EventTypeClassifier
from misc.Utils import str_to_number

SENSORS_TIMESTAMP_KEY = "TimeStamp"
SENSORS_TYPE_KEY = "SensorType"

SENSORS_COLUMN_KEYS = [
    SENSORS_TYPE_KEY,
    SENSORS_TIMESTAMP_KEY
]

SENSORS_ADDITIONAL_COLUMN_KEYS = {
    "PressTemp":
    [
        "Pressure",
        "Temperature"
    ],
    "Accelerometer":
    [
        "AccX",
        "AccY",
        "AccZ"
    ],
    "Magnetometer":
    [
        "MagX",
        "MagY",
        "MagZ"
    ]
}


class SensorsEventTypeClassifier(EventTypeClassifier):
    """
    Assigns a single dummy event type to all events
    """

    def get_event_type(self, event_payload: dict):
        return event_payload[SENSORS_TYPE_KEY]


class SensorsDataFormatter(DataFormatter):
    """
    A data formatter implementation for a Sensors event stream, where each event is given as a string in Sensors
    format.
    """

    def __init__(self, event_type_classifier: EventTypeClassifier = SensorsEventTypeClassifier()):
        super().__init__(event_type_classifier)

    def parse_event(self, raw_data: str):
        """
        Parses a Sensors formatted string into an event.
        """
        event_attributes = raw_data.replace("\n", "").split(",")
        return dict(zip(
            SENSORS_COLUMN_KEYS + SENSORS_ADDITIONAL_COLUMN_KEYS[event_attributes[0]],
            map(str_to_number, event_attributes)
        ))

    def get_event_timestamp(self, event_payload: dict):
        """
        The event timestamp is represented in sensors using a "%m/%d/%Y %H:%M:%S" format.
        """
        timestamp_str = event_payload[SENSORS_TIMESTAMP_KEY]
        return datetime.strptime(timestamp_str, "%m/%d/%Y %H:%M:%S")


if __name__ == '__main__':
    types_list = list(SENSORS_ADDITIONAL_COLUMN_KEYS.keys())
    with open(r"..\..\test\EventFiles\Sensors.dat", "w+") as output_file:
        for i in range(1000):
            choice = random.choice(types_list)
            timestamp = (datetime.now() + timedelta(seconds=i)).strftime("%m/%d/%Y %H:%M:%S")
            if choice == 'PressTemp':
                temperature = str(round(25 + random.uniform(-2, 2), 3))
                pressure = str(round(950 + random.uniform(-10, 10), 3))
                output_file.write(choice + "," + timestamp + "," + temperature + "," + pressure + "\n")
            elif choice == 'Accelerometer':
                accX = str(round(random.uniform(-50, 50), 3))
                accY = str(round(random.uniform(-100, 100), 3))
                accZ = str(round(random.uniform(-20, 20), 3))
                output_file.write(choice + "," + timestamp + "," + accX + "," + accY + "," + accZ + "\n")
            elif choice == 'Magnetometer':
                magX = str(round(random.uniform(-50, 50), 3))
                magY = str(round(random.uniform(-100, 100), 3))
                magZ = str(round(random.uniform(-20, 20), 3))
                output_file.write(choice + "," + timestamp + "," + magX + "," + magY + "," + magZ + "\n")
    data_formatter = SensorsDataFormatter()
    with open(r"..\..\test\EventFiles\Sensors.dat", "r") as input_file:
        for line in input_file:
            parsed = data_formatter.parse_event(line)
            type = data_formatter.get_event_type(parsed)
            timestamp = data_formatter.get_event_timestamp(parsed)
            print(parsed, "\n", type, "\n", timestamp)
