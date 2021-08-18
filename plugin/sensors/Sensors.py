from datetime import datetime, timedelta
import random

from base.DataFormatter import DataFormatter, EventTypeClassifier
from misc.Utils import str_to_number

SENSORS_TIMESTAMP_KEY = "TimeStamp"
SENSORS_TYPE_KEY = "SensorType"

SENSORS_COMMON_KEYS = [
    SENSORS_TYPE_KEY,
    SENSORS_TIMESTAMP_KEY,
    "Amplitude"
]

SENSORS_KEYS_DICT = {
    "PressTemp":
        SENSORS_COMMON_KEYS +
        [
            "Pressure",
            "Temperature"
        ],
    "Accelerometer":
        SENSORS_COMMON_KEYS +
        [
            "AccX",
            "AccY",
            "AccZ"
        ],
    "Magnetometer":
        SENSORS_COMMON_KEYS +
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
            SENSORS_KEYS_DICT[event_attributes[0]],
            map(str_to_number, event_attributes)
        ))

    def get_event_timestamp(self, event_payload: dict):
        """
        The event timestamp is represented in sensors using a "%m/%d/%Y %H:%M:%S" format.
        """
        timestamp_str = event_payload[SENSORS_TIMESTAMP_KEY]
        return datetime.strptime(timestamp_str, "%m/%d/%Y %H:%M:%S")


def random_str(lowest, highest):
    return str(round(lowest + random.uniform(0, highest - lowest), 3))


if __name__ == '__main__':
    #  create test data
    num_of_samples = 10000
    types_list = list(SENSORS_KEYS_DICT.keys())
    curr_time = datetime.now()
    with open(r"..\..\test\EventFiles\Sensors_long_time.dat", "w+") as output_file:
        for i in range(num_of_samples):
            choice = random.choice(types_list)
            timestamp = (curr_time + timedelta(seconds=i*30)).strftime("%m/%d/%Y %H:%M:%S")
            amplitude = random_str(lowest=0, highest=0.003)
            output = [choice, timestamp, amplitude]
            if choice == 'PressTemp':
                temperature = random_str(lowest=23, highest=27)
                pressure = random_str(lowest=940, highest=960)
                output.extend([temperature, pressure])
            elif choice == 'Accelerometer':
                accX = random_str(lowest=-50, highest=50)
                accY = random_str(lowest=-100, highest=100)
                accZ = random_str(lowest=-20, highest=20)
                output.extend([accX, accY, accZ])
            elif choice == 'Magnetometer':
                magX = random_str(lowest=-50, highest=50)
                magY = random_str(lowest=-100, highest=100)
                magZ = random_str(lowest=-20, highest=20)
                output.extend([magX, magY, magZ])
            output_file.write(','.join(output) + "\n")

    #  check the data_formatter functionality
    data_formatter = SensorsDataFormatter()
    with open(r"..\..\test\EventFiles\Sensors_long_time.dat", "r") as input_file:
        for line in input_file:
            parsed = data_formatter.parse_event(line)
            sensor_type = data_formatter.get_event_type(parsed)
            timestamp = data_formatter.get_event_timestamp(parsed)
            print(parsed, "\n", sensor_type, "\n", timestamp)
