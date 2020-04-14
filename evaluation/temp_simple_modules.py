class Event:
    def __init__(self, payload, event_type, time):
        self.payload = payload
        self.event_type = event_type
        self.timestamp = time

    def __repr__(self):
        return "((event_type: {}), (payload: {}), (timestamp: {}))".format(
            self.event_type, self.payload, self.timestamp
        )
