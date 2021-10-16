import json
import sys
from time import sleep
from datetime import datetime
from kafka import KafkaProducer
import numpy as np

user_id = str(sys.argv[1])
data_freq = float(sys.argv[2])

producer = KafkaProducer(bootstrap_servers="localhost:9092")

while True:
    json_msg = {
        "timestamp": datetime.today().isoformat(),
        "a_x": np.random.normal(),
        "a_y": np.random.normal(),
        "a_z": np.random.normal(),
        "w_x": np.random.normal(),
        "w_y": np.random.normal(),
        "w_z": np.random.normal()
    }

    producer.send(
        'watch',
        key=bytes(user_id.encode("utf-8")),
        value=bytes(json.dumps(json_msg).encode("utf-8"))
    )
    print("Sending message to Kafka")

    sleep(1/data_freq)
