import json
from time import sleep
from datetime import datetime
from kafka import KafkaProducer
import numpy as np

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

    producer.send('watch', key=bytes("1234".encode("utf-8")), value=bytes(json.dumps(json_msg).encode("utf-8")))
    print("sending data to kafka")

    sleep(0.02)
