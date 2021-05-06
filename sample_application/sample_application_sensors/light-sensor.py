import random
import sys
from faker import Faker
from kafka import KafkaProducer
import json
import time
import threading
from kafka import KafkaConsumer


def json_serializer(data):
    return data.encode()


def get_partition(key, all, available):
    return 0


#fake = Faker()
def create_lux():
    return int(random.uniform(20,500))

def get_data():
    return {
        "light": create_lux()
    }


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=json_serializer)
control_topic = sys.argv[2]


# control function
def set_data(data):
    if (data < 50):
        print('Lights ON')
    # elif (data > 50):
    #     print('Lights Off')
    # else:
    #     print('Invalid Input')


def consumer_thread():
    consumer = KafkaConsumer(control_topic,
                             bootstrap_servers='localhost:9092',
                             auto_offset_reset='earliest',
                             group_id='consumer-group-a')

    for msg in consumer:
        set_data(int(msg.value.decode()))


if __name__ == '__main__':
    topic_name = sys.argv[1]
    threading.Thread(target=consumer_thread, args=()).start()
    placeholder = sys.argv[3]

    while 1 == 1:
        registered_user = get_data()
        # print(registered_user["humidity"])
        print("light")
        print(str(registered_user["light"]))
        producer.send(topic_name, str(registered_user["light"]))
        time.sleep(5)