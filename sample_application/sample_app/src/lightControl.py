import sys
import platform_libfile
import time
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka import KafkaConsumer
import os
import math

kafka_address = os.environ['KAFKA_ADDRESS']

def json_serializer(data):
    return data.encode()

producer = KafkaProducer(bootstrap_servers=[kafka_address],
                         value_serializer=json_serializer)



def lightControl():
    filled = False
    #bus_light_topicName = platform_libfile.getSensorData(sys.argv[1],0)
    bus_light_topicName = 'bus_light'

    #bus_biometric_topicName = platform_libfile.getSensorData(sys.argv[2],0)
    bus_biometric_topicName = 'bus_bio'

    #light_Control_topic_name = platform_libfile.setSensorData(sys.argv[1],0)
    light_Control_topic_name = 'light_cont_bus1'

    consumer_bus_bio = KafkaConsumer(bus_biometric_topicName,bootstrap_servers=kafka_address,auto_offset_reset = "latest")
    consumer_bus_light = KafkaConsumer(bus_light_topicName,bootstrap_servers=kafka_address,auto_offset_reset = "latest")

    for msg_light in consumer_bus_light:
        if(filled):
            light = float(msg_light.value.decode('utf-8'))
            print(light)
            if(light < 40 ):
                print("switching on Lights ")
                producer.send(light_Control_topic_name, '1')
        else: 
            print("waiting for passenger ")
            for msg_bio in consumer_bus_bio:
                print("passenger embarked ")
                filled = True
                break
            

lightControl()