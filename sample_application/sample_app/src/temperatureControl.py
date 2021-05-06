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



def temperatureControl():
    filled = False
    #bus_temp_topicName = platform_libfile.getSensorData(sys.argv[1],0)
    bus_temp_topicName = 'bus_temp'

    #bus_biometric_topicName = platform_libfile.getSensorData(sys.argv[2],0)
    bus_biometric_topicName = 'bus_bio'

    #temp_Control_topic_name = platform_libfile.setSensorData(sys.argv[1],0)
    temp_Control_topic_name = 'temp_cont_bus1'

    consumer_bus_bio = KafkaConsumer(bus_biometric_topicName,bootstrap_servers=kafka_address,auto_offset_reset = "latest")
    consumer_bus_temp = KafkaConsumer(bus_temp_topicName,bootstrap_servers=kafka_address,auto_offset_reset = "latest")

    for msg_temp in consumer_bus_temp:
        if(filled):
            temp = float(msg_temp.value.decode('utf-8'))
            print(temp)
            if(temp > 20 ):
                print("switching on AC ")
                producer.send(temp_Control_topic_name, '1')
        else: 
            print("waiting for passenger ")
            for msg_bio in consumer_bus_bio:
                print("passenger embarked ")
                filled = True
                break
            






temperatureControl()