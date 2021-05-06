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

def getCoordinates(gps):
    _,bus_id,x,y = gps.split(":")
    return tuple([float(x),float(y)]),bus_id

def lightControl():
    filled = False
    #bus_light_topicName = platform_libfile.getSensorData(sys.argv[1],0)
    bus_light_topicName = 'bus_light'

    #bus_gps_topicName = platform_libfile.getSensorData(sys.argv[2],0)
    bus_gps_topicName = 'bus_gps'

    #light_Control_topic_name = platform_libfile.setSensorData(sys.argv[1],0)
    light_Control_topic_name = 'light_cont_bus1'

    consumer_bus_gps = KafkaConsumer(bus_gps_topicName,bootstrap_servers=kafka_address,auto_offset_reset = "latest")
    consumer_bus_light = KafkaConsumer(bus_light_topicName,bootstrap_servers=kafka_address,auto_offset_reset = "latest")

    for msg in consumer_bus_gps:
        _,bus_id = getCoordinates(msg.value.decode())
        break

    for msg_light in consumer_bus_light:
        if(filled):
            light = float(msg_light.value.decode('utf-8'))
            print(light)
            if(light < 40 ):
                print("switching on Lights ")
                producer.send(light_Control_topic_name, '1')
                dahsboardMsg = json.dumps({"Light": 'Switch on light as lux is {}'.format(light)})
                #print("topic name : ",'bus_'+bus_id)
                producer.send('bus_'+bus_id,dahsboardMsg) 
        else: 
            print("waiting for passenger ")
            for msg_bio in consumer_bus_bio:
                print("passenger embarked ")
                filled = True
                break
            

lightControl()