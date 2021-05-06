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
    _,_,x,y = gps.split(":")
    return tuple([float(x),float(y)])

def getDistance(a,b):
    return math.sqrt((a[0]-b[0])**2 + (a[1] - b[1])**2)

#gps,bio ; gps,bio ; gps,bio; gps -> of admin
def calculate_fare(): #
    #get topic names from the platform

    #bus_gps_topicName = platform_libfile.getSensorData(sys.argv[1],0)
    bus_gps_topicName = 'bus_gps'

    #bus_biometric_topicName = platform_libfile.getSensorData(sys.argv[2],0)
    bus_biometric_topicName = 'bus_bio'

    #admin_gps_topicName = platform_libfile.getSensorData(sys.argv[3],0)
    admin_gps_topicName = 'admin_gps'

    consumer_bus_gps = KafkaConsumer(bus_gps_topicName,bootstrap_servers=kafka_address,auto_offset_reset = "latest")
    consumer_bus_bio = KafkaConsumer(bus_biometric_topicName,bootstrap_servers=kafka_address,auto_offset_reset = "latest")
    consumer_admin_gps = KafkaConsumer(admin_gps_topicName,bootstrap_servers=kafka_address,auto_offset_reset = "latest")


    for msg in consumer_bus_bio:
        id = msg.value.decode('utf-8')
        #print(id)
        admin_gps = None
        bus_gps = None
        for msg_admin in consumer_admin_gps:
            admin_gps = msg_admin.value.decode('utf-8')
            break
        admin_coord = getCoordinates(admin_gps)
        #print('admin location is : ',admin_coord)
        for msg_bus in consumer_bus_gps:
            bus_gps = msg_bus.value.decode('utf-8')
            break
        bus_coord = getCoordinates(bus_gps)
        #print('bus location is : ', bus_coord)
        fare = max(5,int(getDistance(bus_coord,admin_coord)*1.5))
        print('passenger : ',id,' has to pay fare of',fare )
        
        producer.send('display_fareDetails_1', str(id)+':'+str(fare)) #sending passenger id:fare


calculate_fare()