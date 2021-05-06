import sys
import platform_libfile
import time
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka import KafkaConsumer
import os

kafka_address = os.environ['KAFKA_ADDRESS']
#gps,bio ; gps,bio ; gps,bio; gps -> of admin
def calculate_fare(): #
    #get topic names from the platform
    bus_gps_topicName = platform_libfile.getSensorData(sys.argv[1],0)
    bus_biometric_topicName = platform_libfile.getSensorData(sys.argv[2],0)
    admin_gps_topicName = platform_libfile.getSensorData(sys.argv[3],0)

    consumer_bus_gps = KafkaConsumer(bus_gps_topicName,bootstrap_servers=kafka_address,auto_offset_reset = "latest")
    consumer_bus_bio = KafkaConsumer(bus_biometric_topicName,bootstrap_servers=kafka_address,auto_offset_reset = "latest")
    consumer_admin_gps = KafkaConsumer(admin_gps_topicName,bootstrap_servers=kafka_address,auto_offset_reset = "latest")


calculate_fare()