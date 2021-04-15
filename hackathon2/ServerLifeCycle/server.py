import threading
import socket
from kafka import KafkaProducer,KafkaConsumer
import time
import os
import json


def kafka_producer(id,ip,port,isActive,topic):
    producer = KafkaProducer(bootstrap_servers = '52.146.2.26:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    while(1):
        #print('server')
        #print('generating:',id)
        data = {'number' : id ,'ip': ip} 
        producer.send(topic,value=data)
        print(id)
        time.sleep(5)
# def listen_request(clientSocket):

# def deployFile(..):

def deployApp(deploy_request):
    req_file = deploy_request['req_file']
    docker_file = deploy_request['docker_file']
    deploy_config_file = deploy_request['deploy_config_file']
    instance_id = deploy_config_file['instance_id']
    script_path = deploy_config_file['script_file_path']
    os.system('cp {} .'.format(script_path))
    os.system("sudo docker image rm {}".format(instance_id))
    os.system("sudo docker build -t {} .".format(instance_id))
    os.system('sudo docker run {}'.format(instance_id))

def consumer(consumer_topic):
    consumer = KafkaConsumer(
       consumer_topic,
       bootstrap_servers='52.146.2.6:9092',
       auto_offset_reset='earliest',
       group_id='consumer-group-a')
    for msg in consumer:
        request = json.loads(msg.value)
        th = threading.Thread(target=deployApp, args=(request))
        th.start()

class Server:

    def __init__(self,id,ip,port,status,topic,consumer_topic): # port is string
        self.id = id
        self.ip = ip
        self.port = int(port)
        self.isActive = status
        self.topic = topic
        self.consumer_topic = consumer_topic
        # self.create_socket()
        kafka_thread = threading.Thread(target=kafka_producer,args=(self.id,self.ip,self.port,self.isActive,self.topic))
        kafka_thread.start() 

        consumer_thread = threading.Thread(target=consumer, args=(self.consumer_topic,))
        consumer_thread.start()

