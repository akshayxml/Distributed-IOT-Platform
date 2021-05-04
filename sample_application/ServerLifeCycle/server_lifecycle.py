import json
import threading
import socket
from server import Server
from kafka import KafkaConsumer, KafkaProducer
from json import loads
import os

sudoPassword = "nambigari95"

topics = list()
data = {}
server_status = set()

server_status.add("1")




def json_serializer(data):
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(bootstrap_servers=['52.146.2.6:9092'],
                        value_serializer=json_serializer)

def readServerInfo():

    global topics
    global data

    with open('servers.json', 'r') as serverInfo:
        data = json.load(serverInfo)

    for server in data:
        topics.append(data[server]['topic'])


def consumer_handler(server_topic):
    global server_status
    #print('consuming from: ', server_topic)
    consumer = KafkaConsumer(
        server_topic,
        bootstrap_servers=['52.146.2.26:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: loads(x.decode('utf-8')),
        consumer_timeout_ms=15000)

    for message in consumer:
        message = message.value
        print(server_topic,':',message)

    server_status.remove(server_topic)

    print('***SERVER ',server_topic,' CRASHED***')
    print("***Restarting Server:",server_topic,'***')
    consumer.close()
    # start_machine('server'+str(server_topic),server_topic)
    # filename = "machine_127.0.0.1_6566"
    tmp = data["server"+str(server_topic)]
    filename = "machine_"+tmp["ip"]+"_"+tmp["port"]
    start_machine(filename, server_topic)

    

def create_new_machine(id,ip,port,status,topic,consume_topic):
    
    x = ''

    print('creating new machine')

    global topics

    machine_name = 'machine_' + ip + '_' + port

    with open(machine_name+'.py','w') as machine:
        machine.write("from server import Server\n")
        machine.write("import json\n")
        machine_instance = 'Server(' + '\"' + str(machine_name) +'\",\"' + str(ip) + '\",\"' + str(port) + '\",\"' + str(status) + '\",\"' + str(topic) +'\"'+ ',\"' + str(consume_topic) +'\"' +')'
        machine.write("machine_instance = " + machine_instance)
    # updating in json

    name = "server" + str(id)


    y = {name:{"id":id, "ip":ip, "port":port , "isActive": 0 ,"topic":topic ,"consume_topic":consume_topic}}
    try:
        with open('servers.json', 'r') as serverInfo:
            x = json.load(serverInfo)
            x.update(y)
            topics.append(consume_topic)
            print('topics:',topics)
    except:
        with open('servers.json', 'w') as serverInfo:
            x = json.load(serverInfo)
            x.update(y)
            topics.append(consume_topic)
            print('topics:',topics)
        
    with open('servers.json','w') as f:
        json.dump(x, f)


#### check
def start_machine(machine_name,topic):
    os.system("gnome-terminal -e \"python3 " + machine_name + ".py\" &")

def sendRequestToServer(deploy_request, b = 'default', c = 'default'):
    print("----")
    print(b)
    print(c)
    print(deploy_request)

    req_file = deploy_request['req_file']
    docker_file = deploy_request['docker_file']

    with open("requirements.txt", "w") as fw:
        fw.write(req_file)

    with open("Dockerfile", "w") as fw:
        fw.write(docker_file)

    deploy_config_file = deploy_request['deploy_config_file']
    instance_id = deploy_config_file['instance_id']
    script_paths = deploy_config_file['script_file_path']
    print(script_paths)
    for file_path in script_paths:
        os.system('cp {} .'.format(file_path))
    print("All files copied")
    # command = "sudo docker image rm {}".format(instance_id)

    # os.system('echo %s|sudo -S %s' % (sudoPassword, command))
    command = "sudo docker build -t {} .".format(instance_id)
    os.system('echo %s|sudo -S %s' % (sudoPassword, command))
    command = "sudo docker run --net=host {}".format(instance_id)
    os.system('echo %s|sudo -S %s' % (sudoPassword, command))

    # server_no = "server1" ## get this from the load balancer
    # with open('servers.json', 'r') as serverInfo:
    #     json_data = json.load(serverInfo)

    # server_consumer_topic = json_data[server_no]['consume_topic']
    # print(server_consumer_topic)
    # producer.send(server_consumer_topic, deploy_request)

def check_server_status():

    global topics

    readServerInfo()

    while(len(topics) == 0):
        continue
    
    #print(topics)

    for i in topics:
        
        if i in server_status:
            continue
        
        server_status.add(i)
        print('creating thread:',i)
        consumer_thread = threading.Thread(target=consumer_handler,args=(i,))
        consumer_thread.start()


def check():
    while(1):
        check_server_status()

def deployer_request():

    deployer_to_slc_consumer = KafkaConsumer(
       "deployer_to_slc",
       bootstrap_servers='52.146.2.26:9092',
       auto_offset_reset='earliest',
       group_id='consumer-group-a')
    
    for msg in deployer_to_slc_consumer:
        deploy_request = json.loads(msg.value)
        print("----")
        print(type(deploy_request))
        print(deploy_request)
        th = threading.Thread(target=sendRequestToServer, args=(deploy_request, ))
        th.start()

def main():
    status = threading.Thread(target=check,args=())
    status.start()

    deploy = threading.Thread(target=deployer_request,args=())
    deploy.start()


if __name__ == "__main__":
   # stuff only to run when not called via 'import' here
    main()

