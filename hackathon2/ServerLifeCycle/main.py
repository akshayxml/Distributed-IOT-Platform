# from server_lifecycle import create_new_machine, start_machine
# import os


# create_new_machine("127.0.0.1","8090","0","3")
# # create_new_machine("127.0.0.1","5555","0","5")
# create_new_machine("127.0.0.1","9010","0","1")
# # start_machine("machine_127.0.0.1_5555",5)
# start_machine("machine_127.0.0.1_9010",1)
# start_machine("machine_127.0.0.1_8090",3)
from server_lifecycle import create_new_machine, start_machine
import os
import sys

while(1):
    
    cmd = input().split(' ')

    id = cmd[1]
    ip = cmd[2]
    port = cmd[3]
    isActive = 0
    topic = str(id)
    consume_topic = "consume"+ str(id)

    create_new_machine(id,ip,port,isActive,topic,consume_topic)
    machine_name = 'machine_' + ip + '_' + port
    start_machine(machine_name,topic)

