#!/usr/bin/python3
from kafka import KafkaConsumer
from kafka import KafkaProducer
import threading
import json
import os
 
 
class Deployer:
    def __init__(self, deploy_config_file):
        self.deploy_config_file = deploy_config_file
        self.files = {"deploy_config_file":deploy_config_file}

    def json_serializer(self,data):
        return json.dumps(data).encode('utf-8')

    def create_docker_file(self):
        # lang = self.deploy_config_file["environment"]["lang"] ## NOT USED .
        script = self.deploy_config_file["script_name"]
        instance_id = self.deploy_config_file["instance_id"]
        # docker_file = open("dockerfile","w")
        # lang = self.deploy_config_file["environment"]["lang"] ## NOT USED .
        # script = self.deploy_config_file["script_name"]

        # docker_file.write("FROM python:3\n") ## "FROM " + lang
        # docker_file.write("COPY requirements.txt ./\n")
        # docker_file.write("RUN pip install --upgrade pip\n")
        # docker_file.write("pip install --no-cache-dir -r requirements.txt\n")
        # docker_file.write("ADD "+script+"\n")
        # docker_file.write("CMD [\"python\", \"-u\"," + "\"{}\"".format(script)+ "]")
        # docker_file.close()
        script_paths = self.deploy_config_file["script_file_path"]
        add_script_str = ""
        for file_path in script_paths:
            file_name = os.path.basename(file_path)
            add_script_str += "ADD {} .\n".format(file_name)
            

        # docker_file = "FROM python:3\n" + "COPY requirements.txt ./\n" + "RUN pip install --upgrade pip\n" + "RUN pip install --no-cache-dir -r requirements.txt\n" + "ADD "+script+" .\n" + "CMD [\"python\", \"-u\"," + "\"{}\",".format(script)+  "\"{}\"".format(instance_id) + "]"
        docker_file = "FROM python:3\n" + "COPY requirements.txt ./\n" + "RUN pip install --upgrade pip\n" + "RUN pip install --no-cache-dir -r requirements.txt\n" + add_script_str + "CMD [\"python\", \"-u\"," + "\"{}\",".format(script)+  "\"{}\"".format(instance_id) + "]"
         
        self.files["docker_file"] = docker_file


    def create_req_file(self):
        # req_file = open("requirements.txt","w")
        req_file = ""

        dependencies = self.deploy_config_file["environment"]["dependencies"]

        for dependency in dependencies:
            if(dependency[1]!=""):
                req_file += dependency[0]+"=="+dependency[1]+"\n"
            else:
                req_file += dependency[0]+"\n"
        self.files["req_file"] = req_file

    def create_files(self):
        self.create_req_file()
        self.create_docker_file()
        producer = KafkaProducer(bootstrap_servers=['52.146.2.26:9092'],
                        value_serializer=self.json_serializer)
        producer.send('deployer_to_slc',self.files)



if __name__=='__main__':
   consumer = KafkaConsumer(
       "scheduler_to_deployer",
       bootstrap_servers='52.146.2.26:9092',
       auto_offset_reset='earliest',
       group_id='consumer-group-a')
   print('starting the consumer')
   for msg in consumer:
       print("Reg user = {}".format(json.loads(msg.value)))
       deploy_config_file = json.loads(msg.value)
       dep_obj = Deployer(deploy_config_file)
       tid=threading.Thread(target=dep_obj.create_files)
       tid.start()
    # threading.Thread(target=)

# ###KAFKA###
# f = open("deployConfig.json","r")
# ##########
# json_data = f.read()
# parsed = json.loads(json_data)
# f.close()
# req_type = parsed["scheduling_info"]["request_type"]
# if(req_type=="stop"):
#     ##FORWARD THE REQUEST
#     exit(0)
# req_file = open("requirements.txt","w")

# dependencies = parsed["environment"]["dependencies"]

# for dependency in dependencies:
#     if(dependency[1]!=""):
#         req_file.write(dependency[0]+"=="+dependency[1]+"\n")
#     else:
#         req_file.write(dependency[0]+"\n")
# req_file.close()

# docker_file = open("dockerfile","w")

# lang = parsed["environment"]["lang"] ## NOT USED .
# script = parsed["script_name"]


# docker_file.write("FROM python:3\n") ## "FROM " + lang
# docker_file.write("COPY requirements.txt ./\n")
# docker_file.write("RUN pip install --upgrade pip\n")
# docker_file.write("pip install --no-cache-dir -r requirements.txt\n")
# docker_file.write("ADD "+script+"\n")
# docker_file.write("CMD [\"python\", \"-u\"," + "\"{}\"".format(script)+ "]")
# docker_file.close()

# # FROM python:3
# # COPY requirements.txt ./


# # RUN pip install --upgrade pip && \
# #  pip install --no-cache-dir -r requirements.txt
# # ADD actionmanager.py .
# # CMD ["python","-u","actionmanager.py"]

