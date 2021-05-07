from flask import Flask, render_template, request
#from werkzeug import secure_filename
from bson.objectid import ObjectId
import os
import pymongo
import json
import bson
import base64
from flask import Response
from bson.binary import Binary
from pymongo import MongoClient
import validator
from kafka import KafkaConsumer
from kafkaConnector import kafkaConnector

app = Flask(__name__)
kafka = kafkaConnector()
kafka_address = os.environ['KAFKA_ADDRESS']

bus_1_dashboard_consumer = KafkaConsumer("bus_1", bootstrap_servers=kafka_address, auto_offset_reset='earliest', group_id='consumer-group-bus_1')
bus_2_dashboard_consumer = KafkaConsumer("bus_2", bootstrap_servers=kafka_address, auto_offset_reset='earliest', group_id='consumer-group-bus_2')
bus_3_dashboard_consumer = KafkaConsumer("bus_3", bootstrap_servers=kafka_address, auto_offset_reset='earliest', group_id='consumer-group-bus_3')
bus_4_dashboard_consumer = KafkaConsumer("bus_4", bootstrap_servers=kafka_address, auto_offset_reset='earliest', group_id='consumer-group-bus_4')

def gettopic(instance_id,index):
    #instance_id
    #index=0
    #o=ObjectId("6068d0bac3adf59832fa20b7")
    o = ObjectId(instance_id)
    print (o)
    cluster = MongoClient(
        "mongodb+srv://shweta_10:shweta10@cluster0.bh25q.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")

    #fetch sensor id from sensor_map
    db = cluster["binding_db"]
    #collection = db["sensor_map"]
    #db.sensor_instance.find({"location":"Indore"} )
    myCursor = db.sensor_map.find({"_id":o} )
    sensor_id = None
    for car in myCursor:
        sensor_id=car[str(index)]
        print("sensor id :",sensor_id)


    #fetch topic from sensor instances
    db = cluster["sensor_registory"]
    #collection = db["sensor_instance"]
    myCursor1 = db.sensor_instance.find({"_id": sensor_id})
    for car in myCursor1:
        topic_name=car["topic"]
        print("topic name :",topic_name)
        return (topic_name)

def getControltopic(instance_id,index):
    #instance_id
    #index=0
    #o=ObjectId("6068d0bac3adf59832fa20b7")
    o = ObjectId(instance_id)
    print (o)
    cluster = MongoClient(
        "mongodb+srv://shweta_10:shweta10@cluster0.bh25q.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")

    #fetch sensor id from sensor_map
    db = cluster["binding_db"]
    #collection = db["sensor_map"]
    #db.sensor_instance.find({"location":"Indore"} )
    myCursor = db.sensor_map.find({"_id":o} )
    sensor_id = None
    for car in myCursor:
        sensor_id=car[str(index)]
        print("sensor id :",sensor_id)


    #fetch topic from sensor instances
    db = cluster["sensor_registory"]
    #collection = db["sensor_instance"]
    myCursor1 = db.sensor_instance.find({"_id": sensor_id})
    for car in myCursor1:
        topic_name=car["topic_control"]
        print("topic name :",topic_name)
        return (topic_name)

@app.route('/')
def index():
   return render_template('index.html')

@app.route('/dashboard', methods=["GET"])
def dashboard():
    return render_template('dashboard.html')

@app.route('/dashboard/bus_<string:bus_id>', methods=["GET"])
def bus_dashboard(bus_id):    
	return render_template('bus_dashboard.html', bus_id = bus_id, col1 = ['Fare Details'], col2 = ['Other Details'])

@app.route('/dashboard/bus_refresh_<string:bus_id>', methods=["GET"])
def bus_dashboard_refresher(bus_id):    

	req_dashboard = bus_1_dashboard_consumer
	
	if(bus_id == '2'):
		req_dashboard = bus_2_dashboard_consumer
	elif(bus_id == '3'):
		req_dashboard = bus_3_dashboard_consumer
	elif(bus_id == '4'):
		req_dashboard = bus_4_dashboard_consumer
	
	for msg in req_dashboard:
		vals = json.loads(str(msg.value.decode()))
		col1 = []
		if('fare' in vals and vals['fare'] != ''):
			col1 = [vals['fare']]
		col2 = [v for (k,v) in vals.items() if v != '' if k != 'fare']
		return render_template('bus_dashboard.html', col1=col1, col2=col2)
	 
@app.route('/getSensorTopic', methods=["POST"])
def getSensorTopic():
    #content  = request.get_json(force=True)
    content = request.json
    instance_id=content["id"]
    index=content["index"]
    return {"data": gettopic(instance_id,index)}

@app.route('/setSensorTopic', methods=["POST"])
def setSensorTopic():
    #content  = request.get_json(force=True)
    content = request.json
    instance_id=content["id"]
    index=content["index"]
    return {"data": getControltopic(instance_id,index)}
    
@app.route('/', methods=['POST'])
def uploade_file():
    uploaded_file = request.files['file']
    if uploaded_file.filename != '':
        uploaded_file.save(uploaded_file.filename)
    #return redirect(url_for('index'))
    return "file uploadeed"

@app.route('/uploadAppZip', methods = ['GET', 'POST'])
def upload_file():
   if request.method == 'POST':
      f=request.files['file']
      print(f.name)
      f = request.files['file']
      f.save("app.zip")
      # validation 
      valid = validator.validate_appzip('./app.zip')
      if(valid == -1):
         return 'app.zip is not formatted properly please check'
      else:
         with open("app.zip","rb") as f:
            encoded = Binary(f.read())
         collection.insert_one({"Application id ": 1,"folder":encoded})
         return 'file uploaded successfully'

@app.route('/uploadDeployConfig', methods = ['GET', 'POST'])
def uploadDeploy_file():
   if request.method == 'POST':
      f = request.files['file']
      f.save('deployConfig.json')
      valid = validator.validate_deployConfig('./deployConfig.json')
      valid=1
      if(valid==-1):
         return 'error in deploy file'
      else:
         with open('./deployConfig.json') as f:
            data = json.load(f)
            kafka.sendJsonData('pm_to_sensor_binder',data)
         return 'file uploaded successfully'

@app.route('/uploadSensorType', methods = ['GET', 'POST'])
def uploadSensorType_file():
   if request.method == 'POST':
      f = request.files['file']
      f.save('sensorTypeRegistration.json')
      valid = validator.validate_sensor_type('./sensorTypeRegistration.json')
      if(valid==-1):
         return 'error in sensor type registration file'
      else:
         with open('./sensorTypeRegistration.json') as f:
            data = json.load(f)
            kafka.sendJsonData('pm_to_sensor_type_reg',data)
         return 'file uploaded successfully'

@app.route('/uploadSensorInstance', methods = ['GET', 'POST'])
def uploadSensorInstance_file():
   if request.method == 'POST':
      f = request.files['file']
      f.save('sensorInstance.json')
      valid = validator.validate_sensor_instance('./sensorInstance.json')
      if(valid==-1):
         return 'error in sensor instance file'
      else:
         with open('./sensorInstance.json') as f:
            data = json.load(f)
            kafka.sendJsonData('pm_to_sensor_ins_reg',data)
         return 'file uploaded successfully'
		
if __name__ == '__main__':
   cluster = MongoClient("mongodb+srv://akshay:akshay123@cluster0.bh25q.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
   db = cluster["AppRepo"]
   collection = db["ID_sourceFolder"]
   

   app.run(host='0.0.0.0', port=5001,debug=True)
