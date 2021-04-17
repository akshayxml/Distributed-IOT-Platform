#platform manager
ssh IAS-Node-2@52.146.1.189 sudo docker build -t plat-man -f /home/IAS-Node-2/iot_platform/platformManager_docker/dockerfile /home/IAS-Node-2/iot_platform/platformManager_docker/

ssh IAS-Node-2@52.146.1.189 sudo docker run -d --name platform-manager -p 5001:5001 -e KAFKA_ADDRESS="52.146.2.26:9092" plat-man &

#sensor type registration 
ssh IAS-Node-2@52.146.1.189 sudo docker build -t sensor-type -f /home/IAS-Node-2/iot_platform/Sensor_Management_type_docker/dockerfile /home/IAS-Node-2/iot_platform/Sensor_Management_type_docker/

ssh IAS-Node-2@52.146.1.189 sudo docker run -d --name sensor-type -e KAFKA_ADDRESS="52.146.2.26:9092" sensor-type &

#sensor instance registration
ssh IAS-Node-2@52.146.1.189 sudo docker build -t sensor-instance -f /home/IAS-Node-2/iot_platform/Sensor_Management_instance_docker/dockerfile /home/IAS-Node-2/iot_platform/Sensor_Management_instance_docker/

ssh IAS-Node-2@52.146.1.189 sudo docker run -d --name sensor-instance -e KAFKA_ADDRESS="52.146.2.26:9092" sensor-instance &


#scheduler
ssh IAS-Node-2@52.146.1.189 sudo docker build -t scheduler -f /home/IAS-Node-2/iot_platform/scheduler_docker/dockerfile /home/IAS-Node-2/iot_platform/scheduler_docker/

ssh IAS-Node-2@52.146.1.189 sudo docker run -d --name scheduler -e KAFKA_ADDRESS="52.146.2.26:9092" scheduler &
