ssh IAS-Node-2@52.146.1.189 sudo docker build -t plat-man -f /home/IAS-Node-2/iot_platform/platformManager_docker/dockerfile /home/IAS-Node-2/iot_platform/platformManager_docker/

ssh IAS-Node-2@52.146.1.189 sudo docker run -p 5001:5001 -e KAFKA_ADDRESS="52.146.2.26" plat-man
