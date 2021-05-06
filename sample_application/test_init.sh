#!/bin/bash

kafka_address="52.146.2.26:9092"
node2_address=IAS-Node-2@52.146.1.189

#platform manager
ssh $node2_address sudo docker build -t plat-man -f /home/IAS-Node-2/iot_platform/platformManager_docker/dockerfile /home/IAS-Node-2/iot_platform/platformManager_docker/