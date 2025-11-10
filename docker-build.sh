#!/bin/bash

docker build -t jiandaoyundatasyncsystem:2.0.0 -f Dockerfile .

docker save -o jiandaoyundatasyncsystem_2.0.0.tar jiandaoyundatasyncsystem:2.0.0

tar -zcvf jiandaoyundatasyncsystem_2.0.0.tar.gz jiandaoyundatasyncsystem_2.0.0.tar && rm -f jiandaoyundatasyncsystem_2.0.0.tar

#tar -zxvf jiandaoyundatasyncsystem_2.0.0.tar.gz
#docker load -i jiandaoyundatasyncsystem_2.0.0.tar

echo "Y" | docker system prune