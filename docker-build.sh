#!/bin/bash

echo "Building jiandaoyundatasyncsystem docker image..."
docker build -t jiandaoyundatasyncsystem:2.1.1 -f Dockerfile .
docker save -o jiandaoyundatasyncsystem_2.1.1.tar jiandaoyundatasyncsystem:2.1.1
tar -zcvf jiandaoyundatasyncsystem_2.1.1.tar.gz jiandaoyundatasyncsystem_2.1.1.tar && rm -f jiandaoyundatasyncsystem_2.1.1.tar
echo "Building jiandaoyundatasyncsystem docker image done"

## 解压和加载
#tar -zxvf jiandaoyundatasyncsystem_2.1.1.tar.gz && rm -f jiandaoyundatasyncsystem_2.1.1.tar.gz
#docker load -i jiandaoyundatasyncsystem_2.1.1.tar

echo "Pull mysql docker image..."
docker pull mysql:8.0
docker save -o mysql_8.0.tar mysql:8.0
tar -zcvf mysql_8.0.tar.gz mysql_8.0.tar && rm -f mysql_8.0.tar
echo "Pull mysql docker image done"

## 解压和加载
#tar -zxvf mysql_8.0.tar.gz && rm -f mysql_8.0.tar.gz
#docker load -i mysql_8.0.tar

echo "Clean docker..."
echo "Y" | docker system prune
echo "Clean docker done"