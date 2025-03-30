#!/usr/bin/env bash
root_dir=$(cd "$(dirname "$0")"; cd ..; pwd)

container_name=my-kafka
container=$(docker ps -a | grep $container_name | awk '{print $1}')
if [ ${#container} -gt 0 ]; then
    docker rm -f $container_name
fi

cd $root_dir
docker-compose up -d
