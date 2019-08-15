#!/bin/sh

docker-compose up -d
# give change to complete
sleep 10
export RBMQUP=YES
# Cannot run in parallel - single queue
tox 
docker-compose down
