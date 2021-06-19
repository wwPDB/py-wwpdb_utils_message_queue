#!/bin/sh

docker-compose up -d
# give change to complete
sleep 10
export RBMQUP=YES
# Cannot run in parallel - single queue
tox -epy39,py27
docker-compose down
