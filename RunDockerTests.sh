#!/bin/sh

docker-compose up -d
# give change to complete
sleep 10
export RBMQUP=YES
tox -epy27
#tox -epy37
#docker-compose down
