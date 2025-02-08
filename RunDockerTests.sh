#!/bin/sh

(cd wwpdb/mock-data/da_top/rbmq; ./cleanup.sh ; ./generate_server_certs.sh ; ./generate_client_keys.sh)
docker compose up -d
# give change to complete
sleep 10
export RBMQUP=YES
# Cannot run in parallel - single queue
tox -epy39
docker compose down
