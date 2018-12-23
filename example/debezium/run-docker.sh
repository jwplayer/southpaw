#!/bin/bash

# Enable job control
set -o monitor

HOSTNAME="${HOSTNAME:-localhost}"

# Start in bg
/docker-entrypoint.sh start &

# Poll for debezium API to be up
while [[ $(curl --silent --output /dev/null --header "Accept:application/json" --write-out ''%{http_code}'' $HOSTNAME:8083/) != 200 ]]; do
  echo "Waiting for debezium to be ready";
  sleep 1;
done

# Delete each existing connector configuration from kafka connect cluster
echo "Delete all existing connectors"
curl -X GET -H "Accept:application/json" -H "Content-Type:application/json" ${HOSTNAME}:8083/connectors/ | \
    jq '.[]' | \
    xargs -L1 -I'{}' curl -i -X DELETE -H "Accept:application/json" -H "Content-Type:application/json" ${HOSTNAME}:8083/connectors/{}

# Register/overwrite database connector configuration
CONNECTOR_CFG=register-debezium.json
while [[ $(curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://$HOSTNAME:8083/connectors -d @$CONNECTOR_CFG) == 409 ]]; do
  echo "Failed to upload connector configuration because rebalance is in progress; retrying";
  sleep 1;
done

# Bring debezium to fg
fg
