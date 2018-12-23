#! /bin/sh -e

: ${HEAP_OPTS:="-Xms500m -Xmx500m"}

if [[ "x$SOUTHPAW_CONFIG" = "x" ]]; then
    echo "The SOUTHPAW_CONFIG variable must be set"
    exit 1
fi

if [[ "x$SOUTHPAW_RELATIONS_FILE" = "x" ]]; then
    echo "The SOUTHPAW_RELATIONS_FILE variable must be set"
    exit 1
fi

: ${SOUTHPAW_RUN_OPTS:="--restore --build"}
export SOUTHPAW_OPTS="--config ${SOUTHPAW_CONFIG} --relations ${SOUTHPAW_RELATIONS_FILE} ${SOUTHPAW_RUN_OPTS}"


# Setup JMX settings
: ${JMX_PORT:-9010}
: ${JMX_AUTH:="false"}
: ${JMX_SSL:="false"}

if [[ -n "$JMX_HOSTNAME" ]]; then
    echo "Enabling JMX on ${JMX_HOSTNAME}:${JMX_PORT}"
    export JMX_OPTS="-Djava.rmi.server.hostname=${JMX_HOSTNAME} -Dcom.sun.management.jmxremote.rmi.port=${JMX_PORT} -Dcom.sun.management.jmxremote.port=${JMX_PORT} -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=${JMX_AUTH} -Dcom.sun.management.jmxremote.ssl=${JMX_SSL} "
fi

# Create the default directory (TODO: fix)
mkdir -p /tmp/RocksDB

java \
    ${JMX_OPTS} \
    ${HEAP_OPTS} \
    ${SOUTHPAW_LOG4J_OPTS} \
    -XX:+UnlockExperimentalVMOptions \
    -XX:+UseCGroupMemoryLimitForHeap \
    -cp "${SOUTHPAW_CLASS_PATH}/*" com.jwplayer.southpaw.Southpaw \
    ${SOUTHPAW_OPTS}
