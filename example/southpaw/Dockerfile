# Southpaw build layer
FROM maven:3.6-jdk-8-alpine AS build

ARG BRANCH=master
ARG SRC_DIR=/usr/src/southpaw

RUN apk add --no-cache \
        git \
        libc6-compat \
    && mkdir -p $SRC_DIR \
    && git clone https://github.com/jwplayer/southpaw.git $SRC_DIR \
    && cd $SRC_DIR \
    && git checkout $BRANCH \
    && git pull origin $BRANCH \
    && mkdir -p /tmp/RocksDB \
    && mvn clean package

# Southpaw image layer
FROM openjdk:8-jdk-alpine

# System dependencies
RUN apk add --no-cache libc6-compat

ARG SRC_DIR=/usr/src/southpaw

ENV LC_ALL=en_US.utf-8
ENV SOUTHPAW_HOME /southpaw
ENV SOUTHPAW_CLASS_PATH /etc/southpaw/jars

WORKDIR $SOUTHPAW_HOME

COPY --from=build $SRC_DIR/target/southpaw.jar $SOUTHPAW_CLASS_PATH/southpaw.jar

# TODO: move to child Dockerfile that inherits from above
COPY conf/ $SOUTHPAW_HOME/conf/
COPY relations/ $SOUTHPAW_HOME/relations/

ENV SOUTHPAW_CONFIG=$SOUTHPAW_HOME/conf/example.yaml
ENV SOUTHPAW_RELATIONS_FILE=$SOUTHPAW_HOME/relations/relations.json

RUN mkdir -p /tmp/RocksDB

CMD java -XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap \
  -cp "${SOUTHPAW_CLASS_PATH}/*" com.jwplayer.southpaw.Southpaw \
  --config ${SOUTHPAW_CONFIG} \
  --relations ${SOUTHPAW_RELATIONS_FILE} \
  --build
