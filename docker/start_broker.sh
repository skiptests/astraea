#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

declare -r DOCKER_FOLDER=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
source $DOCKER_FOLDER/docker_build_common.sh

# ===============================[global variables]===============================
declare -r VERSION=${REVISION:-${VERSION:-3.2.0}}
declare -r DOCKERFILE=$DOCKER_FOLDER/broker.dockerfile
declare -r CONFLUENT_BROKER=${CONFLUENT_BROKER:-false}
declare -r CONFLUENT_VERSION=${CONFLUENT_VERSION:-7.0.1}
declare -r DATA_FOLDER_IN_CONTAINER_PREFIX="/tmp/log-folder"
declare -r EXPORTER_VERSION="0.16.1"
declare -r EXPORTER_PORT=${EXPORTER_PORT:-"$(getRandomPort)"}
declare -r BROKER_PORT=${BROKER_PORT:-"$(getRandomPort)"}
declare -r CONTAINER_NAME="broker-$BROKER_PORT"
declare -r BROKER_JMX_PORT="${BROKER_JMX_PORT:-"$(getRandomPort)"}"
declare -r ADMIN_NAME="admin"
declare -r ADMIN_PASSWORD="admin-secret"
declare -r USER_NAME="user"
declare -r USER_PASSWORD="user-secret"
declare -r JMX_CONFIG_FILE="${JMX_CONFIG_FILE}"
declare -r JMX_CONFIG_FILE_IN_CONTAINER_PATH="/opt/jmx_exporter/jmx_exporter_config.yml"
declare -r JMX_OPTS="-Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -Dcom.sun.management.jmxremote.port=$BROKER_JMX_PORT \
  -Dcom.sun.management.jmxremote.rmi.port=$BROKER_JMX_PORT \
  -Djava.rmi.server.hostname=$ADDRESS"
declare -r ZOOKEEPER_CONNECT=$(echo $1 | sed -n 's/^zookeeper.connect=\(.\+\)$/\1/p')
declare -r HEAP_OPTS="${HEAP_OPTS:-"-Xmx2G -Xms2G"}"
declare -r BROKER_PROPERTIES="/tmp/server-${BROKER_PORT}.properties"
if [[ "$CONFLUENT_BROKER" = "true" ]]; then
    declare -r REPO=${REPO:-ghcr.io/skiptests/astraea/confluent.broker}
    declare -r IMAGE_NAME="$REPO:$CONFLUENT_VERSION"
    declare -r SCRIPT_LOCATION_IN_CONTAINER="./bin/kafka-server-start"
else
    declare -r REPO=${REPO:-ghcr.io/skiptests/astraea/broker}
    declare -r IMAGE_NAME="$REPO:$VERSION"
    declare -r SCRIPT_LOCATION_IN_CONTAINER="./bin/kafka-server-start.sh"
fi
# cleanup the file if it is existent
[[ -f "$BROKER_PROPERTIES" ]] && rm -f "$BROKER_PROPERTIES"

# ===================================[functions]===================================

function showHelp() {
  echo "Usage: [ENV] start_broker.sh [ ARGUMENTS ]"
  echo "Required Argument: "
  echo "    zookeeper.connect=node:22222             set zookeeper connection"
  echo "Optional Arguments: "
  echo "    num.io.threads=10                        set broker I/O threads"
  echo "    num.network.threads=10                   set broker network threads"
  echo "ENV: "
  echo "    REPO=astraea/broker                      set the docker repo"
  echo "    HEAP_OPTS=\"-Xmx2G -Xms2G\"                set broker JVM memory"
  echo "    REVISION=trunk                           set revision of kafka source code to build container"
  echo "    VERSION=3.2.0                            set version of kafka distribution"
  echo "    BUILD=false                              set true if you want to build image locally"
  echo "    RUN=false                                set false if you want to build/pull image only"
  echo "    DATA_FOLDERS=/tmp/folder1                set host folders used by broker"
}

function rejectProperty() {
  local key=$1
  if [[ -f "$BROKER_PROPERTIES" ]] && [[ "$(cat $BROKER_PROPERTIES | grep $key)" != "" ]]; then
    echo "$key is NOT configurable"
    exit 2
  fi
}

function requireProperty() {
  local key=$1
  if [[ ! -f "$BROKER_PROPERTIES" ]] || [[ "$(cat $BROKER_PROPERTIES | grep $key)" == "" ]]; then
    echo "$key is required"
    exit 2
  fi
}

function generateConfluentDockerfile() {
  echo "# this dockerfile is generated dynamically
FROM confluentinc/cp-server:$CONFLUENT_VERSION
USER root
RUN usermod -l $USER appuser && groupadd $USER && usermod -a -G $USER $USER
RUN yum -y update && yum -y install git unzip

# download jmx exporter
RUN mkdir /opt/jmx_exporter
WORKDIR /opt/jmx_exporter
RUN wget https://raw.githubusercontent.com/prometheus/jmx_exporter/master/example_configs/kafka-2_0_0.yml --output-document=$JMX_CONFIG_FILE_IN_CONTAINER_PATH
RUN wget https://REPO1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${EXPORTER_VERSION}/jmx_prometheus_javaagent-${EXPORTER_VERSION}.jar

# change user
RUN chown -R $USER:$USER /tmp
RUN chown -R $USER:$USER /var
USER $USER

WORKDIR /
" >"$DOCKERFILE"
}

function generateDockerfileBySource() {
  echo "# this dockerfile is generated dynamically
FROM ubuntu:22.04 AS build

# install tools
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y openjdk-11-jdk wget git curl

# add user
RUN groupadd $USER && useradd -ms /bin/bash -g $USER $USER

# download jmx exporter
RUN mkdir /opt/jmx_exporter
WORKDIR /opt/jmx_exporter
RUN wget https://raw.githubusercontent.com/prometheus/jmx_exporter/master/example_configs/kafka-2_0_0.yml --output-document=$JMX_CONFIG_FILE_IN_CONTAINER_PATH
RUN wget https://REPO1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${EXPORTER_VERSION}/jmx_prometheus_javaagent-${EXPORTER_VERSION}.jar

# build kafka from source code
RUN git clone https://github.com/apache/kafka /tmp/kafka
WORKDIR /tmp/kafka
RUN git checkout $VERSION
RUN ./gradlew clean releaseTarGz
RUN mkdir /opt/kafka
RUN tar -zxvf \$(find ./core/build/distributions/ -maxdepth 1 -type f -name kafka_*SNAPSHOT.tgz) -C /opt/kafka --strip-components=1

FROM ubuntu:22.04

# install tools
RUN apt-get update && apt-get install -y openjdk-11-jre

# copy kafka
COPY --from=build /opt/jmx_exporter /opt/jmx_exporter
COPY --from=build /opt/kafka /opt/kafka

# add user
RUN groupadd $USER && useradd -ms /bin/bash -g $USER $USER

# change user
RUN chown -R $USER:$USER /opt/kafka
USER $USER

# export ENV
ENV KAFKA_HOME /opt/kafka
WORKDIR /opt/kafka
" >"$DOCKERFILE"
}

function generateDockerfileByVersion() {
  echo "# this dockerfile is generated dynamically
FROM ubuntu:22.04 AS build

# install tools
RUN apt-get update && apt-get install -y wget

# download jmx exporter
RUN mkdir /opt/jmx_exporter
WORKDIR /opt/jmx_exporter
RUN wget https://raw.githubusercontent.com/prometheus/jmx_exporter/master/example_configs/kafka-2_0_0.yml --output-document=$JMX_CONFIG_FILE_IN_CONTAINER_PATH
RUN wget https://REPO1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${EXPORTER_VERSION}/jmx_prometheus_javaagent-${EXPORTER_VERSION}.jar

# download kafka
WORKDIR /tmp
RUN wget https://archive.apache.org/dist/kafka/${VERSION}/kafka_2.13-${VERSION}.tgz
RUN mkdir /opt/kafka
RUN tar -zxvf kafka_2.13-${VERSION}.tgz -C /opt/kafka --strip-components=1
WORKDIR /opt/kafka

FROM ubuntu:22.04

# install tools
RUN apt-get update && apt-get install -y openjdk-11-jre

# copy kafka
COPY --from=build /opt/jmx_exporter /opt/jmx_exporter
COPY --from=build /opt/kafka /opt/kafka

# add user
RUN groupadd $USER && useradd -ms /bin/bash -g $USER $USER

# change user
RUN chown -R $USER:$USER /opt/kafka
USER $USER

# export ENV
ENV KAFKA_HOME /opt/kafka
WORKDIR /opt/kafka
" >"$DOCKERFILE"
}

function generateDockerfile() {
  if [[ "$CONFLUENT_BROKER" = "true" ]]; then
    generateConfluentDockerfile
  else
    if [[ -n "$REVISION" ]]; then
      generateDockerfileBySource
    else
      generateDockerfileByVersion
    fi
  fi
}

function setListener() {
  if [[ "$SASL" == "true" ]]; then
    echo "listeners=SASL_PLAINTEXT://:9092" >>"$BROKER_PROPERTIES"
    echo "advertised.listeners=SASL_PLAINTEXT://${ADDRESS}:$BROKER_PORT" >>"$BROKER_PROPERTIES"
    echo "security.inter.broker.protocol=SASL_PLAINTEXT" >>"$BROKER_PROPERTIES"
    echo "sasl.mechanism.inter.broker.protocol=PLAIN" >>"$BROKER_PROPERTIES"
    echo "sasl.enabled.mechanisms=PLAIN" >>"$BROKER_PROPERTIES"
    echo "listener.name.sasl_plaintext.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
                 username="$ADMIN_NAME" \
                 password="$ADMIN_PASSWORD" \
                 user_${ADMIN_NAME}="$ADMIN_PASSWORD" \
                 user_${USER_NAME}="${USER_PASSWORD}";" >>"$BROKER_PROPERTIES"
    echo "authorizer.class.name=kafka.security.authorizer.AclAuthorizer" >>"$BROKER_PROPERTIES"
    # allow brokers to communicate each other
    echo "super.users=User:admin" >>"$BROKER_PROPERTIES"
  else
    echo "listeners=PLAINTEXT://:9092" >>"$BROKER_PROPERTIES"
    echo "advertised.listeners=PLAINTEXT://${ADDRESS}:$BROKER_PORT" >>"$BROKER_PROPERTIES"
    if [[ "$CONFLUENT_BROKER" = "true" ]]; then
      echo "confluent.metadata.server.listeners=http://0.0.0.0:$BROKER_PORT" >>"$BROKER_PROPERTIES"
    fi
  fi
}

function setLogDirs() {
  declare -i count=0
  if [[ -n "$DATA_FOLDERS" ]]; then
    IFS=',' read -ra folders <<<"$DATA_FOLDERS"
    for folder in "${folders[@]}"; do
      # create the folder if it is nonexistent
      mkdir -p "$folder"
      count=$((count + 1))
    done
  else
    count=3
  fi

  local logConfigs="log.dirs=$DATA_FOLDER_IN_CONTAINER_PREFIX-0"
  for ((i = 1; i < count; i++)); do
    logConfigs="$logConfigs,$DATA_FOLDER_IN_CONTAINER_PREFIX-$i"
  done

  echo $logConfigs >>"$BROKER_PROPERTIES"
}

function generateResourceConstraintCommand() {
    if [[ -n "$MEMORY" ]]; then
        echo "--memory=$MEMORY"
    fi
    if [[ -n "$CPUS" ]]; then
        echo "--cpus=$CPUS"
    fi
}

function generateJmxConfigMountCommand() {
    if [[ "$JMX_CONFIG_FILE" != "" ]]; then
        echo "--mount type=bind,source=$JMX_CONFIG_FILE,target=$JMX_CONFIG_FILE_IN_CONTAINER_PATH"
    else
        echo ""
    fi
}

function generateDataFolderMountCommand() {
  local mount=""
  if [[ -n "$DATA_FOLDERS" ]]; then
    IFS=',' read -ra folders <<<"$DATA_FOLDERS"
    # start with 0 rather than 1 (see setLogDirs)
    declare -i count=0

    for folder in "${folders[@]}"; do
      mount="$mount -v $folder:$DATA_FOLDER_IN_CONTAINER_PREFIX-$count"
      count=$((count + 1))
    done
  fi
  echo "$mount"
}

function setPropertyIfEmpty() {
  local key=$1
  local value=$2
  # performance configs
  if [[ "$(cat $BROKER_PROPERTIES | grep $key)" == "" ]]; then
    echo "$key=$value" >>"$BROKER_PROPERTIES"
  fi
}

function fetchBrokerId() {
  local id=""
  for i in {1..10}; do
    id=$(docker logs $CONTAINER_NAME | grep -o "KafkaServer id=[0-9]*" | cut -d = -f 2)
    if [[ "$id" != "" ]]; then
      break
    fi
    sleep 1
  done
  if [[ "$id" == "" ]]; then
    echo "failed to get broker id from container: $CONTAINER_NAME"
  else
    echo "$id"
  fi
}

# ===================================[main]===================================

checkDocker
buildImageIfNeed "$IMAGE_NAME"
if [[ "$RUN" != "true" ]]; then
  echo "docker image: $IMAGE_NAME is created"
  exit 0
fi

checkNetwork

while [[ $# -gt 0 ]]; do
  if [[ "$1" == "help" ]]; then
    showHelp
    exit 0
  fi
  echo "$1" >>"$BROKER_PROPERTIES"
  shift
done

rejectProperty "listeners"
rejectProperty "log.dirs"
rejectProperty "broker.id"
requireProperty "zookeeper.connect"

setListener
setPropertyIfEmpty "num.io.threads" "8"
setPropertyIfEmpty "num.network.threads" "8"
setPropertyIfEmpty "num.partitions" "8"
setPropertyIfEmpty "transaction.state.log.replication.factor" "1"
setPropertyIfEmpty "offsets.topic.replication.factor" "1"
setPropertyIfEmpty "transaction.state.log.min.isr" "1"
setPropertyIfEmpty "min.insync.replicas" "1"

if [[ "$CONFLUENT_BROKER" = "true" ]]; then
    rejectProperty "metric.reporters"
    setPropertyIfEmpty "confluent.metrics.reporter.zookeeper.connect" "$ZOOKEEPER_CONNECT"
    setPropertyIfEmpty "metric.reporters" "io.confluent.metrics.reporter.ConfluentMetricsReporter"
    setPropertyIfEmpty "confluent.metrics.reporter.bootstrap.servers" "${ADDRESS}:$BROKER_PORT"
    setPropertyIfEmpty "confluent.metrics.reporter.topic.replicas" "1"
    setPropertyIfEmpty "confluent.topic.replication.factor" "1"
    setPropertyIfEmpty "confluent.license.topic.replication.factor" "1"
    setPropertyIfEmpty "confluent.metadata.topic.replication.factor" "1"
    setPropertyIfEmpty "confluent.security.event.logger.exporter.kafka.topic.replicas" "1"
    setPropertyIfEmpty "confluent.balancer.topic.replication.factor" "1"
    setPropertyIfEmpty "confluent.balancer.enable" "true"
    setPropertyIfEmpty "confluent.topic.bootstrap.servers" "${ADDRESS}:$BROKER_PORT"
fi
setLogDirs

docker run -d --init \
  --name $CONTAINER_NAME \
  -e KAFKA_HEAP_OPTS="$HEAP_OPTS" \
  -e KAFKA_JMX_OPTS="$JMX_OPTS" \
  -e KAFKA_OPTS="-javaagent:/opt/jmx_exporter/jmx_prometheus_javaagent-${EXPORTER_VERSION}.jar=$EXPORTER_PORT:$JMX_CONFIG_FILE_IN_CONTAINER_PATH" \
  -v $BROKER_PROPERTIES:/tmp/broker.properties:ro \
  $(generateResourceConstraintCommand) \
  $(generateJmxConfigMountCommand) \
  $(generateDataFolderMountCommand) \
  -p $BROKER_PORT:9092 \
  -p $BROKER_JMX_PORT:$BROKER_JMX_PORT \
  -p $EXPORTER_PORT:$EXPORTER_PORT \
  "$IMAGE_NAME" "$SCRIPT_LOCATION_IN_CONTAINER" /tmp/broker.properties

echo "================================================="
[[ -n "$DATA_FOLDERS" ]] && echo "mount $DATA_FOLDERS to container: $CONTAINER_NAME"
echo "broker id: $(fetchBrokerId)"
echo "broker address: ${ADDRESS}:$BROKER_PORT"

#confluent broker may not support JMX server now,so remove it temporarily
if [[ "$CONFLUENT_BROKER" != "true" ]]; then
    echo "jmx address: ${ADDRESS}:$BROKER_JMX_PORT"
fi
echo "exporter address: ${ADDRESS}:$EXPORTER_PORT"
if [[ "$SASL" == "true" ]]; then
  user_jaas_file=/tmp/user-jaas-${BROKER_PORT}.conf
  echo "
  sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=${USER_NAME} password=${USER_PASSWORD};
  security.protocol=SASL_PLAINTEXT
  sasl.mechanism=PLAIN
  " >$user_jaas_file

  admin_jaas_file=/tmp/admin-jaas-${BROKER_PORT}.conf
  echo "
  sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=${ADMIN_NAME} password=${ADMIN_PASSWORD};
  security.protocol=SASL_PLAINTEXT
  sasl.mechanism=PLAIN
  " >$admin_jaas_file
  echo "SASL_PLAINTEXT is enabled. user config: $user_jaas_file admin config: $admin_jaas_file"
fi
echo "================================================="
