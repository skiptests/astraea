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
declare -r VERSION=${VERSION:-3.3.4}
declare -r REPO=${REPO:-ghcr.io/skiptests/astraea/hadoop}
declare -r IMAGE_NAME="$REPO:$VERSION"
declare -r DOCKERFILE=$DOCKER_FOLDER/namenode.dockerfile
declare -r NAMENODE_PORT=${NAMENODE_PORT:-"$(getRandomPort)"}
declare -r NAMENODE_JMX_PORT="${NAMENODE_JMX_PORT:-"$(getRandomPort)"}"
declare -r CONTAINER_NAME="namenode-$NAMENODE_PORT"
declare -r HDFS_SITE_XML="/tmp/namenode-${NAMENODE_PORT}-hdfs.xml"
declare -r CORE_SITE_XML="/tmp/namenode-${NAMENODE_PORT}-core.xml"
# cleanup the file if it is existent
[[ -f "$HDFS_SITE_XML" ]] && rm -f "$HDFS_SITE_XML"
[[ -f "$CORE_SITE_XML" ]] && rm -f "$CORE_SITE_XML"

# ===================================[functions]===================================

function showHelp() {
  echo "Usage: [ENV] start_hadoop.sh"
  echo "ENV: "
  echo "    REPO=astraea/namenode     set the docker repo"
  echo "    VERSION=3.3.4              set version of hadoop distribution"
  echo "    BUILD=false                set true if you want to build image locally"
  echo "    RUN=false                  set false if you want to build/pull image only"
}

function generateDockerfile() {
  echo "#this dockerfile is generated dynamically
FROM ubuntu:22.04 AS build

#install tools
RUN apt-get update && apt-get install -y wget

#download hadoop
WORKDIR /tmp
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-${VERSION}/hadoop-${VERSION}.tar.gz
RUN mkdir /opt/hadoop
RUN tar -zxvf hadoop-${VERSION}.tar.gz -C /opt/hadoop --strip-components=1

FROM ubuntu:22.04

#install tools
RUN apt-get update && apt-get install -y openjdk-11-jre

#copy hadoop
COPY --from=build /opt/hadoop /opt/hadoop

#add user
RUN groupadd $USER && useradd -ms /bin/bash -g $USER $USER

#edit hadoop-env.sh
RUN echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> /opt/hadoop/etc/hadoop/hadoop-env.sh

#change user
RUN chown -R $USER:$USER /opt/hadoop
USER $USER

#export ENV
ENV HADOOP_HOME /opt/hadoop
WORKDIR /opt/hadoop
" >"$DOCKERFILE"
}

function setPropertyIfEmpty() {
  local name=$1
  local value=$2

  if ! grep -q "<name>$name</name>" $HDFS_SITE_XML; then
      local entry="<property><name>$name</name><value>$value</value></property>"
      local escapedEntry=$(echo $entry | sed 's/\//\\\//g')
      sed -i "/<\/configuration>/ s/.*/${escapedEntry}\n&/" $HDFS_SITE_XML
  fi
}

function setProperty() {
  local name=$1
  local value=$2
  local path=$3

  local entry="<property><name>$name</name><value>$value</value></property>"
  local escapedEntry=$(echo $entry | sed 's/\//\\\//g')
  sed -i "/<\/configuration>/ s/.*/${escapedEntry}\n&/" $path
}

# ===================================[main]===================================

checkDocker
buildImageIfNeed "$IMAGE_NAME"
if [[ "$RUN" != "true" ]]; then
  echo "docker image: $IMAGE_NAME is created"
  exit 0
fi

checkNetwork

echo -e "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n<configuration>\n</configuration>" > $HDFS_SITE_XML
setProperty dfs.namenode.datanode.registration.ip-hostname-check false $HDFS_SITE_XML

while [[ $# -gt 0 ]]; do
  if [[ "$1" == "help" ]]; then
    showHelp
    exit 0
  fi
  name=${1%=*}
  value=${1#*=}
  setPropertyIfEmpty name value
  shift
done

echo -e "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n<configuration>\n</configuration>" > "$CORE_SITE_XML"
setProperty fs.defaultFS hdfs://$CONTAINER_NAME:8020 $CORE_SITE_XML

docker run -d --init \
  --name $CONTAINER_NAME \
  -h $CONTAINER_NAME \
  -v $HDFS_SITE_XML:/opt/hadoop/etc/hadoop/hdfs-site.xml:ro \
  -v $CORE_SITE_XML:/opt/hadoop/etc/hadoop/core-site.xml:ro \
  -p $NAMENODE_JMX_PORT:9870 \
  "$IMAGE_NAME" /bin/bash -c "./bin/hdfs namenode -format && ./bin/hdfs namenode"

echo "================================================="
echo "jmx address: ${ADDRESS}:$NAMENODE_JMX_PORT/jmx"
echo "run $DOCKER_FOLDER/start_datanode.sh fs.defaultFS=hdfs://$CONTAINER_NAME:8020 to join datanode"
echo "================================================="