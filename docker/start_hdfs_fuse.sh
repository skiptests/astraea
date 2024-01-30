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
declare -r REPO=${REPO:-ghcr.io/skiptests/astraea/hdfs-fuse}
declare -r IMAGE_NAME="$REPO:$VERSION"
declare -r TMP_DOCKER_FOLDER=/tmp/docker
declare -r DOCKERFILE=$TMP_DOCKER_FOLDER/hdfs-fuse.dockerfile
declare -r CONTAINER_NAME="hdfs-fuse"
declare -r HADOOP_SRC_PATH=$TMP_DOCKER_FOLDER/hadoop-src
declare -r FUSE_DFS_WRAPPER_SH=$HADOOP_SRC_PATH/fuse_dfs_wrapper.sh

# ===================================[functions]===================================

function showHelp() {
  echo "Usage: [ENV] start_hdfs_fuse.sh"
  echo "ENV: "
  echo "    REPO=astraea/datanode     set the docker repo"
  echo "    VERSION=3.3.4              set version of hadoop distribution"
  echo "    BUILD=false                set true if you want to build image locally"
  echo "    RUN=false                  set false if you want to build/pull image only"
}

function generateDockerfile() {
  echo "#this dockerfile is generated dynamically
FROM ubuntu:22.04

#copy hadoop
COPY hadoop-src/ /opt/hadoop/

#install tools
RUN apt-get update && apt-get install -y openjdk-11-jre fuse

RUN echo \"user_allow_other\" >> /etc/fuse.conf

ENV HADOOP_HOME /opt/hadoop
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
WORKDIR /opt/hadoop

#add user
RUN groupadd astraea && useradd -ms /bin/bash -g astraea astraea

RUN mkdir /mnt/hdfs

#change user
RUN chown -R $USER:$USER /opt/hadoop /mnt/hdfs
RUN chmod 755 fuse_dfs_wrapper.sh
USER $USER

" >"$DOCKERFILE"
}

function checkGit() {
  if [[ "$(which git)" == "" ]]; then
    echo "you have to install git"
    exit 2
  fi
}

function cloneSrcIfNeed() {
  if [[ ! -d "$HADOOP_SRC_PATH" ]]; then
    mkdir -p $HADOOP_SRC_PATH
    git clone https://github.com/apache/hadoop.git $HADOOP_SRC_PATH
  fi
}

function replaceLine() {
  local line_number=$1
  local text=$2
  local file=$3

  if [[ "$(uname)" == "Darwin" ]]; then
    sed -i "" "${line_number}s/.*/${text}/" $file
  else
    sed -i "${line_number}s/.*/${text}/" $file
  fi
}

function buildSrc() {
  checkGit
  cloneSrcIfNeed
  cd $HADOOP_SRC_PATH
  git checkout rel/release-${VERSION}
  replaceLine 17 USER=\$\(whoami\) start-build-env.sh
  ./start-build-env.sh mvn clean package -Pnative -DskipTests -Drequire.fuse=true -Dmaven.javadoc.skip=true
}

function generateFuseDfsWrapper() {
  cat > "$FUSE_DFS_WRAPPER_SH" << 'EOF'
#!/usr/bin/env bash

export FUSEDFS_PATH="$HADOOP_HOME/hadoop-hdfs-project/hadoop-hdfs-native-client/target/main/native/fuse-dfs"
export LIBHDFS_PATH="$HADOOP_HOME/hadoop-hdfs-project/hadoop-hdfs-native-client/target/native/target/usr/local/lib"
export PATH=$FUSEDFS_PATH:$PATH
export LD_LIBRARY_PATH=$LIBHDFS_PATH:$JAVA_HOME/lib/server
while IFS= read -r -d '' file
do
  export CLASSPATH=$CLASSPATH:$file
done < <(find "$HADOOP_HOME/hadoop-tools" -name "*.jar" -print0)

fuse_dfs "$@"
EOF
}

function buildImageIfNeed() {
  local imageName="$1"
  if [[ "$(docker images -q "$imageName" 2>/dev/null)" == "" ]]; then
    local needToBuild="true"
    if [[ "$BUILD" == "false" ]]; then
      docker pull "$imageName" 2>/dev/null
      if [[ "$?" == "0" ]]; then
        needToBuild="false"
      else
        echo "Can't find $imageName from repo. Will build $imageName on the local"
      fi
    fi
    if [[ "$needToBuild" == "true" ]]; then
      buildSrc
      generateFuseDfsWrapper
      generateDockerfile
      docker build --no-cache -t "$imageName" -f "$DOCKERFILE" "$TMP_DOCKER_FOLDER"
      if [[ "$?" != "0" ]]; then
        exit 2
      fi
    fi
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

if [[ $# -gt 0 ]]; then
  HDFS=$1
  HDFS_PORT="${HDFS: -5}"
fi

docker run -d --init \
  --name $CONTAINER_NAME-$HDFS_PORT \
  --device /dev/fuse \
  --cap-add SYS_ADMIN \
  --security-opt apparmor:unconfined \
  "$IMAGE_NAME" /bin/bash -c "./fuse_dfs_wrapper.sh -d $HDFS /mnt/hdfs"

docker exec -it -w /mnt/hdfs $CONTAINER_NAME-$HDFS_PORT /bin/bash