#!/bin/bash
# This script is used to compile the OmniAdaptor from the source code, but it is not the full source code of OmniAdaptor.
set -ex
SCRIPT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SOURCE_ROOT=$(cd "${SCRIPT_ROOT}/../.." && pwd)
export WORKSPACE="${SOURCE_ROOT}/../"

function jdk17_install(){
    export JAVA_HOME=/opt/buildtools/bisheng-jdk-17.0.18
}

function jdk8_set(){
    export JAVA_HOME=/opt/buildtools/bisheng-jdk1.8.0_342
    export JRE_HOME=/opt/buildtools/bisheng-jdk1.8.0_342/jre
    export PATH=$JRE_HOME/bin:$PATH
    export CLASSPATH=${JRE_HOME}/lib:$CLASSPATH
}

jdk17_install
export MAVEN_HOME=/opt/buildtools/apache-maven-3.9.9
export MAVEN_OPTS="${MAVEN_OPTS:-} -Dmaven.repo.local=/home/jenkins/agent/build_cache/.maven_cache"
export PATH=${MAVEN_HOME}/bin:${JAVA_HOME}/bin:$PATH
export CLASSPATH=${JAVA_HOME}/lib:$CLASSPATH

function install_flink17(){
    mkdir -p /home/jenkins/agent/build_cache/.maven_cache/org/apache/flink/
    # git clone --depth 1 -b release-1.17 https://github.com/apache/flink.git
    # pushd flink && mvn clean install -DskipTests && popd
    pushd /home/jenkins/agent/build_cache/.maven_cache/org/apache/flink/
    wget -q https://boostkit-bigdata-public.obs.cn-north-4.myhuaweicloud.com/artifact/OmniStream/OmniStream_rely/flink1.17/flink_1.17.zip
    unzip -qq -o flink_1.17.zip
    popd
}

function compile_flink_java(){
    install_flink17
    echo "start compile flink-extension"
    pushd ${WORKSPACE}/OmniAdaptor/omnistream/omniop-flink-extension/omni-flink-bundle/
    mvn clean package -DskipTests && popd
}

function OmniAdaptor_build(){
    parameter="$1"
    case ${parameter} in
        "compile")
            compile_flink_java
            ;;
        *)
            echo "ERROR: unsupported parameter '${parameter:-<empty>}', expected: $0 compile" >&2
            exit 1
            ;;
    esac
}

# 主入口
main(){
    local action="$1"
    OmniAdaptor_build "${action}"
}

main "$@"
