#!/bin/bash
# Copyright Huawei Technologies Co., Ltd. 2010-2018. All rights reserved.
set -ex

SCRIPT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SOURCE_ROOT=$(cd "${SCRIPT_ROOT}/../.." && pwd)
export WORKSPACE="${SOURCE_ROOT}/../"
export JAVA_HOME=/opt/buildtools/bisheng-jdk1.8.0_342
export MAVEN_HOME=/opt/buildtools/apache-maven-3.9.9
export PATH="${MAVEN_HOME}/bin:${JAVA_HOME}/bin:${JAVA_HOME}/jre/bin:$PATH"
export CLASSPATH=${JAVA_HOME}/lib:${JAVA_HOME}/jre/lib:$CLASSPATH
export MAVEN_OPTS="${MAVEN_OPTS:-} -Dmaven.repo.local=/home/jenkins/agent/build_cache/.maven_cache"
export omnihelper_version="2.0.0"
export Product_Version="26.3.RC1"
mkdir -p ${WORKSPACE}/output

function centos7_x86_build(){
    source /etc/profile
    export LD_LIBRARY_PATH=/opt/buildtools/python3.11.4/lib:$LD_LIBRARY_PATH
    sh OmniAdaptor/omnihelper/build.sh
    cp -rf ${WORKSPACE}/OmniAdaptor/omnihelper/*.tar.gz ${WORKSPACE}/output
}
function openeuler2203_arm_build(){
    sh OmniAdaptor/omnihelper/build.sh
    cp -rf ${WORKSPACE}/OmniAdaptor/omnihelper/*.tar.gz ${WORKSPACE}/output
}

function BoostKit_omnistream_version_info(){
    touch version.txt
    echo "Product Name: Kunpeng BoostKit" >> version.txt
    echo "Product Version: ${Product_Version}" >> version.txt
    echo "Component Name: BoostKit-omnihelper" >> version.txt
    echo "Component Version: ${omnihelper_version}" >> version.txt
}

function dopackage(){
    pushd ${WORKSPACE}
    wget https://boostkit-bigdata-public.obs.cn-north-4.myhuaweicloud.com/artifact/OmniHelper/cache/omnihelper_release_arm.tar.gz
    wget https://boostkit-bigdata-public.obs.cn-north-4.myhuaweicloud.com/artifact/OmniHelper/cache/omnihelper_release_x86.tar.gz
    BoostKit_omnistream_version_info
    cp -rvf ${WORKSPACE}/OmniAdaptor/omnihelper/README.md ./
    zip BoostKit-omniruntime-omnihelper-${omnihelper_version}.zip *.tar.gz version.txt README.md
    #########生成json文件#########################
    python3 ${WORKSPACE}/OmniAdaptor/.gitcode/scripts/collect_software_info.py ${WORKSPACE} BoostKit-omniruntime-omnihelper-${omnihelper_version}.zip
    ###############end############################
    popd
    cp -rf ${WORKSPACE}/BoostKit-omniruntime-omnihelper-${omnihelper_version}.zip* ${WORKSPACE}/output
}

function OmniHelper_build(){
    parameter="$1"
    case ${parameter} in
        "x86_compile")
            centos7_x86_build
            ;;
        "arm_compile")
            openeuler2203_arm_build
            ;;
        "package")
            dopackage
    esac
}

OmniHelper_build $@
