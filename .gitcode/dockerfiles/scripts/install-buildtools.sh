#!/bin/bash

set -euo pipefail

install_bisheng_jdk_17_0_18() {
    mkdir -p /opt/buildtools && pushd /opt/buildtools
    JDK_TAR="bisheng-jdk-17.0.18-b13-linux-aarch64.tar.gz"
    wget --no-check-certificate "https://mirrors.huaweicloud.com/kunpeng/archive/compiler/bisheng_jdk/${JDK_TAR}"

    # Read the top-level directory name from the tar archive.
    set +o pipefail
    JDK_DIR=$(tar -tf "${JDK_TAR}" | head -1 | cut -d/ -f1)
    set -o pipefail

    tar -zxvf "${JDK_TAR}"
    chown -R root:root "${JDK_DIR}"
    rm -f "${JDK_TAR}"

    echo "JDK installed to: /opt/buildtools/${JDK_DIR}"
    sed -i '/JAVA_HOME/d' /etc/profile
    echo "export JAVA_HOME=/opt/buildtools/${JDK_DIR}" >> /etc/profile
    echo 'export PATH=$JAVA_HOME/bin:$PATH' >> /etc/profile
    echo 'export C_INCLUDE_PATH=$JAVA_HOME/include:$JAVA_HOME/include/linux:$C_INCLUDE_PATH' >> /etc/profile
    echo 'export CPLUS_INCLUDE_PATH=$JAVA_HOME/include:$JAVA_HOME/include/linux:$CPLUS_INCLUDE_PATH' >> /etc/profile
    echo 'export LIBRARY_PATH=$JAVA_HOME/lib:$JAVA_HOME/lib/server:$LIBRARY_PATH' >> /etc/profile
    echo 'export LD_LIBRARY_PATH=$JAVA_HOME/lib:$JAVA_HOME/lib/server:$LD_LIBRARY_PATH' >> /etc/profile
}

install_bisheng_jdk_8u342(){
    local jdk_pkg_arch
    case "$(uname -m)" in
        x86_64|amd64)
            jdk_pkg_arch="x64"
            ;;
        aarch64|arm64)
            jdk_pkg_arch="aarch64"
            ;;
        *)
            echo "Unsupported architecture for cmake: $(uname -m)" >&2
            return 1
            ;;
    esac
    mkdir -p /opt/buildtools && pushd /opt/buildtools
    JDK8_TAR="bisheng-jdk-8u342-linux-${jdk_pkg_arch}.tar.gz"
    wget -q https://mirrors.huaweicloud.com/kunpeng/archive/compiler/bisheng_jdk/${JDK8_TAR}
    tar -xf ${JDK8_TAR} && rm -rf ${JDK8_TAR} && popd
}


install_cmake_3_28_2() {
    local cmake_pkg_arch
    case "$(uname -m)" in
        x86_64|amd64)
            cmake_pkg_arch="linux-x86_64"
            ;;
        aarch64|arm64)
            cmake_pkg_arch="linux-aarch64"
            ;;
        *)
            echo "Unsupported architecture for cmake: $(uname -m)" >&2
            return 1
            ;;
    esac
    mkdir -p /opt/buildtools && pushd /opt/buildtools
    export CMAKE_VERSION="3.28.2"
    local cmake_pkg="cmake-${CMAKE_VERSION}-${cmake_pkg_arch}"
    wget -q "https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/${cmake_pkg}.tar.gz"
    tar -xf "${cmake_pkg}.tar.gz" && rm -rf "${cmake_pkg}.tar.gz" && rm -rf /usr/local/bin/cmake
    cp -r "${cmake_pkg}/bin/"* /usr/local/bin
    cp -r "${cmake_pkg}/share/"* /usr/local/share
    popd
}

install_git_2_39_3() {
    local git_version="2.39.3"
    local make_args=(prefix=/usr/local NO_GETTEXT=1 NO_TCLTK=1 NO_PERL=1 CFLAGS="-g -O2 -Wall -std=gnu99")
    mkdir -p /opt/buildtools && pushd /opt/buildtools
    wget -q "https://gitee.com/mirrors/git/repository/archive/v${git_version}.tar.gz" -O "git-v${git_version}.tar.gz"
    tar -xf "git-v${git_version}.tar.gz" && rm -f "git-v${git_version}.tar.gz"
    pushd "git-v${git_version}"
    make "${make_args[@]}" -j"$(nproc)" all
    make "${make_args[@]}" install
    popd && popd
    ln -snf /usr/local/bin/git /usr/bin/git
}

install_maven_3_9_9() {
    export MAVEN_VERSION="3.9.9"
    mkdir -p /opt/buildtools && pushd /opt/buildtools
    wget -q https://repo1.maven.org/maven2/org/apache/maven/apache-maven/${MAVEN_VERSION}/apache-maven-${MAVEN_VERSION}-bin.zip
    unzip apache-maven-${MAVEN_VERSION}-bin.zip && rm -rf apache-maven-${MAVEN_VERSION}-bin.zip && popd
    echo "export MAVEN_HOME=/opt/buildtools/apache-maven-${MAVEN_VERSION}" >> /etc/profile
    echo 'export PATH=$MAVEN_HOME/bin:$PATH' >> /etc/profile
}

install_lcov() {
    mkdir -p /opt/buildtools && pushd /opt/buildtools
    wget -q https://github.com/linux-test-project/lcov/releases/download/v1.16/lcov-1.16.tar.gz && tar -xf lcov-1.16.tar.gz && rm -rf lcov-1.16.tar.gz
    pushd lcov-1.16 && make rpms && rpm -ivh lcov-*.noarch.rpm && popd && popd
}

install_python_3_11() {
    local python_prefix="/opt/buildtools/python3.11.4"
    local openssl_prefix="/opt/buildtools/openssl-1.1.1w"
    mkdir -p /opt/buildtools && pushd /opt/buildtools
    git clone --depth 1 --branch OpenSSL_1_1_1w https://gitee.com/mirrors/openssl.git
    pushd openssl
    # --libdir=lib：x86_64 默认装 lib64，而 CPython 的 --with-openssl 只加 -L<dir>/lib
    ./config --prefix="${openssl_prefix}" --openssldir="${openssl_prefix}/ssl" --libdir=lib
    make -j8 && make install
    popd
    git clone --depth 1 --branch v3.11.4 https://gitee.com/mirrors/cpython.git
    pushd cpython
    ./configure --prefix="${python_prefix}" \
        --with-openssl="${openssl_prefix}" \
        --with-openssl-rpath="${openssl_prefix}/lib" \
        --enable-shared
    make -j8 && make install
    popd
    echo "${openssl_prefix}/lib" > /etc/ld.so.conf.d/openssl111.conf
    echo "${python_prefix}/lib" > /etc/ld.so.conf.d/python3.11.4.conf
    ldconfig
    ln -snf "${python_prefix}/bin/"* /usr/bin/
    "${python_prefix}/bin/python3" -c 'import ssl, _ssl; print("ssl ok:", ssl.OPENSSL_VERSION)'
    rm -rf /opt/buildtools/openssl /opt/buildtools/cpython
    popd
}

setup_maven() {
    cat > /opt/buildtools/apache-maven-3.9.9/conf/settings.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.2.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.2.0 https://maven.apache.org/xsd/settings-1.2.0.xsd">
    <localRepository>/root/build_cache/.maven_cache</localRepository>
    <mirrors>
        <mirror>
            <id>huaweicloud-maven</id>
            <name>Huawei Cloud Maven Repository</name>
            <url>https://mirrors.huaweicloud.com/repository/maven/</url>
            <!-- mirrorOf=central：仅匹配中央仓库的依赖，优先级最高 -->
            <mirrorOf>central</mirrorOf>
        </mirror>
        <mirror>
            <id>maven-central</id>
            <name>Maven Central Repository</name>
            <url>https://repo1.maven.org/maven2</url>
            <!-- mirrorOf=external:*.googleapis.com：仅匹配谷歌仓库等特殊依赖 -->
            <mirrorOf>external:*.googleapis.com,!huaweicloud-maven</mirrorOf>
        </mirror>
    </mirrors>
    <profiles>
        <profile>
            <id>huaweicloud</id>
            <activation>
                <activeByDefault>true</activeByDefault>
            </activation>
            <repositories>
                <repository>
                    <id>huaweicloud-maven</id>
                    <name>Huawei Cloud Maven Repository</name>
                    <url>https://mirrors.huaweicloud.com/repository/maven/</url>
                    <releases>
                        <enabled>true</enabled>
                    </releases>
                    <snapshots>
                        <enabled>true</enabled>
                    </snapshots>
                </repository>
            </repositories>
            <pluginRepositories>
                <pluginRepository>
                    <id>huaweicloud-maven</id>
                    <name>Huawei Cloud Maven Repository</name>
                    <url>https://mirrors.huaweicloud.com/repository/maven/</url>
                    <releases>
                        <enabled>true</enabled>
                    </releases>
                    <snapshots>
                        <enabled>true</enabled>
                    </snapshots>
                </pluginRepository>
            </pluginRepositories>
        </profile>
    </profiles>
    <activeProfiles>
        <activeProfile>default-profile</activeProfile>
    </activeProfiles>
    <servers>
        <server>
            <id>huaweicloud-maven</id>
            <configuration>
                <connectTimeout>10000</connectTimeout>
                <requestTimeout>120000</requestTimeout>
            </configuration>
        </server>
    </servers>
</settings>
EOF
}
