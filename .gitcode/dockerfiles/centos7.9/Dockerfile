FROM swr.cn-north-1.myhuaweicloud.com/codeci/special_base_image:centos7-base-1.0.2
## Set public envs
ENV REPO_URL_CI=""
ENV REPO_URL_BMC=""
ENV INSTALL_LINUX_DIR="/tmp"

RUN set -ex && rm -rf /etc/yum.repos.d/* && curl -o /etc/yum.repos.d/CentOS-Base.repo http://mirrors.myhuaweicloud.com/repo/CentOS-Base-7.repo && yum clean all && yum makecache \
&& yum install -y perl-Digest-MD5 perl-IO-Compress perl-Module-Load perl-Module-Load-Conditional krb5-devel libatomic-devel \
&& yum install -y sqlite-devel dos2unix sudo zlib-devel openssl-devel pcre-devel libffi-devel ncurses-devel readline-devel gcc g++ make autoconf automake wget zip unzip \
    jdk java bzip2-devel xz-devel epel-release python3 python3-devel rpm-build rpmdevtools cpio libtool python3-pip python3-requests git curl perl curl-devel expat-devel

COPY .gitcode/dockerfiles/scripts /opt/docker_build_scripts

RUN bash /opt/docker_build_scripts/install-dependencies.sh --install bisheng-jdk-8u342 \
&& bash /opt/docker_build_scripts/install-dependencies.sh --install maven-3.9.9 \
&& bash /opt/docker_build_scripts/install-dependencies.sh --install cmake-3.28.2 \
&& bash /opt/docker_build_scripts/install-dependencies.sh --install git-2.39.3 \
&& bash /opt/docker_build_scripts/install-dependencies.sh --install lcov \
&& bash /opt/docker_build_scripts/install-dependencies.sh --install python-3.11 \
&& bash /opt/docker_build_scripts/install-dependencies.sh --setup maven \
&& pip3 install --no-cache-dir setuptools wheel -i http://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com \
&& /usr/bin/git --version

ENV Jenkins_USER=jenkins \
    Jenkins_GROUP=jenkins \
    Jenkins_UID=1000 \
    Jenkins_GID=1000 \
    AGENT_WORKDIR=/home/jenkins/agent

RUN set -x \
    && groupadd -g "${Jenkins_GID}" "${Jenkins_GROUP}" \
    && useradd -c "Jenkins user" -d "/home/${Jenkins_USER}" -u "${Jenkins_UID}" -g "${Jenkins_GID}" -m "${Jenkins_USER}" \
    && echo "${Jenkins_USER} ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers \
    && mkdir -p "/home/${Jenkins_USER}/.jenkins" "${AGENT_WORKDIR}" /tmp \
    && chown -R "${Jenkins_UID}:${Jenkins_GID}" /tmp ${AGENT_WORKDIR} \
    && chmod -R 755 ${AGENT_WORKDIR} \
    && chmod 1777 /tmp

USER ${Jenkins_USER}
VOLUME ["/home/jenkins/.jenkins","/home/jenkins/agent"]
WORKDIR ${AGENT_WORKDIR}
