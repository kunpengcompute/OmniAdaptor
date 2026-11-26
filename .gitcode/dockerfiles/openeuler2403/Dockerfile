FROM swr.cn-north-4.myhuaweicloud.com/cloud_boostkit/openeuler-24.03-lts-sp3:latest

RUN rm -rf /etc/yum.repos.d/* && touch /etc/yum.repos.d/openEuler.repo \
&& echo "[openEuler-everything]                                                                                            " >> /etc/yum.repos.d/openEuler.repo \
&& echo "name=openEuler-everything                                                                                         " >> /etc/yum.repos.d/openEuler.repo \
&& echo "baseurl=http://repo.openeuler.org/openEuler-24.03-LTS-SP3/everything/aarch64/                                     " >> /etc/yum.repos.d/openEuler.repo \
&& echo "enabled=1                                                                                                         " >> /etc/yum.repos.d/openEuler.repo \
&& echo "gpgcheck=0                                                                                                        " >> /etc/yum.repos.d/openEuler.repo \
&& echo "gpgkey=http://repo.openeuler.org/openEuler-24.03-LTS-SP3/everything/aarch64/RPM-GPG-KEY-openEuler                 " >> /etc/yum.repos.d/openEuler.repo \
&& echo "                                                                                                                  " >> /etc/yum.repos.d/openEuler.repo \
&& echo "[openEuler-EPOL]                                                                                                  " >> /etc/yum.repos.d/openEuler.repo \
&& echo "name=openEuler-epol                                                                                               " >> /etc/yum.repos.d/openEuler.repo \
&& echo "baseurl=http://repo.openeuler.org/openEuler-24.03-LTS-SP3/EPOL/main/aarch64/                                      " >> /etc/yum.repos.d/openEuler.repo \
&& echo "enabled=1                                                                                                         " >> /etc/yum.repos.d/openEuler.repo \
&& echo "gpgcheck=0                                                                                                        " >> /etc/yum.repos.d/openEuler.repo \
&& echo "                                                                                                                  " >> /etc/yum.repos.d/openEuler.repo \
&& echo "[openEuler-update]                                                                                                " >> /etc/yum.repos.d/openEuler.repo \
&& echo "name=openEuler-update                                                                                             " >> /etc/yum.repos.d/openEuler.repo \
&& echo "baseurl=http://repo.openeuler.org/openEuler-24.03-LTS-SP3/update/aarch64/                                         " >> /etc/yum.repos.d/openEuler.repo \
&& echo "enabled=1                                                                                                         " >> /etc/yum.repos.d/openEuler.repo \
&& echo "gpgcheck=0                                                                                                        " >> /etc/yum.repos.d/openEuler.repo \
&& yum clean all && yum makecache

COPY .gitcode/dockerfiles/scripts /opt/docker_build_scripts

RUN ls -al /opt/docker_build_scripts \
&& bash /opt/docker_build_scripts/install-dependencies.sh --install-base-pkgs \
&& bash /opt/docker_build_scripts/install-dependencies.sh --install bisheng-jdk-8u342 \
&& bash /opt/docker_build_scripts/install-dependencies.sh --install bisheng-jdk-17.0.18 \
&& bash /opt/docker_build_scripts/install-dependencies.sh --install maven-3.9.9 \
&& bash /opt/docker_build_scripts/install-dependencies.sh --install cmake-3.28.2 \
&& bash /opt/docker_build_scripts/install-dependencies.sh --install lcov \
&& bash /opt/docker_build_scripts/install-dependencies.sh --setup maven \
&& pip3 install --no-cache-dir setuptools wheel -i http://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com

ENV LANG=C.utf8 \
    LC_ALL=C.utf8 \
    LANGUAGE=C.utf8

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
