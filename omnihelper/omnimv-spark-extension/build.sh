#!/bin/bash
#set -x
#
## 当前脚本目录：.../omnihelper/omnimv-spark-extension
#SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
#
## resources 目录：.../omnihelper/resources
#RES_DIR="${SCRIPT_DIR}/../resources"
#mkdir -p "${RES_DIR}"
set -euo pipefail
set -x

# 当前脚本目录：.../omnihelper/omnimv-spark-extension
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# resources 目录：.../omnihelper/resources
RES_DIR="${SCRIPT_DIR}/../resources"
mkdir -p "${RES_DIR}"

M2_REPO="/repository/local/maven"
LOCKS_DIR="${M2_REPO}/.locks"

prepare_m2_repo() {
  # 1) 确保目录存在
  mkdir -p "${LOCKS_DIR}"

  # 2) 尝试给仓库目录加写权限（你想试的 chmod +w）
  #    如果你不是 owner，这一步可能不生效，但不会影响后续判断
  chmod -R +w "${M2_REPO}" || true

  # 3) 做一次写权限探测：能创建文件才算真正可写
  local probe="${LOCKS_DIR}/.write_probe.$$"
  if ! ( : > "${probe}" ) 2>/dev/null; then
    echo "[ERROR] Maven local repo is NOT writable: ${M2_REPO}"
    echo "[ERROR] 请确认当前用户对该目录有写权限，或改用 ~/.m2/repository（建议）。"
    ls -ld "${M2_REPO}" "${LOCKS_DIR}" || true
    exit 2
  fi
  rm -f "${probe}"

  # 4) 你要的 ll 打印（文件可能尚未生成，所以允许不存在）
  echo "[INFO] ls -ld ${M2_REPO} ${LOCKS_DIR}"
  ls -ld "${M2_REPO}" "${LOCKS_DIR}" || true

  echo "[INFO] ls -l ${LOCKS_DIR}/org.antlr~antlr4-maven-plugin~4.9.3*"
  ls -l "${LOCKS_DIR}"/org.antlr~antlr4-maven-plugin~4.9.3* 2>/dev/null || true
}

prepare_m2_repo

rm -rf ${SCRIPT_DIR}/target/*.jar
# 构建 log-parser 模块
mvn -f "${SCRIPT_DIR}/pom.xml" package -U -P spark-3.4 -DskipTests -pl log-parser -am

# log-parser 模块产物路径
JAR_PATH=$(find "${SCRIPT_DIR}/log-parser/target" \
  -maxdepth 1 \
  -type f \
  -name "boostkit-omnimv-logparser-spark-*-aarch64.jar" \
  ! -name "*sources*" \
  ! -name "*tests*" \
  | head -n 1)

if [ -z "${JAR_PATH}" ]; then
  echo "[ERROR] log-parser jar not found under ${SCRIPT_DIR}/log-parser/target/"
  exit 1
fi

echo "[INFO] copy jar to ${RES_DIR}"
cp -f "${JAR_PATH}" "${RES_DIR}/"

echo "[INFO] done: $(basename "${JAR_PATH}")"