#!/bin/bash
set -x

# 当前脚本目录：.../omnihelper/omnimv-spark-extension
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# resources 目录：.../omnihelper/resources
RES_DIR="${SCRIPT_DIR}/../resources"
mkdir -p "${RES_DIR}"

rm -rf ${SCRIPT_DIR}/target/*.jar
# 构建 log-parser 模块
mvn -f "${SCRIPT_DIR}/pom.xml" clean package -P spark-3.4 -DskipTests -pl log-parser -am

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