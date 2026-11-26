#!/bin/bash
# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# OmniAdaptor is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan
# PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#         http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
# KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

source /etc/profile

set -ex

# 获取脚本所在目录的绝对路径
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# 构建 Spark log-parser
bash "${SCRIPT_DIR}/omnimv-spark-extension/build.sh"

# 检测系统架构（只识别x86和arm）
get_architecture() {
    local arch
    arch=$(uname -m)

    # 转换为小写方便匹配
    arch_lower=$(echo "$arch" | tr '[:upper:]' '[:lower:]')

    # 判断是否为x86架构
    case "$arch_lower" in
        x86_64|amd64|i386|i686|x64)
            echo "x86"
            return 0
            ;;
    esac

    # 判断是否为arm架构
    case "$arch_lower" in
        aarch64|arm64|armv7l|armv8l|armhf|arm)
            echo "arm"
            return 0
            ;;
    esac

    # 默认返回原始架构名
    echo "$arch"
    return 1
}


# 获取架构信息
ARCH=$(get_architecture)

python3 -c 'import ssl' 2>/dev/null || {
    echo "ERROR: python3 无法 import ssl（$(python3 -c 'import sys; print(sys.executable, sys.version.split()[0])')），请检查镜像" >&2
    exit 1
}

echo "create virtualenv"
rm -rf build_venv
python3 -m venv build_venv
source build_venv/bin/activate

echo "install dependencies"

PIP_ARGS=(-i https://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com --retries 5 --timeout 30)
if [ "$(uname -m)" == "aarch64" ]; then
    python -m pip install "${SCRIPT_DIR}[dev,kerberos]" "${PIP_ARGS[@]}"
else
    # CentOS7 x86_64 gssapi
    CFLAGS="-std=c99" python -m pip install "${SCRIPT_DIR}[dev,kerberos]" "${PIP_ARGS[@]}"
fi

# 使用绝对路径
pyinstaller --onefile \
            --name "omnihelper" \
            --distpath "${SCRIPT_DIR}/dist" \
            --workpath "${SCRIPT_DIR}/build" \
            --specpath "${SCRIPT_DIR}" \
            --hidden-import=gssapi \
            --hidden-import=gssapi.raw.cython_converters \
            --collect-all=gssapi \
            "${SCRIPT_DIR}/main.py"

resources_src="${SCRIPT_DIR}/resources"
resources_dst="${SCRIPT_DIR}/dist/resources"
archive_name="omnihelper_release"

# 检查资源目录是否存在
if [ ! -d "$resources_src" ]; then
    echo "资源目录 $resources_src 不存在"
    exit 1
fi

# 复制资源目录到dist
if [ -d "$resources_dst" ]; then
    rm -rf "$resources_dst"
fi

cp -r "$resources_src" "$resources_dst"

echo "资源文件已复制到 $resources_dst"

dist_dir="${SCRIPT_DIR}/dist"

cd "$dist_dir" || exit 1

tar -czf "${SCRIPT_DIR}/${archive_name}_${ARCH}.tar.gz" .

echo "打包完成：${SCRIPT_DIR}/${archive_name}_${ARCH}.tar.gz"
