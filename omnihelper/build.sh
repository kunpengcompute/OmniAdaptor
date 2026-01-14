#!/bin/bash

set -ex

# 获取脚本所在目录的绝对路径
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# 使用绝对路径
pyinstaller --onefile \
            --name omnihelper \
            --distpath "${SCRIPT_DIR}/dist" \
            --workpath "${SCRIPT_DIR}/build" \
            --specpath "${SCRIPT_DIR}" \
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

tar -czf "${SCRIPT_DIR}/${archive_name}.tar.gz" .

echo "打包完成：${SCRIPT_DIR}/${archive_name}.tar.gz"
