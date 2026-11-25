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

set -e # 命令执行失败时异常退出
set +x # 关闭命令打印调试；调试时开启set -x

# 定义需外部传入的参数
REPO_URL=${REPO_URL} # 代码仓地址，例如https://gitcode.com/xx/xxx.git
echo "[INFO] 代码仓: ${REPO_URL}"

PR_ID=${PR_ID} # PR号，例如11,取自https://gitcode.com/xx/xxx/pull/11
echo "[INFO] PR号: ${PR_ID}"

TARGET_BRANCH=${TARGET_BRANCH} # PR待合入的目标分支
echo "[INFO] 目标分支: ${TARGET_BRANCH}"

# 新增：基础入参非空校验
if [[ -z "${REPO_URL}" || -z "${PR_ID}" || -z "${TARGET_BRANCH}" ]]; then
    echo "[ERROR] REPO_URL / PR_ID / TARGET_BRANCH 环境变量不能为空"
    exit 1
fi

# 脚本标题
echo "================================================"
echo "          Pre-Commit CI 增量检查"
echo "================================================"

# 处理凭据认证，仅私仓需要配置，需要外部传入GIT_TOKEN
# 修复原逻辑判断错误：GIT_TOKEN是字符串而非文件路径
if [[ -n "${GIT_TOKEN}" ]]; then
	echo "GIT_TOKEN已传入，开始设置代码仓凭据"
	# 自动从REPO_URL 提取域名
	REPO_DOMAIN=$(echo "${REPO_URL}" | awk -F/ '{print $3}')
	# 配置 Git 凭据
	git config --global credential.helper store
	echo "https://oauth2:${GIT_TOKEN}@${REPO_DOMAIN}" >~/.git-credentials
fi

# ================================================================

# 克隆PR目标分支代码仓到指定目录
SOURCE_CODE_DIR="source_code"                                # 克隆到本地的代码仓目录
rm -rf ${SOURCE_CODE_DIR}                                    # 清理可能存在的缓存
git clone ${REPO_URL} -b ${TARGET_BRANCH} ${SOURCE_CODE_DIR} # 克隆代码仓目标分支
cd ${SOURCE_CODE_DIR}                                        # 切换到本地代码仓根目录

# 通用Git配置
git config --global user.email "openlibing-robot@openlibing.com"
git config --global user.name "openlibing-robot"
git config core.quotePath false # 配置 Git 中文文件名支持

# 拉取PR源分支代码
echo -e "\n[INFO] 拉取源分支代码"
LOCAL_SOURCE_BRANCH="pr_${PR_ID}" # 拉取到本地的源分支名称
git fetch origin refs/merge-requests/${PR_ID}/head:${LOCAL_SOURCE_BRANCH}

# 预合并目标分支代码
git pull                             # 更新最新的远程分支代码
git checkout ${LOCAL_SOURCE_BRANCH}  # 切换到本地源分支
git merge ${TARGET_BRANCH} --no-edit # 合并目标分支

# 配置文件.pre-commit-config.yaml校验
PRE_COMMIT_CONFIG_YAML=".pre-commit-config.yaml"
if [ ! -f ${PRE_COMMIT_CONFIG_YAML} ]; then
	echo "[SUCCESS] 未找到${PRE_COMMIT_CONFIG_YAML}，检查通过"
	exit 0
fi

# 获取变更文件列表
echo -e "\n[INFO] 获取变更文件列表"
# 替换原有不安全数组读取，兼容带空格文件名，移除SC2207屏蔽注释
FILES_ARR=()
while IFS= read -r file; do
    FILES_ARR+=("$file")
done < <(git diff --name-only --diff-filter=ACMR origin/${TARGET_BRANCH} HEAD | sort -u)

# 【适配Scala/Java多模块】过滤编译产物、生成代码，跳过无需pre-commit扫描文件
FILTERED_FILES=()
for f in "${FILES_ARR[@]}"; do
    # 过滤所有子模块target、生成目录、class/jar包
    if [[ "$f" =~ target/ || "$f" =~ generated/ || "$f" =~ \.class$ || "$f" =~ \.jar$ ]]; then
        continue
    fi
    FILTERED_FILES+=("$f")
done
FILES_ARR=("${FILTERED_FILES[@]}")

# 无变更文件直接退出
if [ ${#FILES_ARR[@]} -eq 0 ]; then
	echo "[INFO] 无变更文件，检查通过"
	exit 0
fi

# 输出变更文件信息
echo -e "\n[INFO] 变更文件数量: ${#FILES_ARR[@]}"
echo "[INFO] 变更文件列表:"
for f in "${FILES_ARR[@]}"; do echo "  $f"; done

# 安装pre-commit工具
echo -e "\n[INFO] 安装 pre-commit"
pip config set global.index-url https://repo.huaweicloud.com/repository/pypi/simple # 配置国内pip源加速
pip config set global.trusted-host repo.huaweicloud.com
pip install pre-commit # 安装pre-commit
pre-commit --version

# 执行pre-commit检查
echo -e "\n[INFO] 开始 pre-commit 检查"
echo "[COMMAND] pre-commit run --files ${FILES_ARR[*]}"
set +e
pre-commit run --show-diff-on-failure --files "${FILES_ARR[@]}"
CODE=$?
set -e

# 输出检查结果
echo -e "\n================================================================"
if [ ${CODE} -eq 0 ]; then
	echo "[INFO] pre-commit 检查全部通过"
else
	echo "[ERROR] pre-commit 检查失败"
	echo "[INFO] 请在本地执行以下命令修复后重新提交:"
	echo ""
	echo "1. 安装/初始化环境"
	echo "pip install pre-commit && pre-commit install --install-hooks"
	echo ""
	echo "2. 检查并修复变更文件(密钥/拼写/配置文件类问题)"
	echo "pre-commit run --files ${FILES_ARR[*]}"
	echo ""
	# 新增适配Scala/Java Maven格式化提示
	echo "3. Scala/Java代码格式问题(本脚本不校验，单独执行Maven格式化)"
	echo "进入对应子模块目录执行：mvn spotless:apply"
fi
echo "================================================================"

# 新增：执行完成清理临时源码目录
cd ../
rm -rf ${SOURCE_CODE_DIR}

exit ${CODE}
