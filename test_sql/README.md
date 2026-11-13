cd /opt/notebook_my/install_script/OmniStream
source /etc/profile
bash ./install_omni_adaptor.sh
bash ./install_omni_stream.sh Release


完整table api流程编译流程：
# Omnistream编译
cd /opt/buildtools/OmniStream/cpp/build
export HOME=/opt/buildtools/
cmake -DCMAKE_BUILD_TYPE=Release   ..
make install -j16
查看产物
ls /opt/buildtools/OmniStream/cpp/build/jni/libboostkit-omnistream.so.1.4.0 

# OmniAdaptor编译
cd /opt/buildtools/OmniAdaptor/omnistream/omniop-flink-extension/omni-flink-bundle/
mvn clean package -DskipTests -Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true
查看产物
ls /opt/buildtools/OmniAdaptor/omnistream/omniop-flink-extension/java/target/boostkit-omniadaptor-flink-1.4.0.jar
移动
cp /opt/buildtools/OmniAdaptor/omnistream/omniop-flink-extension/java/target/boostkit-omniadaptor-flink-1.4.0.jar /opt/flink-1.16.3/lib

# table api打包：
mkdir -p /opt/buildtools/top2_udf/com/omnistream/udf/
mkdir -p 	/opt/buildtools/top2_udf/com/omnistream/acceptance/

cd /opt/buildtools/top2_udf

# 1. 只改了 Top2Acceptance.java 时的增量编译（最常用）
javac -encoding UTF-8 -cp "/opt/flink-1.16.3/lib/*:top2-udf.jar" -d . \
    com/omnistream/acceptance/Top2Acceptance.java
jar uf top2-udf.jar com/omnistream/acceptance

# 2. 改了 UDF（Top2/Top3）时
javac -encoding UTF-8 -cp "/opt/flink-1.16.3/lib/*:top2-udf.jar" -d . \
    com/omnistream/udf/Top2.java com/omnistream/udf/Top3.java
jar uf top2-udf.jar com/omnistream/udf

# 3. 从零全量重建（jar 不存在或损坏时）
javac -encoding UTF-8 -cp "/opt/flink-1.16.3/lib/*" -d . \
    com/omnistream/udf/Top2.java com/omnistream/udf/Top3.java
jar cf top2-udf.jar com/omnistream/udf
javac -encoding UTF-8 -cp "/opt/flink-1.16.3/lib/*:top2-udf.jar" -d . \
    com/omnistream/acceptance/Top2Acceptance.java
jar uf top2-udf.jar com/omnistream/acceptance



标准结果验证：
native加速版：
# 1. 确认 OmniAdaptor 在位
ls /opt/flink-1.16.3/lib/boostkit-omniadaptor-flink-1.4.0.jar

# 2. 重启集群（必须带 WRITE_TO_FILE，TM 才会写输出文件）
export WRITE_TO_FILE=TRUE
/opt/flink-1.16.3/bin/stop-cluster.sh && /opt/flink-1.16.3/bin/start-cluster.sh

# 3. 跑作业
rm -f /tmp/flink_output.txt
flink run -c com.omnistream.acceptance.Top2Acceptance /opt/buildtools/top2_udf/top2-udf.jar run

# 4. 取结果
cat /tmp/flink_output.txt



# 1. 移走 OmniAdaptor
mv /opt/flink-1.16.3/lib/boostkit-omniadaptor-flink-1.4.0.jar /root/backup-boostkit.jar

# 2. 重启集群
unset WRITE_TO_FILE
/opt/flink-1.16.3/bin/stop-cluster.sh && /opt/flink-1.16.3/bin/start-cluster.sh

# 3. 跑同一个作业
flink run -c com.omnistream.acceptance.Top2Acceptance /opt/buildtools/top2_udf/top2-udf.jar run

# 4. 取结果（原生 print 输出在 TM 的 .out 里）
grep -E '^[+-][IUD]\[' $(ls -t /opt/flink/log/flink-root-taskexecutor-*.out | head -1) | tail -20 > /tmp/vanilla_out.txt
cat /tmp/vanilla_out.txt

# 5. 跑完恢复 native 环境
mv /root/backup-boostkit.jar /opt/flink-1.16.3/lib/


四种运行模式：
# run    默认模式：Top2 正常验收（native 加速路径）
flink run -c com.omnistream.acceptance.Top2Acceptance /opt/buildtools/top2_udf/top2-udf.jar run

# explain 只打印执行计划，不提交作业（双引擎计划对比用）
flink run -c com.omnistream.acceptance.Top2Acceptance /opt/buildtools/top2_udf/top2-udf.jar explain

# top3   回退验证：注册 Top3 UDF，验证非白名单 UDF 自动回退 Java
flink run -c com.omnistream.acceptance.Top2Acceptance /opt/buildtools/top2_udf/top2-udf.jar top3

# rocksdb 后端验证：换 RocksDB 状态后端跑同作业（状态兼容性）
flink run -c com.omnistream.acceptance.Top2Acceptance /opt/buildtools/top2_udf/top2-udf.jar rocksdb


多场景测试（Top2Scenarios.java + 5 个数据集）：

场景矩阵（每个场景一次作业，输入数据不同、查询/UDF/sink 与 Top2Acceptance 完全相同）：
| 场景        | 数据集            | 覆盖行为                                         | 期望行数(-D/+I) |
|-------------|-------------------|--------------------------------------------------|-----------------|
| single      | top2_single.csv   | 单组单行最小链路（仅首行 +I）                    | 1               |
| down        | top2_down.csv     | 榜满后每行仍全量重发（vanilla 不做差量 diff）    | 16              |
| dupfirst    | top2_dupfirst.csv | 组内首个值就重复：相等值落第二席，5,5 占满两席   | 8               |
| groups      | top2_groups.csv   | 3 组交错输入：keyed 状态组间隔离                 | 16              |
| large       | top2_large.csv    | 60 行 2 组（稳定性/数据量烟测）                  | 234             |

数据集与 top2_bid.csv 同目录（构建机
/opt/notebook_my/install_script/queries/csv_test/GroupTableAggregate/）。

编译打包（增量：只加 Top2Scenarios，不动已有类）：
cd /opt/buildtools/top2_udf
javac -encoding UTF-8 -cp "/opt/flink-1.16.3/lib/*:top2-udf.jar" -d . \
    com/omnistream/acceptance/Top2Scenarios.java
jar uf top2-udf.jar com/omnistream/acceptance

运行某个场景（参数二可选 explain，只打印计划不提交）：
flink run -c com.omnistream.acceptance.Top2Scenarios /opt/buildtools/top2_udf/top2-udf.jar single
flink run -c com.omnistream.acceptance.Top2Scenarios /opt/buildtools/top2_udf/top2-udf.jar down
flink run -c com.omnistream.acceptance.Top2Scenarios /opt/buildtools/top2_udf/top2-udf.jar dupfirst
flink run -c com.omnistream.acceptance.Top2Scenarios /opt/buildtools/top2_udf/top2-udf.jar groups
flink run -c com.omnistream.acceptance.Top2Scenarios /opt/buildtools/top2_udf/top2-udf.jar large

逐场景双引擎比对（native 侧：跑完立刻把输出文件改名保存，防止下一个场景覆盖）：
for s in single down dupfirst groups large; do
    rm -f /tmp/flink_output.txt
    flink run -c com.omnistream.acceptance.Top2Scenarios /opt/buildtools/top2_udf/top2-udf.jar $s
    mv /tmp/flink_output.txt /tmp/native_$s.txt
done
# vanilla 侧：移走 OmniAdaptor 重启后同样循环，输出取自 .out：
for s in single down dupfirst groups large; do
    flink run -c com.omnistream.acceptance.Top2Scenarios /opt/buildtools/top2_udf/top2-udf.jar $s
    grep -E '^[+-][IUD]\[' $(ls -t /opt/flink/log/flink-root-taskexecutor-*.out | head -1) \
        | tail -$(wc -l < /tmp/native_$s.txt) > /tmp/vanilla_$s.txt
done
# 逐场景 diff：
for s in single down dupfirst groups large; do
    sed 's/^\([+-][IUD]\)\[\(.*\)\]$/\1,\2/; s/, /,/g' /tmp/vanilla_$s.txt \
        | diff - /tmp/native_$s.txt > /dev/null && echo "$s: 一致" || echo "$s: 有差异"
done
