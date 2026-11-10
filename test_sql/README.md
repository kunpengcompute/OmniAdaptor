完整流程编译流程：
# Omnistream编译
cd /opt/buildtools/OmniStream/cpp/build
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