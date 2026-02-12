# OmniHelper 大数据日志分析工具

## 项目概述
OmniHelper 是专为大数据平台设计的日志分析工具，旨在帮助开发者和运维人员高效分析执行日志中的算子/表达式/函数使用情况。该工具作为Omni生态的配套组件，能够识别原生算子与Omni算子的混合执行情况，分析相关参数类型，识别不支持的算子/表达式/函数并生成结构化的分析报告，为性能优化提供数据支持。

## 版本兼容性
- 支持架构：x86、ARM
- 支持的操作系统版本：
  - x86: CentOS 7.6
  - ARM: OpenEuler22.03/24.03
- JDK版本：1.8
- Spark版本：3.5.1


## 使用方法

```
usage: omnihelper [-h] --input_data INPUT_DATA [--output_dir OUTPUT_DIR]
                  [--show-op-details] [--java-path JAVA_PATH] --class-path
                  CLASS_PATH

Big Data Operator Scanning Command Line Tool

optional arguments:
  -h, --help            show this help message and exit
  --input_data INPUT_DATA, -i INPUT_DATA
                        Input directory path or single file path (required).
                        If a single .lz4 or .zstd file is provided, only that
                        file will be processed.
  --output_dir OUTPUT_DIR, -o OUTPUT_DIR
                        Output directory path (default: ./output)
  --show-op-details, -s
                        Disable displaying op file sizes and output rows

Java Configuration:
  --java-path JAVA_PATH
                        Java executable path (default: "java" from system
                        PATH)
  --class-path CLASS_PATH
                        Complete Java classpath string
```


## 快速上手
### 环境准备
1、安装 Java 环境
推荐使用 JDK 1.8 版本，并确保 java 命令可用或在参数中指定完整路径。

2、准备依赖 JAR 包

（1）日志解析依赖包：
- 从resources文件夹中获取 boostkit-omnimv-logparser-spark-3.4.3-1.2.0-aarch64.jar依赖包

（2）Spark相关依赖包：
- 通过[链接](https://repo.huaweicloud.com/apache/spark/spark-3.4.3/)获取spark-3.4.3-bin-hadoop3.tgz包，解压后的jars文件夹中包含Spark相关依赖

3、准备事件日志：
- 支持单文件或目录输入（注意需要通过参数配置输出算子信息且不进行日志截断）
- 日志文件格式支持.lz4/.zstd/纯文本格式


### 使用示例
1、解压BoostKit-omniruntime-omnihelper-*.zip文件。
```
unzip BoostKit-omniruntime-omnihelper-*.zip
```
2、进入解压后的文件夹，解压对应压缩包。
```
tar -zxvf omnihelper_release_arm.tar.gz # ARM架构
tar -zxvf omnihelper_release_x86.tar.gz # x86架构
```
3、解析单个日志文件。
```
./omnihelper -i ./input_data/eventlog.lz4 -o ./output_dir
--java-path /path/to/java/bin/java
--class-path /path/to/boostkit-omnimv-logparser-spark-3.4.3-1.2.0-aarch64.jar:/path/to/spark-3.4.3-bin-hadoop3/jars/*
```
4、解析日志文件目录。
```
./omnihelper -i ./input_dir -o ./output_dir
--java-path /path/to/java/bin/java
--class-path /path/to/boostkit-omnimv-logparser-spark-3.4.3-1.2.0-aarch64.jar:/path/to/spark-3.4.3-bin-hadoop3/jars/*
```
5、解析日志文件目录，结果不显示算子file sizes和output rows信息。
```
./omnihelper -i ./input_dir -o ./output_dir -s
--java-path /path/to/java/bin/java
--class-path /path/to/boostkit-omnimv-logparser-spark-3.4.3-1.2.0-aarch64.jar:/path/to/spark-3.4.3-bin-hadoop3/jars/*
```