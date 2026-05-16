# OmniHelper 大数据日志分析工具

## 最新消息

- \[2025.02.04\]：新增算子识别能力，修复函数表达式名称/类型匹配问题。
- \[2025.01.15\]：首次正式发布OmniOperator 1.0.0。支持执行日志中表达式、函数使用情况的高效分析。

## 项目简介

### 简介

OmniHelper是专为大数据平台设计的日志分析工具，旨在帮助开发者和运维人员高效分析执行日志中的算子、表达式和函数使用情况。该工具作为Omni生态的配套组件，能够识别原生算子与Omni算子的混合执行情况，分析相关参数类型，识别不支持的算子/表达式/函数并生成结构化的分析报告，为性能优化提供数据支持。

### 架构介绍

核心架构组件包括：

- 命令行接口层：使用命令行解析模块构建命令行参数解析，支持输入路径、输出路径、Java配置等参数，提供友好的帮助信息和使用示例。
- 日志解析模块：日志文件处理，支持通过正则表达式自动识别日志文件模式，单个或批量处理日志文件。
- 算子/函数/表达式分析模块：高效分析执行日志中的算子/表达式/函数使用情况，识别不支持部分并提取算子执行时间和资源消耗情况。
- 结果处理模块：合并多个任务的分析结果，计算统计信息并生成带样式的Excel报告。

### 应用场景

OmniHelper主要应用于大数据平台设计的日志分析，通过高效分析执行日志中的算子、表达式、函数使用情况，识别不支持的算子、表达式、函数并生成结构化的分析报告，为性能优化提供数据支持。


### 相关概念

Omni算子：高性能算子，使用Native Code（C/C++）替换了大数据底层的物理算子，提升了计算速度。

## 约束与限制

### 公共约束

为了更准确地规划与使用OmniHelper 工具，建议合理规避可能的风险和限制。

- 支持分析的白名单范围限制：当前分析基于resources文件夹内的白名单对算子、函数、表达式的支持性进行判断，如需扩展分析范围需对应扩展白名单。
- 日志文件限制：当前分析基于大数据日志文件，要求采集的日志文件中包含算子信息，且日志文件不可进行截断。

## 目录结构

项目全量目录层级介绍如下：
```

├── omnihelper/                                             # 项目主目录
│   ├── docs/                                               # 文档目录
│   │   ├── release_notes.md                                # OmniHelper版本说明书
│   ├── enum/                                               # 枚举类型定义目录
│   │   ├── type_enum.py                                    # 数据类型枚举
│   │   └── function_enum.py                                # 函数类型枚举
│   ├── util/                                               # 工具类目录
│   │   ├── excel_util.py                                   # Excel处理工具
│   │   ├── common_util.py                                  # 通用工具函数
│   │   └── func_util.py                                    # 函数处理工具
│   ├── parser/                                             # 日志解析模块目录
│   │   ├── op_parser.py                                    # 算子解析器
│   │   ├── function_parser.py                              # 函数解析器
│   │   ├── type_matcher.py                                 # 类型匹配器
│   │   └── function_checker.py                             # 函数校验器
│   ├── resources/                                          # 资源文件目录
│   │   ├── udf_dictionary.json                             # UDF字典
│   │   ├── omni_op_dictionary.json                         # Omni算子字典
│   │   ├── omni_function_dictionary.json                   # Omni函数字典
│   │   └── omni_opname_mapping_dictionary.json             # 算子名称映射字典
│   ├── main.py                                             # 主程序入口
│   ├── build.sh                                            # 构建脚本
│   ├── README.md                                           # 项目说明文档
│   └── __init__.py                                         # Python包初始化文件
```

## 版本说明

每个版本的特性变更详细信息，请参见《[版本说明书](docs/zh/release_notes.md)》。

## 环境部署

1. 安装Java环境。
     推荐使用JDK 1.8版本，并确保**java**命令可用，或在参数中指定Java的完整执行路径。

2. 准备依赖JAR包。

     - 方式一：从`resources`目录获取预编译包boostkit-omnimv-logparser-spark-3.4.3-1.2.0-aarch64.jar。

     - 方式二：如果没有使用`resources`目录的预编译包，可按照下步骤自行构建。
         1. 确保已准备好构建环境。<br>构建环境要求：JDK 1.8，Maven 3.6+, Linux/macOS或Windows（推荐使用Git Bash/WSL执行脚本）。

         2. 进入构建目录

             ```bash
             cd omnihelper/omnimv-spark-extension
             ```

         3. 执行构建脚本。

             ```bash
             bash build.sh
             ```

             构建完成后，生成的JAR包会自动拷贝至`omnihelper/resources/`。<br>
             生成文件示例：

              ```bash
              boostkit-omnimv-logparser-spark-3.4.3-1.2.0-aarch64.jar

             ```

         4. 下载Spark相关依赖包。<br>获取并解压[spark-3.4.3-bin-hadoop3.tgz](https://repo.huaweicloud.com/apache/spark/spark-3.4.3/)，解压后的jars文件夹中包含Spark的相关依赖。

3. 准备事件日志。
     - 支持单文件或目录输入。注意需要通过参数配置输出算子信息且不进行日志截断。
     - 日志文件格式支持.lz4/.zstd/纯文本格式。

4. 准备Spark表结构信息。<br>为了提升类型识别的准确性，需导出Spark表结构至`resources/spark_table_schema.csv`，文件需包含三列：`full_table_name`（库名。表名）、`column_name`（列名）、`data_type`（数据类型）。其中表名使用数据库名+"."+表名的格式，含逗号的类型需用双引号包裹。

     表结构导出示例：

     ```csv
     full_table_name,column_name,data_type
     test_db.table1,column1,bigint
     test_db.table1,column2,double
     test_db.table1,column3,"map<string,string>"
     test_db3.table2,column1,"decimal(20,4)"
     ```

     导出Spark表结构的方法可以参考以下基于PySpark的脚本方法：<br>
     1. 安装PySpark。

         ```bash
         pip3 install pyspark==3.4.3
         ```

         请将版本号`3.4.3`替换为您实际使用的Spark版本。
     2. 执行脚本。
     
            ```python
            import csv
            import os
            from pyspark.sql import SparkSession

            def export_spark_schema_to_csv(output_path):
                """
                导出Spark所有数据库的表结构到本地CSV文件
                """
                print("正在初始化 SparkSession...")
                # 初始化 SparkSession
                spark = SparkSession.builder.appName("ExportSparkSchema").enableHiveSupport().getOrCreate()

                print("开始获取数据库列表...")
                # 获取所有数据库
                databases = spark.catalog.listDatabases()
                
                rows_data = []
                
                total_dbs = len(databases)
                print(f"共发现 {total_dbs} 个数据库，开始遍历...")

                for db_index, db in enumerate(databases):
                    db_name = db.name
                    print(f"[{db_index + 1}/{total_dbs}] 正在处理数据库: {db_name}")
                    
                    try:
                        # 获取当前数据库下的所有表
                        tables = spark.catalog.listTables(db_name)
                        
                        if not tables:
                            continue

                        for table in tables:
                            # 构造完整的表名 (数据库.表名)
                            full_table_name = f"{db_name}.{table.name}"
                            
                            # 获取该表的列信息
                            # listColumns 返回的是 List[Column]，包含 name, dataType, nullable 等属性
                            columns = spark.catalog.listColumns(table.name, db_name)
                            
                            for col in columns:
                                rows_data.append({
                                    "full_table_name": full_table_name,
                                    "column_name": col.name,
                                    "data_type": col.dataType
                                })
                                
                    except Exception as e:
                        print(f"处理数据库 {db_name} 时出错: {str(e)}")
                        continue

                print(f"数据收集完成，共收集到 {len(rows_data)} 条列信息。")
                
                # 写入本地CSV文件
                print(f"正在写入本地文件: {output_path} ...")
                try:
                    with open(output_path, mode='w', newline='', encoding='utf-8') as csvfile:
                        fieldnames = ['full_table_name', 'column_name', 'data_type']
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                        
                        # 写入表头
                        writer.writeheader()
                        # 写入数据行
                        writer.writerows(rows_data)
                        
                    print("导出成功！")
                    
                except IOError as e:
                    print(f"文件写入失败: {e}")

                # 结束 SparkSession
                spark.stop()

            if __name__ == "__main__":
                # 定义输出文件路径
                output_file = "spark_table_schema.csv"
                
                # 检查文件是否已存在，避免追加写入混乱
                if os.path.exists(output_file):
                    os.remove(output_file)
                    
                export_spark_schema_to_csv(output_file)
            ```


## 快速入门

### 环境准备

  1. 解压BoostKit-omniruntime-omnihelper-*.zip文件。

     ```bash
     unzip BoostKit-omniruntime-omnihelper-*.zip
     ```

 2. 进入解压后的文件夹，解压对应压缩包。
     - Arm架构：

     ```bash
     tar -zxvf omnihelper_release_arm.tar.gz
     ```

     - x86架构：

     ```bash
     tar -zxvf omnihelper_release_x86.tar.gz 
     ```


### 命令行使用方法

**命令格式**

```bash
omnihelper [-h] --input_data INPUT_DATA [--output_dir OUTPUT_DIR]
           [--show-op-details] [--java-path JAVA_PATH] --class-path CLASS_PATH
```
**参数说明**


**表1** 基础参数
|参数|简写|是否必选|说明
|--|--|--|--|
|--help|-h|否|查看帮助信息.|
|--input_data|-i|是|输入路径（目录或单个文件）。<br>支持`.lz4`/`.zstd`格式单文件直接处理。|
|--output_dir|-o|否|输出目录。<br>默认值：`./output`。|
|--show-op-details|-s|否|隐藏算子文件大小、输出行数信息。|

**表2** Java 配置参数
|参数|是否必选|说明
|--|--|--|
|--java-path|否|Java可执行文件路径，默认调用系统PATH中的`java`。|
|--class-path|是|完整Java类路径（需包含解析依赖JAR包。）|


**使用方法**

```bash
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

**使用示例**

**示例一**： 解析单个日志文件。

     ```bash
     ./omnihelper -i ./input_data/eventlog.lz4 -o ./output_dir
     --java-path /path/to/java/bin/java
     --class-path /path/to/boostkit-omnimv-logparser-spark-3.4.3-1.2.0-aarch64.jar:/path/to/spark-3.4.3-bin-hadoop3/jars/*
     ```

**示例二**： 解析日志文件目录。

     ```bash
     ./omnihelper -i ./input_dir -o ./output \
     --java-path /usr/local/jdk1.8/bin/java \
     --class-path /opt/omnihelper/resources/boostkit-omnimv-logparser-spark-3.4.3-1.2.0-aarch64.jar:/opt/spark-3.4.3-bin-hadoop3/jars/*
     ```

**示例三**：解析日志文件目录并隐藏算子file sizes和output rows信息。

     ```bash
     ./omnihelper -i ./input_dir -o ./output_dir -s
     --java-path /path/to/java/bin/java
     --class-path /path/to/boostkit-omnimv-logparser-spark-3.4.3-1.2.0-aarch64.jar:/path/to/spark-3.4.3-bin-hadoop3/jars/*
     ```

### 自定义函数配置

支持用户配置自定义函数识别规则，修改`resources/udf_dictionary.json`即可。<br>
找到`resources`目录下的内置文件`udf_dictionary.json`，填写需要识别的自定义函数，格式如下：

```json
[
    {
        "func_name": "int_plus_10",
        "is_support_func": false
    },
    {
        "func_name": "abs",
        "is_support_func": false
    }
]
```

字段说明：

- func_name表示自定义函数名称。
- is_support_func表示是否标记为支持该函数，需要识别则填写`false`。

优先级规则：<br>
若自定义函数与Spark内置函数同名，则优先识别为自定义函数。


**分析报告参数说明**

在Omni不支持的表达式或内置函数的识别中，`Input`列默认表示函数的输入参数类型，特殊参数说明：

- cast函数：`Input`列表示源参数类型和目标参数类型，例如`cast(c_int as long)`，返回源参数`c_int`的类型和目标参数类型`long`。
- if、case函数：`Input`列表示结果的参数类型，例如`if(a==1) 0 else 1`，返回结果`0`的类型`a`。


## 安全声明

### 防病毒软件例行检查

定期开展对集群和Spark组件的防病毒扫描，防病毒例行检查会帮助集群免受病毒、恶意代码、间谍软件以及恶意程序，降低系统瘫痪、信息泄露等风险。建议使用业界主流防病毒软件进行防病毒检查。


### 日志控制

- 检查系统是否可以限制单个日志文件的大小。
- 检查日志空间占满后，是否存在机制进行清理。


### 漏洞修复

为保证生产环境的安全，降低被攻击的风险，请开启防火墙，并定期修复以下漏洞。

- 操作系统漏洞
- JDK漏洞
- Spark漏洞
- 其他相关组件漏洞

    以CVE-2021-37137为例。

    漏洞描述：

    Netty 4.1.17版本存在两个Content-Length的http header可能会发生混淆的风险通告，漏洞编号：CVE-2021-37137。

    本系统使用hdfs-ceph（version 3.2.0）服务作为存算分离的存储对象，它因依赖aws-java-sdk-bundle-1.11.375.jar而涉及该漏洞。建议用户及时更新漏洞补丁进行防护，以免遭受黑客攻击。

    影响范围：

    Netty 4.1.68及以前版本。

    修复建议：

    目前厂商已发布升级补丁以修复漏洞，请参见[GitHub](https://github.com/netty/netty/security/advisories/GHSA-9vjp-v76f-g363)修复漏洞。


### SSH加固

在部署安装过程中，需要通过SSH连接服务器。由于root用户拥有最高权限，直接使用root用户登录服务器可能会存在安全风险。建议您使用普通用户登录服务器进行安装部署，并建议您通过配置禁止root用户SSH登录的选项，来提升系统安全性。操作步骤：

用户登录系统后检查`/etc/ssh/sshd\_config`配置项`PermitRootLogin`。

- 如果显示`no`，说明禁止了root用户SSH登录。
- 如果显示`yes`，说明需要修改PermitRootLogin为`no`。


## 免责声明

**致OmniHelper使用者**

- 本工具仅供调试和开发之用，使用者需自行承担使用风险，并理解以下内容：
    - 数据处理及删除：用户在使用本工具过程中产生的数据属于用户责任范畴。建议用户在使用完毕后及时删除相关数据，以防信息泄露。
    - 数据保密与传播：使用者了解并同意不得将通过本工具产生的数据随意外发或传播。对于由此产生的信息泄露、数据泄露或其他不良后果，本工具及其开发者概不负责。
    - 用户输入安全性：用户需自行保证输入的命令行的安全性，并承担因输入不当而导致的任何安全风险或损失。对于输入命令行不当所导致的问题，本工具及其开发者概不负责。

- 免责声明范围：本免责声明适用于所有使用本工具的个人或实体。使用本工具即表示您同意并接受本声明的内容，并愿意承担因使用该功能而产生的风险和责任，如有异议请停止使用本工具。
- 在使用本工具之前，请**谨慎阅读并理解以上免责声明的内容**。对于使用本工具所产生的任何问题或疑问，请及时联系开发者。

**致数据所有者**

如果您不希望您的模型或数据集等信息在OmniHelper中被提及，或希望更新OmniHelper中有关的描述，请在GitCode提交issue，我们将根据您的issue要求删除或更新您相关描述。衷心感谢您对OmniHelper的理解和贡献。


## License

本项目的文档适用CC-BY 4.0许可证，具体请参见[LICENSE](docs/LICENSE)文件。


## 贡献声明

1. 提交错误报告：如果您在OmniHelper中发现了一个不存在安全问题的漏洞，请在OmniHelper仓库中的Issues中搜索，以防该漏洞被重复提交，如果找不到漏洞可以创建一个新的Issues。如果发现了一个安全问题请不要将其公开，请参阅安全问题处理方式。提交错误报告时应该包含完整信息。
2. 安全问题处理：本项目中对安全问题处理的形式，请通过邮箱通知项目核心人员确认编辑。
3. 解决现有问题：通过查看仓库的Issues列表可以发现需要处理的问题信息，可以尝试解决其中的某个问题。
4. 如何提出新功能：请使用Issues的Feature标签进行标记，我们会定期处理和确认开发。
5. 开始贡献：
    1. Fork本项目的仓库。
    2. Clone到本地。
    3. 创建开发分支。
    4. 本地测试：提交前请通过所有单元测试，包括新增的测试用例。
    5. 提交代码。
    6. 新建Pull Request。
    7. 代码检视：您需要根据评审意见修改代码，并重新提交更新。此流程可能涉及多轮迭代。
    8. 当您的PR获得足够数量的检视者批准后，Committer会进行最终审核。
    9. 审核和测试通过后，CI会将您的PR合并入到项目的主干分支。



## 建议与交流

欢迎大家为社区做贡献。如果有任何疑问或建议，请提交[Issues](https://gitcode.com/openeuler/OmniAdaptor)，我们会尽快回复。感谢您的支持。


## 致谢

OmniHelper由华为公司的下列部门联合贡献：

- 鲲鹏计算DevKit开发部
- 鲲鹏计算BoostKit开发部

感谢来自社区的每一个PR，欢迎贡献OmniHelper！
