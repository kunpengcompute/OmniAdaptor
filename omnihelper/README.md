# OmniHelper 大数据日志分析工具<a name="ZH-CN_TOPIC_0000002515818290"></a>

## 最新消息<a name="ZH-CN_TOPIC_0000002547298195"></a>

- \[2025.02.04\]：支持算子识别，修复函数表达式名称/类型匹配问题。
- \[2025.01.15\]：上线OmniOperator 1.0.0，支持高效分析执行日志中表达式/函数使用情况。

## 项目简介<a name="ZH-CN_TOPIC_0000002547258199"></a>

### 简介<a name="ZH-CN_TOPIC_0000002515658378"></a>

OmniHelper 是专为大数据平台设计的日志分析工具，旨在帮助开发者和运维人员高效分析执行日志中的算子/表达式/函数使用情况。该工具作为Omni生态的配套组件，能够识别原生算子与Omni算子的混合执行情况，分析相关参数类型，识别不支持的算子/表达式/函数并生成结构化的分析报告，为性能优化提供数据支持。

### 架构介绍<a name="ZH-CN_TOPIC_0000002547298205"></a>

核心架构组件包括：
1. 命令行接口层：使用命令行解析模块构建命令行参数解析，支持输入路径、输出路径、Java配置等参数，提供友好的帮助信息和使用示例。
2. 日志解析模块：日志文件处理，支持通过正则表达式自动识别日志文件模式，单个或批量处理日志文件。
3. 算子/函数/表达式分析模块：高效分析执行日志中的算子/表达式/函数使用情况，识别不支持部分并提取算子执行时间和资源消耗情况。
4. 结果处理模块：合并多个任务的分析结果，计算统计信息并生成带样式的Excel报告

### 应用场景<a name="ZH-CN_TOPIC_0000002515818282"></a>

OmniHelper 主要应用于大数据平台设计的日志分析，通过高效分析执行日志中的算子/表达式/函数使用情况，识别不支持的算子/表达式/函数并生成结构化的分析报告，为性能优化提供数据支持。


### 相关概念<a name="ZH-CN_TOPIC_0000002547299019"></a>

- Omni算子：高性能算子，使用Native Code（C/C++）替换了大数据底层的物理算子，提升了计算速度。

## 约束与限制<a name="ZH-CN_TOPIC_0000002515665484"></a>

### 公共约束<a name="ZH-CN_TOPIC_0000002547305319"></a>

为了更准确地规划与使用OmniHelper 工具，建议合理规避可能的风险和限制。
- 支持分析的白名单范围限制：当前分析基于resources文件夹内的白名单对算子/函数/表达式的支持性进行判断，如需扩展分析范围需对应扩展白名单。
- 日志文件限制：当前分析基于大数据日志文件，要求采集的日志文件中包含算子信息，且日志文件不可进行截断。

## 目录结构<a name="ZH-CN_TOPIC_0000002547258197"></a>

项目全量目录层级介绍如下：
```

├── omnihelper/                                             # 项目主目录
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

## 版本说明<a name="ZH-CN_TOPIC_0000002515658372"></a>

每个版本的特性变更详细信息，请参见[release_notes.md](docs/release_notes.md)。

## 环境部署<a name="ZH-CN_TOPIC_0000002515658370"></a>

1、安装 Java 环境
推荐使用 JDK 1.8 版本，并确保 java 命令可用或在参数中指定完整路径。

2、准备依赖 JAR 包

（1）日志解析依赖包：
- 从resources文件夹中获取 boostkit-omnimv-logparser-spark-3.4.3-1.2.0-aarch64.jar依赖包

（2）自行构建 JAR 包（可选）：
- 如未使用 resources 中的预编译包，可按以下步骤自行构建：
- 构建环境要求：JDK 1.8;Maven 3.6+, Linux/macOS 或 Windows（推荐使用 Git Bash / WSL 执行脚本）

  ① 进入构建目录
  ```
  cd omnihelper/omnimv-spark-extension
  ```
  ② 执行构建脚本：
  ```
  bash build.sh
  ```
  构建完成后，生成的 JAR 包会自动拷贝至：
  ```
  omnihelper/resources/
  ```
  生成文件示例：
  ```
  boostkit-omnimv-logparser-spark-3.4.3-1.2.0-aarch64.jar
  ```

（3）Spark相关依赖包：
- 通过[链接](https://repo.huaweicloud.com/apache/spark/spark-3.4.3/)获取spark-3.4.3-bin-hadoop3.tgz包，解压后的jars文件夹中包含Spark相关依赖

3、准备事件日志：
- 支持单文件或目录输入（注意需要通过参数配置输出算子信息且不进行日志截断）
- 日志文件格式支持.lz4/.zstd/纯文本格式

## 快速入门<a name="ZH-CN_TOPIC_0000002547258201"></a>

### 使用方法<a name="ZH-CN_TOPIC_0000002547269013"></a>

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


### 使用示例<a name="ZH-CN_TOPIC_0000002515658378"></a>
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

### 自定义函数配置<a name="ZH-CN_TOPIC_0000002515658378"></a>

支持用户配置自定义函数，工具根据提供的函数名称进行匹配识别，在报告中展示。

#### 配置方法
找到resources目录下的内置文件udf_dictionary.json，填写需要识别的自定义函数，格式如下：
```
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
参数func_name表示自定义函数名，is_support_func表示是否支持该函数，需要识别则填写false，填写后保存即可。

> 注意：若自定义函数与spark内置函数同名，优先识别为自定义函数。



## 安全声明<a name="ZH-CN_TOPIC_0000002547265321"></a>

### 防病毒软件例行检查<a name="ZH-CN_TOPIC_0000002547269013"></a>

定期开展对集群和Spark组件的防病毒扫描，防病毒例行检查会帮助集群免受病毒、恶意代码、间谍软件以及恶意程序，降低系统瘫痪、信息泄露等风险。建议使用业界主流防病毒软件进行防病毒检查。


### 日志控制<a name="ZH-CN_TOPIC_0000002515669178"></a>

- 检查系统是否可以限制单个日志文件的大小。
- 检查日志空间占满后，是否存在机制进行清理。


### 漏洞修复<a name="ZH-CN_TOPIC_0000002515829100"></a>

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

    目前厂商已发布升级补丁以修复漏洞，请参见[Github](https://github.com/netty/netty/security/advisories/GHSA-9vjp-v76f-g363)修复漏洞。


### SSH加固<a name="ZH-CN_TOPIC_0000002547309011"></a>

在部署安装过程中，需要通过SSH连接服务器。由于root用户拥有最高权限，直接使用root用户登录服务器可能会存在安全风险。建议您使用普通用户登录服务器进行安装部署，并建议您通过配置禁止root用户SSH登录的选项，来提升系统安全性。操作步骤：

用户登录系统后检查“/etc/ssh/sshd\_config”配置项“PermitRootLogin“。

- 如果显示no，说明禁止了root用户SSH登录。
- 如果显示yes，说明需要修改PermitRootLogin为no。


## 免责声明<a name="ZH-CN_TOPIC_0000002515818292"></a>

**致OmniHelper使用者**

- 本工具仅供调试和开发之用，使用者需自行承担使用风险，并理解以下内容：
    - 数据处理及删除：用户在使用本工具过程中产生的数据属于用户责任范畴。建议用户在使用完毕后及时删除相关数据，以防信息泄露。
    - 数据保密与传播：使用者了解并同意不得将通过本工具产生的数据随意外发或传播。对于由此产生的信息泄露、数据泄露或其他不良后果，本工具及其开发者概不负责。
    - 用户输入安全性：用户需自行保证输入的命令行的安全性，并承担因输入不当而导致的任何安全风险或损失。对于输入命令行不当所导致的问题，本工具及其开发者概不负责。

- 免责声明范围：本免责声明适用于所有使用本工具的个人或实体。使用本工具即表示您同意并接受本声明的内容，并愿意承担因使用该功能而产生的风险和责任，如有异议请停止使用本工具。
- 在使用本工具之前，请**谨慎阅读并理解以上免责声明的内容**。对于使用本工具所产生的任何问题或疑问，请及时联系开发者。

**致数据所有者**

如果您不希望您的模型或数据集等信息在OmniHelper中被提及，或希望更新OmniHelper中有关的描述，请在GitCode提交issue，我们将根据您的issue要求删除或更新您相关描述。衷心感谢您对OmniHelper的理解和贡献。


## License<a name="ZH-CN_TOPIC_0000002547298197"></a>

本项目的文档适用CC-BY 4.0许可证，具体请参见[LICENSE](docs/LICENSE)文件。


## 贡献声明<a name="ZH-CN_TOPIC_0000002547298203"></a>

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


## 法律声明<a name="ZH-CN_TOPIC_0000002515818288"></a>


## 建议与交流<a name="ZH-CN_TOPIC_0000002547258203"></a>

欢迎大家为社区做贡献。如果有任何疑问或建议，请提交[Issues](https://gitcode.com/openeuler/OmniAdaptor)，我们会尽快回复。感谢您的支持。


## 致谢<a name="ZH-CN_TOPIC_0000002515658382"></a>

OmniHelper由华为公司的下列部门联合贡献：

- 鲲鹏计算DevKit开发部
- 鲲鹏计算BoostKit开发部

感谢来自社区的每一个PR，欢迎贡献OmniHelper！