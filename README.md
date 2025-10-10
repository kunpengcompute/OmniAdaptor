# 项目介绍

## OmniAdaptor-Flink介绍

OmniAdaptor-Flink是Flink与OmniStream之间的桥梁组件，作为胶水层承接两者的运行时协作。OmniAdaptor-Flink基于Flink原生算子执行计划生成Native算子执行计划，将Flink原生算子替换为OmniStream的Native算子，使能OmniStream算子加速，并提供Naitve Runtime Framework，调度Native算子完成Flink任务。目前已覆盖Nexmark、wordcount等查询中主要算子的Native加速路径，同时保证了与Flink原生算子的100%兼容。

## OmniAdapter-Spark介绍

OmniAdaptor-Spark是Spark与OmniOperator之间的桥梁组件，作为胶水层承接两者的运行时协作。OmniAdaptor-Spark基于Spark原生算子执行计划生成Native算子执行计划，将Flink原生算子替换为OmniOperator的Native算子，使能OmniOperator算子加速。



# 版本说明

**当前版本适用于开源软件哪个版本，如**

| 开源软件 | 开源版本            |
| -------- | ------------------- |
| Flink    | 1.16.3              |
| spark    | 3.3.1、3.4.3、3.5.2 |

# 快速上手

### OmniAdapter-Flink 快速上手

####  编译命令

OmniAdaptor-Flink编译命令如下

```
cd omnistream/omniop-flink-extension/omni-flink-bundle
mvn clean package -DskipTests
```

#### 环境部署

OmniAdaptor-Flink部署请参考以下链接：

https://www.hikunpeng.com/document/detail/zh/kunpengbds/appAccelFeatures/sqlqueryaccelf/kunpengbds_omniruntime_20_09018.html

#### 测试验证

OmniAdaptor-Flink验证前需要安装OmniStream。进入Flink安装目录下的bin目录，并启动Flink

```
cd $FLINK_HOME/bin/ && ./start-cluster.sh
```

调用sql-client后，进行测试

```
./sql-client.sh
```

在命令行中输入

```
SELECT 'Hello, Flink!';
```

可以正常输出结果即安装正常。

### OmniAdapter-Spark 快速上手

#### 编译命令

````shell
# profile_id可选3.3、3.4、3.5
export profile_id=3.3
cd omniop-spark-extension
mvn clean package -Pspark-${profile_id} -DskipTests -Ddep.os.arch=aarch64 -Dscoverage.skip=true
````

注：OmniAdapter-Spark编译和运行均依赖OmniOperator，若未编译部署OmniOperator，请先参考OmniOperator 相关文档进行编译部署。

#### 环境部署

OmniAdaptor-Spark部署请参考以下链接：

https://www.hikunpeng.com/document/detail/zh/kunpengbds/appAccelFeatures/sqlqueryaccelf/kunpengbds_omniruntime_20_0212.html

#### 测试验证

* 测试步骤：

1. 使用hive-testbench导入2GB TPCDS数据集
2. 参考下面文档链接，添加omni相关配置文件，并提交sql执行
3. 检查执行计划中的算子是否包含Omni

详细测试验证步骤请参考以下链接：

https://www.hikunpeng.com/document/detail/zh/kunpengbds/appAccelFeatures/sqlqueryaccelf/kunpengbds_omniruntime_20_0241.html



# 贡献指南

如果使用过程中有任何问题，或者需要反馈特性需求和bug报告，可以提交isssues联系我们，具体贡献方法可参考[这里](https://gitcode.com/boostkit/community/blob/master/docs/contributor/contributing.md)。

# 免责声明

此代码仓计划参与Flink、Spark软件开源，仅作Flink、Spark功能扩展/Flink、Spark性能提升，编码风格遵照原生开源软件，继承原生开源软件安全设计，不破坏原生开源软件设计及编码风格和方式，软件的任何漏洞与安全问题，均由相应的上游社区根据其漏洞和安全响应机制解决。请密切关注上游社区发布的通知和版本更新。鲲鹏计算社区对软件的漏洞及安全问题不承担任何责任。

# 许可证书

**若是参与开源，参考上游社区所用开源协议，在代码仓根目录下放置LICENSE文件。
若是主导开源，则自行决定开源协议类型，然后同样代码仓根目录下放置LICENSE文件**

# 参考文档（可选）

OmniAdapter-Flink 安装指南：

https://www.hikunpeng.com/document/detail/zh/kunpengbds/appAccelFeatures/sqlqueryaccelf/kunpengbds_omniruntime_20_09018.html

OmniAdapter-Spark 安装文档：

https://www.hikunpeng.com/document/detail/zh/kunpengbds/appAccelFeatures/sqlqueryaccelf/kunpengbds_omniruntime_20_0212.html

开发者文档（链接）
测试指导文档（链接）