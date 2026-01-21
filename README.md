# Kinesis Producer Utility

一个简单的工具库，用于向 Amazon Kinesis Data Streams 发送 String 类型的消息。

## 特性

- 基于 AWS SDK 2.x 构建
- 支持发送 String 类型消息
- 简单易用的 API

## 依赖要求

- Java 11 或更高版本
- Maven 3.6 或更高版本

## 安装

将此项目构建并安装到本地 Maven 仓库：

```bash
mvn clean install
```

在其他项目中使用时，添加以下依赖：

```xml
<dependency>
    <groupId>com.example</groupId>
    <artifactId>kinesis-producer-util</artifactId>
    <version>1.0.0</version>
</dependency>
```

## 使用方法

```java
import com.example.kinesis.producer.StringKinesisProducer;
import com.example.kinesis.producer.KinesisProducerConfig;
import software.amazon.awssdk.regions.Region;

// 创建配置
KinesisProducerConfig config = new KinesisProducerConfig()
    .setRegion(Region.US_EAST_1);

// 创建 Producer
StringKinesisProducer producer = new StringKinesisProducer(config);

// 发送消息
try {
    PutRecordResponse response = producer.send("my-stream", "partition-key-1", "Hello Kinesis!");
    System.out.println("Sequence number: " + response.sequenceNumber());
} catch (Exception e) {
    e.printStackTrace();
}

// 关闭 Producer
producer.close();
```

## 配置选项

`KinesisProducerConfig` 支持以下配置：

- `region`: AWS 区域（可选）

## AWS 凭证配置

AWS SDK 2.x 会自动从以下位置查找凭证：

1. Java 系统属性
2. 环境变量
3. 默认凭证文件（`~/.aws/credentials`）
4. IAM 角色（在 EC2/ECS/Lambda 上运行时）

## 许可证

MIT License
