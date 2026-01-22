# Kinesis Producer Util

一个用于向 Amazon Kinesis Data Streams 发送消息的工具库。支持发送 String 类型的消息，支持批量发送。

## 特性

- ✅ 支持发送单个 String 消息
- ✅ 支持批量发送多条消息（自动处理 Kinesis 的 500 条记录限制）
- ✅ 使用 AWS SDK for Java 2
- ✅ 自动使用 EKS IAM role credentials（通过 DefaultCredentialsProvider）
- ✅ 完善的错误处理和日志记录

## 依赖要求

- Java 11 或更高版本
- Maven 3.6 或更高版本

## 安装

### 方式一：作为 Maven 依赖添加到其他项目

如果此项目已发布到 Maven 仓库，可以在其他项目的 `pom.xml` 中添加：

```xml
<dependency>
    <groupId>com.example.kinesis</groupId>
    <artifactId>kinesis-producer-util</artifactId>
    <version>1.0.0</version>
</dependency>
```

### 方式二：本地安装

```bash
mvn clean install
```

然后在其他项目的 `pom.xml` 中添加依赖（同上）。

## 使用方法

### 基本配置

```java
import com.example.kinesis.producer.KinesisProducerConfig;
import com.example.kinesis.producer.StringKinesisProducer;
import software.amazon.awssdk.regions.Region;

// 创建配置
KinesisProducerConfig config = KinesisProducerConfig.builder()
    .streamName("my-kinesis-stream")
    .region(Region.US_EAST_1)  // 或使用 .region("us-east-1")
    .partitionKey("my-partition-key")  // 可选，如果不设置，发送时会使用默认值
    .build();

// 创建 Producer
StringKinesisProducer producer = new StringKinesisProducer(config);
```

### 发送单个消息

```java
// 使用配置中的 partition key
producer.send("Hello, Kinesis!");

// 或指定 partition key
producer.send("Hello, Kinesis!", "custom-partition-key");
```

### 批量发送消息

```java
List<String> messages = Arrays.asList(
    "Message 1",
    "Message 2",
    "Message 3"
);

// 使用配置中的 partition key
List<Integer> failedIndices = producer.sendBatch(messages);

// 或指定 partition key
List<Integer> failedIndices = producer.sendBatch(messages, "custom-partition-key");

// 检查失败的记录
if (!failedIndices.isEmpty()) {
    System.out.println("Failed to send messages at indices: " + failedIndices);
}
```

### 资源管理

`StringKinesisProducer` 实现了 `AutoCloseable` 接口，建议使用 try-with-resources：

```java
try (StringKinesisProducer producer = new StringKinesisProducer(config)) {
    producer.send("Message 1");
    producer.sendBatch(messages);
} // 自动关闭客户端
```

## EKS IAM Role 凭证

此工具库使用 AWS SDK for Java 2 的 `DefaultCredentialsProvider`，它会自动按以下顺序查找凭证：

1. **Web Identity Token**（EKS Pod 使用）- 通过环境变量 `AWS_WEB_IDENTITY_TOKEN_FILE` 和 `AWS_ROLE_ARN`
2. 环境变量（`AWS_ACCESS_KEY_ID` 和 `AWS_SECRET_ACCESS_KEY`）
3. Java 系统属性
4. EC2 实例配置文件
5. 共享凭证文件（`~/.aws/credentials`）

在 EKS 环境中，通常通过 ServiceAccount 和 IAM Role 配置，SDK 会自动使用 Web Identity Token 获取临时凭证。

### EKS 配置示例

确保你的 Pod 配置了正确的 ServiceAccount：

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: my-service-account
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::ACCOUNT_ID:role/KinesisProducerRole
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
spec:
  template:
    spec:
      serviceAccountName: my-service-account
      containers:
      - name: app
        image: my-app:latest
```

确保 IAM Role 具有以下权限：

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "kinesis:PutRecord",
        "kinesis:PutRecords"
      ],
      "Resource": "arn:aws:kinesis:REGION:ACCOUNT_ID:stream/STREAM_NAME"
    }
  ]
}
```

## 批量发送说明

- Kinesis `PutRecords` API 限制每次最多 500 条记录
- 此工具库会自动将超过 500 条的消息分批发送
- 返回失败记录的索引列表，方便重试处理

## 构建项目

```bash
mvn clean compile
```

## 运行测试

```bash
mvn test
```

## 打包

```bash
mvn clean package
```

生成的 JAR 文件位于 `target/kinesis-producer-util-1.0.0.jar`

## 许可证

[在此添加许可证信息]
