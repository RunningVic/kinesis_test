# Kinesis Producer Utility

这是一个用于向 Amazon Kinesis Data Streams 发送数据的工具库，支持 String 和 AVRO 两种数据格式。

## 特性

- 基于 AWS SDK 2.x 原生客户端构建
- 支持 String 格式数据
- 支持 AVRO 格式数据（GenericRecord 和 SpecificRecord）
- 支持同步和异步发送
- **批量发送支持**：使用 PutRecords API 批量发送多条记录，提高吞吐量
- **自动批量缓冲**：自动将记录缓冲并批量发送，减少网络往返
- **性能优化**：
  - 可配置的连接池（默认50个连接）
  - 线程池优化（可配置线程数）
  - 连接复用和连接生命周期管理
  - 自动重试失败记录
- 可配置的连接池、超时和重试策略

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

### String 格式 Producer

```java
import com.example.kinesis.producer.StringKinesisProducer;
import com.example.kinesis.producer.KinesisProducerFactory;
import software.amazon.awssdk.regions.Region;

// 创建 Producer
StringKinesisProducer producer = KinesisProducerFactory.createStringProducer(Region.US_EAST_1);

// 同步发送
try {
    PutRecordResponse response = producer.sendString("my-stream", "partition-key-1", "Hello Kinesis!");
    System.out.println("Sequence number: " + response.sequenceNumber());
} catch (Exception e) {
    e.printStackTrace();
}

// 异步发送
producer.sendStringAsync("my-stream", "partition-key-1", "Hello Kinesis!")
    .thenAccept(response -> {
        System.out.println("Sequence number: " + response.sequenceNumber());
    })
    .exceptionally(e -> {
        e.printStackTrace();
        return null;
    });

// 关闭 Producer
producer.close();
```

### 批量发送示例

```java
import com.example.kinesis.producer.StringKinesisProducer;
import com.example.kinesis.producer.KinesisProducerFactory;
import software.amazon.awssdk.regions.Region;
import java.util.*;

// 创建 Producer
StringKinesisProducer producer = KinesisProducerFactory.createStringProducer(Region.US_EAST_1);

// 批量发送（使用Map）
Map<String, String> records = new HashMap<>();
records.put("partition-key-1", "Record 1");
records.put("partition-key-2", "Record 2");
records.put("partition-key-3", "Record 3");

PutRecordsResponse response = producer.sendStringBatch("my-stream", records);
System.out.println("Failed records: " + response.failedRecordCount());

// 批量异步发送
producer.sendStringBatchAsync("my-stream", records)
    .thenAccept(resp -> {
        System.out.println("Batch sent successfully");
    })
    .exceptionally(e -> {
        e.printStackTrace();
        return null;
    });

// 关闭 Producer
producer.close();
```

### AVRO 格式 Producer

```java
import com.example.kinesis.producer.AvroKinesisProducer;
import com.example.kinesis.producer.KinesisProducerFactory;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import software.amazon.awssdk.regions.Region;

// 创建 Producer
AvroKinesisProducer producer = KinesisProducerFactory.createAvroProducer(Region.US_EAST_1);

// 创建 AVRO Schema
String schemaString = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}]}";
Schema schema = new Schema.Parser().parse(schemaString);

// 创建 GenericRecord
GenericRecord record = new GenericData.Record(schema);
record.put("id", 1);
record.put("name", "John Doe");

// 同步发送
try {
    PutRecordResponse response = producer.sendAvro("my-stream", "partition-key-1", record);
    System.out.println("Sequence number: " + response.sequenceNumber());
} catch (Exception e) {
    e.printStackTrace();
}

// 异步发送
producer.sendAvroAsync("my-stream", "partition-key-1", record)
    .thenAccept(response -> {
        System.out.println("Sequence number: " + response.sequenceNumber());
    })
    .exceptionally(e -> {
        e.printStackTrace();
        return null;
    });

// 关闭 Producer
producer.close();
```

### AVRO 批量发送示例

```java
import com.example.kinesis.producer.AvroKinesisProducer;
import com.example.kinesis.producer.KinesisProducerFactory;
import org.apache.avro.generic.GenericRecord;
import software.amazon.awssdk.regions.Region;
import java.util.*;

// 创建 Producer
AvroKinesisProducer producer = KinesisProducerFactory.createAvroProducer(Region.US_EAST_1);

// 批量发送 GenericRecord
Map<String, GenericRecord> records = new HashMap<>();
records.put("partition-key-1", record1);
records.put("partition-key-2", record2);
records.put("partition-key-3", record3);

PutRecordsResponse response = producer.sendGenericRecordBatch("my-stream", records);
System.out.println("Failed records: " + response.failedRecordCount());
```

### 自定义配置

```java
import com.example.kinesis.producer.KinesisProducerConfig;
import com.example.kinesis.producer.KinesisProducerFactory;
import software.amazon.awssdk.regions.Region;
import java.time.Duration;

// 创建自定义配置（包含批量优化）
KinesisProducerConfig config = new KinesisProducerConfig()
    .setRegion(Region.US_EAST_1)
    .setMaxConnections(50)  // 增加连接数以提高吞吐量
    .setRequestTimeoutMillis(30000)
    .setMaxRetries(5)
    .setBatchSize(500)  // 批量大小（最大500）
    .setBatchFlushIntervalMillis(1000)  // 自动刷新间隔
    .setEnableAutoFlush(true)  // 启用自动批量刷新
    .setThreadPoolSize(20)  // 异步操作线程池大小
    .setEnableConnectionPooling(true)  // 启用连接池
    .setConnectionTimeToLive(Duration.ofMinutes(5));  // 连接生存时间

// 使用自定义配置创建 Producer
StringKinesisProducer producer = KinesisProducerFactory.createStringProducer(config);
```

## 配置选项

`KinesisProducerConfig` 支持以下配置：

### 基本配置
- `region`: AWS 区域
- `endpointOverride`: 自定义端点（用于本地测试）
- `maxConnections`: 最大连接数（默认：50）
- `requestTimeoutMillis`: 请求超时时间（默认：60000 毫秒）
- `maxRetries`: 最大重试次数（默认：3）

### 批量配置
- `batchSize`: 批量大小，每次发送的最大记录数（默认：500，Kinesis限制）
- `batchFlushIntervalMillis`: 自动刷新间隔（默认：1000 毫秒）
- `maxBatchSizeBytes`: 最大批量大小（默认：5MB，Kinesis限制）
- `enableAutoFlush`: 是否启用自动批量刷新（默认：true）

### 性能优化配置
- `threadPoolSize`: 异步操作线程池大小（默认：10）
- `enableConnectionPooling`: 是否启用连接池（默认：true）
- `connectionTimeToLive`: 连接生存时间（默认：5分钟）
- `connectionAcquisitionTimeoutSeconds`: 连接获取超时时间（默认：60秒）

## AWS 凭证配置

AWS SDK 2.x 会自动从以下位置查找凭证：

1. Java 系统属性
2. 环境变量
3. 默认凭证文件（`~/.aws/credentials`）
4. IAM 角色（在 EC2/ECS/Lambda 上运行时）

## 许可证

MIT License
