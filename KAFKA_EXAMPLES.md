# Kafka Integration Examples

This document provides examples of configuring the workflow engine with different Kafka-compatible message brokers.

## Apache Kafka

### Local Development

```json
{
  "Kafka": {
    "Enabled": true,
    "BootstrapServers": "localhost:9092",
    "TopicName": "workflow-events"
  }
}
```

### Production with SASL/SSL

```json
{
  "Kafka": {
    "Enabled": true,
    "BootstrapServers": "kafka-broker1:9093,kafka-broker2:9093,kafka-broker3:9093",
    "TopicName": "workflow-events",
    "SecurityProtocol": "SaslSsl",
    "SaslMechanism": "ScramSha256",
    "SaslUsername": "workflow-engine",
    "SaslPassword": "your-password-here"
  }
}
```

## Redpanda

Redpanda is a Kafka-compatible streaming platform that's simpler to deploy and operate.

### Docker Compose Setup

```yaml
version: '3.8'
services:
  redpanda:
    image: docker.redpanda.com/redpandadata/redpanda:latest
    command:
      - redpanda start
      - --smp 1
      - --overprovisioned
      - --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092
      - --advertise-kafka-addr internal://redpanda:9092,external://localhost:19092
    ports:
      - "19092:19092"

  workflow-engine:
    build: .
    environment:
      - Kafka__Enabled=true
      - Kafka__BootstrapServers=redpanda:9092
      - Kafka__TopicName=workflow-events
    depends_on:
      - redpanda
```

### Configuration

```json
{
  "Kafka": {
    "Enabled": true,
    "BootstrapServers": "localhost:19092",
    "TopicName": "workflow-events"
  }
}
```

## Apache Pulsar

Apache Pulsar can expose a Kafka-compatible protocol on port 9092.

### Configuration

```json
{
  "Kafka": {
    "Enabled": true,
    "BootstrapServers": "pulsar-broker:9092",
    "TopicName": "persistent://public/default/workflow-events"
  }
}
```

## Confluent Cloud

### Configuration

```json
{
  "Kafka": {
    "Enabled": true,
    "BootstrapServers": "pkc-xxxxx.us-east-1.aws.confluent.cloud:9092",
    "TopicName": "workflow-events",
    "SecurityProtocol": "SaslSsl",
    "SaslMechanism": "Plain",
    "SaslUsername": "your-api-key",
    "SaslPassword": "your-api-secret"
  }
}
```

## Testing Your Configuration

### Using kafkacat/kcat

Test that messages are being published:

```bash
# Install kcat (formerly kafkacat)
# On macOS: brew install kcat
# On Linux: apt-get install kafkacat

# Consume messages from the workflow-events topic
kcat -b localhost:9092 -t workflow-events -C
```

### Using Kafka Console Consumer

```bash
# For Apache Kafka
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic workflow-events --from-beginning

# For Redpanda
rpk topic consume workflow-events
```

## Monitoring

### Check Topic Creation

```bash
# Apache Kafka
kafka-topics.sh --bootstrap-server localhost:9092 --list

# Redpanda
rpk topic list
```

### View Messages

Create a workflow request and observe the event in your Kafka consumer:

```bash
curl -X POST http://localhost:5000/api/workflow/requests \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Test Request",
    "description": "Testing Kafka integration"
  }'
```

Then approve it:

```bash
curl -X POST http://localhost:5000/api/workflow/requests/{id}/approve \
  -H "Content-Type: application/json" \
  -d '{
    "approvedBy": "Test User",
    "comments": "Approved for testing"
  }'
```

You should see a `WorkflowApprovedEvent` appear in your Kafka consumer.
