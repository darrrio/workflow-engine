# Architecture Diagram

## Workflow Engine with Kafka Integration

```
┌─────────────────────────────────────────────────────────────────┐
│                      Workflow Engine API                         │
│                     (ASP.NET Core 9.0)                          │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ├─ POST /api/workflow/requests
                            ├─ GET  /api/workflow/requests/{id}
                            ├─ POST /api/workflow/requests/{id}/approve
                            └─ POST /api/workflow/requests/{id}/reject
                            │
                            ▼
            ┌───────────────────────────────┐
            │    WorkflowController         │
            └───────────────┬───────────────┘
                            │
                            ▼
            ┌───────────────────────────────┐
            │     WorkflowService           │
            └───────────────┬───────────────┘
                            │
                            ▼
            ┌───────────────────────────────┐
            │  IWorkflowEventPublisher      │
            │         (Interface)           │
            └───────────────┬───────────────┘
                            │
                ┌───────────┴──────────┐
                │                      │
                ▼                      ▼
    ┌─────────────────────┐  ┌──────────────────────────┐
    │WorkflowEventPublisher│  │KafkaWorkflowEventPublisher│
    │   (In-Memory)        │  │    (Kafka-based)         │
    │                      │  │                          │
    │  - Stores events     │  │  - Publishes to Kafka    │
    │    in memory list    │  │  - Supports security     │
    │  - For testing       │  │  - Configurable          │
    └─────────────────────┘  └────────────┬──────────────┘
                                           │
                                           ▼
                        ┌─────────────────────────────────┐
                        │    Kafka-Compatible Brokers     │
                        ├─────────────────────────────────┤
                        │  • Apache Kafka                 │
                        │  • Redpanda                     │
                        │  • Apache Pulsar (Kafka API)    │
                        │  • Confluent Cloud              │
                        └─────────────────────────────────┘
                                           │
                                           ▼
                        ┌─────────────────────────────────┐
                        │    Topic: workflow-events       │
                        │                                 │
                        │  Events:                        │
                        │  - WorkflowApprovedEvent        │
                        │  - WorkflowRejectedEvent        │
                        └─────────────────────────────────┘
```

## Configuration Flow

```
┌──────────────────────┐
│  appsettings.json    │
│  or Environment Vars │
└──────────┬───────────┘
           │
           ▼
    ┌──────────────┐
    │ KafkaOptions │
    │              │
    │ Enabled?     │
    └──────┬───────┘
           │
    ┌──────┴──────┐
    │             │
    ▼             ▼
  true          false
    │             │
    ▼             ▼
Kafka       In-Memory
Publisher   Publisher
```

## Event Flow

```
User Request
    │
    ▼
[POST /api/workflow/requests/{id}/approve]
    │
    ▼
WorkflowService.ApproveAsync()
    │
    ├─ Update workflow status
    │
    ├─ Create WorkflowApprovedEvent
    │
    └─ Call IWorkflowEventPublisher.PublishAsync()
           │
           ▼
    [If Kafka Enabled]
           │
           ├─ Serialize event to JSON
           │
           ├─ Set message key = RequestId
           │
           └─ ProduceAsync to Kafka topic
                  │
                  ▼
           Kafka Broker
                  │
                  ▼
           [Consumers can subscribe
            to process events]
```

## Security Configuration

```
┌─────────────────────────────────┐
│    Kafka Security Options       │
├─────────────────────────────────┤
│                                 │
│  ┌─────────────────────────┐   │
│  │ SecurityProtocol        │   │
│  ├─────────────────────────┤   │
│  │ • Plaintext (default)   │   │
│  │ • Ssl                   │   │
│  │ • SaslPlaintext         │   │
│  │ • SaslSsl              │   │
│  └─────────────────────────┘   │
│                                 │
│  ┌─────────────────────────┐   │
│  │ SaslMechanism           │   │
│  ├─────────────────────────┤   │
│  │ • Plain                 │   │
│  │ • ScramSha256          │   │
│  │ • ScramSha512          │   │
│  └─────────────────────────┘   │
│                                 │
│  ┌─────────────────────────┐   │
│  │ Authentication          │   │
│  ├─────────────────────────┤   │
│  │ • SaslUsername          │   │
│  │ • SaslPassword          │   │
│  └─────────────────────────┘   │
└─────────────────────────────────┘
```

## Deployment Options

### Option 1: In-Memory (Default)
```
Kafka:Enabled = false
→ Uses WorkflowEventPublisher
→ Events stored in memory
→ Good for development/testing
```

### Option 2: Local Kafka/Redpanda
```
Kafka:Enabled = true
Kafka:BootstrapServers = localhost:9092
→ Uses KafkaWorkflowEventPublisher
→ Events published to local broker
→ Good for local development
```

### Option 3: Production Kafka Cluster
```
Kafka:Enabled = true
Kafka:BootstrapServers = broker1:9093,broker2:9093,broker3:9093
Kafka:SecurityProtocol = SaslSsl
Kafka:SaslMechanism = ScramSha256
Kafka:SaslUsername = <username>
Kafka:SaslPassword = <password>
→ Uses KafkaWorkflowEventPublisher
→ Events published to secured cluster
→ Good for production
```

### Option 4: Confluent Cloud
```
Kafka:Enabled = true
Kafka:BootstrapServers = pkc-xxxxx.region.provider.confluent.cloud:9092
Kafka:SecurityProtocol = SaslSsl
Kafka:SaslMechanism = Plain
Kafka:SaslUsername = <api-key>
Kafka:SaslPassword = <api-secret>
→ Uses KafkaWorkflowEventPublisher
→ Events published to managed Kafka
→ Good for production (managed service)
```
