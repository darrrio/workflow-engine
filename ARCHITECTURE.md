# Architecture Diagram

## Workflow Engine with Persistence and Kafka Integration

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
            └───────┬───────────────┬───────┘
                    │               │
        ┌───────────┘               └──────────┐
        │                                      │
        ▼                                      ▼
┌───────────────────┐          ┌───────────────────────────────┐
│ IWorkflowRepository│          │  IWorkflowEventPublisher      │
│   (Interface)     │          │         (Interface)           │
└────────┬──────────┘          └───────────────┬───────────────┘
         │                                     │
    ┌────┴────┐                    ┌───────────┴──────────┐
    │         │                    │                      │
    ▼         ▼                    ▼                      ▼
┌─────────┐ ┌──────────┐  ┌─────────────────────┐  ┌──────────────────────────┐
│InMemory │ │FileBased │  │WorkflowEventPublisher│  │KafkaWorkflowEventPublisher│
│Repository│ │Repository│  │   (In-Memory)        │  │    (Kafka-based)         │
│         │ │          │  │                      │  │                          │
│ - Stores│ │ - Saves  │  │  - Stores events     │  │  - Publishes to Kafka    │
│   in    │ │   to JSON│  │    in memory list    │  │  - Supports security     │
│   memory│ │   files  │  │  - For testing       │  │  - Configurable          │
│ - Fast  │ │ - Durable│  └─────────────────────┘  └────────────┬──────────────┘
│ - Default│ │ - Survives│                                      │
│         │ │   restarts│                                      ▼
└─────────┘ └─────┬────┘               ┌─────────────────────────────────┐
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
           ├───────────────────┐
           │                   │
           ▼                   ▼
    ┌──────────────┐    ┌─────────────────┐
    │ KafkaOptions │    │PersistenceOptions│
    │              │    │                 │
    │ Enabled?     │    │ Enabled?        │
    └──────┬───────┘    └────────┬────────┘
           │                     │
    ┌──────┴──────┐       ┌──────┴──────┐
    │             │       │             │
    ▼             ▼       ▼             ▼
  true          false   true          false
    │             │       │             │
    ▼             ▼       ▼             ▼
Kafka       In-Memory  FileBased   InMemory
Publisher   Publisher  Repository  Repository
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
    ├─ Get request from IWorkflowRepository
    │
    ├─ Update workflow status
    │
    ├─ Save to IWorkflowRepository (persist changes)
    │       │
    │       └─ [If Persistence Enabled]
    │              │
    │              └─ Write JSON to file system
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
Persistence:Enabled = false
Kafka:Enabled = false
→ Uses InMemoryWorkflowRepository
→ Uses WorkflowEventPublisher
→ All data stored in memory
→ Good for development/testing
→ Data lost on restart
```

### Option 2: File-Based Persistence
```
Persistence:Enabled = true
Persistence:StoragePath = workflow-data
Kafka:Enabled = false
→ Uses FileBasedWorkflowRepository
→ Uses WorkflowEventPublisher
→ Requests persisted to JSON files
→ Survives restarts/crashes
→ Good for production without external dependencies
```

### Option 3: Local Kafka/Redpanda
```
Persistence:Enabled = true
Kafka:Enabled = true
Kafka:BootstrapServers = localhost:9092
→ Uses FileBasedWorkflowRepository
→ Uses KafkaWorkflowEventPublisher
→ Requests persisted + Events published to local broker
→ Good for local development with event streaming
```

### Option 4: Production with Persistence and Kafka
```
Persistence:Enabled = true
Persistence:StoragePath = /var/workflow-data
Kafka:Enabled = true
Kafka:BootstrapServers = broker1:9093,broker2:9093,broker3:9093
Kafka:SecurityProtocol = SaslSsl
Kafka:SaslMechanism = ScramSha256
Kafka:SaslUsername = <username>
Kafka:SaslPassword = <password>
→ Uses FileBasedWorkflowRepository
→ Uses KafkaWorkflowEventPublisher
→ Full durability and event streaming
→ Good for production
```

### Option 5: Confluent Cloud
```
Persistence:Enabled = true
Kafka:Enabled = true
Kafka:BootstrapServers = pkc-xxxxx.region.provider.confluent.cloud:9092
Kafka:SecurityProtocol = SaslSsl
Kafka:SaslMechanism = Plain
Kafka:SaslUsername = <api-key>
Kafka:SaslPassword = <api-secret>
→ Uses FileBasedWorkflowRepository
→ Uses KafkaWorkflowEventPublisher
→ Events published to managed Kafka
→ Good for production (managed service)
```
