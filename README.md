# workflow-engine

A C# .NET Web service that implements an event-driven workflow engine.

## Features

- **WorkflowRequest**: Describes workflow requests with optional enrichment data
- **Approval Action**: Triggers events and relays requirement data from the request to complete the action
- **Rejection Action**: Triggers events to handle workflow rejections
- **Event-Driven Architecture**: Uses an event publisher to notify subscribers of workflow state changes
- **Kafka Integration**: Support for publishing events to Kafka-compatible message brokers (Kafka, Redpanda, Apache Pulsar, etc.)
- **Persistent Storage**: File-based persistence for workflow requests that survives process restarts and crashes

## Documentation

- **[Integration Guide](INTEGRATION_GUIDE.md)** - Complete guide for integrating new services with the workflow engine, including:
  - Step-by-step integration walkthrough with C# examples
  - Real-world examples (Proforma Invoice, Budget Approval)
  - Event consumption patterns and Kafka integration
  - Best practices and error handling
- **[Architecture](ARCHITECTURE.md)** - System architecture and design decisions
- **[Kafka Examples](KAFKA_EXAMPLES.md)** - Configuration examples for different Kafka brokers

## Project Structure

```
WorkflowEngine/
├── Configuration/       # Configuration models
│   ├── KafkaOptions.cs
│   └── PersistenceOptions.cs
├── Models/              # Domain models
│   ├── WorkflowRequest.cs
│   ├── ApprovalAction.cs
│   └── RejectionAction.cs
├── Events/              # Event definitions
│   ├── IWorkflowEvent.cs
│   ├── WorkflowApprovedEvent.cs
│   └── WorkflowRejectedEvent.cs
├── Services/            # Business logic
│   ├── IWorkflowService.cs
│   ├── WorkflowService.cs
│   ├── IWorkflowRepository.cs
│   ├── InMemoryWorkflowRepository.cs
│   ├── FileBasedWorkflowRepository.cs
│   ├── IWorkflowEventPublisher.cs
│   ├── WorkflowEventPublisher.cs
│   └── KafkaWorkflowEventPublisher.cs
└── Controllers/         # API endpoints
    └── WorkflowController.cs
```

## API Endpoints

### Create a Workflow Request
```
POST /api/workflow/requests
Content-Type: application/json

{
  "title": "Budget Approval Request",
  "description": "Requesting approval for Q4 budget",
  "enrichmentData": {
    "amount": 50000,
    "department": "IT"
  }
}
```

### Get a Workflow Request
```
GET /api/workflow/requests/{id}
```

### Get All Workflow Requests
```
GET /api/workflow/requests
```

### Approve a Workflow Request
```
POST /api/workflow/requests/{id}/approve
Content-Type: application/json

{
  "approvedBy": "John Doe",
  "comments": "Approved for processing",
  "actionData": {
    "priority": "high"
  }
}
```

### Reject a Workflow Request
```
POST /api/workflow/requests/{id}/reject
Content-Type: application/json

{
  "rejectedBy": "Jane Smith",
  "reason": "Insufficient budget justification"
}
```

## Building and Running

### Prerequisites
- .NET 9.0 SDK

### Build
```bash
dotnet build
```

### Run Tests
```bash
dotnet test
```

### Run the Application
```bash
cd WorkflowEngine
dotnet run
```

The API will be available at `http://localhost:5000`

## Persistence

The workflow engine supports persistent storage of workflow requests to ensure data durability across process restarts and crashes.

### Configuration

Configure persistence settings in `appsettings.json` or `appsettings.Development.json`:

```json
{
  "Persistence": {
    "Enabled": true,
    "StoragePath": "workflow-data"
  }
}
```

### Configuration Options

- **Enabled**: Set to `true` to enable file-based persistence, `false` to use in-memory storage (default: `false`)
- **StoragePath**: Directory path where workflow request files will be stored (default: `workflow-data`)

### Storage Modes

#### In-Memory Storage (Default)

When persistence is disabled, workflow requests are stored in memory. This mode is suitable for:
- Development and testing
- Scenarios where durability is not required
- Temporary workflow data

**Note**: All data is lost when the application restarts.

#### File-Based Persistence

When persistence is enabled, workflow requests are saved as individual JSON files in the configured storage directory. This mode provides:
- **Durability**: Requests survive application restarts and crashes
- **Recovery**: Automatic loading of pending requests on startup
- **Reliability**: Each request is persisted immediately upon creation or update
- **Simplicity**: No external database required

Each workflow request is stored as a separate JSON file named `{requestId}.json`.

### Environment Variables

You can also configure persistence using environment variables:

```bash
export Persistence__Enabled=true
export Persistence__StoragePath=/var/workflow-data
```

### Example: Running with Persistence

1. Update `appsettings.json`:
```json
{
  "Persistence": {
    "Enabled": true,
    "StoragePath": "workflow-data"
  }
}
```

2. Run the application:
```bash
cd WorkflowEngine
dotnet run
```

3. Create a workflow request:
```bash
curl -X POST http://localhost:5000/api/workflow/requests \
  -H "Content-Type: application/json" \
  -d '{"title": "Test Request", "description": "Test"}'
```

4. Verify persistence:
```bash
ls workflow-data/
# You should see a .json file with the request ID
```

5. Restart the application - the request will still be available:
```bash
curl http://localhost:5000/api/workflow/requests
```

## Kafka Integration

The workflow engine supports publishing events to Kafka-compatible message brokers like Apache Kafka, Redpanda, or Apache Pulsar.

### Configuration

Configure Kafka settings in `appsettings.json` or `appsettings.Development.json`:

```json
{
  "Kafka": {
    "Enabled": true,
    "BootstrapServers": "localhost:9092",
    "TopicName": "workflow-events",
    "SecurityProtocol": "",
    "SaslMechanism": "",
    "SaslUsername": "",
    "SaslPassword": ""
  }
}
```

### Configuration Options

- **Enabled**: Set to `true` to enable Kafka event publishing, `false` to use in-memory publisher
- **BootstrapServers**: Comma-separated list of Kafka broker addresses (default: `localhost:9092`)
- **TopicName**: Name of the Kafka topic to publish events to (default: `workflow-events`)
- **SecurityProtocol**: Security protocol (e.g., `SaslSsl`, `Ssl`, `SaslPlaintext`, `Plaintext`)
- **SaslMechanism**: SASL mechanism (e.g., `Plain`, `ScramSha256`, `ScramSha512`)
- **SaslUsername**: SASL username for authentication
- **SaslPassword**: SASL password for authentication

### Environment Variables

You can also configure Kafka using environment variables:

```bash
export Kafka__Enabled=true
export Kafka__BootstrapServers=your-kafka-broker:9092
export Kafka__TopicName=workflow-events
export Kafka__SecurityProtocol=SaslSsl
export Kafka__SaslMechanism=Plain
export Kafka__SaslUsername=your-username
export Kafka__SaslPassword=your-password
```

### Example: Running with Kafka/Redpanda

1. Start a Kafka-compatible broker (e.g., Redpanda):
```bash
docker run -d --name redpanda \
  -p 9092:9092 \
  docker.redpanda.com/redpandadata/redpanda:latest \
  redpanda start --smp 1 --overprovisioned \
  --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092 \
  --advertise-kafka-addr internal://redpanda:9092,external://localhost:19092
```

2. Update `appsettings.Development.json`:
```json
{
  "Kafka": {
    "Enabled": true,
    "BootstrapServers": "localhost:19092",
    "TopicName": "workflow-events"
  }
}
```

3. Run the application:
```bash
cd WorkflowEngine
dotnet run
```

### Event Format

Events are published as JSON messages with the request ID as the message key:

```json
{
  "RequestId": "guid",
  "OccurredAt": "timestamp",
  "EventType": "WorkflowApproved",
  "Request": { ... },
  "ApprovalAction": { ... }
}
```

## Testing

The project includes comprehensive unit tests in the `WorkflowEngine.Tests` project, covering:
- Creating workflow requests
- Retrieving requests
- Approving workflows with event publishing
- Rejecting workflows with event publishing
- Error handling for invalid operations
- File-based persistence operations
- In-memory repository operations
- Kafka event publisher configuration and initialization
- Approving workflows with event publishing
- Rejecting workflows with event publishing
- Error handling for invalid operations
- Kafka event publisher configuration and initialization