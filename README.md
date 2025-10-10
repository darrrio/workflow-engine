# workflow-engine

A C# .NET Web service that implements an event-driven workflow engine.

## Features

- **WorkflowRequest**: Describes workflow requests with optional enrichment data
- **Approval Action**: Triggers events and relays requirement data from the request to complete the action
- **Rejection Action**: Triggers events to handle workflow rejections
- **Event-Driven Architecture**: Uses an event publisher to notify subscribers of workflow state changes

## Project Structure

```
WorkflowEngine/
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
│   ├── IWorkflowEventPublisher.cs
│   └── WorkflowEventPublisher.cs
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

## Testing

The project includes comprehensive unit tests in the `WorkflowEngine.Tests` project, covering:
- Creating workflow requests
- Retrieving requests
- Approving workflows with event publishing
- Rejecting workflows with event publishing
- Error handling for invalid operations