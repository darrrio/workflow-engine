# Workflow Engine Integration Guide

This guide provides a complete walkthrough for integrating new services with the workflow engine, including step-by-step instructions, real-world examples, and best practices.

## Table of Contents

1. [Overview](#overview)
2. [Integration Steps](#integration-steps)
3. [Workflow Input/Output Definitions](#workflow-inputoutput-definitions)
4. [Real-World Examples](#real-world-examples)
5. [Consuming Approval/Rejection Messages](#consuming-approvalrejection-messages)
6. [Best Practices](#best-practices)
7. [Edge Cases and Error Handling](#edge-cases-and-error-handling)

## Overview

The workflow engine is designed as an event-driven system that processes workflow requests through approval or rejection paths. It publishes events to Kafka-compatible message brokers, allowing downstream services to react to workflow state changes.

### Key Components

- **WorkflowRequest**: The core entity representing a workflow
- **ApprovalAction**: Captures approval decisions and metadata
- **RejectionAction**: Captures rejection reasons and metadata
- **Events**: Published to Kafka when workflows are approved or rejected

### Architecture Flow

```
Your Service → Workflow Engine → Kafka → Your Event Consumers
    │              │                │              │
    │              │                │              └─ React to approval
    │              │                └─ Event published
    │              └─ Process workflow
    └─ Create request
```

## Integration Steps

### Step 1: Configure Your Environment

First, ensure the workflow engine is configured with the appropriate persistence and Kafka settings.

#### Configuration Example

```json
{
  "Persistence": {
    "Enabled": true,
    "StoragePath": "workflow-data"
  },
  "Kafka": {
    "Enabled": true,
    "BootstrapServers": "localhost:9092",
    "TopicName": "workflow-events",
    "SecurityProtocol": "SaslSsl",
    "SaslMechanism": "ScramSha256",
    "SaslUsername": "your-username",
    "SaslPassword": "your-password"
  }
}
```

### Step 2: Define Your Workflow Request Structure

Identify the data your service needs to pass through the workflow. Use the `EnrichmentData` dictionary to include service-specific information.

#### Example: Budget Approval Request Structure

```json
{
  "title": "Q4 Budget Approval - Engineering Department",
  "description": "Annual budget approval request for Q4 2024",
  "enrichmentData": {
    "departmentId": "ENG-001",
    "departmentName": "Engineering",
    "fiscalYear": 2024,
    "quarter": "Q4",
    "requestedAmount": 500000.00,
    "currency": "USD",
    "budgetCategory": "operational",
    "requestedBy": "john.doe@company.com",
    "costCenters": [
      {
        "code": "CC-100",
        "name": "Software Development",
        "amount": 300000.00
      },
      {
        "code": "CC-101",
        "name": "Infrastructure",
        "amount": 200000.00
      }
    ]
  }
}
```

### Step 3: Create Workflow Requests from Your Service

Integrate the workflow engine API into your service to create workflow requests.

#### Example: C# Service Integration

```csharp
using System.Net.Http.Json;

public class BudgetService
{
    private readonly HttpClient _httpClient;
    private readonly string _workflowEngineUrl;

    public BudgetService(HttpClient httpClient, IConfiguration configuration)
    {
        _httpClient = httpClient;
        _workflowEngineUrl = configuration["WorkflowEngine:Url"];
    }

    public async Task<Guid> SubmitBudgetApprovalRequest(BudgetRequest budgetRequest)
    {
        var workflowRequest = new
        {
            title = $"Q{budgetRequest.Quarter} Budget Approval - {budgetRequest.DepartmentName}",
            description = $"Annual budget approval request for Q{budgetRequest.Quarter} {budgetRequest.FiscalYear}",
            enrichmentData = new Dictionary<string, object>
            {
                { "departmentId", budgetRequest.DepartmentId },
                { "departmentName", budgetRequest.DepartmentName },
                { "fiscalYear", budgetRequest.FiscalYear },
                { "quarter", $"Q{budgetRequest.Quarter}" },
                { "requestedAmount", budgetRequest.RequestedAmount },
                { "currency", budgetRequest.Currency },
                { "budgetCategory", budgetRequest.Category },
                { "requestedBy", budgetRequest.RequestedBy },
                { "costCenters", budgetRequest.CostCenters }
            }
        };

        var response = await _httpClient.PostAsJsonAsync(
            $"{_workflowEngineUrl}/api/workflow/requests",
            workflowRequest
        );

        response.EnsureSuccessStatusCode();
        var result = await response.Content.ReadFromJsonAsync<WorkflowRequestResponse>();
        return result.Id;
    }
}
```

#### Example: Python Service Integration

```python
import requests
from typing import Dict, Any

class WorkflowClient:
    def __init__(self, workflow_engine_url: str):
        self.workflow_engine_url = workflow_engine_url
        self.base_url = f"{workflow_engine_url}/api/workflow/requests"
    
    def create_workflow_request(self, title: str, description: str, 
                                enrichment_data: Dict[str, Any]) -> str:
        """
        Create a new workflow request.
        
        Returns:
            str: The workflow request ID (GUID)
        """
        payload = {
            "title": title,
            "description": description,
            "enrichmentData": enrichment_data
        }
        
        response = requests.post(self.base_url, json=payload)
        response.raise_for_status()
        
        result = response.json()
        return result["id"]
    
    def get_workflow_status(self, request_id: str) -> Dict[str, Any]:
        """
        Get the current status of a workflow request.
        
        Returns:
            dict: Workflow request details including status
        """
        response = requests.get(f"{self.base_url}/{request_id}")
        response.raise_for_status()
        return response.json()

# Usage example
workflow_client = WorkflowClient("http://localhost:5000")

# Create a budget approval workflow
request_id = workflow_client.create_workflow_request(
    title="Q4 Budget Approval - Engineering",
    description="Annual budget approval for Q4 2024",
    enrichment_data={
        "departmentId": "ENG-001",
        "fiscalYear": 2024,
        "requestedAmount": 500000.00,
        "currency": "USD"
    }
)

print(f"Workflow request created: {request_id}")
```

#### Example: Node.js Service Integration

```javascript
const axios = require('axios');

class WorkflowClient {
    constructor(workflowEngineUrl) {
        this.baseUrl = `${workflowEngineUrl}/api/workflow/requests`;
    }

    async createWorkflowRequest(title, description, enrichmentData) {
        const payload = {
            title,
            description,
            enrichmentData
        };

        try {
            const response = await axios.post(this.baseUrl, payload);
            return response.data.id;
        } catch (error) {
            console.error('Failed to create workflow request:', error.message);
            throw error;
        }
    }

    async getWorkflowStatus(requestId) {
        try {
            const response = await axios.get(`${this.baseUrl}/${requestId}`);
            return response.data;
        } catch (error) {
            console.error('Failed to get workflow status:', error.message);
            throw error;
        }
    }
}

// Usage example
const workflowClient = new WorkflowClient('http://localhost:5000');

(async () => {
    const requestId = await workflowClient.createWorkflowRequest(
        'Q4 Budget Approval - Engineering',
        'Annual budget approval for Q4 2024',
        {
            departmentId: 'ENG-001',
            fiscalYear: 2024,
            requestedAmount: 500000.00,
            currency: 'USD'
        }
    );

    console.log(`Workflow request created: ${requestId}`);
})();
```

### Step 4: Implement Approval/Rejection Actions

Your approval service or admin interface should integrate with the workflow engine to approve or reject requests.

#### Approval Example

```bash
curl -X POST http://localhost:5000/api/workflow/requests/{requestId}/approve \
  -H "Content-Type: application/json" \
  -d '{
    "approvedBy": "jane.smith@company.com",
    "comments": "Budget approved with conditions - quarterly review required",
    "actionData": {
      "approvalLevel": "executive",
      "conditions": ["quarterly-review", "cost-monitoring"],
      "approvedAmount": 500000.00,
      "approvalDate": "2024-01-15",
      "nextReviewDate": "2024-04-01"
    }
  }'
```

#### Rejection Example

```bash
curl -X POST http://localhost:5000/api/workflow/requests/{requestId}/reject \
  -H "Content-Type: application/json" \
  -d '{
    "rejectedBy": "jane.smith@company.com",
    "reason": "Insufficient justification for requested amount. Please revise and resubmit with detailed cost breakdown and ROI analysis."
  }'
```

### Step 5: Consume Events from Kafka

Subscribe to the `workflow-events` topic to receive approval and rejection notifications.

#### Example: C# Kafka Consumer

```csharp
using Confluent.Kafka;
using System.Text.Json;

public class WorkflowEventConsumer
{
    private readonly IConsumer<string, string> _consumer;

    public WorkflowEventConsumer(string bootstrapServers, string groupId)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        _consumer = new ConsumerBuilder<string, string>(config).Build();
        _consumer.Subscribe("workflow-events");
    }

    public async Task StartConsumingAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = _consumer.Consume(cancellationToken);
                
                var eventType = GetEventType(consumeResult.Message.Value);
                
                if (eventType == "WorkflowApproved")
                {
                    await HandleApprovalEventAsync(consumeResult.Message.Value);
                }
                else if (eventType == "WorkflowRejected")
                {
                    await HandleRejectionEventAsync(consumeResult.Message.Value);
                }

                _consumer.Commit(consumeResult);
            }
            catch (ConsumeException ex)
            {
                Console.WriteLine($"Error consuming message: {ex.Error.Reason}");
            }
        }
    }

    private string GetEventType(string messageJson)
    {
        using var doc = JsonDocument.Parse(messageJson);
        return doc.RootElement.GetProperty("EventType").GetString();
    }

    private async Task HandleApprovalEventAsync(string messageJson)
    {
        var approvalEvent = JsonSerializer.Deserialize<WorkflowApprovedEvent>(messageJson);
        
        // Extract enrichment data to identify the service
        var enrichmentData = approvalEvent.Request.EnrichmentData;
        
        // Process based on your service logic
        Console.WriteLine($"Workflow {approvalEvent.RequestId} approved by {approvalEvent.ApprovalAction.ApprovedBy}");
        
        // Example: Update budget status in your database
        // await _budgetService.UpdateBudgetStatusAsync(
        //     enrichmentData["departmentId"], 
        //     BudgetStatus.Approved
        // );
    }

    private async Task HandleRejectionEventAsync(string messageJson)
    {
        var rejectionEvent = JsonSerializer.Deserialize<WorkflowRejectedEvent>(messageJson);
        
        Console.WriteLine($"Workflow {rejectionEvent.RequestId} rejected: {rejectionEvent.RejectionAction.Reason}");
        
        // Example: Notify the requester about rejection
        // await _notificationService.SendRejectionNotificationAsync(
        //     rejectionEvent.Request.EnrichmentData["requestedBy"],
        //     rejectionEvent.RejectionAction.Reason
        // );
    }
}
```

## Workflow Input/Output Definitions

### Input: WorkflowRequest

The workflow request is the primary input to the workflow engine.

```json
{
  "title": "string (required)",
  "description": "string (required)",
  "enrichmentData": {
    "key": "value",
    "nested": {
      "data": "allowed"
    }
  }
}
```

**Fields:**
- `title`: Brief summary of the workflow (max 200 characters recommended)
- `description`: Detailed description of what needs approval
- `enrichmentData`: Service-specific data (optional, but recommended)

**Response:**

```json
{
  "id": "guid",
  "title": "string",
  "description": "string",
  "enrichmentData": { },
  "status": "Pending|Approved|Rejected",
  "createdAt": "ISO8601 timestamp",
  "updatedAt": "ISO8601 timestamp or null"
}
```

### Input: ApprovalAction

```json
{
  "approvedBy": "string (required)",
  "comments": "string (optional)",
  "actionData": {
    "key": "value"
  }
}
```

**Fields:**
- `approvedBy`: Identifier of the approver (email, username, or ID)
- `comments`: Additional notes or conditions for approval
- `actionData`: Custom data related to the approval decision

### Input: RejectionAction

```json
{
  "rejectedBy": "string (required)",
  "reason": "string (required)"
}
```

**Fields:**
- `rejectedBy`: Identifier of the rejector (email, username, or ID)
- `reason`: Explanation for the rejection

### Output: WorkflowApprovedEvent

Published to Kafka when a workflow is approved.

```json
{
  "requestId": "guid",
  "occurredAt": "ISO8601 timestamp",
  "eventType": "WorkflowApproved",
  "request": {
    "id": "guid",
    "title": "string",
    "description": "string",
    "enrichmentData": { },
    "status": "Approved",
    "createdAt": "ISO8601 timestamp",
    "updatedAt": "ISO8601 timestamp"
  },
  "approvalAction": {
    "requestId": "guid",
    "approvedBy": "string",
    "comments": "string",
    "actionData": { },
    "approvedAt": "ISO8601 timestamp"
  }
}
```

**Message Key**: The `requestId` (GUID) is used as the Kafka message key for partitioning.

### Output: WorkflowRejectedEvent

Published to Kafka when a workflow is rejected.

```json
{
  "requestId": "guid",
  "occurredAt": "ISO8601 timestamp",
  "eventType": "WorkflowRejected",
  "request": {
    "id": "guid",
    "title": "string",
    "description": "string",
    "enrichmentData": { },
    "status": "Rejected",
    "createdAt": "ISO8601 timestamp",
    "updatedAt": "ISO8601 timestamp"
  },
  "rejectionAction": {
    "requestId": "guid",
    "rejectedBy": "string",
    "reason": "string",
    "rejectedAt": "ISO8601 timestamp"
  }
}
```

**Message Key**: The `requestId` (GUID) is used as the Kafka message key for partitioning.

## Real-World Examples

### Example 1: Proforma Invoice Customer Workflow

#### Business Context

A proforma invoice is issued to a customer before the actual sale. It requires approval from both the sales manager and finance department before being sent to the customer.

#### Workflow Request Creation

```json
{
  "title": "Proforma Invoice #PI-2024-0042 - Acme Corporation",
  "description": "Proforma invoice for custom software development project",
  "enrichmentData": {
    "invoiceNumber": "PI-2024-0042",
    "customerId": "CUST-1234",
    "customerName": "Acme Corporation",
    "customerEmail": "finance@acmecorp.com",
    "salesRepId": "SR-789",
    "salesRepName": "John Sales",
    "salesRepEmail": "john.sales@company.com",
    "invoiceDate": "2024-01-15",
    "dueDate": "2024-02-15",
    "currency": "USD",
    "totalAmount": 85000.00,
    "lineItems": [
      {
        "description": "Custom CRM Development",
        "quantity": 1,
        "unitPrice": 60000.00,
        "amount": 60000.00,
        "taxRate": 0.0
      },
      {
        "description": "Integration Services",
        "quantity": 100,
        "unitPrice": 250.00,
        "amount": 25000.00,
        "taxRate": 0.0
      }
    ],
    "paymentTerms": "Net 30",
    "projectId": "PROJ-2024-001",
    "specialConditions": [
      "50% upfront payment required",
      "Remaining 50% upon delivery"
    ]
  }
}
```

#### Creating the Workflow (C#)

```csharp
public class ProformaInvoiceService
{
    private readonly HttpClient _httpClient;
    private readonly ILogger<ProformaInvoiceService> _logger;

    public async Task<Guid> SubmitProformaInvoiceForApprovalAsync(
        ProformaInvoice invoice)
    {
        var workflowRequest = new
        {
            title = $"Proforma Invoice #{invoice.InvoiceNumber} - {invoice.CustomerName}",
            description = $"Proforma invoice for {invoice.ProjectDescription}",
            enrichmentData = new Dictionary<string, object>
            {
                { "invoiceNumber", invoice.InvoiceNumber },
                { "customerId", invoice.CustomerId },
                { "customerName", invoice.CustomerName },
                { "customerEmail", invoice.CustomerEmail },
                { "salesRepId", invoice.SalesRepId },
                { "salesRepName", invoice.SalesRepName },
                { "salesRepEmail", invoice.SalesRepEmail },
                { "invoiceDate", invoice.InvoiceDate },
                { "dueDate", invoice.DueDate },
                { "currency", invoice.Currency },
                { "totalAmount", invoice.TotalAmount },
                { "lineItems", invoice.LineItems },
                { "paymentTerms", invoice.PaymentTerms },
                { "projectId", invoice.ProjectId },
                { "specialConditions", invoice.SpecialConditions }
            }
        };

        var response = await _httpClient.PostAsJsonAsync(
            "http://workflow-engine/api/workflow/requests",
            workflowRequest
        );

        response.EnsureSuccessStatusCode();
        var result = await response.Content.ReadFromJsonAsync<WorkflowRequestResponse>();
        
        _logger.LogInformation(
            "Proforma invoice {InvoiceNumber} submitted for approval. Workflow ID: {WorkflowId}",
            invoice.InvoiceNumber,
            result.Id
        );

        return result.Id;
    }
}
```

#### Approval Scenario

Sales manager approves the invoice:

```bash
curl -X POST http://localhost:5000/api/workflow/requests/a1b2c3d4-5678-90ab-cdef-1234567890ab/approve \
  -H "Content-Type: application/json" \
  -d '{
    "approvedBy": "sarah.manager@company.com",
    "comments": "Pricing and terms approved. Customer has good credit history.",
    "actionData": {
      "approverRole": "sales-manager",
      "approvalLevel": 1,
      "creditCheckPassed": true,
      "nextApprover": "finance-department",
      "approvalTimestamp": "2024-01-15T14:30:00Z"
    }
  }'
```

#### Consuming the Approval Event

```csharp
private async Task HandleProformaInvoiceApprovalAsync(WorkflowApprovedEvent approvalEvent)
{
    var enrichmentData = approvalEvent.Request.EnrichmentData;
    
    // Check if this is a proforma invoice workflow
    if (!enrichmentData.ContainsKey("invoiceNumber"))
        return;

    var invoiceNumber = enrichmentData["invoiceNumber"].ToString();
    var approverRole = approvalEvent.ApprovalAction.ActionData?["approverRole"]?.ToString();

    _logger.LogInformation(
        "Proforma invoice {InvoiceNumber} approved by {ApproverRole}",
        invoiceNumber,
        approverRole
    );

    // Update invoice status in database
    await _invoiceRepository.UpdateStatusAsync(
        invoiceNumber,
        InvoiceStatus.Approved
    );

    // Send notification to sales rep
    var salesRepEmail = enrichmentData["salesRepEmail"].ToString();
    await _emailService.SendInvoiceApprovedNotificationAsync(
        salesRepEmail,
        invoiceNumber,
        approvalEvent.ApprovalAction.Comments
    );

    // If approved by sales, route to finance for second approval
    if (approverRole == "sales-manager")
    {
        await _notificationService.NotifyFinanceDepartmentAsync(
            "New proforma invoice pending finance approval",
            invoiceNumber
        );
    }
    // If approved by finance, generate and send the invoice
    else if (approverRole == "finance-department")
    {
        await _invoiceService.GenerateAndSendProformaInvoiceAsync(
            invoiceNumber,
            enrichmentData["customerEmail"].ToString()
        );
    }
}
```

#### Rejection Scenario

Finance department rejects the invoice due to payment terms:

```bash
curl -X POST http://localhost:5000/api/workflow/requests/a1b2c3d4-5678-90ab-cdef-1234567890ab/reject \
  -H "Content-Type: application/json" \
  -d '{
    "rejectedBy": "finance-dept@company.com",
    "reason": "Payment terms do not align with company policy for new customers. New customers require 100% upfront payment for orders over $50,000. Please revise the payment terms and resubmit."
  }'
```

#### Consuming the Rejection Event

```csharp
private async Task HandleProformaInvoiceRejectionAsync(WorkflowRejectedEvent rejectionEvent)
{
    var enrichmentData = rejectionEvent.Request.EnrichmentData;
    
    if (!enrichmentData.ContainsKey("invoiceNumber"))
        return;

    var invoiceNumber = enrichmentData["invoiceNumber"].ToString();

    _logger.LogWarning(
        "Proforma invoice {InvoiceNumber} rejected: {Reason}",
        invoiceNumber,
        rejectionEvent.RejectionAction.Reason
    );

    // Update invoice status
    await _invoiceRepository.UpdateStatusAsync(
        invoiceNumber,
        InvoiceStatus.Rejected,
        rejectionEvent.RejectionAction.Reason
    );

    // Notify sales rep about rejection
    var salesRepEmail = enrichmentData["salesRepEmail"].ToString();
    await _emailService.SendInvoiceRejectedNotificationAsync(
        salesRepEmail,
        invoiceNumber,
        rejectionEvent.RejectionAction.Reason
    );

    // Create a task for sales rep to revise the invoice
    await _taskService.CreateTaskAsync(new TaskCreate
    {
        AssignedTo = enrichmentData["salesRepId"].ToString(),
        Title = $"Revise Proforma Invoice {invoiceNumber}",
        Description = $"Invoice was rejected: {rejectionEvent.RejectionAction.Reason}",
        DueDate = DateTime.UtcNow.AddDays(2)
    });
}
```

### Example 2: Budget Approval Workflow

#### Business Context

Department managers submit quarterly budget requests that require approval from executive management and CFO before being finalized.

#### Workflow Request Creation

```json
{
  "title": "Q4 2024 Budget - Engineering Department",
  "description": "Quarterly budget request for Engineering department covering personnel, infrastructure, and tools",
  "enrichmentData": {
    "departmentId": "ENG-001",
    "departmentName": "Engineering",
    "departmentHead": "alice.tech@company.com",
    "fiscalYear": 2024,
    "quarter": "Q4",
    "submissionDate": "2024-09-15",
    "currency": "USD",
    "totalRequestedAmount": 750000.00,
    "previousQuarterBudget": 700000.00,
    "budgetCategory": "operational",
    "costCenters": [
      {
        "code": "CC-ENG-100",
        "name": "Software Development",
        "category": "personnel",
        "amount": 450000.00,
        "breakdown": {
          "salaries": 400000.00,
          "contractors": 30000.00,
          "bonuses": 20000.00
        },
        "justification": "Two new senior engineer hires to support product expansion"
      },
      {
        "code": "CC-ENG-101",
        "name": "Infrastructure",
        "category": "operational",
        "amount": 200000.00,
        "breakdown": {
          "cloudServices": 150000.00,
          "licenses": 30000.00,
          "hardware": 20000.00
        },
        "justification": "Increased cloud usage due to new customer onboarding"
      },
      {
        "code": "CC-ENG-102",
        "name": "Tools & Software",
        "category": "capital",
        "amount": 100000.00,
        "breakdown": {
          "developmentTools": 60000.00,
          "monitoring": 25000.00,
          "security": 15000.00
        },
        "justification": "Enterprise licenses for development and security tools"
      }
    ],
    "strategicAlignment": [
      "Support 50% customer growth target",
      "Improve system reliability to 99.9% uptime",
      "Reduce security vulnerabilities by 40%"
    ],
    "riskFactors": [
      "Delayed hiring could impact product delivery timeline",
      "Cloud cost overruns if usage exceeds projections"
    ],
    "alternativeScenarios": {
      "conservative": {
        "amount": 650000.00,
        "description": "Defer one senior hire and reduce cloud buffer"
      },
      "aggressive": {
        "amount": 850000.00,
        "description": "Add three additional engineers and upgrade tooling"
      }
    }
  }
}
```

#### Creating the Workflow (Python)

```python
import requests
from datetime import datetime, timedelta
from typing import Dict, List

class BudgetApprovalService:
    def __init__(self, workflow_engine_url: str):
        self.workflow_engine_url = workflow_engine_url
        self.base_url = f"{workflow_engine_url}/api/workflow/requests"
    
    def submit_budget_request(
        self,
        department_id: str,
        department_name: str,
        fiscal_year: int,
        quarter: str,
        cost_centers: List[Dict],
        total_amount: float
    ) -> str:
        """
        Submit a budget approval request to the workflow engine.
        
        Returns:
            str: Workflow request ID
        """
        enrichment_data = {
            "departmentId": department_id,
            "departmentName": department_name,
            "fiscalYear": fiscal_year,
            "quarter": quarter,
            "submissionDate": datetime.now().isoformat(),
            "currency": "USD",
            "totalRequestedAmount": total_amount,
            "costCenters": cost_centers
        }
        
        workflow_request = {
            "title": f"{quarter} {fiscal_year} Budget - {department_name}",
            "description": f"Quarterly budget request for {department_name} department",
            "enrichmentData": enrichment_data
        }
        
        response = requests.post(self.base_url, json=workflow_request)
        response.raise_for_status()
        
        result = response.json()
        workflow_id = result["id"]
        
        print(f"Budget request submitted. Workflow ID: {workflow_id}")
        return workflow_id

# Usage
service = BudgetApprovalService("http://localhost:5000")

cost_centers = [
    {
        "code": "CC-ENG-100",
        "name": "Software Development",
        "category": "personnel",
        "amount": 450000.00,
        "justification": "Two new senior engineer hires"
    },
    {
        "code": "CC-ENG-101",
        "name": "Infrastructure",
        "category": "operational",
        "amount": 200000.00,
        "justification": "Increased cloud usage"
    }
]

workflow_id = service.submit_budget_request(
    department_id="ENG-001",
    department_name="Engineering",
    fiscal_year=2024,
    quarter="Q4",
    cost_centers=cost_centers,
    total_amount=650000.00
)
```

#### Approval Scenario

CFO approves the budget with modifications:

```bash
curl -X POST http://localhost:5000/api/workflow/requests/b2c3d4e5-6789-01bc-def0-234567890abc/approve \
  -H "Content-Type: application/json" \
  -d '{
    "approvedBy": "cfo@company.com",
    "comments": "Budget approved with 10% reduction in infrastructure costs. Defer hardware purchases to Q1 2025.",
    "actionData": {
      "approverRole": "cfo",
      "approvalLevel": "executive",
      "approvedAmount": 675000.00,
      "modifications": [
        {
          "costCenter": "CC-ENG-101",
          "originalAmount": 200000.00,
          "approvedAmount": 180000.00,
          "notes": "Reduce cloud buffer, defer hardware"
        }
      ],
      "conditions": [
        "Monthly cost reviews required",
        "Q1 2025 re-evaluation for infrastructure needs"
      ],
      "approvalDate": "2024-09-20",
      "effectiveDate": "2024-10-01",
      "expiryDate": "2024-12-31"
    }
  }'
```

#### Consuming the Approval Event (Node.js)

```javascript
const { Kafka } = require('kafkajs');

class BudgetApprovalConsumer {
    constructor(kafkaBrokers, groupId) {
        this.kafka = new Kafka({
            clientId: 'budget-approval-service',
            brokers: kafkaBrokers
        });
        
        this.consumer = this.kafka.consumer({ groupId });
    }

    async start() {
        await this.consumer.connect();
        await this.consumer.subscribe({ 
            topic: 'workflow-events', 
            fromBeginning: false 
        });

        await this.consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const event = JSON.parse(message.value.toString());
                
                if (event.eventType === 'WorkflowApproved') {
                    await this.handleBudgetApproval(event);
                } else if (event.eventType === 'WorkflowRejected') {
                    await this.handleBudgetRejection(event);
                }
            }
        });
    }

    async handleBudgetApproval(event) {
        const { enrichmentData } = event.request;
        
        // Check if this is a budget workflow
        if (!enrichmentData.departmentId) return;

        const { approvalAction } = event;
        const approvedAmount = approvalAction.actionData?.approvedAmount || 
                              enrichmentData.totalRequestedAmount;

        console.log(
            `Budget approved for ${enrichmentData.departmentName}: ` +
            `$${approvedAmount} (${enrichmentData.quarter} ${enrichmentData.fiscalYear})`
        );

        // Update budget status in database
        await this.updateBudgetStatus(
            enrichmentData.departmentId,
            enrichmentData.fiscalYear,
            enrichmentData.quarter,
            'approved',
            approvedAmount
        );

        // Send notification to department head
        await this.sendNotification({
            to: enrichmentData.departmentHead,
            subject: `Budget Approved: ${enrichmentData.quarter} ${enrichmentData.fiscalYear}`,
            body: `Your budget request has been approved.\n\n` +
                  `Approved Amount: $${approvedAmount}\n` +
                  `Comments: ${approvalAction.comments}\n\n` +
                  `Effective Date: ${approvalAction.actionData?.effectiveDate}`
        });

        // If there are modifications, create follow-up tasks
        const modifications = approvalAction.actionData?.modifications || [];
        if (modifications.length > 0) {
            await this.createModificationTasks(
                enrichmentData.departmentId,
                modifications
            );
        }

        // If there are conditions, schedule reminders
        const conditions = approvalAction.actionData?.conditions || [];
        if (conditions.length > 0) {
            await this.scheduleConditionReminders(
                enrichmentData.departmentId,
                conditions
            );
        }
    }

    async handleBudgetRejection(event) {
        const { enrichmentData } = event.request;
        
        if (!enrichmentData.departmentId) return;

        const { rejectionAction } = event;

        console.log(
            `Budget rejected for ${enrichmentData.departmentName}: ${rejectionAction.reason}`
        );

        // Update budget status
        await this.updateBudgetStatus(
            enrichmentData.departmentId,
            enrichmentData.fiscalYear,
            enrichmentData.quarter,
            'rejected',
            0
        );

        // Notify department head
        await this.sendNotification({
            to: enrichmentData.departmentHead,
            subject: `Budget Request Requires Revision`,
            body: `Your budget request requires revision.\n\n` +
                  `Reason: ${rejectionAction.reason}\n\n` +
                  `Please review and resubmit with the requested changes.`
        });

        // Create task to revise budget
        await this.createTask({
            assignedTo: enrichmentData.departmentHead,
            title: `Revise ${enrichmentData.quarter} Budget Request`,
            description: rejectionAction.reason,
            dueDate: new Date(Date.now() + 7 * 24 * 60 * 60 * 1000) // 7 days
        });
    }

    async updateBudgetStatus(departmentId, fiscalYear, quarter, status, amount) {
        // Implementation: Update your database
        console.log(`Updating budget status: ${departmentId} ${quarter} ${fiscalYear} -> ${status}`);
    }

    async sendNotification(notification) {
        // Implementation: Send email or push notification
        console.log(`Sending notification to ${notification.to}`);
    }

    async createTask(task) {
        // Implementation: Create task in your task management system
        console.log(`Creating task: ${task.title}`);
    }

    async createModificationTasks(departmentId, modifications) {
        // Implementation: Create tasks for each modification
        console.log(`Creating ${modifications.length} modification tasks`);
    }

    async scheduleConditionReminders(departmentId, conditions) {
        // Implementation: Schedule reminders for conditions
        console.log(`Scheduling reminders for ${conditions.length} conditions`);
    }
}

// Start the consumer
const consumer = new BudgetApprovalConsumer(['localhost:9092'], 'budget-approval-group');
consumer.start().catch(console.error);
```

## Consuming Approval/Rejection Messages

### Message Format Overview

All workflow events are published to Kafka with the following characteristics:

- **Topic**: Configured via `Kafka:TopicName` (default: `workflow-events`)
- **Message Key**: `requestId` (GUID) - ensures all events for a request go to the same partition
- **Message Value**: JSON-serialized event object
- **Serialization**: UTF-8 encoded JSON

### Consumer Configuration Best Practices

#### Consumer Groups

Use consumer groups to scale message processing and ensure each message is processed only once:

```yaml
# Multiple consumers in the same group share the workload
Consumer Group: "budget-service-processors"
  - Consumer 1 (processes partitions 0-2)
  - Consumer 2 (processes partitions 3-5)
  - Consumer 3 (processes partitions 6-8)
```

#### Offset Management

Choose an appropriate offset strategy:

- **Latest** (default): Process only new messages
- **Earliest**: Process all messages from the beginning (useful for new consumers)
- **Specific Offset**: Resume from a specific point (useful for reprocessing)

#### Example Configuration (All Languages)

##### C# (Confluent.Kafka)

```csharp
var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "my-service-consumer-group",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,  // Manual commit for reliability
    SessionTimeoutMs = 30000,
    MaxPollIntervalMs = 300000,
    EnableAutoOffsetStore = false
};
```

##### Python (kafka-python)

```python
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'workflow-events',
    bootstrap_servers=['localhost:9092'],
    group_id='my-service-consumer-group',
    auto_offset_reset='earliest',
    enable_auto_commit=False,  # Manual commit
    session_timeout_ms=30000,
    max_poll_interval_ms=300000,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)
```

##### Node.js (kafkajs)

```javascript
const consumer = kafka.consumer({
    groupId: 'my-service-consumer-group',
    sessionTimeout: 30000,
    heartbeatInterval: 3000,
    maxPollIntervalMs: 300000
});

await consumer.subscribe({
    topic: 'workflow-events',
    fromBeginning: true
});
```

### Filtering Events

Since all workflow events go to the same topic, filter by `eventType` and `enrichmentData`:

```csharp
private async Task ProcessMessageAsync(string messageValue)
{
    using var doc = JsonDocument.Parse(messageValue);
    var root = doc.RootElement;
    
    var eventType = root.GetProperty("EventType").GetString();
    
    // Filter by event type
    if (eventType != "WorkflowApproved" && eventType != "WorkflowRejected")
        return;
    
    // Check if this event is relevant to your service
    var enrichmentData = root.GetProperty("Request")
                            .GetProperty("EnrichmentData");
    
    // Example: Only process budget workflows
    if (!enrichmentData.TryGetProperty("departmentId", out _))
        return;
    
    // Process the event
    if (eventType == "WorkflowApproved")
    {
        await HandleApprovalAsync(messageValue);
    }
    else
    {
        await HandleRejectionAsync(messageValue);
    }
}
```

### Error Handling and Retry Strategy

Implement robust error handling for message processing:

```csharp
public async Task ConsumeMessagesAsync(CancellationToken cancellationToken)
{
    while (!cancellationToken.IsCancellationRequested)
    {
        try
        {
            var consumeResult = _consumer.Consume(cancellationToken);
            
            try
            {
                await ProcessMessageAsync(consumeResult.Message.Value);
                _consumer.Commit(consumeResult);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, 
                    "Error processing message. Offset: {Offset}, Partition: {Partition}",
                    consumeResult.Offset,
                    consumeResult.Partition
                );
                
                // Decide on retry strategy:
                // 1. Retry immediately (for transient errors)
                // 2. Send to dead letter queue (for poison messages)
                // 3. Skip and commit (if message is invalid)
                
                if (IsTransientError(ex))
                {
                    // Retry with backoff
                    await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
                    await ProcessMessageAsync(consumeResult.Message.Value);
                    _consumer.Commit(consumeResult);
                }
                else if (IsPoisonMessage(ex))
                {
                    // Send to dead letter topic
                    await SendToDeadLetterQueueAsync(consumeResult);
                    _consumer.Commit(consumeResult);
                }
                else
                {
                    // Log and skip
                    _logger.LogWarning("Skipping message due to unrecoverable error");
                    _consumer.Commit(consumeResult);
                }
            }
        }
        catch (ConsumeException ex)
        {
            _logger.LogError(ex, "Error consuming from Kafka");
            await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
        }
    }
}
```

### Idempotent Processing

Ensure your message processing is idempotent to handle duplicate messages:

```csharp
private async Task<bool> ProcessApprovalAsync(WorkflowApprovedEvent approvalEvent)
{
    // Check if already processed using requestId
    if (await _processedEventsRepository.ExistsAsync(approvalEvent.RequestId))
    {
        _logger.LogInformation(
            "Event {RequestId} already processed. Skipping.",
            approvalEvent.RequestId
        );
        return true;  // Consider it successfully processed
    }
    
    // Process the event
    await UpdateBudgetStatusAsync(approvalEvent);
    
    // Mark as processed
    await _processedEventsRepository.AddAsync(new ProcessedEvent
    {
        RequestId = approvalEvent.RequestId,
        EventType = "WorkflowApproved",
        ProcessedAt = DateTime.UtcNow
    });
    
    return true;
}
```

## Best Practices

### 1. Workflow Request Design

#### Use Meaningful Titles and Descriptions

```csharp
// ❌ Bad: Vague title
new WorkflowRequest
{
    Title = "Approval Needed",
    Description = "Please approve"
}

// ✅ Good: Descriptive title with context
new WorkflowRequest
{
    Title = "Q4 Budget Approval - Engineering ($750K)",
    Description = "Annual budget request for Engineering department covering personnel, infrastructure, and operational costs for Q4 2024"
}
```

#### Structure EnrichmentData Consistently

```csharp
// ✅ Good: Well-structured enrichment data
enrichmentData = new Dictionary<string, object>
{
    // Identifiers
    { "serviceType", "budget-approval" },
    { "version", "1.0" },
    
    // Core data
    { "departmentId", "ENG-001" },
    { "fiscalYear", 2024 },
    { "amount", 750000.00 },
    
    // Metadata
    { "requestedBy", "alice@company.com" },
    { "submittedAt", DateTime.UtcNow },
    
    // Optional context
    { "costCenters", costCenterList },
    { "justification", "Growth targets require additional resources" }
}
```

### 2. Event Processing

#### Design for Eventual Consistency

Your service should handle the eventual consistency nature of event-driven architectures:

```csharp
// Store workflow ID immediately
await _repository.CreateBudgetRequestAsync(new BudgetRequest
{
    WorkflowId = workflowId,
    Status = BudgetStatus.PendingApproval,  // Initial state
    // ... other fields
});

// Later, when event arrives, update status
// Handle case where event arrives before initial state is saved
private async Task HandleApprovalEventAsync(WorkflowApprovedEvent evt)
{
    var budget = await _repository.GetByWorkflowIdAsync(evt.RequestId);
    
    if (budget == null)
    {
        // Event arrived before initial save completed
        // Retry or queue for later processing
        await _retryQueue.EnqueueAsync(evt);
        return;
    }
    
    budget.Status = BudgetStatus.Approved;
    await _repository.UpdateAsync(budget);
}
```

#### Monitor Consumer Lag

Track how far behind your consumers are:

```csharp
// Periodically check consumer lag
private async Task MonitorConsumerLagAsync()
{
    var lag = await _adminClient.GetConsumerGroupLagAsync("my-consumer-group");
    
    if (lag > 1000)
    {
        _logger.LogWarning("Consumer lag is {Lag} messages", lag);
        // Alert operations team
    }
}
```

### 3. Security and Access Control

#### Secure Kafka Connection

Always use authentication and encryption in production:

```json
{
  "Kafka": {
    "Enabled": true,
    "BootstrapServers": "kafka.production.com:9093",
    "SecurityProtocol": "SaslSsl",
    "SaslMechanism": "ScramSha256",
    "SaslUsername": "${KAFKA_USERNAME}",
    "SaslPassword": "${KAFKA_PASSWORD}"
  }
}
```

#### Validate Approvers

Implement authorization checks before approving/rejecting:

```csharp
public async Task<bool> ApproveWorkflowAsync(Guid requestId, string approvedBy)
{
    // Verify the approver has permission
    var workflow = await GetWorkflowAsync(requestId);
    var canApprove = await _authService.CanApproveAsync(approvedBy, workflow);
    
    if (!canApprove)
    {
        _logger.LogWarning(
            "User {User} attempted to approve workflow {WorkflowId} without permission",
            approvedBy,
            requestId
        );
        throw new UnauthorizedAccessException("You don't have permission to approve this workflow");
    }
    
    // Proceed with approval
    // ...
}
```

### 4. Monitoring and Observability

#### Log Important Events

```csharp
_logger.LogInformation(
    "Workflow created: {WorkflowId}, Type: {Type}, Amount: {Amount}",
    workflowId,
    workflowType,
    amount
);

_logger.LogInformation(
    "Workflow approved: {WorkflowId}, ApprovedBy: {ApprovedBy}, Duration: {Duration}ms",
    workflowId,
    approvedBy,
    (DateTime.UtcNow - workflow.CreatedAt).TotalMilliseconds
);
```

#### Track Metrics

```csharp
// Track workflow metrics
_metrics.IncrementCounter("workflows.created", tags: new[] { $"type:{workflowType}" });
_metrics.IncrementCounter("workflows.approved", tags: new[] { $"type:{workflowType}" });
_metrics.RecordHistogram("workflows.duration", durationMs, tags: new[] { $"type:{workflowType}" });
```

### 5. Testing

#### Unit Test Workflow Creation

```csharp
[Fact]
public async Task SubmitBudgetRequest_ShouldCreateWorkflowWithCorrectData()
{
    // Arrange
    var budgetRequest = new BudgetRequest
    {
        DepartmentId = "ENG-001",
        Amount = 750000.00
    };
    
    _mockHttpClient
        .Setup(x => x.PostAsJsonAsync(It.IsAny<string>(), It.IsAny<object>()))
        .ReturnsAsync(new HttpResponseMessage
        {
            StatusCode = HttpStatusCode.OK,
            Content = new StringContent(JsonSerializer.Serialize(new { id = Guid.NewGuid() }))
        });
    
    // Act
    var workflowId = await _budgetService.SubmitBudgetRequestAsync(budgetRequest);
    
    // Assert
    Assert.NotEqual(Guid.Empty, workflowId);
}
```

#### Integration Test Event Consumption

```csharp
[Fact]
public async Task Consumer_ShouldProcessApprovalEvent()
{
    // Arrange
    var approvalEvent = new WorkflowApprovedEvent
    {
        RequestId = Guid.NewGuid(),
        // ... populate event
    };
    
    // Act
    await _consumer.HandleApprovalEventAsync(approvalEvent);
    
    // Assert
    var budget = await _repository.GetByWorkflowIdAsync(approvalEvent.RequestId);
    Assert.Equal(BudgetStatus.Approved, budget.Status);
}
```

## Edge Cases and Error Handling

### 1. Workflow Not Found

```csharp
var workflow = await _workflowClient.GetWorkflowAsync(workflowId);

if (workflow == null)
{
    _logger.LogError("Workflow {WorkflowId} not found", workflowId);
    throw new WorkflowNotFoundException($"Workflow {workflowId} not found");
}
```

### 2. Duplicate Approval Attempts

The workflow engine prevents double-approval, but handle gracefully:

```csharp
try
{
    await _workflowClient.ApproveAsync(workflowId, approvalAction);
}
catch (HttpRequestException ex) when (ex.StatusCode == HttpStatusCode.BadRequest)
{
    _logger.LogWarning("Workflow {WorkflowId} is not in pending status", workflowId);
    // Handle already approved/rejected scenario
}
```

### 3. Message Processing Failures

```csharp
private async Task<bool> TryProcessMessageAsync(
    ConsumeResult<string, string> consumeResult,
    int maxRetries = 3)
{
    for (int attempt = 1; attempt <= maxRetries; attempt++)
    {
        try
        {
            await ProcessMessageAsync(consumeResult.Message.Value);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Error processing message. Attempt {Attempt}/{MaxRetries}",
                attempt,
                maxRetries
            );
            
            if (attempt < maxRetries)
            {
                await Task.Delay(TimeSpan.FromSeconds(Math.Pow(2, attempt))); // Exponential backoff
            }
            else
            {
                _logger.LogError(
                    ex,
                    "Failed to process message after {MaxRetries} attempts. Sending to DLQ",
                    maxRetries
                );
                await SendToDeadLetterQueueAsync(consumeResult);
                return false;
            }
        }
    }
    
    return false;
}
```

### 4. Network Failures

```csharp
private async Task<T> ExecuteWithRetryAsync<T>(
    Func<Task<T>> operation,
    int maxRetries = 3)
{
    var retryPolicy = Policy
        .Handle<HttpRequestException>()
        .Or<TimeoutException>()
        .WaitAndRetryAsync(
            maxRetries,
            retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
            (exception, timeSpan, retryCount, context) =>
            {
                _logger.LogWarning(
                    exception,
                    "Retry {RetryCount}/{MaxRetries} after {Delay}ms",
                    retryCount,
                    maxRetries,
                    timeSpan.TotalMilliseconds
                );
            }
        );
    
    return await retryPolicy.ExecuteAsync(operation);
}

// Usage
var workflowId = await ExecuteWithRetryAsync(async () =>
{
    return await _workflowClient.CreateRequestAsync(workflowRequest);
});
```

### 5. Kafka Broker Unavailable

```csharp
private async Task EnsureKafkaConnectionAsync()
{
    while (true)
    {
        try
        {
            await _consumer.ConnectAsync();
            _logger.LogInformation("Successfully connected to Kafka");
            break;
        }
        catch (KafkaException ex)
        {
            _logger.LogError(ex, "Failed to connect to Kafka. Retrying in 5 seconds...");
            await Task.Delay(TimeSpan.FromSeconds(5));
        }
    }
}
```

### 6. Invalid Enrichment Data

```csharp
private bool ValidateEnrichmentData(Dictionary<string, object> enrichmentData)
{
    // Define required fields for your service
    var requiredFields = new[] { "departmentId", "fiscalYear", "amount" };
    
    foreach (var field in requiredFields)
    {
        if (!enrichmentData.ContainsKey(field))
        {
            _logger.LogError("Missing required field in enrichment data: {Field}", field);
            return false;
        }
    }
    
    // Validate data types and ranges
    if (enrichmentData["amount"] is not double amount || amount <= 0)
    {
        _logger.LogError("Invalid amount in enrichment data");
        return false;
    }
    
    return true;
}
```

### 7. Event Ordering Issues

Since Kafka guarantees ordering per partition, use the `requestId` as the message key:

```csharp
// The workflow engine already does this, but if you're publishing custom events:
await _producer.ProduceAsync("workflow-events", new Message<string, string>
{
    Key = requestId.ToString(),  // ✅ Ensures all events for this request go to same partition
    Value = JsonSerializer.Serialize(evt)
});
```

### 8. Long-Running Approvals

Implement timeout and reminder logic:

```csharp
// Periodically check for pending workflows
private async Task CheckPendingWorkflowsAsync()
{
    var pendingWorkflows = await _repository.GetPendingWorkflowsAsync();
    var now = DateTime.UtcNow;
    
    foreach (var workflow in pendingWorkflows)
    {
        var age = now - workflow.CreatedAt;
        
        // Send reminder after 24 hours
        if (age > TimeSpan.FromHours(24) && !workflow.ReminderSent)
        {
            await SendApprovalReminderAsync(workflow);
            workflow.ReminderSent = true;
            await _repository.UpdateAsync(workflow);
        }
        
        // Auto-reject after 7 days
        if (age > TimeSpan.FromDays(7))
        {
            _logger.LogWarning(
                "Auto-rejecting workflow {WorkflowId} due to timeout",
                workflow.Id
            );
            
            await _workflowClient.RejectAsync(workflow.Id, new RejectionAction
            {
                RejectedBy = "system",
                Reason = "Workflow automatically rejected due to no action taken within 7 days"
            });
        }
    }
}
```

## Conclusion

This guide provides a comprehensive foundation for integrating new services with the workflow engine. Key takeaways:

1. **Use enrichmentData liberally** - Include all context your service needs to process events
2. **Design for failure** - Implement retry logic, idempotency, and error handling
3. **Monitor your consumers** - Track lag, processing times, and errors
4. **Test thoroughly** - Unit test workflow creation, integration test event processing
5. **Document your workflows** - Maintain clear documentation of what each workflow type represents

For additional help:
- Review the [ARCHITECTURE.md](ARCHITECTURE.md) for system design
- Check [KAFKA_EXAMPLES.md](KAFKA_EXAMPLES.md) for broker configuration
- See the [README.md](README.md) for API documentation
