using WorkflowEngine.Events;
using WorkflowEngine.Models;

namespace WorkflowEngine.Services;

public class WorkflowService : IWorkflowService
{
    private readonly Dictionary<Guid, WorkflowRequest> _requests = new();
    private readonly IWorkflowEventPublisher _eventPublisher;
    private readonly ILogger<WorkflowService> _logger;

    public WorkflowService(IWorkflowEventPublisher eventPublisher, ILogger<WorkflowService> logger)
    {
        _eventPublisher = eventPublisher;
        _logger = logger;
    }

    public Task<WorkflowRequest> CreateRequestAsync(WorkflowRequest request)
    {
        request.Id = Guid.NewGuid();
        request.Status = WorkflowStatus.Pending;
        request.CreatedAt = DateTime.UtcNow;
        
        _requests[request.Id] = request;
        _logger.LogInformation("Created workflow request {RequestId}", request.Id);
        
        return Task.FromResult(request);
    }

    public Task<WorkflowRequest?> GetRequestAsync(Guid requestId)
    {
        _requests.TryGetValue(requestId, out var request);
        return Task.FromResult(request);
    }

    public Task<IEnumerable<WorkflowRequest>> GetAllRequestsAsync()
    {
        return Task.FromResult<IEnumerable<WorkflowRequest>>(_requests.Values);
    }

    public async Task<WorkflowRequest> ApproveAsync(Guid requestId, ApprovalAction approval)
    {
        if (!_requests.TryGetValue(requestId, out var request))
        {
            throw new InvalidOperationException($"Request {requestId} not found");
        }

        if (request.Status != WorkflowStatus.Pending)
        {
            throw new InvalidOperationException($"Request {requestId} is not in pending status");
        }

        approval.RequestId = requestId;
        approval.ApprovedAt = DateTime.UtcNow;
        
        request.Status = WorkflowStatus.Approved;
        request.UpdatedAt = DateTime.UtcNow;

        var approvalEvent = new WorkflowApprovedEvent
        {
            RequestId = requestId,
            OccurredAt = DateTime.UtcNow,
            Request = request,
            ApprovalAction = approval
        };

        await _eventPublisher.PublishAsync(approvalEvent);
        var sanitizedApprovedBy = approval.ApprovedBy.Replace("\r", "").Replace("\n", "");
        _logger.LogInformation("Approved workflow request {RequestId} by {ApprovedBy}", requestId, sanitizedApprovedBy);

        return request;
    }

    public async Task<WorkflowRequest> RejectAsync(Guid requestId, RejectionAction rejection)
    {
        if (!_requests.TryGetValue(requestId, out var request))
        {
            throw new InvalidOperationException($"Request {requestId} not found");
        }

        if (request.Status != WorkflowStatus.Pending)
        {
            throw new InvalidOperationException($"Request {requestId} is not in pending status");
        }

        rejection.RequestId = requestId;
        rejection.RejectedAt = DateTime.UtcNow;
        
        request.Status = WorkflowStatus.Rejected;
        request.UpdatedAt = DateTime.UtcNow;

        var rejectionEvent = new WorkflowRejectedEvent
        {
            RequestId = requestId,
            OccurredAt = DateTime.UtcNow,
            Request = request,
            RejectionAction = rejection
        };

        await _eventPublisher.PublishAsync(rejectionEvent);
        var sanitizedRejectedBy = rejection.RejectedBy.Replace("\r", "").Replace("\n", "");
        _logger.LogInformation("Rejected workflow request {RequestId} by {RejectedBy}", requestId, sanitizedRejectedBy);

        return request;
    }
}
