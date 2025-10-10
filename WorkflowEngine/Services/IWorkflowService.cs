using WorkflowEngine.Models;

namespace WorkflowEngine.Services;

public interface IWorkflowService
{
    Task<WorkflowRequest> CreateRequestAsync(WorkflowRequest request);
    Task<WorkflowRequest?> GetRequestAsync(Guid requestId);
    Task<IEnumerable<WorkflowRequest>> GetAllRequestsAsync();
    Task<WorkflowRequest> ApproveAsync(Guid requestId, ApprovalAction approval);
    Task<WorkflowRequest> RejectAsync(Guid requestId, RejectionAction rejection);
}
