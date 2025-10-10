using WorkflowEngine.Models;

namespace WorkflowEngine.Services;

public interface IWorkflowRepository
{
    Task SaveRequestAsync(WorkflowRequest request);
    Task<WorkflowRequest?> GetRequestAsync(Guid requestId);
    Task<IEnumerable<WorkflowRequest>> GetAllRequestsAsync();
    Task DeleteRequestAsync(Guid requestId);
}
