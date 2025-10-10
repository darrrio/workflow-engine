using WorkflowEngine.Models;

namespace WorkflowEngine.Services;

public class InMemoryWorkflowRepository : IWorkflowRepository
{
    private readonly Dictionary<Guid, WorkflowRequest> _requests = new();

    public Task SaveRequestAsync(WorkflowRequest request)
    {
        _requests[request.Id] = request;
        return Task.CompletedTask;
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

    public Task DeleteRequestAsync(Guid requestId)
    {
        _requests.Remove(requestId);
        return Task.CompletedTask;
    }
}
