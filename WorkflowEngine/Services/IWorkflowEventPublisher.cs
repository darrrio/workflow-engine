using WorkflowEngine.Events;

namespace WorkflowEngine.Services;

public interface IWorkflowEventPublisher
{
    Task PublishAsync(IWorkflowEvent workflowEvent);
    IEnumerable<IWorkflowEvent> GetPublishedEvents();
}
