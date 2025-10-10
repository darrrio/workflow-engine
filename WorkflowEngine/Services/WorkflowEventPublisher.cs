using WorkflowEngine.Events;

namespace WorkflowEngine.Services;

public class WorkflowEventPublisher : IWorkflowEventPublisher
{
    private readonly List<IWorkflowEvent> _publishedEvents = new();
    private readonly ILogger<WorkflowEventPublisher> _logger;

    public WorkflowEventPublisher(ILogger<WorkflowEventPublisher> logger)
    {
        _logger = logger;
    }

    public Task PublishAsync(IWorkflowEvent workflowEvent)
    {
        _publishedEvents.Add(workflowEvent);
        _logger.LogInformation(
            "Published event {EventType} for request {RequestId} at {OccurredAt}",
            workflowEvent.EventType,
            workflowEvent.RequestId,
            workflowEvent.OccurredAt
        );
        return Task.CompletedTask;
    }

    public IEnumerable<IWorkflowEvent> GetPublishedEvents()
    {
        return _publishedEvents.AsReadOnly();
    }
}
