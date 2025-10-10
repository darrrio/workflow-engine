namespace WorkflowEngine.Events;

public interface IWorkflowEvent
{
    Guid RequestId { get; }
    DateTime OccurredAt { get; }
    string EventType { get; }
}
