using WorkflowEngine.Models;

namespace WorkflowEngine.Events;

public class WorkflowRejectedEvent : IWorkflowEvent
{
    public Guid RequestId { get; set; }
    public DateTime OccurredAt { get; set; }
    public string EventType => "WorkflowRejected";
    public WorkflowRequest Request { get; set; } = null!;
    public RejectionAction RejectionAction { get; set; } = null!;
}
