using WorkflowEngine.Models;

namespace WorkflowEngine.Events;

public class WorkflowApprovedEvent : IWorkflowEvent
{
    public Guid RequestId { get; set; }
    public DateTime OccurredAt { get; set; }
    public string EventType => "WorkflowApproved";
    public WorkflowRequest Request { get; set; } = null!;
    public ApprovalAction ApprovalAction { get; set; } = null!;
}
