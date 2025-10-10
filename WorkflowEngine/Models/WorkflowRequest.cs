namespace WorkflowEngine.Models;

public class WorkflowRequest
{
    public Guid Id { get; set; }
    public string Title { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public Dictionary<string, object>? EnrichmentData { get; set; }
    public WorkflowStatus Status { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime? UpdatedAt { get; set; }
}

public enum WorkflowStatus
{
    Pending,
    Approved,
    Rejected
}
