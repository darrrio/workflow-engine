namespace WorkflowEngine.Models;

public class ApprovalAction
{
    public Guid RequestId { get; set; }
    public string ApprovedBy { get; set; } = string.Empty;
    public string? Comments { get; set; }
    public Dictionary<string, object>? ActionData { get; set; }
    public DateTime ApprovedAt { get; set; }
}
