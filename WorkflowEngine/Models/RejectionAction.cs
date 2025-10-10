namespace WorkflowEngine.Models;

public class RejectionAction
{
    public Guid RequestId { get; set; }
    public string RejectedBy { get; set; } = string.Empty;
    public string Reason { get; set; } = string.Empty;
    public DateTime RejectedAt { get; set; }
}
