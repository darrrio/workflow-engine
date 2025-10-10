namespace WorkflowEngine.Configuration;

public class PersistenceOptions
{
    public const string SectionName = "Persistence";
    
    public bool Enabled { get; set; }
    public string StoragePath { get; set; } = "workflow-data";
}
