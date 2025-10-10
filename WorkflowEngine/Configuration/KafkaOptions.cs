namespace WorkflowEngine.Configuration;

public class KafkaOptions
{
    public const string SectionName = "Kafka";
    
    public bool Enabled { get; set; }
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string TopicName { get; set; } = "workflow-events";
    public string? SaslUsername { get; set; }
    public string? SaslPassword { get; set; }
    public string? SecurityProtocol { get; set; }
    public string? SaslMechanism { get; set; }
}
