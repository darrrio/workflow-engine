using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using WorkflowEngine.Configuration;
using WorkflowEngine.Events;

namespace WorkflowEngine.Services;

public class KafkaWorkflowEventPublisher : IWorkflowEventPublisher, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly KafkaOptions _kafkaOptions;
    private readonly ILogger<KafkaWorkflowEventPublisher> _logger;
    private readonly List<IWorkflowEvent> _publishedEvents = new();

    public KafkaWorkflowEventPublisher(
        IOptions<KafkaOptions> kafkaOptions,
        ILogger<KafkaWorkflowEventPublisher> logger)
    {
        _kafkaOptions = kafkaOptions.Value;
        _logger = logger;

        var config = new ProducerConfig
        {
            BootstrapServers = _kafkaOptions.BootstrapServers
        };

        // Add security configuration if provided
        if (!string.IsNullOrEmpty(_kafkaOptions.SecurityProtocol))
        {
            config.SecurityProtocol = Enum.Parse<SecurityProtocol>(_kafkaOptions.SecurityProtocol);
        }

        if (!string.IsNullOrEmpty(_kafkaOptions.SaslMechanism))
        {
            config.SaslMechanism = Enum.Parse<SaslMechanism>(_kafkaOptions.SaslMechanism);
        }

        if (!string.IsNullOrEmpty(_kafkaOptions.SaslUsername))
        {
            config.SaslUsername = _kafkaOptions.SaslUsername;
        }

        if (!string.IsNullOrEmpty(_kafkaOptions.SaslPassword))
        {
            config.SaslPassword = _kafkaOptions.SaslPassword;
        }

        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    public async Task PublishAsync(IWorkflowEvent workflowEvent)
    {
        _publishedEvents.Add(workflowEvent);

        var eventJson = JsonSerializer.Serialize(workflowEvent, new JsonSerializerOptions
        {
            WriteIndented = false
        });

        try
        {
            var message = new Message<string, string>
            {
                Key = workflowEvent.RequestId.ToString(),
                Value = eventJson
            };

            var deliveryResult = await _producer.ProduceAsync(_kafkaOptions.TopicName, message);

            _logger.LogInformation(
                "Published event {EventType} for request {RequestId} to Kafka topic {Topic} at partition {Partition} offset {Offset}",
                workflowEvent.EventType,
                workflowEvent.RequestId,
                _kafkaOptions.TopicName,
                deliveryResult.Partition.Value,
                deliveryResult.Offset.Value
            );
        }
        catch (ProduceException<string, string> ex)
        {
            _logger.LogError(ex,
                "Failed to publish event {EventType} for request {RequestId} to Kafka",
                workflowEvent.EventType,
                workflowEvent.RequestId
            );
            throw;
        }
    }

    public IEnumerable<IWorkflowEvent> GetPublishedEvents()
    {
        return _publishedEvents.AsReadOnly();
    }

    public void Dispose()
    {
        _producer?.Flush(TimeSpan.FromSeconds(10));
        _producer?.Dispose();
    }
}
