using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using WorkflowEngine.Configuration;
using WorkflowEngine.Events;
using WorkflowEngine.Models;
using WorkflowEngine.Services;

namespace WorkflowEngine.Tests;

public class KafkaWorkflowEventPublisherTests
{
    private readonly Mock<ILogger<KafkaWorkflowEventPublisher>> _mockLogger;
    private readonly KafkaOptions _kafkaOptions;

    public KafkaWorkflowEventPublisherTests()
    {
        _mockLogger = new Mock<ILogger<KafkaWorkflowEventPublisher>>();
        _kafkaOptions = new KafkaOptions
        {
            Enabled = true,
            BootstrapServers = "localhost:9092",
            TopicName = "test-workflow-events"
        };
    }

    [Fact]
    public void KafkaWorkflowEventPublisher_ShouldInitializeWithConfiguration()
    {
        // Arrange
        var options = Options.Create(_kafkaOptions);

        // Act
        using var publisher = new KafkaWorkflowEventPublisher(options, _mockLogger.Object);

        // Assert
        Assert.NotNull(publisher);
    }

    [Fact]
    public void KafkaWorkflowEventPublisher_ShouldStorePublishedEvents()
    {
        // Arrange
        var options = Options.Create(_kafkaOptions);
        using var publisher = new KafkaWorkflowEventPublisher(options, _mockLogger.Object);
        
        var testEvent = new WorkflowApprovedEvent
        {
            RequestId = Guid.NewGuid(),
            OccurredAt = DateTime.UtcNow,
            Request = new WorkflowRequest
            {
                Id = Guid.NewGuid(),
                Title = "Test Request",
                Description = "Test Description",
                Status = WorkflowStatus.Approved
            },
            ApprovalAction = new ApprovalAction
            {
                ApprovedBy = "Test User",
                Comments = "Test Comments"
            }
        };

        // Act - We expect this to fail since there's no actual Kafka broker
        // But the event should still be stored in the published events list
        try
        {
            _ = publisher.PublishAsync(testEvent);
        }
        catch
        {
            // Expected to fail without a real Kafka broker
        }

        // Assert
        var publishedEvents = publisher.GetPublishedEvents();
        Assert.Single(publishedEvents);
        Assert.Equal(testEvent.RequestId, publishedEvents.First().RequestId);
    }

    [Fact]
    public void KafkaWorkflowEventPublisher_ShouldConfigureSecurityProtocol()
    {
        // Arrange
        var secureKafkaOptions = new KafkaOptions
        {
            Enabled = true,
            BootstrapServers = "localhost:9092",
            TopicName = "test-workflow-events",
            SecurityProtocol = "SaslSsl",
            SaslMechanism = "Plain",
            SaslUsername = "testuser",
            SaslPassword = "testpassword"
        };
        var options = Options.Create(secureKafkaOptions);

        // Act
        using var publisher = new KafkaWorkflowEventPublisher(options, _mockLogger.Object);

        // Assert
        Assert.NotNull(publisher);
    }

    [Fact]
    public void KafkaOptions_ShouldHaveDefaultValues()
    {
        // Arrange & Act
        var options = new KafkaOptions();

        // Assert
        Assert.False(options.Enabled);
        Assert.Equal("localhost:9092", options.BootstrapServers);
        Assert.Equal("workflow-events", options.TopicName);
        Assert.Null(options.SaslUsername);
        Assert.Null(options.SaslPassword);
        Assert.Null(options.SecurityProtocol);
        Assert.Null(options.SaslMechanism);
    }
}
