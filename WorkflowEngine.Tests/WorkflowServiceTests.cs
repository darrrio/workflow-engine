using WorkflowEngine.Models;
using WorkflowEngine.Services;
using Microsoft.Extensions.Logging;
using Moq;

namespace WorkflowEngine.Tests;

public class WorkflowServiceTests
{
    private readonly Mock<IWorkflowEventPublisher> _mockEventPublisher;
    private readonly Mock<ILogger<WorkflowService>> _mockLogger;
    private readonly WorkflowService _workflowService;

    public WorkflowServiceTests()
    {
        _mockEventPublisher = new Mock<IWorkflowEventPublisher>();
        _mockLogger = new Mock<ILogger<WorkflowService>>();
        _workflowService = new WorkflowService(_mockEventPublisher.Object, _mockLogger.Object);
    }

    [Fact]
    public async Task CreateRequestAsync_ShouldCreateRequestWithPendingStatus()
    {
        // Arrange
        var request = new WorkflowRequest
        {
            Title = "Test Request",
            Description = "Test Description",
            EnrichmentData = new Dictionary<string, object>
            {
                { "key1", "value1" }
            }
        };

        // Act
        var result = await _workflowService.CreateRequestAsync(request);

        // Assert
        Assert.NotEqual(Guid.Empty, result.Id);
        Assert.Equal(WorkflowStatus.Pending, result.Status);
        Assert.Equal("Test Request", result.Title);
        Assert.True(result.CreatedAt <= DateTime.UtcNow);
        Assert.NotNull(result.EnrichmentData);
        Assert.Equal("value1", result.EnrichmentData["key1"]);
    }

    [Fact]
    public async Task GetRequestAsync_ShouldReturnRequest_WhenRequestExists()
    {
        // Arrange
        var request = new WorkflowRequest
        {
            Title = "Test Request",
            Description = "Test Description"
        };
        var createdRequest = await _workflowService.CreateRequestAsync(request);

        // Act
        var result = await _workflowService.GetRequestAsync(createdRequest.Id);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(createdRequest.Id, result.Id);
        Assert.Equal("Test Request", result.Title);
    }

    [Fact]
    public async Task GetRequestAsync_ShouldReturnNull_WhenRequestDoesNotExist()
    {
        // Act
        var result = await _workflowService.GetRequestAsync(Guid.NewGuid());

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task ApproveAsync_ShouldApproveRequest_AndPublishEvent()
    {
        // Arrange
        var request = new WorkflowRequest
        {
            Title = "Test Request",
            Description = "Test Description"
        };
        var createdRequest = await _workflowService.CreateRequestAsync(request);

        var approval = new ApprovalAction
        {
            ApprovedBy = "John Doe",
            Comments = "Looks good",
            ActionData = new Dictionary<string, object>
            {
                { "department", "IT" }
            }
        };

        // Act
        var result = await _workflowService.ApproveAsync(createdRequest.Id, approval);

        // Assert
        Assert.Equal(WorkflowStatus.Approved, result.Status);
        Assert.NotNull(result.UpdatedAt);
        Assert.Equal(createdRequest.Id, approval.RequestId);
        Assert.True(approval.ApprovedAt <= DateTime.UtcNow);
        _mockEventPublisher.Verify(x => x.PublishAsync(It.IsAny<Events.WorkflowApprovedEvent>()), Times.Once);
    }

    [Fact]
    public async Task ApproveAsync_ShouldThrowException_WhenRequestNotFound()
    {
        // Arrange
        var approval = new ApprovalAction
        {
            ApprovedBy = "John Doe"
        };

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            _workflowService.ApproveAsync(Guid.NewGuid(), approval));
    }

    [Fact]
    public async Task ApproveAsync_ShouldThrowException_WhenRequestAlreadyApproved()
    {
        // Arrange
        var request = new WorkflowRequest
        {
            Title = "Test Request",
            Description = "Test Description"
        };
        var createdRequest = await _workflowService.CreateRequestAsync(request);

        var approval = new ApprovalAction
        {
            ApprovedBy = "John Doe"
        };

        await _workflowService.ApproveAsync(createdRequest.Id, approval);

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            _workflowService.ApproveAsync(createdRequest.Id, approval));
    }

    [Fact]
    public async Task RejectAsync_ShouldRejectRequest_AndPublishEvent()
    {
        // Arrange
        var request = new WorkflowRequest
        {
            Title = "Test Request",
            Description = "Test Description"
        };
        var createdRequest = await _workflowService.CreateRequestAsync(request);

        var rejection = new RejectionAction
        {
            RejectedBy = "Jane Doe",
            Reason = "Insufficient information"
        };

        // Act
        var result = await _workflowService.RejectAsync(createdRequest.Id, rejection);

        // Assert
        Assert.Equal(WorkflowStatus.Rejected, result.Status);
        Assert.NotNull(result.UpdatedAt);
        Assert.Equal(createdRequest.Id, rejection.RequestId);
        Assert.True(rejection.RejectedAt <= DateTime.UtcNow);
        _mockEventPublisher.Verify(x => x.PublishAsync(It.IsAny<Events.WorkflowRejectedEvent>()), Times.Once);
    }

    [Fact]
    public async Task RejectAsync_ShouldThrowException_WhenRequestNotFound()
    {
        // Arrange
        var rejection = new RejectionAction
        {
            RejectedBy = "Jane Doe",
            Reason = "Invalid request"
        };

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            _workflowService.RejectAsync(Guid.NewGuid(), rejection));
    }

    [Fact]
    public async Task GetAllRequestsAsync_ShouldReturnAllRequests()
    {
        // Arrange
        var request1 = await _workflowService.CreateRequestAsync(new WorkflowRequest
        {
            Title = "Request 1",
            Description = "Description 1"
        });

        var request2 = await _workflowService.CreateRequestAsync(new WorkflowRequest
        {
            Title = "Request 2",
            Description = "Description 2"
        });

        // Act
        var result = await _workflowService.GetAllRequestsAsync();

        // Assert
        Assert.Equal(2, result.Count());
    }
}
