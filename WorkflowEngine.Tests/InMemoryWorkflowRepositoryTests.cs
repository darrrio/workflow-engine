using WorkflowEngine.Models;
using WorkflowEngine.Services;

namespace WorkflowEngine.Tests;

public class InMemoryWorkflowRepositoryTests
{
    private readonly InMemoryWorkflowRepository _repository;

    public InMemoryWorkflowRepositoryTests()
    {
        _repository = new InMemoryWorkflowRepository();
    }

    [Fact]
    public async Task SaveRequestAsync_ShouldStoreRequest()
    {
        // Arrange
        var request = new WorkflowRequest
        {
            Id = Guid.NewGuid(),
            Title = "Test Request",
            Description = "Test Description",
            Status = WorkflowStatus.Pending,
            CreatedAt = DateTime.UtcNow
        };

        // Act
        await _repository.SaveRequestAsync(request);

        // Assert
        var retrieved = await _repository.GetRequestAsync(request.Id);
        Assert.NotNull(retrieved);
        Assert.Equal(request.Id, retrieved.Id);
    }

    [Fact]
    public async Task GetRequestAsync_ShouldReturnNull_WhenRequestDoesNotExist()
    {
        // Act
        var result = await _repository.GetRequestAsync(Guid.NewGuid());

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task GetAllRequestsAsync_ShouldReturnAllRequests()
    {
        // Arrange
        var request1 = new WorkflowRequest
        {
            Id = Guid.NewGuid(),
            Title = "Request 1",
            Status = WorkflowStatus.Pending,
            CreatedAt = DateTime.UtcNow
        };
        var request2 = new WorkflowRequest
        {
            Id = Guid.NewGuid(),
            Title = "Request 2",
            Status = WorkflowStatus.Approved,
            CreatedAt = DateTime.UtcNow
        };
        await _repository.SaveRequestAsync(request1);
        await _repository.SaveRequestAsync(request2);

        // Act
        var requests = await _repository.GetAllRequestsAsync();

        // Assert
        Assert.Equal(2, requests.Count());
        Assert.Contains(requests, r => r.Id == request1.Id);
        Assert.Contains(requests, r => r.Id == request2.Id);
    }

    [Fact]
    public async Task DeleteRequestAsync_ShouldRemoveRequest()
    {
        // Arrange
        var request = new WorkflowRequest
        {
            Id = Guid.NewGuid(),
            Title = "Test Request",
            Status = WorkflowStatus.Pending,
            CreatedAt = DateTime.UtcNow
        };
        await _repository.SaveRequestAsync(request);

        // Act
        await _repository.DeleteRequestAsync(request.Id);

        // Assert
        var retrieved = await _repository.GetRequestAsync(request.Id);
        Assert.Null(retrieved);
    }

    [Fact]
    public async Task SaveRequestAsync_ShouldUpdateExistingRequest()
    {
        // Arrange
        var request = new WorkflowRequest
        {
            Id = Guid.NewGuid(),
            Title = "Original Title",
            Status = WorkflowStatus.Pending,
            CreatedAt = DateTime.UtcNow
        };
        await _repository.SaveRequestAsync(request);

        // Act
        request.Title = "Updated Title";
        request.Status = WorkflowStatus.Approved;
        await _repository.SaveRequestAsync(request);

        // Assert
        var retrieved = await _repository.GetRequestAsync(request.Id);
        Assert.NotNull(retrieved);
        Assert.Equal("Updated Title", retrieved.Title);
        Assert.Equal(WorkflowStatus.Approved, retrieved.Status);
    }
}
