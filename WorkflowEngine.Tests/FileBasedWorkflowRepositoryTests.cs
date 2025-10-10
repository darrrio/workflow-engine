using WorkflowEngine.Configuration;
using WorkflowEngine.Models;
using WorkflowEngine.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;

namespace WorkflowEngine.Tests;

public class FileBasedWorkflowRepositoryTests : IDisposable
{
    private readonly string _testStoragePath;
    private readonly FileBasedWorkflowRepository _repository;
    private readonly Mock<ILogger<FileBasedWorkflowRepository>> _mockLogger;

    public FileBasedWorkflowRepositoryTests()
    {
        _testStoragePath = Path.Combine(Path.GetTempPath(), $"workflow-test-{Guid.NewGuid()}");
        _mockLogger = new Mock<ILogger<FileBasedWorkflowRepository>>();
        
        var options = Options.Create(new PersistenceOptions
        {
            Enabled = true,
            StoragePath = _testStoragePath
        });
        
        _repository = new FileBasedWorkflowRepository(options, _mockLogger.Object);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testStoragePath))
        {
            Directory.Delete(_testStoragePath, true);
        }
    }

    [Fact]
    public async Task SaveRequestAsync_ShouldPersistRequestToFile()
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
        var filePath = Path.Combine(_testStoragePath, $"{request.Id}.json");
        Assert.True(File.Exists(filePath));
    }

    [Fact]
    public async Task GetRequestAsync_ShouldRetrievePersistedRequest()
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
        await _repository.SaveRequestAsync(request);

        // Act
        var retrieved = await _repository.GetRequestAsync(request.Id);

        // Assert
        Assert.NotNull(retrieved);
        Assert.Equal(request.Id, retrieved.Id);
        Assert.Equal(request.Title, retrieved.Title);
        Assert.Equal(request.Description, retrieved.Description);
        Assert.Equal(request.Status, retrieved.Status);
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
    public async Task GetAllRequestsAsync_ShouldReturnAllPersistedRequests()
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
    public async Task DeleteRequestAsync_ShouldRemovePersistedRequest()
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
        
        var filePath = Path.Combine(_testStoragePath, $"{request.Id}.json");
        Assert.False(File.Exists(filePath));
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

        // Act - Update the request
        request.Title = "Updated Title";
        request.Status = WorkflowStatus.Approved;
        request.UpdatedAt = DateTime.UtcNow;
        await _repository.SaveRequestAsync(request);

        // Assert
        var retrieved = await _repository.GetRequestAsync(request.Id);
        Assert.NotNull(retrieved);
        Assert.Equal("Updated Title", retrieved.Title);
        Assert.Equal(WorkflowStatus.Approved, retrieved.Status);
        Assert.NotNull(retrieved.UpdatedAt);
    }

    [Fact]
    public async Task Repository_ShouldPersistEnrichmentData()
    {
        // Arrange
        var request = new WorkflowRequest
        {
            Id = Guid.NewGuid(),
            Title = "Test Request",
            Status = WorkflowStatus.Pending,
            CreatedAt = DateTime.UtcNow,
            EnrichmentData = new Dictionary<string, object>
            {
                { "key1", "value1" },
                { "key2", 42 },
                { "key3", true }
            }
        };

        // Act
        await _repository.SaveRequestAsync(request);
        var retrieved = await _repository.GetRequestAsync(request.Id);

        // Assert
        Assert.NotNull(retrieved);
        Assert.NotNull(retrieved.EnrichmentData);
        Assert.Equal(3, retrieved.EnrichmentData.Count);
        Assert.Equal("value1", retrieved.EnrichmentData["key1"].ToString());
    }
}
