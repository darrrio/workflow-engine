using System.Text.Json;
using WorkflowEngine.Configuration;
using WorkflowEngine.Models;
using Microsoft.Extensions.Options;

namespace WorkflowEngine.Services;

public class FileBasedWorkflowRepository : IWorkflowRepository
{
    private readonly string _storagePath;
    private readonly ILogger<FileBasedWorkflowRepository> _logger;
    private readonly SemaphoreSlim _fileLock = new(1, 1);
    private readonly JsonSerializerOptions _jsonOptions = new()
    {
        WriteIndented = true
    };

    public FileBasedWorkflowRepository(
        IOptions<PersistenceOptions> options,
        ILogger<FileBasedWorkflowRepository> logger)
    {
        var persistenceOptions = options.Value;
        _storagePath = persistenceOptions.StoragePath;
        _logger = logger;

        // Ensure storage directory exists
        if (!Directory.Exists(_storagePath))
        {
            Directory.CreateDirectory(_storagePath);
            _logger.LogInformation("Created storage directory at {StoragePath}", _storagePath);
        }
    }

    public async Task SaveRequestAsync(WorkflowRequest request)
    {
        var filePath = GetFilePath(request.Id);
        await _fileLock.WaitAsync();
        try
        {
            var json = JsonSerializer.Serialize(request, _jsonOptions);
            await File.WriteAllTextAsync(filePath, json);
            _logger.LogDebug("Saved workflow request {RequestId} to {FilePath}", request.Id, filePath);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save workflow request {RequestId}", request.Id);
            throw;
        }
        finally
        {
            _fileLock.Release();
        }
    }

    public async Task<WorkflowRequest?> GetRequestAsync(Guid requestId)
    {
        var filePath = GetFilePath(requestId);
        
        if (!File.Exists(filePath))
        {
            return null;
        }

        await _fileLock.WaitAsync();
        try
        {
            var json = await File.ReadAllTextAsync(filePath);
            var request = JsonSerializer.Deserialize<WorkflowRequest>(json);
            _logger.LogDebug("Retrieved workflow request {RequestId} from {FilePath}", requestId, filePath);
            return request;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to retrieve workflow request {RequestId}", requestId);
            return null;
        }
        finally
        {
            _fileLock.Release();
        }
    }

    public async Task<IEnumerable<WorkflowRequest>> GetAllRequestsAsync()
    {
        if (!Directory.Exists(_storagePath))
        {
            return Enumerable.Empty<WorkflowRequest>();
        }

        await _fileLock.WaitAsync();
        try
        {
            var files = Directory.GetFiles(_storagePath, "*.json");
            var requests = new List<WorkflowRequest>();

            foreach (var file in files)
            {
                try
                {
                    var json = await File.ReadAllTextAsync(file);
                    var request = JsonSerializer.Deserialize<WorkflowRequest>(json);
                    if (request != null)
                    {
                        requests.Add(request);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to read workflow request from {FilePath}", file);
                }
            }

            _logger.LogDebug("Retrieved {Count} workflow requests", requests.Count);
            return requests;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to retrieve all workflow requests");
            return Enumerable.Empty<WorkflowRequest>();
        }
        finally
        {
            _fileLock.Release();
        }
    }

    public async Task DeleteRequestAsync(Guid requestId)
    {
        var filePath = GetFilePath(requestId);
        
        if (!File.Exists(filePath))
        {
            return;
        }

        await _fileLock.WaitAsync();
        try
        {
            File.Delete(filePath);
            _logger.LogDebug("Deleted workflow request {RequestId} from {FilePath}", requestId, filePath);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to delete workflow request {RequestId}", requestId);
            throw;
        }
        finally
        {
            _fileLock.Release();
        }
    }

    private string GetFilePath(Guid requestId)
    {
        return Path.Combine(_storagePath, $"{requestId}.json");
    }
}
