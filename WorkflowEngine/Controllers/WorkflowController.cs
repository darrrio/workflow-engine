using Microsoft.AspNetCore.Mvc;
using WorkflowEngine.Models;
using WorkflowEngine.Services;

namespace WorkflowEngine.Controllers;

[ApiController]
[Route("api/[controller]")]
public class WorkflowController : ControllerBase
{
    private readonly IWorkflowService _workflowService;
    private readonly ILogger<WorkflowController> _logger;

    public WorkflowController(IWorkflowService workflowService, ILogger<WorkflowController> logger)
    {
        _workflowService = workflowService;
        _logger = logger;
    }

    [HttpPost("requests")]
    public async Task<ActionResult<WorkflowRequest>> CreateRequest([FromBody] WorkflowRequest request)
    {
        var createdRequest = await _workflowService.CreateRequestAsync(request);
        return CreatedAtAction(nameof(GetRequest), new { id = createdRequest.Id }, createdRequest);
    }

    [HttpGet("requests/{id}")]
    public async Task<ActionResult<WorkflowRequest>> GetRequest(Guid id)
    {
        var request = await _workflowService.GetRequestAsync(id);
        if (request == null)
        {
            return NotFound();
        }
        return Ok(request);
    }

    [HttpGet("requests")]
    public async Task<ActionResult<IEnumerable<WorkflowRequest>>> GetAllRequests()
    {
        var requests = await _workflowService.GetAllRequestsAsync();
        return Ok(requests);
    }

    [HttpPost("requests/{id}/approve")]
    public async Task<ActionResult<WorkflowRequest>> ApproveRequest(Guid id, [FromBody] ApprovalAction approval)
    {
        try
        {
            var request = await _workflowService.ApproveAsync(id, approval);
            return Ok(request);
        }
        catch (InvalidOperationException ex)
        {
            _logger.LogError(ex, "Error approving request {RequestId}", id);
            return BadRequest(new { error = ex.Message });
        }
    }

    [HttpPost("requests/{id}/reject")]
    public async Task<ActionResult<WorkflowRequest>> RejectRequest(Guid id, [FromBody] RejectionAction rejection)
    {
        try
        {
            var request = await _workflowService.RejectAsync(id, rejection);
            return Ok(request);
        }
        catch (InvalidOperationException ex)
        {
            _logger.LogError(ex, "Error rejecting request {RequestId}", id);
            return BadRequest(new { error = ex.Message });
        }
    }
}
