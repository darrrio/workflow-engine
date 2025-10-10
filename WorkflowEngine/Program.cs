using WorkflowEngine.Services;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddControllers();
builder.Services.AddOpenApi();

// Register workflow services
builder.Services.AddSingleton<IWorkflowEventPublisher, WorkflowEventPublisher>();
builder.Services.AddSingleton<IWorkflowService, WorkflowService>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.MapControllers();

app.Run();
