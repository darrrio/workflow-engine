using WorkflowEngine.Configuration;
using WorkflowEngine.Services;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddControllers();
builder.Services.AddOpenApi();

// Configure Kafka options
builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection(KafkaOptions.SectionName));

// Configure Persistence options
builder.Services.Configure<PersistenceOptions>(builder.Configuration.GetSection(PersistenceOptions.SectionName));

// Register workflow repository - conditionally use file-based or in-memory
var persistenceOptions = builder.Configuration.GetSection(PersistenceOptions.SectionName).Get<PersistenceOptions>();
if (persistenceOptions?.Enabled == true)
{
    builder.Services.AddSingleton<IWorkflowRepository, FileBasedWorkflowRepository>();
}
else
{
    builder.Services.AddSingleton<IWorkflowRepository, InMemoryWorkflowRepository>();
}

// Register workflow services - conditionally use Kafka or in-memory publisher
var kafkaOptions = builder.Configuration.GetSection(KafkaOptions.SectionName).Get<KafkaOptions>();
if (kafkaOptions?.Enabled == true)
{
    builder.Services.AddSingleton<IWorkflowEventPublisher, KafkaWorkflowEventPublisher>();
}
else
{
    builder.Services.AddSingleton<IWorkflowEventPublisher, WorkflowEventPublisher>();
}

builder.Services.AddSingleton<IWorkflowService, WorkflowService>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.MapControllers();

app.Run();
