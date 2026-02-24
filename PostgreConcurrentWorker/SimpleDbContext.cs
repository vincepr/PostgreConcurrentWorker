using System.ComponentModel.DataAnnotations;
using System.Diagnostics.CodeAnalysis;
using Microsoft.EntityFrameworkCore;

namespace PostgreConcurrentWorker.DatabaseContexts;

public class SimpleDbContext : DbContext
{
    public SimpleDbContext(DbContextOptions options) : base(options)
    {
    }

    public DbSet<QueuedTask> QueuedTasks { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);
        SeedTasks(modelBuilder);
    }

    private static void SeedTasks(ModelBuilder modelBuilder)
    {
        var taskTypes = new[]
        {
            TaskType.Automation,
            TaskType.SmallModelAgentTask,
            TaskType.ExpensiveAgentTask,
            TaskType.HumanEscalationTask,
        };

        var random = new Random(12345);

        static QueuedTask CreateTask(int id, TaskType type) => new()
        {
            Id = id,
            MetaData = $"{{counter: {id}}}",
            Type = type,
        };

        var seededTasks = new List<QueuedTask>(1_000)
        {
            CreateTask(1, TaskType.Automation),
            CreateTask(2, TaskType.SmallModelAgentTask),
            CreateTask(3, TaskType.ExpensiveAgentTask),
            CreateTask(4, TaskType.HumanEscalationTask),
        };

        seededTasks.AddRange(
            Enumerable.Range(5, 1_000 - 4)
                .Select(i => CreateTask(i, taskTypes[random.Next(taskTypes.Length)]))
        );

        modelBuilder.Entity<QueuedTask>().HasData(seededTasks);
    }
}

/// <summary>
/// Represents a async Task queued up. Scalable workers (like kubernetes pods) will work through these asynchronously
/// </summary>
[SuppressMessage("ReSharper", "EntityFramework.ModelValidation.UnlimitedStringLength", Justification = "Postgres specific")]
public class QueuedTask
{
    /// <summary>
    /// Database id.
    /// </summary>
    [Key] public int Id { get; set; }

    /// <summary>
    /// DateTime when the Task was started beeing progressed. Null if no worker is currently working on it.
    /// </summary>
    public DateTimeOffset? InProgressSince { get; set; }

    /// <summary>
    /// Some json meta data describing the task to be done by the worker.
    /// </summary>
    public required string MetaData { get; set; }
    
    /// <summary>
    /// 
    /// </summary>
    public required TaskType Type { get; set; }
}

public enum TaskType
{
    /// <summary>
    /// A simple automation can pre filter easy tasks via patern matching. Done to avoid costs.
    /// Might result in the task get categorized to another Type later.
    /// </summary>
    Automation,
    
    /// <summary>
    /// Categorized as simple task for cheap models.
    /// </summary>
    SmallModelAgentTask,
    
    /// <summary>
    /// Categorized as complex task for expensive models.
    /// </summary>
    ExpensiveAgentTask,
    
    /// <summary>
    /// Human intervention needed. Might be alerted into slack etc...
    /// </summary>
    HumanEscalationTask,
}



