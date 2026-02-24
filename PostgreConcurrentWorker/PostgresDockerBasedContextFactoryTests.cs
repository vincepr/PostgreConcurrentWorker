using Microsoft.EntityFrameworkCore;
using PostgreConcurrentWorker.DatabaseContexts;
using TestingFixtures;

namespace PostgreConcurrentWorker;

[Parallelizable]
public class PostgresDockerBasedContextFactoryTests
{


    [Parallelizable]
    [Test]
    public async Task Assert_FactoryIsSeededCorrectly()
    {
        await using var contextFactory = await PostgresDockerContextFactory<SimpleDbContext>.NewAsync();
        await using var db = await contextFactory.CreateDbContextAsync();
        var count = await db.QueuedTasks.CountAsync();
        Assert.That(count, Is.EqualTo(10_000));
    }
    
}
