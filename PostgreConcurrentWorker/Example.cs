using Microsoft.EntityFrameworkCore;
using PostgreConcurrentWorker.DatabaseContexts;
using TestingFixtures;

namespace PostgreConcurrentWorker;

[Parallelizable]
public class Example
{
    [Test]
    [Parallelizable]
    public async Task ExampleWorker_SimpleTest()
    {
        await using var contextFactory = await PostgresDockerContextFactory<SimpleDbContext>.NewAsync();

        var worker = new ExampleWorkerUsingTransaction(contextFactory, leaseTimeout: TimeSpan.FromMinutes(5));

        var first = await worker.RunSingle();
        Assert.That(first, Is.EqualTo(1));

        var second = await worker.RunSingle();
        Assert.That(second, Is.EqualTo(2));

        await using var assertContext = await contextFactory.CreateDbContextAsync();
        var firstDb = assertContext.QueuedTasks.Single(t => t.Id == first);
        var secondDb = assertContext.QueuedTasks.Single(t => t.Id == second);
        
        Assert.That(firstDb.InProgressSince, Is.Not.Null);
        Assert.That(secondDb.InProgressSince, Is.Not.Null);
    }
    /*
     *
     * 
     */
    [Test]
    [Parallelizable]
    public async Task ExampleWorker_ReclaimsExpiredTaskAndClaimsNewOnes()
    {
        await using var contextFactory = await PostgresDockerContextFactory<SimpleDbContext>.NewAsync();
        
        await using (var db = await contextFactory.CreateDbContextAsync())
        {
            var expiredTask = await db.QueuedTasks.FindAsync(1);
            Assert.That(expiredTask, Is.Not.Null);
            expiredTask!.InProgressSince = DateTimeOffset.UtcNow.AddMinutes(-30);
            await db.SaveChangesAsync();
        }

        var worker = new ExampleWorkerUsingTransaction(contextFactory, leaseTimeout: TimeSpan.FromMinutes(5));

        var single = worker.RunSingle();
        Assert.That(await single, Is.EqualTo(1));

        var two = worker.RunSingle();
        Assert.That(await two, Is.EqualTo(2));
    }

    [Test]
    [Parallelizable]
    public async Task ExampleWorker_ReclaimsExpiredTaskAndClaimsNewOnes_LastBatchStartedBeforeNextBatch()
    {
        await using var contextFactory = await PostgresDockerContextFactory<SimpleDbContext>.NewAsync();

        await using (var db = await contextFactory.CreateDbContextAsync())
        {
            var expiredTask = await db.QueuedTasks.FindAsync(1);
            Assert.That(expiredTask, Is.Not.Null);
            expiredTask!.InProgressSince = DateTimeOffset.UtcNow.AddMinutes(-30);
            await db.SaveChangesAsync();
        }

        var worker = new ExampleWorkerUsingTransaction(contextFactory, leaseTimeout: TimeSpan.FromMinutes(5));

        var single = worker.RunSingle();
        Assert.That(await single, Is.EqualTo(1));

        var lastBatch = await worker.RunBatch(4);
        var nextBatch = await worker.RunBatch(300);

        Assert.That(lastBatch, Is.EqualTo(new[] { 2, 3, 4, 5 }));
        Assert.That(nextBatch, Has.Count.EqualTo(300));
        Assert.That(lastBatch, Has.Count.EqualTo(4));
    }

    [Test]
    [Parallelizable]
    public async Task ExampleWorker_ReclaimsExpiredTaskAndClaimsNewOnes_NextBatchStartedBeforeLastBatch()
    {
        await using var contextFactory = await PostgresDockerContextFactory<SimpleDbContext>.NewAsync();

        await using (var db = await contextFactory.CreateDbContextAsync())
        {
            var expiredTask = await db.QueuedTasks.FindAsync(1);
            Assert.That(expiredTask, Is.Not.Null);
            expiredTask!.InProgressSince = DateTimeOffset.UtcNow.AddMinutes(-30);
            await db.SaveChangesAsync();
        }

        var worker = new ExampleWorkerUsingTransaction(contextFactory, leaseTimeout: TimeSpan.FromMinutes(5));

        var single = worker.RunSingle();
        Assert.That(await single, Is.EqualTo(1));

        var nextBatch = await worker.RunBatch(300);
        var lastBatch = await worker.RunBatch(4);

        Assert.That(lastBatch, Is.EqualTo(new[] { 302, 303, 304, 305 }));
        Assert.That(nextBatch, Has.Count.EqualTo(300));
        Assert.That(lastBatch, Has.Count.EqualTo(4));
    }

    [Test]
    [Parallelizable]
    public async Task ExampleWorker_ReclaimsExpiredTaskAndClaimsNewOnes_AllClaimedIdsAreUnique()
    {
        await using var contextFactory = await PostgresDockerContextFactory<SimpleDbContext>.NewAsync();

        await using (var db = await contextFactory.CreateDbContextAsync())
        {
            var expiredTask = await db.QueuedTasks.FindAsync(1);
            Assert.That(expiredTask, Is.Not.Null);
            expiredTask!.InProgressSince = DateTimeOffset.UtcNow.AddMinutes(-30);
            await db.SaveChangesAsync();
        }

        var worker = new ExampleWorkerUsingTransaction(contextFactory, leaseTimeout: TimeSpan.FromMinutes(5));

        var single = worker.RunSingle();
        Assert.That(await single, Is.EqualTo(1));

        var nextBatch = await worker.RunBatch(300);
        var lastBatch = await worker.RunBatch(4);
        var allIds = nextBatch.Concat(lastBatch).Append(1).ToList();

        Assert.That(allIds, Has.Count.EqualTo(305));
        Assert.That(allIds.Distinct().Count(), Is.EqualTo(allIds.Count));
    }
    
    [Test]
    [Parallelizable]
    public async Task Assert_FactoryIsSeededCorrectly()
    {
        await using var contextFactory = await PostgresDockerContextFactory<SimpleDbContext>.NewAsync();
        await using var db = await contextFactory.CreateDbContextAsync();
        var count = await db.QueuedTasks.CountAsync();
        Assert.That(count, Is.EqualTo(1000));
    }
}
