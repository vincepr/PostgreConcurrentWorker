using PostgreConcurrentWorker.DatabaseContexts;
using TestingFixtures;

namespace PostgreConcurrentWorker;

[Parallelizable]
public class PostgresDockerBasedContextFactoryTests
{


    [Parallelizable]
    [Test]
    public async Task Asert_FactoryIsSeededCorrectly()
    {
        await using var contextFactory = await PostgresDockerContextFactory<SimpleDbContext>.NewAsync();
    }
    
}