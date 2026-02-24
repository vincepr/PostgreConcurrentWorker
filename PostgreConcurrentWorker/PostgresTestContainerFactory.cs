using Microsoft.EntityFrameworkCore;
using PostgreConcurrentWorker.DatabaseContexts;
using Testcontainers.PostgreSql;
using TestingFixtures;

namespace PostgreConcurrentWorker;

public class PostgresTestContainerFactory : PostgresDockerContextFactory<SimpleDbContext>
{
    protected PostgresTestContainerFactory(DbContextOptions<SimpleDbContext> options,
        Func<DbContextOptions<SimpleDbContext>, SimpleDbContext> ctxFactory,
        PostgreSqlContainer postgreSqlContainer) : base(options, ctxFactory, postgreSqlContainer)
    {
    }
    
    public static Task<PostgresDockerContextFactory<SimpleDbContext>> NewFuncWithoutReflection() 
        => NewAsync(opts => new SimpleDbContext(opts));
}