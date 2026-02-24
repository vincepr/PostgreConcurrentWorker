using System.Data;
using Microsoft.EntityFrameworkCore;
using PostgreConcurrentWorker.DatabaseContexts;

namespace PostgreConcurrentWorker;

/// <summary>
/// Claims queued task IDs without an explicit transaction by updating timestamps and returning IDs in one statement; expired leases allow safe re-claim after timeout.
/// </summary>
public class SimpleExampleWorker(IDbContextFactory<SimpleDbContext> dbContextFactory, TimeSpan? leaseTimeout = null)
{
    private readonly TimeSpan _leaseTimeout = leaseTimeout ?? TimeSpan.FromMinutes(5);

    public async Task<IReadOnlyList<int>> RunBatch(int size = 10, CancellationToken cancellationToken = default)
    {
        if (size <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(size), size, "Batch size must be greater than zero.");
        }

        await using var db = await dbContextFactory.CreateDbContextAsync(cancellationToken);
        await db.Database.OpenConnectionAsync(cancellationToken);

        await using var cmd = db.Database.GetDbConnection().CreateCommand();
        cmd.CommandText = """
                          WITH picked AS (
                            SELECT "Id"
                            FROM "QueuedTasks"
                            WHERE "InProgressSince" IS NULL
                               OR "InProgressSince" < now() - (@lease_seconds * interval '1 second')
                            ORDER BY "Id"
                            LIMIT @size
                          )
                          UPDATE "QueuedTasks" t
                          SET "InProgressSince" = now()
                          FROM picked
                          WHERE t."Id" = picked."Id"
                          RETURNING t."Id";
                          """;

        var leaseSeconds = cmd.CreateParameter();
        leaseSeconds.ParameterName = "lease_seconds";
        leaseSeconds.DbType = DbType.Int32;
        leaseSeconds.Value = (int)Math.Ceiling(_leaseTimeout.TotalSeconds);
        cmd.Parameters.Add(leaseSeconds);

        var batchSize = cmd.CreateParameter();
        batchSize.ParameterName = "size";
        batchSize.DbType = DbType.Int32;
        batchSize.Value = size;
        cmd.Parameters.Add(batchSize);

        var claimedIds = new List<int>(size);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            claimedIds.Add(reader.GetInt32(0));
        }

        return claimedIds;
    }

    public async Task<int?> RunSingle(CancellationToken cancellationToken = default)
    {
        var ids = await RunBatch(1, cancellationToken);
        return ids.Count == 0 ? null : ids[0];
    }
}
