using System.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using PostgreConcurrentWorker.DatabaseContexts;

namespace PostgreConcurrentWorker;

/// <summary>
/// Claims queued task IDs using an explicit database transaction so row locks are held for the full claim statement.
/// </summary>
public class ExampleWorkerUsingTransaction(IDbContextFactory<SimpleDbContext> dbContextFactory, TimeSpan? leaseTimeout = null)
{
    private readonly TimeSpan _leaseTimeout = leaseTimeout ?? TimeSpan.FromMinutes(5);

    public async Task<IReadOnlyList<int>> RunBatch(int size = 10, CancellationToken cancellationToken = default)
    {
        if (size <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(size), size, "Batch size must be greater than zero.");
        }

        await using var db = await dbContextFactory.CreateDbContextAsync(cancellationToken);
        await using var tx = await db.Database.BeginTransactionAsync(cancellationToken);
        await db.Database.OpenConnectionAsync(cancellationToken);

        await using var cmd = db.Database.GetDbConnection().CreateCommand();
        cmd.Transaction = tx.GetDbTransaction();
        cmd.CommandText = """
                          WITH picked AS (
                            SELECT "Id"
                            FROM "QueuedTasks"
                            WHERE "InProgressSince" IS NULL
                               OR "InProgressSince" < now() - (@lease_seconds * interval '1 second')
                            ORDER BY "Id"
                            FOR UPDATE SKIP LOCKED
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
        {
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                claimedIds.Add(reader.GetInt32(0));
            }
        }

        await tx.CommitAsync(cancellationToken);
        return claimedIds;
    }

    public async Task<int?> RunSingle(CancellationToken cancellationToken = default)
    {
        var ids = await RunBatch(1, cancellationToken);
        return ids.Count == 0 ? null : ids[0];
    }
}
