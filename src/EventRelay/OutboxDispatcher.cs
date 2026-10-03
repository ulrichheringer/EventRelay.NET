using Microsoft.EntityFrameworkCore;

namespace EventRelay;

public sealed record DispatchOptions
{
    public int BatchSize { get; init; } = 100;
    public int MaxAttempts { get; init; } = 10;
    public TimeSpan LeaseDuration { get; init; } = TimeSpan.FromMinutes(1);
    public TimeSpan PublishTimeout { get; init; } = TimeSpan.FromSeconds(20);
    public TimeSpan RetryDelay { get; init; } = TimeSpan.FromSeconds(10);
}

/// <summary>A bounded poll using atomic lease acquisition. Use a dedicated context per worker.</summary>
public sealed class OutboxDispatcher
{
    private readonly OutboxDbContext db;
    private readonly IMessagePublisher publisher;
    private readonly DispatchOptions options;
    private readonly TimeProvider clock;

    public OutboxDispatcher(OutboxDbContext db, IMessagePublisher publisher, DispatchOptions? options = null, TimeProvider? clock = null)
    {
        this.db = db ?? throw new ArgumentNullException(nameof(db));
        this.publisher = publisher ?? throw new ArgumentNullException(nameof(publisher));
        this.options = options ?? new();
        this.clock = clock ?? TimeProvider.System;
        if (this.options.BatchSize < 1 || this.options.MaxAttempts < 1 || this.options.RetryDelay < TimeSpan.Zero ||
            this.options.PublishTimeout <= TimeSpan.Zero || this.options.LeaseDuration <= this.options.PublishTimeout)
            throw new ArgumentOutOfRangeException(nameof(options));
    }

    /// <summary>Return the number of acknowledged messages. Failure records are retained for retry or inspection.</summary>
    public async Task<int> DispatchAsync(CancellationToken cancellationToken = default)
    {
        var now = clock.GetUtcNow().UtcDateTime;
        await Eligible(now).Where(x => x.Attempts >= options.MaxAttempts).ExecuteUpdateAsync(s => s
            .SetProperty(x => x.Dead, true).SetProperty(x => x.LeaseId, (Guid?)null)
            .SetProperty(x => x.LeaseUntilUtc, (DateTime?)null), cancellationToken).ConfigureAwait(false);
        var ids = await Eligible(now).OrderBy(x => x.OccurredUtc).ThenBy(x => x.Id)
            .Take(options.BatchSize).Select(x => x.Id).ToListAsync(cancellationToken).ConfigureAwait(false);
        var delivered = 0;
        foreach (var id in ids)
        {
            cancellationToken.ThrowIfCancellationRequested();
            now = clock.GetUtcNow().UtcDateTime;
            var leaseId = Guid.NewGuid();
            var until = now + options.LeaseDuration;
            var claimed = await Eligible(now).Where(x => x.Id == id && x.Attempts < options.MaxAttempts).ExecuteUpdateAsync(s => s
                .SetProperty(x => x.LeaseId, leaseId).SetProperty(x => x.LeaseUntilUtc, until)
                .SetProperty(x => x.Attempts, x => x.Attempts + 1), cancellationToken).ConfigureAwait(false);
            if (claimed == 0) continue;
            var message = await db.Outbox.AsNoTracking().SingleAsync(x => x.Id == id && x.LeaseId == leaseId, cancellationToken).ConfigureAwait(false);
            try
            {
                using var timeout = new CancellationTokenSource(options.PublishTimeout, clock);
                using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);
                await publisher.PublishAsync(message, linked.Token).AsTask().WaitAsync(linked.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
            catch (Exception error)
            {
                var retryAt = clock.GetUtcNow().UtcDateTime + options.RetryDelay;
                var detail = error.GetType().Name + ": " + error.Message;
                detail = detail[..Math.Min(detail.Length, 2000)];
                await Owned(id, leaseId).ExecuteUpdateAsync(s => s.SetProperty(x => x.LastError, detail)
                    .SetProperty(x => x.Dead, message.Attempts >= options.MaxAttempts)
                    .SetProperty(x => x.AvailableUtc, retryAt).SetProperty(x => x.LeaseId, (Guid?)null)
                    .SetProperty(x => x.LeaseUntilUtc, (DateTime?)null), cancellationToken).ConfigureAwait(false);
                continue;
            }
            var deliveredAt = clock.GetUtcNow().UtcDateTime;
            delivered += await Owned(id, leaseId).ExecuteUpdateAsync(s => s.SetProperty(x => x.DeliveredUtc, deliveredAt)
                .SetProperty(x => x.LeaseId, (Guid?)null).SetProperty(x => x.LeaseUntilUtc, (DateTime?)null)
                .SetProperty(x => x.LastError, (string?)null), cancellationToken).ConfigureAwait(false);
        }
        return delivered;
    }

    private IQueryable<OutboxMessage> Eligible(DateTime now) => db.Outbox.Where(x => !x.Dead && x.DeliveredUtc == null &&
        x.AvailableUtc <= now && (x.LeaseUntilUtc == null || x.LeaseUntilUtc <= now));

    private IQueryable<OutboxMessage> Owned(Guid id, Guid leaseId) => db.Outbox.Where(x => x.Id == id && x.LeaseId == leaseId);
}
