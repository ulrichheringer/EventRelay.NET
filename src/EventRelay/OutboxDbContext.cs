using Microsoft.EntityFrameworkCore;

namespace EventRelay;

/// <summary>Stores domain changes and events in the same relational SaveChanges transaction.</summary>
public abstract class OutboxDbContext(DbContextOptions options) : DbContext(options)
{
    public DbSet<OutboxMessage> Outbox => Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);
        var message = modelBuilder.Entity<OutboxMessage>();
        message.ToTable("EventRelayOutbox");
        message.HasKey(x => x.Id);
        message.Property(x => x.Type).HasMaxLength(200);
        message.Property(x => x.LastError).HasMaxLength(2000);
        message.HasIndex(x => new { x.Dead, x.DeliveredUtc, x.AvailableUtc });
    }

    public override int SaveChanges(bool acceptAllChangesOnSuccess)
    {
        var staged = StageEvents();
        var result = base.SaveChanges(acceptAllChangesOnSuccess);
        Acknowledge(staged);
        return result;
    }

    public override async Task<int> SaveChangesAsync(bool acceptAllChangesOnSuccess, CancellationToken cancellationToken = default)
    {
        var staged = StageEvents();
        var result = await base.SaveChangesAsync(acceptAllChangesOnSuccess, cancellationToken).ConfigureAwait(false);
        Acknowledge(staged);
        return result;
    }

    private List<(EventSource Source, Guid[] Ids)> StageEvents()
    {
        if (!Database.IsRelational()) throw new InvalidOperationException("A relational EF Core provider is required.");
        if (Database.AutoTransactionBehavior == AutoTransactionBehavior.Never)
            throw new InvalidOperationException("Automatic transactions cannot be disabled for a transactional outbox.");
        var sources = ChangeTracker.Entries().Select(x => x.Entity).OfType<EventSource>().ToArray();
        var staged = new List<(EventSource, Guid[])>();
        var tracked = Outbox.Local.Select(x => x.Id).ToHashSet();
        foreach (var source in sources)
        {
            var pending = source.PendingEvents.ToArray();
            foreach (var e in pending)
            {
                if (tracked.Add(e.Id))
                    Outbox.Add(new OutboxMessage { Id = e.Id, Type = e.Type, Payload = e.Payload, OccurredUtc = e.OccurredUtc, AvailableUtc = e.OccurredUtc });
            }
            staged.Add((source, pending.Select(x => x.Id).ToArray()));
        }
        return staged;
    }

    private static void Acknowledge(List<(EventSource Source, Guid[] Ids)> staged)
    {
        foreach (var (source, ids) in staged) source.Acknowledge(ids);
    }
}
