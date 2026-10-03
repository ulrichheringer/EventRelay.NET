using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;

namespace EventRelay.Tests;

public sealed class OutboxTests
{
    [Fact]
    public async Task SaveStagesOnceAndFailureCanBeRetried()
    {
        await using var fixture = await Fixture.Create();
        await using var db = fixture.Context();
        db.Add(new Order { Name = "same" });
        await db.SaveChangesAsync();
        var order = new Order { Name = "same" };
        order.Submit();
        db.Add(order);
        await Assert.ThrowsAsync<DbUpdateException>(() => db.SaveChangesAsync());
        Assert.Single(order.PendingEvents);
        await using (var reader = fixture.Context()) Assert.Empty(await reader.Outbox.ToListAsync());
        order.Name = "different";
        await db.SaveChangesAsync();
        await db.SaveChangesAsync();
        Assert.Empty(order.PendingEvents);
        Assert.Equal(1, await db.Outbox.CountAsync());
    }

    [Fact]
    public async Task ExplicitRollbackIsAtomic()
    {
        await using var fixture = await Fixture.Create();
        await using (var db = fixture.Context())
        {
            await using var tx = await db.Database.BeginTransactionAsync();
            var order = new Order { Name = "one" };
            order.Submit();
            db.Add(order);
            await db.SaveChangesAsync();
            await tx.RollbackAsync();
        }
        await using var reader = fixture.Context();
        Assert.Empty(await reader.Orders.ToListAsync());
        Assert.Empty(await reader.Outbox.ToListAsync());
    }

    [Fact]
    public async Task ConcurrentWorkerCannotPublishActiveLease()
    {
        await using var fixture = await Fixture.Create();
        await fixture.Seed();
        await using var db1 = fixture.Context();
        await using var db2 = fixture.Context();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var publisher = new Publisher(async (_, ct) => { entered.SetResult(); await release.Task.WaitAsync(ct); });
        var first = new OutboxDispatcher(db1, publisher).DispatchAsync();
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(0, await new OutboxDispatcher(db2, publisher).DispatchAsync());
        release.SetResult();
        Assert.Equal(1, await first);
        Assert.Equal(0, await new OutboxDispatcher(db2, publisher).DispatchAsync());
    }

    [Fact]
    public async Task FailureRetriesThenDeadLetters()
    {
        await using var fixture = await Fixture.Create();
        await fixture.Seed();
        await using var db = fixture.Context();
        var dispatcher = new OutboxDispatcher(db, new Publisher((_, _) => throw new IOException("broker down")), new() { MaxAttempts = 2, RetryDelay = TimeSpan.Zero });
        Assert.Equal(0, await dispatcher.DispatchAsync());
        Assert.Equal(0, await dispatcher.DispatchAsync());
        var row = await db.Outbox.AsNoTracking().SingleAsync();
        Assert.True(row.Dead);
        Assert.Equal(2, row.Attempts);
        Assert.Contains("broker down", row.LastError);
        Assert.Equal(0, await dispatcher.DispatchAsync());
    }

    [Fact]
    public async Task ExpiredLeaseIsReclaimedAndFutureRetryDeferred()
    {
        await using var fixture = await Fixture.Create();
        await fixture.Seed();
        await using var db = fixture.Context();
        var row = await db.Outbox.SingleAsync();
        row.LeaseId = Guid.NewGuid();
        row.LeaseUntilUtc = DateTime.UtcNow.AddMinutes(-1);
        row.AvailableUtc = DateTime.UtcNow.AddHours(1);
        await db.SaveChangesAsync();
        var dispatcher = new OutboxDispatcher(db, new Publisher((_, _) => Task.CompletedTask));
        Assert.Equal(0, await dispatcher.DispatchAsync());
        row.AvailableUtc = DateTime.UtcNow.AddMinutes(-1);
        await db.SaveChangesAsync();
        Assert.Equal(1, await dispatcher.DispatchAsync());
    }

    [Fact]
    public async Task CancellationLeavesLeaseForRecovery()
    {
        await using var fixture = await Fixture.Create();
        await fixture.Seed();
        await using var db = fixture.Context();
        using var canceled = new CancellationTokenSource();
        var publisher = new Publisher((_, ct) => { canceled.Cancel(); ct.ThrowIfCancellationRequested(); return Task.CompletedTask; });
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => new OutboxDispatcher(db, publisher).DispatchAsync(canceled.Token));
        var row = await db.Outbox.AsNoTracking().SingleAsync();
        Assert.NotNull(row.LeaseId);
        Assert.Null(row.DeliveredUtc);
    }

    [Fact]
    public async Task ExhaustedCrashLeaseIsDeadLettered()
    {
        await using var fixture = await Fixture.Create();
        await fixture.Seed();
        await using var db = fixture.Context();
        var row = await db.Outbox.SingleAsync();
        row.Attempts = 10;
        row.LeaseId = Guid.NewGuid();
        row.LeaseUntilUtc = DateTime.UtcNow.AddMinutes(-1);
        await db.SaveChangesAsync();
        Assert.Equal(0, await new OutboxDispatcher(db, new Publisher((_, _) => throw new Exception("should not publish"))).DispatchAsync());
        Assert.True((await db.Outbox.AsNoTracking().SingleAsync()).Dead);
    }

    [Fact]
    public async Task FormerOwnerCannotAcknowledgeReclaimedMessage()
    {
        await using var fixture = await Fixture.Create();
        await fixture.Seed();
        await using var db = fixture.Context();
        var publisher = new Publisher(async (message, ct) =>
        {
            await using var other = fixture.Context();
            await other.Outbox.Where(x => x.Id == message.Id).ExecuteUpdateAsync(s => s.SetProperty(x => x.LeaseId, Guid.NewGuid()), ct);
        });
        Assert.Equal(0, await new OutboxDispatcher(db, publisher).DispatchAsync());
        Assert.Null((await db.Outbox.AsNoTracking().SingleAsync()).DeliveredUtc);
    }

    [Fact]
    public async Task NonCooperativePublisherDoesNotBlockPollPastTimeout()
    {
        await using var fixture = await Fixture.Create();
        await fixture.Seed();
        await using var db = fixture.Context();
        var never = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = new OutboxDispatcher(db, new Publisher((_, _) => never.Task), new()
        {
            PublishTimeout = TimeSpan.FromMilliseconds(50),
            LeaseDuration = TimeSpan.FromSeconds(1),
            MaxAttempts = 1
        });
        Assert.Equal(0, await dispatcher.DispatchAsync().WaitAsync(TimeSpan.FromSeconds(5)));
        never.SetResult();
        Assert.True((await db.Outbox.AsNoTracking().SingleAsync()).Dead);
    }

    [Fact]
    public async Task SyncSaveAndValidationWork()
    {
        await using var fixture = await Fixture.Create();
        await using var db = fixture.Context();
        var order = new Order { Name = "one" };
        order.Submit();
        db.Add(order);
        db.SaveChanges();
        Assert.Empty(order.PendingEvents);
        Assert.Equal(1, db.Outbox.Count());
        Assert.Throws<ArgumentOutOfRangeException>(() => new OutboxDispatcher(db, new Publisher((_, _) => Task.CompletedTask), new() { BatchSize = 0 }));
        db.Database.AutoTransactionBehavior = AutoTransactionBehavior.Never;
        Assert.Throws<InvalidOperationException>(() => db.SaveChanges());
    }

    private sealed class Publisher(Func<OutboxMessage, CancellationToken, Task> publish) : IMessagePublisher
    {
        public ValueTask PublishAsync(OutboxMessage message, CancellationToken cancellationToken = default) => new(publish(message, cancellationToken));
    }

    private sealed class Fixture(SqliteConnection connection) : IAsyncDisposable
    {
        public static async Task<Fixture> Create()
        {
            var connection = new SqliteConnection("Data Source=:memory:");
            await connection.OpenAsync();
            var fixture = new Fixture(connection);
            await using var db = fixture.Context();
            await db.Database.EnsureCreatedAsync();
            return fixture;
        }
        public TestContext Context() => new(new DbContextOptionsBuilder<TestContext>().UseSqlite(connection).Options);
        public async Task Seed()
        {
            await using var db = Context();
            var order = new Order { Name = "one" };
            order.Submit();
            db.Add(order);
            await db.SaveChangesAsync();
        }
        public ValueTask DisposeAsync() => connection.DisposeAsync();
    }
}

public sealed class Order : EventSource
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public void Submit() => Raise("order.submitted.v1", new { Name });
}

public sealed class TestContext(DbContextOptions<TestContext> options) : OutboxDbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);
        modelBuilder.Entity<Order>().HasIndex(x => x.Name).IsUnique();
    }
}
