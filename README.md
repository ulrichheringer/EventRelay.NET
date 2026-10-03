# EventRelay.NET

Small transactional domain events for .NET 8 and EF Core 8. Raise an event on an aggregate, save your unit of work, and publish from a durable outbox. Includes a separate RabbitMQ adapter with persistent messages, mandatory routing and publisher confirms.

MIT licensed. The originally considered name `DomainEvents.NET` overlaps an existing GitHub library; EventRelay.NET was checked against GitHub and NuGet before implementation. No packages have been published to NuGet. Reference the projects locally or build packages with `dotnet pack`.

## Quick start

Reference `src/EventRelay/EventRelay.csproj` and add your EF Core relational provider. See the runnable [SQLite example](examples/EventRelay.Example/Program.cs).

```csharp
using EventRelay;
using Microsoft.EntityFrameworkCore;

public sealed class Order : EventSource
{
    public int Id { get; set; }
    public string Customer { get; set; } = "";
    public void Submit() => Raise("order.submitted.v1", new { Customer });
}

public sealed class OrdersContext(DbContextOptions<OrdersContext> options)
    : OutboxDbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();
    protected override void OnModelCreating(ModelBuilder model)
    {
        base.OnModelCreating(model); // Required: installs EventRelayOutbox mapping.
        // Your domain mappings here.
    }
}

// In your application unit of work:
var order = new Order { Customer = "Ada" };
order.Submit();
db.Add(order);
await db.SaveChangesAsync(cancellationToken);
```

Create and apply normal EF Core migrations for your context, including the `EventRelayOutbox` table. `EnsureCreated` in the example is only for demonstration. Pending events are not mapped by EF; only serialized payloads enter the outbox. JSON serialization happens when raising an event, so serialization errors occur before persistence. Stable versioned wire names avoid unsafe CLR type deserialization.

## Dispatch and brokers

Reference `src/EventRelay.RabbitMQ/EventRelay.RabbitMQ.csproj` for RabbitMQ:

```csharp
using EventRelay.RabbitMQ;
using RabbitMQ.Client;

await using var connection = await new ConnectionFactory
{
    Uri = new Uri(configuration["RabbitMQ"]!)
}.CreateConnectionAsync(cancellationToken);
await using var publisher = await RabbitMqPublisher.CreateAsync(
    connection, exchange: "domain-events", cancellationToken: cancellationToken);

// Use a dedicated, short-lived context for each poll; never share contexts concurrently.
await using var workerDb = new OrdersContext(options);
var dispatcher = new OutboxDispatcher(workerDb, publisher, new DispatchOptions
{
    BatchSize = 100,
    MaxAttempts = 10,
    PublishTimeout = TimeSpan.FromSeconds(20),
    LeaseDuration = TimeSpan.FromMinutes(1),
    RetryDelay = TimeSpan.FromSeconds(10)
});
int acknowledged = await dispatcher.DispatchAsync(cancellationToken);
```

Create durable exchanges, durable queues and bindings before publishing. The routing key equals `message.Type`. With the default empty exchange, declare a queue with that name. The adapter serializes access to its owned channel, awaits confirmations, rejects unroutable messages, sends persistent JSON bodies and carries `Id` in AMQP `MessageId`. Dispose the publisher after polls finish; the supplied connection remains caller-owned. Keep the publisher long-lived. Channel-level protocol errors require recreating it.

Other brokers implement `IMessagePublisher.PublishAsync(OutboxMessage, CancellationToken)`. Return only after durable acceptance, honor cancellation, and preserve the stable ID. No hidden background process or DI container is required: call `DispatchAsync` in your own `BackgroundService` loop, with a cancellation-aware delay between polls.

## Guarantees and operational behavior

- Business changes and outbox rows are saved in the same relational `SaveChanges` transaction. Disabling EF automatic transactions is rejected. Call the base model configuration and do not override saving without calling the base implementation.
- Events remain pending after a failed save; retries do not stage duplicate tracked envelopes. A successful save acknowledges that batch on the aggregate, including when `acceptAllChangesOnSuccess` is false. In that mode, call `ChangeTracker.AcceptAllChanges()` after your commit before saving again, as required by EF Core.
- When using an explicit or ambient transaction, commit before dispatching. A successful save acknowledges in-memory events before the outer transaction commits. **After rollback, discard the context and aggregate instances and rebuild/retry the entire unit of work.** Reusing those instances after rollback is unsupported.
- Workers acquire leases with conditional SQL updates, incrementing attempts at acquisition. Failed publication schedules a retry; reaching `MaxAttempts` marks the row dead. An expired final-attempt lease is also marked dead at the next poll. Active leases are not stolen; former owners cannot acknowledge or update reclaimed rows.
- Delivery is **at least once**, with no global ordering guarantee. A crash after broker acceptance but before acknowledgment can duplicate delivery. Consumers must deduplicate `Id`; use their own transactional inbox for side effects.
- Publication is bounded by `PublishTimeout`; leases must be longer than that timeout. Arbitrary publishers cannot be forcibly stopped: if an implementation ignores cancellation it may continue publishing after timeout, creating duplicates or retaining resources. RabbitMQ receives the cancellation token; a canceled publish can still have reached the broker.
- Dispatcher cancellation leaves the lease for expiry recovery. SQL updates bypass EF change tracking: inspect statuses with a fresh context or `AsNoTracking`. Never dispatch inside an application transaction or with unsaved domain changes.
- Retain delivered/dead rows for diagnostics, then delete in bounded batches under your own retention policy. To redrive a dead row, explicitly reset `Dead`, `Attempts`, `AvailableUtc`, `LeaseId` and `LeaseUntilUtc`; only do this when no worker owns it. Monitor backlog age, attempts, dead count and poll errors. `LastError` is truncated to 2,000 characters and can contain application information.

SQLite is exercised in tests; the dispatcher uses provider-translated EF Core `ExecuteUpdate` for compare-and-set ownership, with no database-specific locking SQL. Validate schema and provider behavior for your PostgreSQL/SQL Server/MySQL deployment. This project does not provide broker consumers, tracing middleware, migrations, a hosted worker or automatic cleanup.

## Development and validation

Requires .NET SDK 8. Run:

```sh
dotnet restore
dotnet format --verify-no-changes --no-restore
dotnet build -c Release
dotnet test -c Release --no-build
dotnet run --project examples/EventRelay.Example
dotnet pack src/EventRelay -c Release
dotnet pack src/EventRelay.RabbitMQ -c Release
```

Warnings and built-in analyzer diagnostics are errors. `.editorconfig` and `dotnet format` enforce formatting/code style. CI runs all checks on Linux and provisions RabbitMQ for integration tests. The broker test is opt-in locally:

```sh
docker run --rm -d --name eventrelay-rabbit -p 5672:5672 rabbitmq:4-alpine
# Wait until rabbitmq-diagnostics -q ping succeeds, then:
export EVENTRELAY_RABBITMQ_URI=amqp://guest:guest@localhost:5672/
dotnet test -c Release
docker stop eventrelay-rabbit
```

PowerShell: `$env:EVENTRELAY_RABBITMQ_URI = 'amqp://guest:guest@localhost:5672/'`. Tests cover atomic rollback, save failure/retry, sync saving, active leases, stale ownership, expiry recovery, retry exhaustion, cancellation, bounded non-cooperative publication, confirmed RabbitMQ delivery and unroutable rejection. Without the variable the broker test is explicitly skipped; CI always sets it.

Contributions should include focused tests and pass the commands above. Report security-sensitive details privately to the repository owner. See [MIT license](LICENSE).
