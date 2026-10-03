using EventRelay;
using Microsoft.EntityFrameworkCore;

await using var db = new OrdersContext(new DbContextOptionsBuilder<OrdersContext>().UseSqlite("Data Source=example.db").Options);
await db.Database.EnsureCreatedAsync();
var order = new Order { Name = "sample order" };
order.Submit();
db.Add(order);
await db.SaveChangesAsync();
Console.WriteLine($"Delivered: {await new OutboxDispatcher(db, new ConsolePublisher()).DispatchAsync()}");

sealed class Order : EventSource
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public void Submit() => Raise("order.submitted.v1", new { Name });
}

sealed class OrdersContext(DbContextOptions<OrdersContext> options) : OutboxDbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();
}

sealed class ConsolePublisher : IMessagePublisher
{
    public ValueTask PublishAsync(OutboxMessage message, CancellationToken cancellationToken = default)
    {
        Console.WriteLine($"{message.Id} {message.Type}: {message.Payload}");
        return ValueTask.CompletedTask;
    }
}
