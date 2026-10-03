using EventRelay.RabbitMQ;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace EventRelay.Tests;

public sealed class RabbitMqTests
{
    [Fact]
    public async Task ConfirmedPersistentMessageArrivesAndUnroutableMessageFails()
    {
        var uri = Environment.GetEnvironmentVariable("EVENTRELAY_RABBITMQ_URI");
        if (uri is null) return; // Opt-in locally; CI always supplies a broker.
        await using var connection = await new ConnectionFactory { Uri = new Uri(uri) }.CreateConnectionAsync();
        await using var channel = await connection.CreateChannelAsync();
        var queue = "eventrelay-" + Guid.NewGuid().ToString("N");
        await channel.QueueDeclareAsync(queue, true, false, false);
        await using var publisher = await RabbitMqPublisher.CreateAsync(connection);
        var id = Guid.NewGuid();
        await publisher.PublishAsync(new OutboxMessage { Id = id, Type = queue, Payload = "{\"ok\":true}", OccurredUtc = DateTime.UtcNow });
        var result = await channel.BasicGetAsync(queue, true);
        Assert.NotNull(result);
        Assert.Equal(id.ToString(), result.BasicProperties.MessageId);
        Assert.Equal(DeliveryModes.Persistent, result.BasicProperties.DeliveryMode);
        await Assert.ThrowsAsync<PublishReturnException>(async () => await publisher.PublishAsync(new OutboxMessage { Id = Guid.NewGuid(), Type = queue + "-missing", Payload = "{}" }));
        await channel.QueueDeleteAsync(queue);
    }
}
