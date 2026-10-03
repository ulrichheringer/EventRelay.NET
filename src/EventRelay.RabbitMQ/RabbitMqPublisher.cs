using System.Text;
using RabbitMQ.Client;

namespace EventRelay.RabbitMQ;

/// <summary>Persistent mandatory publishing with broker confirmations. Owns its channel, not the supplied connection.</summary>
public sealed class RabbitMqPublisher : IMessagePublisher, IAsyncDisposable
{
    private readonly IChannel channel;
    private readonly string exchange;
    private readonly SemaphoreSlim gate = new(1, 1);

    private RabbitMqPublisher(IChannel channel, string exchange)
    {
        this.channel = channel;
        this.exchange = exchange;
    }

    public static async Task<RabbitMqPublisher> CreateAsync(IConnection connection, string exchange = "", CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(exchange);
        var channel = await connection.CreateChannelAsync(new CreateChannelOptions(true, true), cancellationToken).ConfigureAwait(false);
        return new(channel, exchange);
    }

    public async ValueTask PublishAsync(OutboxMessage message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var properties = new BasicProperties
            {
                MessageId = message.Id.ToString(),
                Type = message.Type,
                ContentType = "application/json",
                Persistent = true,
                Timestamp = new AmqpTimestamp(new DateTimeOffset(DateTime.SpecifyKind(message.OccurredUtc, DateTimeKind.Utc)).ToUnixTimeSeconds())
            };
            await channel.BasicPublishAsync(exchange, message.Type, true, properties, Encoding.UTF8.GetBytes(message.Payload), cancellationToken).ConfigureAwait(false);
        }
        finally { gate.Release(); }
    }

    public async ValueTask DisposeAsync()
    {
        await channel.DisposeAsync().ConfigureAwait(false);
        gate.Dispose();
    }
}
