using System.ComponentModel.DataAnnotations.Schema;
using System.Text.Json;

namespace EventRelay;

/// <summary>An explicitly versioned, stable wire name and JSON payload.</summary>
public sealed record DomainEvent(Guid Id, string Type, string Payload, DateTime OccurredUtc)
{
    /// <summary>Serialize an event without embedding CLR type names.</summary>
    public static DomainEvent Create<T>(string type, T payload, JsonSerializerOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(type);
        if (type.Length > 200) throw new ArgumentOutOfRangeException(nameof(type));
        return new(Guid.NewGuid(), type, JsonSerializer.Serialize(payload, options), DateTime.UtcNow);
    }
}

/// <summary>Optional aggregate base class; EF ignores its pending events.</summary>
public abstract class EventSource
{
    private readonly List<DomainEvent> events = [];

    [NotMapped]
    public IReadOnlyList<DomainEvent> PendingEvents => events.AsReadOnly();

    /// <summary>Raise a serialized event for the next successful SaveChanges.</summary>
    protected void Raise<T>(string type, T payload) => events.Add(DomainEvent.Create(type, payload));

    internal void Acknowledge(IReadOnlyCollection<Guid> ids) => events.RemoveAll(e => ids.Contains(e.Id));
}

/// <summary>Persistent envelope. Consumers must deduplicate by Id.</summary>
public sealed class OutboxMessage
{
    public Guid Id { get; set; }
    public string Type { get; set; } = "";
    public string Payload { get; set; } = "";
    public DateTime OccurredUtc { get; set; }
    public DateTime AvailableUtc { get; set; }
    public DateTime? LeaseUntilUtc { get; set; }
    public Guid? LeaseId { get; set; }
    public int Attempts { get; set; }
    public DateTime? DeliveredUtc { get; set; }
    public bool Dead { get; set; }
    public string? LastError { get; set; }
}

/// <summary>Return only after the broker has accepted the message durably.</summary>
public interface IMessagePublisher
{
    ValueTask PublishAsync(OutboxMessage message, CancellationToken cancellationToken = default);
}
