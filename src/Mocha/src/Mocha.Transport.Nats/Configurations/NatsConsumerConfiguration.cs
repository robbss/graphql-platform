using Mocha;
using NATS.Client.JetStream.Models;

namespace Mocha.Transport.Nats;

/// <summary>
/// Configuration options for a NATS JetStream consumer topology object.
/// </summary>
public sealed class NatsConsumerConfiguration : TopologyConfiguration
{
    /// <summary>
    /// Gets or sets the durable consumer name.
    /// </summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the stream name to consume from.
    /// </summary>
    public string StreamName { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the filter subject for this consumer.
    /// </summary>
    public string? FilterSubject { get; set; }

    /// <summary>
    /// Gets or sets the delivery policy (All, Last, New, etc.).
    /// </summary>
    public ConsumerConfigDeliverPolicy DeliverPolicy { get; set; } = ConsumerConfigDeliverPolicy.All;

    /// <summary>
    /// Gets or sets the ack policy (Explicit, None, All).
    /// </summary>
    public ConsumerConfigAckPolicy AckPolicy { get; set; } = ConsumerConfigAckPolicy.Explicit;

    /// <summary>
    /// Gets or sets the ack wait timeout.
    /// </summary>
    public TimeSpan AckWait { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the maximum delivery attempts before terminating.
    /// </summary>
    public int MaxDeliver { get; set; } = 5;
}
