namespace Mocha.Transport.Nats;

/// <summary>
/// Configuration for a receive endpoint consuming from a durable JetStream pull consumer.
/// </summary>
public sealed class NatsReceiveEndpointConfiguration : ReceiveEndpointConfiguration
{
    /// <summary>
    /// Gets or sets the durable consumer name.
    /// </summary>
    public string? ConsumerName { get; set; }

    /// <summary>
    /// Gets or sets the stream this consumer reads from.
    /// </summary>
    /// <remarks>
    /// Left unset for subjects published by another service, where the owning stream is resolved
    /// against the server during start-up.
    /// </remarks>
    public string? StreamName { get; set; }

    /// <summary>
    /// Gets or sets the subjects this endpoint receives.
    /// </summary>
    public List<string> FilterSubjects { get; set; } = [];
}
