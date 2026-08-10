namespace Mocha.Transport.Nats;

/// <summary>
/// Configuration for a NATS receive endpoint.
/// </summary>
public sealed class NatsReceiveEndpointConfiguration : ReceiveEndpointConfiguration
{
    /// <summary>
    /// Gets or sets the name of the NATS consumer to bind to.
    /// </summary>
    public string? ConsumerName { get; set; }

    /// <summary>
    /// Gets or sets the name of the NATS stream.
    /// </summary>
    public string? StreamName { get; set; }

    /// <summary>
    /// Gets or sets the subject pattern for filtering messages.
    /// </summary>
    public string? Subject { get; set; }

    /// <summary>
    /// Gets or sets the maximum prefetch / batch fetch count.
    /// </summary>
    public ushort MaxPrefetch { get; set; } = 100;
}

/// <summary>
/// Configuration for a NATS dispatch endpoint.
/// </summary>
public sealed class NatsDispatchEndpointConfiguration : DispatchEndpointConfiguration
{
    /// <summary>
    /// Gets or sets the default subject to publish messages to.
    /// </summary>
    public string? Subject { get; set; }

    /// <summary>
    /// Gets or sets the target stream name.
    /// </summary>
    public string? StreamName { get; set; }
}
