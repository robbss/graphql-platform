namespace Mocha.Transport.Nats;

/// <summary>
/// Configuration for a dispatch endpoint publishing to a JetStream subject.
/// </summary>
public sealed class NatsDispatchEndpointConfiguration : DispatchEndpointConfiguration
{
    /// <summary>
    /// Gets or sets the subject this endpoint publishes to.
    /// </summary>
    public string? Subject { get; set; }

    /// <summary>
    /// Gets or sets the stream expected to capture the subject, when known at configuration time.
    /// </summary>
    public string? StreamName { get; set; }
}
