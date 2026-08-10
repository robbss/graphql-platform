namespace Mocha.Transport.Nats.Features;

/// <summary>
/// Provides access to NATS-specific dispatch options when publishing messages.
/// </summary>
public sealed class NatsDispatchFeature
{
    /// <summary>
    /// Gets or sets the target subject to publish to.
    /// </summary>
    public string? Subject { get; set; }

    /// <summary>
    /// Gets or sets the target stream name if dispatching directly to a stream.
    /// </summary>
    public string? StreamName { get; set; }
}
