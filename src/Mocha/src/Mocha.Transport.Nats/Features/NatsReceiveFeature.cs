using NATS.Client.Core;

namespace Mocha.Transport.Nats.Features;

/// <summary>
/// Provides access to NATS-specific message context for incoming messages.
/// </summary>
public sealed class NatsReceiveFeature
{
    /// <summary>
    /// Gets or sets the underlying NATS message.
    /// </summary>
    public NatsMsg<byte[]>? Message { get; set; }

    /// <summary>
    /// Gets or sets the subject on which the message was received.
    /// </summary>
    public string? Subject => Message?.Subject;

    /// <summary>
    /// Gets or sets the reply subject if present.
    /// </summary>
    public string? ReplyTo => Message?.ReplyTo;
}
