using Mocha.Middlewares;
using Mocha.Scheduling;

namespace Mocha.Transport.Nats;

/// <summary>
/// Hands scheduled messages to JetStream rather than storing them.
/// </summary>
/// <remarks>
/// The Postgres transport persists scheduled messages in a table and a worker dispatches them when
/// due. JetStream holds a scheduled message itself, so this store publishes straight away with the
/// schedule headers attached and lets the server release it at the right time. Mocha still requires
/// a store to be registered, because its dispatch pipeline refuses a scheduled send for a transport
/// that has not declared one.
/// </remarks>
internal sealed class NatsScheduledMessageStore : IScheduledMessageStore
{
    /// <summary>
    /// The prefix identifying cancellation tokens issued by this store.
    /// </summary>
    internal const string TokenPrefix = "nats-transport:";

    /// <inheritdoc />
    public async ValueTask<string> PersistAsync(
        IDispatchContext context,
        CancellationToken cancellationToken)
    {
        if (context.Endpoint is not NatsDispatchEndpoint endpoint)
        {
            throw new InvalidOperationException(
                "The NATS scheduled message store requires a NATS dispatch endpoint.");
        }

        if (context.Envelope is not { } envelope)
        {
            throw new InvalidOperationException("Envelope is not set.");
        }

        if (envelope.ScheduledTime is null)
        {
            throw new InvalidOperationException("Scheduled time is not set on the envelope.");
        }

        await endpoint.DispatchScheduledAsync(context, cancellationToken);

        return TokenPrefix + (envelope.MessageId ?? Guid.NewGuid().ToString("N"));
    }

    /// <inheritdoc />
    /// <remarks>
    /// Once JetStream holds a scheduled message there is no supported way to withdraw it by
    /// identifier, so cancellation is reported as unsupported rather than silently claiming success.
    /// </remarks>
    public ValueTask<bool> CancelAsync(string token, CancellationToken cancellationToken)
        => ValueTask.FromResult(false);
}
