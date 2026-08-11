using NATS.Client.JetStream.Models;

namespace Mocha.Transport.Nats;

/// <summary>
/// Fluent descriptor for declaring a JetStream stream.
/// </summary>
public interface INatsStreamTopologyDescriptor : IMessagingDescriptor<NatsStreamConfiguration>
{
    /// <summary>
    /// Adds a subject the stream captures.
    /// </summary>
    /// <param name="subject">The subject or wildcard filter, for example <c>order-service.&gt;</c>.</param>
    /// <returns>The descriptor for method chaining.</returns>
    INatsStreamTopologyDescriptor Subject(string subject);

    /// <summary>
    /// Sets the retention policy.
    /// </summary>
    /// <param name="retention">The retention policy.</param>
    /// <returns>The descriptor for method chaining.</returns>
    INatsStreamTopologyDescriptor Retention(StreamConfigRetention retention);

    /// <summary>
    /// Sets the storage backend.
    /// </summary>
    /// <param name="storage">The storage backend.</param>
    /// <returns>The descriptor for method chaining.</returns>
    INatsStreamTopologyDescriptor Storage(StreamConfigStorage storage);

    /// <summary>
    /// Sets how long messages are retained.
    /// </summary>
    /// <param name="maxAge">The retention period.</param>
    /// <returns>The descriptor for method chaining.</returns>
    INatsStreamTopologyDescriptor MaxAge(TimeSpan maxAge);

    /// <summary>
    /// Sets the number of replicas.
    /// </summary>
    /// <param name="replicas">The replica count.</param>
    /// <returns>The descriptor for method chaining.</returns>
    INatsStreamTopologyDescriptor Replicas(int replicas);

    /// <summary>
    /// Enables broker-side deduplication over the window, keyed on the message identifier.
    /// </summary>
    /// <param name="window">The deduplication window.</param>
    /// <returns>The descriptor for method chaining.</returns>
    /// <remarks>
    /// Deduplication is disabled by default. Enabling it is a lighter-weight alternative to the
    /// inbox pattern for publishers that may retry.
    /// </remarks>
    INatsStreamTopologyDescriptor DeduplicateWithin(TimeSpan window);

    /// <summary>
    /// Enables per-message time to live, requiring NATS server 2.11 or later.
    /// </summary>
    /// <param name="allow">Whether to honour TTL headers.</param>
    /// <returns>The descriptor for method chaining.</returns>
    INatsStreamTopologyDescriptor AllowMessageTtl(bool allow = true);

    /// <summary>
    /// Enables message scheduling, requiring NATS server 2.12 or later.
    /// </summary>
    /// <param name="allow">Whether to allow scheduled messages.</param>
    /// <returns>The descriptor for method chaining.</returns>
    INatsStreamTopologyDescriptor AllowMessageSchedules(bool allow = true);

    /// <summary>
    /// Controls whether this stream is provisioned during start-up.
    /// </summary>
    /// <param name="autoProvision">Whether to provision the stream.</param>
    /// <returns>The descriptor for method chaining.</returns>
    INatsStreamTopologyDescriptor AutoProvision(bool autoProvision = true);
}
