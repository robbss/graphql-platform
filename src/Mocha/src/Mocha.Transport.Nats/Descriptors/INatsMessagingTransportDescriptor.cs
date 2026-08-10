namespace Mocha.Transport.Nats;

/// <summary>
/// Fluent descriptor for configuring a NATS (JetStream) messaging transport.
/// </summary>
public interface INatsMessagingTransportDescriptor : IMessagingTransportDescriptor
{
    /// <summary>
    /// Configures the connection provider for NATS.
    /// </summary>
    /// <param name="provider">A delegate that resolves or creates an <see cref="INatsConnectionProvider"/>.</param>
    /// <returns>The descriptor instance for method chaining.</returns>
    INatsMessagingTransportDescriptor Connection(Func<IServiceProvider, INatsConnectionProvider> provider);

    /// <summary>
    /// Configures NATS connection parameters using NATS host and port.
    /// </summary>
    /// <param name="host">The NATS server host name or IP address.</param>
    /// <param name="port">The NATS server port (defaults to 4222).</param>
    /// <returns>The descriptor instance for method chaining.</returns>
    INatsMessagingTransportDescriptor Host(string host, int port = 4222);

    /// <summary>
    /// Enables or disables automatic provisioning of Streams and Consumers.
    /// </summary>
    /// <param name="autoProvision"><c>true</c> to auto-provision topology on startup; otherwise <c>false</c>.</param>
    /// <returns>The descriptor instance for method chaining.</returns>
    INatsMessagingTransportDescriptor AutoProvision(bool autoProvision = true);

    /// <summary>
    /// Adds and configures a JetStream stream topology resource.
    /// </summary>
    /// <param name="name">The name of the JetStream stream.</param>
    /// <param name="configure">A delegate to configure stream parameters.</param>
    /// <returns>The descriptor instance for method chaining.</returns>
    INatsMessagingTransportDescriptor AddStream(string name, Action<INatsStreamDescriptor> configure);

    /// <summary>
    /// Adds and configures a JetStream consumer topology resource.
    /// </summary>
    /// <param name="name">The name of the JetStream consumer.</param>
    /// <param name="streamName">The name of the target JetStream stream.</param>
    /// <param name="configure">A delegate to configure consumer parameters.</param>
    /// <returns>The descriptor instance for method chaining.</returns>
    INatsMessagingTransportDescriptor AddConsumer(string name, string streamName, Action<INatsConsumerDescriptor> configure);

    /// <summary>
    /// Adds a receive endpoint to the transport.
    /// </summary>
    /// <param name="consumerName">The consumer name bound to this receive endpoint.</param>
    /// <param name="streamName">The stream name bound to this receive endpoint.</param>
    /// <param name="configure">A delegate to configure receive endpoint parameters.</param>
    /// <returns>The descriptor instance for method chaining.</returns>
    INatsMessagingTransportDescriptor AddReceiveEndpoint(string consumerName, string streamName, Action<INatsReceiveEndpointDescriptor> configure);

    /// <summary>
    /// Adds a dispatch endpoint to the transport.
    /// </summary>
    /// <param name="subject">The NATS subject pattern to publish messages to.</param>
    /// <param name="configure">A delegate to configure dispatch endpoint parameters.</param>
    /// <returns>The descriptor instance for method chaining.</returns>
    INatsMessagingTransportDescriptor AddDispatchEndpoint(string subject, Action<INatsDispatchEndpointDescriptor> configure);
}

/// <summary>
/// Fluent descriptor interface for configuring a JetStream stream.
/// </summary>
public interface INatsStreamDescriptor
{
    /// <summary>
    /// Specifies the subject patterns assigned to this stream.
    /// </summary>
    /// <param name="subjects">One or more NATS subject patterns.</param>
    /// <returns>The descriptor instance for method chaining.</returns>
    INatsStreamDescriptor Subjects(params string[] subjects);

    /// <summary>
    /// Sets the storage type for this stream (File or Memory).
    /// </summary>
    /// <param name="storage">The storage configuration type.</param>
    /// <returns>The descriptor instance for method chaining.</returns>
    INatsStreamDescriptor Storage(NATS.Client.JetStream.Models.StreamConfigStorage storage);

    /// <summary>
    /// Sets the message retention policy for this stream.
    /// </summary>
    /// <param name="retention">The retention policy configuration type.</param>
    /// <returns>The descriptor instance for method chaining.</returns>
    INatsStreamDescriptor Retention(NATS.Client.JetStream.Models.StreamConfigRetention retention);
}

/// <summary>
/// Fluent descriptor interface for configuring a JetStream consumer.
/// </summary>
public interface INatsConsumerDescriptor
{
    /// <summary>
    /// Sets the filter subject for the consumer.
    /// </summary>
    /// <param name="filterSubject">The NATS subject pattern to filter by.</param>
    /// <returns>The descriptor instance for method chaining.</returns>
    INatsConsumerDescriptor FilterSubject(string filterSubject);

    /// <summary>
    /// Sets the acknowledgement wait timeout for unacknowledged messages.
    /// </summary>
    /// <param name="ackWait">The acknowledgement timeout duration.</param>
    /// <returns>The descriptor instance for method chaining.</returns>
    INatsConsumerDescriptor AckWait(TimeSpan ackWait);

    /// <summary>
    /// Sets the maximum delivery attempts for a message before it is considered unrecoverable.
    /// </summary>
    /// <param name="maxDeliver">The maximum delivery count.</param>
    /// <returns>The descriptor instance for method chaining.</returns>
    INatsConsumerDescriptor MaxDeliver(int maxDeliver);
}

/// <summary>
/// Fluent descriptor interface for configuring a NATS receive endpoint.
/// </summary>
public interface INatsReceiveEndpointDescriptor : IReceiveEndpointDescriptor<NatsReceiveEndpointConfiguration>
{
    /// <summary>
    /// Specifies the subject filter pattern for this receive endpoint.
    /// </summary>
    /// <param name="subject">The NATS subject pattern.</param>
    /// <returns>The descriptor instance for method chaining.</returns>
    INatsReceiveEndpointDescriptor Subject(string subject);

    /// <summary>
    /// Sets the maximum prefetch / batch fetch count.
    /// </summary>
    /// <param name="maxPrefetch">The maximum number of messages to pull per batch.</param>
    /// <returns>The descriptor instance for method chaining.</returns>
    INatsReceiveEndpointDescriptor MaxPrefetch(ushort maxPrefetch);
}

/// <summary>
/// Fluent descriptor interface for configuring a NATS dispatch endpoint.
/// </summary>
public interface INatsDispatchEndpointDescriptor : IDispatchEndpointDescriptor<NatsDispatchEndpointConfiguration>
{
    /// <summary>
    /// Specifies the target JetStream stream name.
    /// </summary>
    /// <param name="streamName">The stream name.</param>
    /// <returns>The descriptor instance for method chaining.</returns>
    INatsDispatchEndpointDescriptor StreamName(string streamName);
}
