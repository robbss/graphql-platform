using Mocha.Middlewares;

namespace Mocha.Transport.Nats;

/// <summary>
/// Fluent descriptor for a receive endpoint backed by a durable JetStream consumer.
/// </summary>
public interface INatsReceiveEndpointDescriptor
    : IReceiveEndpointDescriptor<NatsReceiveEndpointConfiguration>
{
    /// <inheritdoc cref="IReceiveEndpointDescriptor{T}.Handler{THandler}" />
    new INatsReceiveEndpointDescriptor Handler<THandler>() where THandler : class, IHandler;

    /// <inheritdoc cref="IReceiveEndpointDescriptor{T}.Handler(Type)" />
    new INatsReceiveEndpointDescriptor Handler(Type handlerType);

    /// <inheritdoc cref="IReceiveEndpointDescriptor{T}.Consumer{TConsumer}" />
    new INatsReceiveEndpointDescriptor Consumer<TConsumer>() where TConsumer : class, IConsumer;

    /// <inheritdoc cref="IReceiveEndpointDescriptor{T}.Consumer(Type)" />
    new INatsReceiveEndpointDescriptor Consumer(Type consumerType);

    /// <inheritdoc cref="IReceiveEndpointDescriptor{T}.Receives{TMessage}" />
    new INatsReceiveEndpointDescriptor Receives<TMessage>();

    /// <inheritdoc cref="IReceiveEndpointDescriptor{T}.Receives(Type)" />
    new INatsReceiveEndpointDescriptor Receives(Type messageType);

    /// <inheritdoc cref="IReceiveEndpointDescriptor{T}.MaxConcurrency" />
    new INatsReceiveEndpointDescriptor MaxConcurrency(int maxConcurrency);

    /// <summary>
    /// Reads from a specific stream instead of resolving the owning stream at start-up.
    /// </summary>
    /// <param name="streamName">The stream name.</param>
    /// <returns>The descriptor for method chaining.</returns>
    INatsReceiveEndpointDescriptor FromStream(string streamName);

    /// <summary>
    /// Sets the durable consumer name, which defaults to the sanitised endpoint name.
    /// </summary>
    /// <param name="consumerName">The durable consumer name.</param>
    /// <returns>The descriptor for method chaining.</returns>
    INatsReceiveEndpointDescriptor ConsumerName(string consumerName);

    /// <summary>
    /// Adds a subject this endpoint receives, in addition to any derived from its handlers.
    /// </summary>
    /// <param name="subject">The subject or wildcard filter.</param>
    /// <returns>The descriptor for method chaining.</returns>
    INatsReceiveEndpointDescriptor Subject(string subject);

    /// <inheritdoc cref="IReceiveEndpointDescriptor{T}.UseReceive" />
    new INatsReceiveEndpointDescriptor UseReceive(
        ReceiveMiddlewareConfiguration configuration,
        string? before = null,
        string? after = null);
}
