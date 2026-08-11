using Microsoft.Extensions.DependencyInjection;
using Mocha.Middlewares;
using Mocha.Scheduling;
using Mocha.Transport.Nats.Middlewares;

namespace Mocha.Transport.Nats;

/// <summary>
/// Extension methods for registering the NATS JetStream messaging transport on an
/// <see cref="IMessageBusHostBuilder"/>.
/// </summary>
public static class MessageBusBuilderExtensions
{
    /// <summary>
    /// Adds a NATS JetStream messaging transport to the message bus, applying the specified
    /// configuration delegate after default conventions and middleware have been registered.
    /// </summary>
    /// <param name="busBuilder">The message bus host builder to extend.</param>
    /// <param name="configure">A delegate that configures the NATS transport descriptor.</param>
    /// <returns>The builder for method chaining.</returns>
    public static IMessageBusHostBuilder AddNats(
        this IMessageBusHostBuilder busBuilder,
        Action<INatsMessagingTransportDescriptor> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var transport = new NatsMessagingTransport(x => configure(x.AddDefaults()));

        busBuilder.ConfigureMessageBus(b => b.AddTransport(transport));

        // JetStream holds a scheduled message itself, so the store publishes immediately with the
        // schedule headers rather than persisting anything of its own.
        busBuilder.Services.AddSingleton(
            new ScheduledMessageStoreRegistration(
                transport,
                NatsScheduledMessageStore.TokenPrefix,
                _ => new NatsScheduledMessageStore()));

        return busBuilder;
    }

    /// <summary>
    /// Adds a NATS JetStream messaging transport to the message bus with default configuration.
    /// </summary>
    /// <param name="busBuilder">The message bus host builder to extend.</param>
    /// <returns>The builder for method chaining.</returns>
    public static IMessageBusHostBuilder AddNats(this IMessageBusHostBuilder busBuilder)
    {
        return busBuilder.AddNats(static _ => { });
    }
}

/// <summary>
/// Extension methods applying the transport's built-in defaults to a descriptor.
/// </summary>
public static class NatsMessagingTransportDescriptorExtensions
{
    /// <summary>
    /// The URI scheme used by the NATS transport.
    /// </summary>
    public const string DefaultSchema = NatsTransportConfiguration.DefaultSchema;

    /// <summary>
    /// Applies the transport defaults before any user configuration runs.
    /// </summary>
    /// <param name="descriptor">The descriptor to configure.</param>
    /// <returns>The descriptor for method chaining.</returns>
    public static INatsMessagingTransportDescriptor AddDefaults(
        this INatsMessagingTransportDescriptor descriptor)
    {
        ArgumentNullException.ThrowIfNull(descriptor);

        descriptor
            .Schema(DefaultSchema)
            .UseRoutingStrategy(static _ => new NatsRoutingStrategy());

        descriptor.UseReceive(
            NatsReceiveMiddlewares.Acknowledgement,
            after: ReceiveMiddlewares.ConcurrencyLimiter.Key);

        descriptor.UseReceive(
            NatsReceiveMiddlewares.Parsing,
            after: NatsReceiveMiddlewares.Acknowledgement.Key);

        return descriptor;
    }
}
