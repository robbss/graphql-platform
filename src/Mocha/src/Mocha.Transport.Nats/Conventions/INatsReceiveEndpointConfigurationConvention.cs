namespace Mocha.Transport.Nats;

/// <summary>
/// Convention that applies default configuration values to NATS receive endpoint configurations.
/// </summary>
public interface INatsReceiveEndpointConfigurationConvention
    : IEndpointConfigurationConvention<ReceiveEndpointConfiguration>
{
    void IEndpointConfigurationConvention<ReceiveEndpointConfiguration>.Configure(
        IMessagingConfigurationContext context,
        MessagingTransport transport,
        ReceiveEndpointConfiguration configuration)
    {
        if (configuration is not NatsReceiveEndpointConfiguration natsConfiguration)
        {
            return;
        }

        if (transport is not NatsMessagingTransport natsTransport)
        {
            return;
        }

        Configure(context, natsTransport, natsConfiguration);
    }

    /// <summary>
    /// Applies convention-defined defaults to a NATS receive endpoint configuration.
    /// </summary>
    void Configure(
        IMessagingConfigurationContext context,
        NatsMessagingTransport transport,
        NatsReceiveEndpointConfiguration configuration);
}
