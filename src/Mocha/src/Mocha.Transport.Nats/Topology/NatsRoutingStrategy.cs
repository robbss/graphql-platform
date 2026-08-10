namespace Mocha.Transport.Nats;

/// <summary>
/// Defines the endpoint and topology routing layout for NATS (JetStream).
/// </summary>
public sealed class NatsRoutingStrategy : RoutingStrategy<NatsMessagingTransport>
{
    public override DispatchEndpointConfiguration? CreateEndpointConfiguration(
        IMessagingConfigurationContext context,
        OutboundRoute route)
    {
        var subject = route.MessageType is not null
            ? context.Naming.GetPublishEndpointName(route.MessageType.RuntimeType)
            : "default";

        return new NatsDispatchEndpointConfiguration
        {
            Subject = subject,
            Name = subject
        };
    }

    public override DispatchEndpointConfiguration? CreateEndpointConfiguration(
        IMessagingConfigurationContext context,
        Uri address)
    {
        var subject = address.AbsolutePath.TrimStart('/');
        return new NatsDispatchEndpointConfiguration
        {
            Subject = subject,
            Name = subject
        };
    }

    public override ReceiveEndpointConfiguration? CreateEndpointConfiguration(
        IMessagingConfigurationContext context,
        InboundRoute route)
    {
        var consumerName = context.Naming.GetReceiveEndpointName(route, ReceiveEndpointKind.Default);

        return new NatsReceiveEndpointConfiguration
        {
            ConsumerName = consumerName,
            StreamName = "MOCHA_EVENTS",
            Name = consumerName
        };
    }
}
