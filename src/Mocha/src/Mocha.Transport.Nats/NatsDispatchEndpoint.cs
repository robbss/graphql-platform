using Mocha.Middlewares;
using Mocha.Transport.Nats.Features;

namespace Mocha.Transport.Nats;

/// <summary>
/// NATS dispatch endpoint that publishes outbound messages to NATS subjects via JetStream.
/// </summary>
/// <param name="transport">The owning NATS transport instance.</param>
public sealed class NatsDispatchEndpoint(NatsMessagingTransport transport)
    : DispatchEndpoint<NatsDispatchEndpointConfiguration>(transport)
{
    public string Subject { get; private set; } = string.Empty;

    protected override void OnInitialize(
        IMessagingConfigurationContext context,
        NatsDispatchEndpointConfiguration configuration)
    {
        Subject = configuration.Subject ?? Name;
    }

    protected override void OnComplete(
        IMessagingConfigurationContext context,
        NatsDispatchEndpointConfiguration configuration)
    {
        var topology = (NatsMessagingTopology)Transport.Topology;
        var stream = topology.Streams.FirstOrDefault(s => s.Name == (configuration.StreamName ?? "MOCHA_EVENTS"));
        if (stream is not null)
        {
            Destination = stream;
        }
    }

    protected override async ValueTask DispatchAsync(IDispatchContext context)
    {
        if (context.Envelope is not { } envelope)
        {
            throw new InvalidOperationException("Envelope is not set");
        }

        if (Transport is not NatsMessagingTransport natsTransport)
        {
            throw new InvalidOperationException("Transport is not a NatsMessagingTransport");
        }

        var feature = context.Features.Get<NatsDispatchFeature>();
        var targetSubject = feature?.Subject ?? Subject;

        var natsHeaders = NatsMessageHeaders.ToNatsHeaders(envelope);
        var payload = envelope.Body.ToArray();

        await natsTransport.PublishAsync(targetSubject, payload, natsHeaders, context.CancellationToken).ConfigureAwait(false);
    }
}
