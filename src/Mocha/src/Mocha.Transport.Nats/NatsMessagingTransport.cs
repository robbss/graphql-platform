using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;

namespace Mocha.Transport.Nats;

/// <summary>
/// NATS (JetStream) implementation of <see cref="MessagingTransport"/>.
/// </summary>
public sealed class NatsMessagingTransport(Action<INatsMessagingTransportDescriptor> configure) : MessagingTransport
{
    private NatsMessagingTopology _topology = null!;

    public const string DefaultSchema = "nats";

    public override MessagingTopology Topology => _topology;

    public INatsConnection Connection { get; private set; } = null!;
    public INatsJSContext JSContext { get; private set; } = null!;
    public ILogger Logger { get; private set; } = null!;

    protected override void OnAfterInitialized(IMessagingSetupContext context)
    {
        Schema = DefaultSchema;
        var config = (NatsTransportConfiguration)Configuration;

        var provider = config.ConnectionProvider?.Invoke(context.Services)
            ?? new NatsConnectionProvider(
                context.Services.GetService<INatsConnection>()
                ?? new NatsConnection(NatsOpts.Default));

        var rootUri = new Uri($"{Schema}://{provider.Host}:{provider.Port}");
        _topology = new NatsMessagingTopology(this, rootUri, config.AutoProvision ?? true);

        foreach (var s in config.Streams)
        {
            _topology.AddStream(s);
        }

        foreach (var c in config.Consumers)
        {
            _topology.AddConsumer(c);
        }
    }

    protected override async ValueTask OnBeforeStartAsync(
        IMessagingConfigurationContext context,
        CancellationToken cancellationToken)
    {
        var config = (NatsTransportConfiguration)Configuration;
        var provider = config.ConnectionProvider?.Invoke(context.Services)
            ?? new NatsConnectionProvider(
                context.Services.GetService<INatsConnection>()
                ?? new NatsConnection(NatsOpts.Default));

        Connection = await provider.GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        JSContext = new NatsJSContext(Connection);

        var loggerFactory = context.Services.GetService<ILoggerFactory>();
        Logger = loggerFactory?.CreateLogger<NatsMessagingTransport>()
            ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<NatsMessagingTransport>.Instance;

        await _topology.AutoProvisionAsync(JSContext, cancellationToken).ConfigureAwait(false);
    }

    protected override MessagingTransportConfiguration CreateConfiguration(IMessagingSetupContext context)
    {
        var descriptor = new NatsMessagingTransportDescriptor(context);
        configure(descriptor);
        return descriptor.CreateConfiguration();
    }

    protected override ReceiveEndpoint CreateReceiveEndpoint()
    {
        return new NatsReceiveEndpoint(this);
    }

    protected override DispatchEndpoint CreateDispatchEndpoint()
    {
        return new NatsDispatchEndpoint(this);
    }

    public override bool TryGetDispatchEndpoint(Uri address, [NotNullWhen(true)] out DispatchEndpoint? endpoint)
    {
        foreach (var candidate in DispatchEndpoints)
        {
            if (!candidate.IsCompleted)
            {
                continue;
            }

            if (candidate.Address == address)
            {
                endpoint = candidate;
                return true;
            }
        }

        endpoint = null;
        return false;
    }

    public async ValueTask PublishAsync(
        string subject,
        byte[] payload,
        NatsHeaders? headers = null,
        CancellationToken cancellationToken = default)
    {
        await JSContext.PublishAsync(
            subject,
            payload,
            headers: headers,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    public override async ValueTask DisposeAsync()
    {
        if (Connection is not null)
        {
            await Connection.DisposeAsync().ConfigureAwait(false);
        }
    }
}
