using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;

namespace Mocha.Transport.Nats;

/// <summary>
/// NATS JetStream implementation of <see cref="MessagingTransport"/> that manages the connection,
/// stream and consumer provisioning, and the lifecycle of receive and dispatch endpoints.
/// </summary>
public sealed class NatsMessagingTransport : MessagingTransport
{
    private readonly Action<INatsMessagingTransportDescriptor> _configure;
    private NatsMessagingTopology _topology = null!;
    private string _serviceName = NatsTransportConfiguration.DefaultName;

    /// <summary>
    /// Creates a new NATS transport with the specified configuration delegate.
    /// </summary>
    /// <param name="configure">A delegate that configures the transport descriptor.</param>
    public NatsMessagingTransport(Action<INatsMessagingTransportDescriptor> configure)
    {
        _configure = configure;
    }

    /// <inheritdoc />
    public override MessagingTopology Topology => _topology;

    /// <summary>
    /// Gets the JetStream context used to provision topology and publish messages.
    /// </summary>
    public INatsJSContext JetStream { get; private set; } = null!;

    /// <summary>
    /// Gets the provider supplying the connection this transport uses.
    /// </summary>
    public INatsConnectionProvider Connection { get; private set; } = null!;

    /// <inheritdoc />
    protected override void OnAfterInitialized(IMessagingSetupContext context)
    {
        var configuration = (NatsTransportConfiguration)Configuration;
        var services = context.Services.GetApplicationServices();

        Connection =
            configuration.ConnectionProvider?.Invoke(context.Services)
            ?? new NatsConnectionProvider(services.GetRequiredService<INatsConnection>());

        JetStream = new NatsJSContext(Connection.Connection);

        _serviceName =
            configuration.ServiceName
            ?? context.Host?.ServiceName
            ?? NatsTransportConfiguration.DefaultName;

        SchedulingEnabled = configuration.EnableScheduling;

        WarnOnLossySubscriptionDefaults(services);

        var builder = new UriBuilder
        {
            Scheme = Schema,
            Host = Connection.Host,
            Port = Connection.Port
        };

        _topology = new NatsMessagingTopology(this, builder.Uri, configuration.AutoProvision ?? true);

        foreach (var stream in configuration.Streams)
        {
            _topology.AddStream(stream);
        }

        foreach (var consumer in configuration.Consumers)
        {
            _topology.AddConsumer(consumer);
        }
    }

    private void WarnOnLossySubscriptionDefaults(IServiceProvider services)
    {
        if (Connection.Connection.Opts.SubPendingChannelFullMode == BoundedChannelFullMode.Wait)
        {
            return;
        }

        services
            .GetRequiredService<ILogger<NatsMessagingTransport>>()
            .LogWarning(
                "The NATS connection uses SubPendingChannelFullMode '{FullMode}', so a subscriber "
                + "falling more than {Capacity} messages behind drops messages instead of applying "
                + "back pressure. JetStream traffic is unaffected because pull consumers are bound "
                + "by MaxAckPending, but request/reply responses can be lost. Set "
                + "SubPendingChannelFullMode to Wait on NatsOpts to avoid this.",
                Connection.Connection.Opts.SubPendingChannelFullMode,
                Connection.Connection.Opts.SubPendingChannelCapacity);
    }

    /// <inheritdoc />
    public override bool TryGetDispatchEndpoint(Uri address, [NotNullWhen(true)] out DispatchEndpoint? endpoint)
    {
        if (TryGetReplyDispatchEndpoint(address, out endpoint))
        {
            return true;
        }

        foreach (var candidate in DispatchEndpoints)
        {
            if (candidate.IsCompleted && candidate.Address == address)
            {
                endpoint = candidate;
                return true;
            }
        }

        if (_topology?.Address.IsBaseOf(address) == true)
        {
            foreach (var candidate in DispatchEndpoints)
            {
                if (candidate.IsCompleted && candidate.Destination?.Address == address)
                {
                    endpoint = candidate;
                    return true;
                }
            }
        }

        if (NatsDestinations.TryResolveExplicit(Schema, address, out var subject))
        {
            foreach (var candidate in DispatchEndpoints)
            {
                if (candidate.IsCompleted
                    && candidate is NatsDispatchEndpoint { Subject.Subject: { } candidateSubject }
                    && candidateSubject == subject)
                {
                    endpoint = candidate;
                    return true;
                }
            }
        }

        endpoint = null;
        return false;
    }

    /// <summary>
    /// Gets the version-gated JetStream features the connected server supports.
    /// </summary>
    public NatsServerCapabilities Capabilities { get; private set; } = NatsServerCapabilities.FromServerVersion(null);

    /// <summary>
    /// Gets a value indicating whether the transport was configured for scheduled and expiring
    /// messages.
    /// </summary>
    public bool SchedulingEnabled { get; private set; }

    /// <summary>
    /// Provisions streams, binds each consumer to the stream capturing its subjects, and then
    /// provisions the consumers, before any endpoint starts.
    /// </summary>
    /// <param name="context">The configuration context for the current start-up phase.</param>
    /// <param name="cancellationToken">A token to cancel start-up.</param>
    /// <remarks>
    /// The ordering is load-bearing. A JetStream publish to a subject no stream captures fails with
    /// no-responders rather than silently succeeding the way RabbitMQ does, and consumers can only
    /// be created once their stream exists.
    /// </remarks>
    protected override async ValueTask OnBeforeStartAsync(
        IMessagingConfigurationContext context,
        CancellationToken cancellationToken)
    {
        Capabilities = NatsServerCapabilities.FromServerVersion(Connection.Connection.ServerInfo?.Version);

        EnsureConventionStream();

        await ProvisionStreamsAsync(cancellationToken);

        await NatsStreamResolver.ResolveAsync(JetStream, _topology, cancellationToken);

        await NatsStreamResolver.VerifySubjectsAsync(JetStream, _topology, cancellationToken);

        await ProvisionConsumersAsync(cancellationToken);
    }

    /// <summary>
    /// Creates the stream capturing this service's own subjects when none was declared.
    /// </summary>
    /// <remarks>
    /// Deferred until start-up because the subjects a service publishes are only known once routing
    /// has resolved its endpoints. Deriving the stream from a configured name instead would produce
    /// a subject filter that does not match the subjects Mocha's naming conventions actually
    /// generate, and every consumer would then fail to resolve its stream.
    /// <para>
    /// Everything this needs is read from the topology rather than from
    /// <see cref="MessagingTransport.Configuration"/>, which is no longer available once setup has
    /// finished.
    /// </para>
    /// </remarks>
    private void EnsureConventionStream()
    {
        if (_topology.Streams.Count > 0)
        {
            return;
        }

        var subjects = _topology.Subjects
            .Where(s => !s.IsCore)
            .Select(s => s.Subject)
            .Distinct(StringComparer.Ordinal)
            .ToList();

        if (subjects.Count == 0)
        {
            return;
        }

        if (SchedulingEnabled)
        {
            subjects.AddRange([.. subjects.Select(NatsScheduling.ToSchedulingSubject)]);
        }

        _topology.AddStream(new NatsStreamConfiguration
        {
            Name = NatsNaming.ToStreamName(_serviceName),
            Subjects = subjects,
            AllowMsgTtl = SchedulingEnabled,
            AllowMsgSchedules = SchedulingEnabled,
            Origin = TopologyOrigin.Convention
        });
    }

    private async ValueTask ProvisionStreamsAsync(CancellationToken cancellationToken)
    {
        foreach (var stream in _topology.Streams)
        {
            if (!ShouldProvision(stream))
            {
                continue;
            }

            AssertStreamSupported(stream);

            await stream.ProvisionAsync(JetStream, cancellationToken);
        }
    }

    private async ValueTask ProvisionConsumersAsync(CancellationToken cancellationToken)
    {
        foreach (var consumer in _topology.Consumers)
        {
            if (ShouldProvision(consumer))
            {
                await consumer.ProvisionAsync(JetStream, cancellationToken);
            }
        }
    }

    private bool ShouldProvision(INatsResource resource)
        => resource.AutoProvision ?? _topology.AutoProvision;

    private void AssertStreamSupported(NatsStream stream)
    {
        if (stream.AllowMsgTtl && !Capabilities.SupportsMessageTtl)
        {
            throw new InvalidOperationException(
                $"Stream '{stream.Name}' enables per-message TTL, which requires NATS server 2.11 "
                + $"or later, but the server reports {Capabilities.Version}.");
        }

        if (stream.AllowMsgSchedules && !Capabilities.SupportsMessageSchedules)
        {
            throw new InvalidOperationException(
                $"Stream '{stream.Name}' enables message schedules, which requires NATS server 2.12 "
                + $"or later, but the server reports {Capabilities.Version}.");
        }
    }

    /// <inheritdoc />
    public override TransportDescription Describe()
    {
        var entities = new List<TopologyEntityDescription>();
        var links = new List<TopologyLinkDescription>();

        foreach (var stream in _topology.Streams)
        {
            entities.Add(
                new TopologyEntityDescription(
                    MochaUrn.TopologyEntity(stream.Address?.ToString(), "stream", stream.Name),
                    "stream",
                    stream.Name,
                    stream.Address?.ToString(),
                    "inbound",
                    new Dictionary<string, object?>
                    {
                        ["subjects"] = string.Join(", ", stream.Subjects),
                        ["duplicateWindow"] = stream.DuplicateWindow,
                        ["allowMsgTtl"] = stream.AllowMsgTtl,
                        ["allowMsgSchedules"] = stream.AllowMsgSchedules,
                        ["autoProvision"] = stream.AutoProvision ?? _topology.AutoProvision,
                        ["origin"] = stream.Origin
                    }));
        }

        foreach (var subject in _topology.Subjects)
        {
            entities.Add(
                new TopologyEntityDescription(
                    MochaUrn.TopologyEntity(subject.Address?.ToString(), "subject", subject.Subject),
                    "subject",
                    subject.Subject,
                    subject.Address?.ToString(),
                    "inbound",
                    new Dictionary<string, object?>
                    {
                        ["stream"] = subject.StreamName,
                        ["core"] = subject.IsCore,
                        ["origin"] = subject.Origin
                    }));
        }

        foreach (var consumer in _topology.Consumers)
        {
            entities.Add(
                new TopologyEntityDescription(
                    MochaUrn.TopologyEntity(consumer.Address?.ToString(), "consumer", consumer.Name),
                    "consumer",
                    consumer.Name,
                    consumer.Address?.ToString(),
                    "outbound",
                    new Dictionary<string, object?>
                    {
                        ["stream"] = consumer.StreamName,
                        ["filterSubjects"] = string.Join(", ", consumer.FilterSubjects),
                        ["maxAckPending"] = consumer.MaxAckPending,
                        ["ackProgressInterval"] = consumer.AckProgressInterval,
                        ["autoProvision"] = consumer.AutoProvision ?? _topology.AutoProvision,
                        ["origin"] = consumer.Origin
                    }));

            if (consumer.StreamName is not { } streamName)
            {
                continue;
            }

            var stream = _topology.Streams.FirstOrDefault(s => s.Name == streamName);

            links.Add(
                new TopologyLinkDescription(
                    MochaUrn.TopologyLink(null, "binding", stream?.Address?.ToString(), consumer.Address?.ToString()),
                    "binding",
                    consumer.Address?.ToString(),
                    stream?.Address?.ToString(),
                    consumer.Address?.ToString(),
                    "forward",
                    new Dictionary<string, object?>
                    {
                        ["filterSubjects"] = string.Join(", ", consumer.FilterSubjects)
                    }));
        }

        return new TransportDescription(
            Urn,
            _topology.Address.ToString(),
            Name,
            Schema,
            GetType().Name,
            [.. ReceiveEndpoints.Select(e => e.Describe())],
            [.. DispatchEndpoints.Select(e => e.Describe())],
            new TopologyDescription(_topology.Address.ToString(), entities, links));
    }

    /// <inheritdoc />
    protected override MessagingTransportConfiguration CreateConfiguration(IMessagingSetupContext context)
    {
        var descriptor = new NatsMessagingTransportDescriptor(context);

        _configure(descriptor);

        return descriptor.CreateConfiguration();
    }

    /// <inheritdoc />
    protected override ReceiveEndpoint CreateReceiveEndpoint()
    {
        return new NatsReceiveEndpoint(this);
    }

    /// <inheritdoc />
    protected override DispatchEndpoint CreateDispatchEndpoint()
    {
        return new NatsDispatchEndpoint(this);
    }
}
