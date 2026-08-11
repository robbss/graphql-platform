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
    /// <summary>
    /// JetStream's <c>err_code</c> for a stream whose subjects overlap an existing stream's.
    /// </summary>
    private const int SubjectOverlapErrorCode = 10065;

    private readonly Action<INatsMessagingTransportDescriptor> _configure;
    private NatsMessagingTopology _topology = null!;
    private ILogger _logger = null!;
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

        _logger = services.GetRequiredService<ILogger<NatsMessagingTransport>>();

        Connection =
            configuration.ConnectionProvider?.Invoke(context.Services)
            ?? new NatsConnectionProvider(services.GetRequiredService<INatsConnection>());

        JetStream = new NatsJSContext(Connection.Connection);

        _serviceName =
            configuration.ServiceName
            ?? context.Host?.ServiceName
            ?? NatsTransportConfiguration.DefaultName;

        SchedulingEnabled = configuration.EnableScheduling;

        WarnOnLossySubscriptionDefaults();
        WarnOnDivergentServiceNames(configuration.ServiceName, context.Host?.ServiceName);

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

    /// <summary>
    /// Reports the case where the transport is named one thing and the host another, because the two
    /// names control different halves of the topology.
    /// </summary>
    // The transport name only names the convention stream. Durable consumer names come from the
    // shared naming conventions, which scope them by the host's service name, and that falls back to
    // the entry assembly name. Two services that end up with the same host service name therefore
    // share a durable and silently compete for messages instead of each receiving a copy.
    private void WarnOnDivergentServiceNames(string? transportServiceName, string? hostServiceName)
    {
        if (transportServiceName is null
            || string.Equals(transportServiceName, hostServiceName, StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        _logger.DivergentServiceNames(transportServiceName, hostServiceName);
    }

    private void WarnOnLossySubscriptionDefaults()
    {
        if (Connection.Connection.Opts.SubPendingChannelFullMode == BoundedChannelFullMode.Wait)
        {
            return;
        }

        _logger.LossySubscriptionDefaults(
            Connection.Connection.Opts.SubPendingChannelFullMode.ToString(),
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
    // The ordering is load-bearing. A JetStream publish to a subject no stream captures fails with
    // no-responders rather than silently succeeding the way RabbitMQ does, and consumers can only be
    // created once their stream exists.
    protected override async ValueTask OnBeforeStartAsync(
        IMessagingConfigurationContext context,
        CancellationToken cancellationToken)
    {
        Capabilities = NatsServerCapabilities.FromServerVersion(Connection.Connection.ServerInfo?.Version);

        await EnsureConventionStreamAsync(cancellationToken);

        WarnOnUnwieldyNames();

        await ProvisionStreamsAsync(cancellationToken);

        await NatsStreamResolver.ResolveAsync(JetStream, _topology, cancellationToken);

        await NatsStreamResolver.VerifySubjectsAsync(JetStream, _topology, cancellationToken);

        await ProvisionConsumersAsync(cancellationToken);
    }

    /// <summary>
    /// Creates a stream for the subjects this service publishes that nothing else already captures.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    // Subjects are derived from message types, so every service that touches a message type derives
    // the same subject, and JetStream requires a stream's subjects to be disjoint from every other
    // stream's. A service therefore claims only what is unclaimed and binds to the owning stream for
    // the rest, which is what lets several services subscribe to one event.
    //
    // Deferred until start-up because the subjects a service publishes are only known once routing
    // has resolved its endpoints, and because whether a subject is already captured is a question
    // only the server can answer.
    private async ValueTask EnsureConventionStreamAsync(CancellationToken cancellationToken)
    {
        var unclaimed = new List<string>();

        foreach (var subject in _topology.Subjects)
        {
            if (subject.IsCore
                || subject.StreamName is not null
                || unclaimed.Contains(subject.Subject, StringComparer.Ordinal))
            {
                continue;
            }

            // A stream declared on this bus is authoritative, so no round trip is needed for it.
            if (_topology.FindStreamForSubject(subject.Subject) is not null)
            {
                continue;
            }

            if (await NatsStreamResolver.IsCapturedAsync(JetStream, subject.Subject, cancellationToken))
            {
                continue;
            }

            unclaimed.Add(subject.Subject);
        }

        if (unclaimed.Count == 0)
        {
            return;
        }

        if (SchedulingEnabled)
        {
            unclaimed.AddRange([.. unclaimed.Select(NatsScheduling.ToSchedulingSubject)]);
        }

        _topology.AddStream(new NatsStreamConfiguration
        {
            Name = NatsNaming.ToStreamName(_serviceName),
            Subjects = unclaimed,
            AllowMsgTtl = SchedulingEnabled,
            AllowMsgSchedules = SchedulingEnabled,
            Origin = TopologyOrigin.Convention
        });
    }

    /// <summary>
    /// Reports stream and consumer names long enough to make the server's storage directory names
    /// unwieldy.
    /// </summary>
    private void WarnOnUnwieldyNames()
    {
        foreach (var stream in _topology.Streams)
        {
            if (stream.Name.Length > NatsNaming.RecommendedMaxNameLength)
            {
                _logger.UnwieldyName("stream", stream.Name, NatsNaming.RecommendedMaxNameLength);
            }
        }

        foreach (var consumer in _topology.Consumers)
        {
            if (consumer.Name.Length > NatsNaming.RecommendedMaxNameLength)
            {
                _logger.UnwieldyName("consumer", consumer.Name, NatsNaming.RecommendedMaxNameLength);
            }
        }
    }

    private async ValueTask ProvisionStreamsAsync(CancellationToken cancellationToken)
    {
        foreach (var stream in _topology.Streams.ToList())
        {
            if (!ShouldProvision(stream))
            {
                continue;
            }

            AssertStreamSupported(stream);

            try
            {
                await stream.ProvisionAsync(JetStream, cancellationToken);
            }
            catch (NatsJSApiException exception)
                when (stream.Origin is TopologyOrigin.Convention && IsSubjectOverlap(exception))
            {
                // Another service claimed these subjects between the check and the create. Yielding
                // rather than failing start-up: the subjects resolve to the stream that won, which
                // is the same outcome as having lost the race by a second.
                _logger.YieldedConventionStream(stream.Name, exception.Error.Description);

                _topology.RemoveStream(stream);
            }
        }
    }

    private static bool IsSubjectOverlap(NatsJSApiException exception)
        => exception.Error.ErrCode == SubjectOverlapErrorCode;

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

                        // Zero means the server's default applies rather than deduplication being
                        // off, so reporting the number would misdescribe the stream.
                        ["duplicateWindow"] = stream.DuplicateWindow == TimeSpan.Zero
                            ? "server default"
                            : stream.DuplicateWindow.ToString(),
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
