using Mocha.Features;

namespace Mocha.Transport.Nats;

/// <summary>
/// Resolves Mocha routes into JetStream subjects and durable consumers.
/// </summary>
public sealed class NatsRoutingStrategy : RoutingStrategy<NatsMessagingTransport>
{
    private NatsMessagingTopology Topology => (NatsMessagingTopology)Transport.Topology;

    /// <inheritdoc />
    public override DispatchEndpointConfiguration? CreateEndpointConfiguration(
        IMessagingConfigurationContext context,
        OutboundRoute route)
    {
        if (route.Kind is not (OutboundRouteKind.Send or OutboundRouteKind.Publish))
        {
            return null;
        }

        var subject = NatsDestinations.Resolve(Transport.Schema, context.Naming, route);

        return new NatsDispatchEndpointConfiguration
        {
            Subject = subject,
            Name = NatsAddress.SubjectSegment + "/" + subject
        };
    }

    /// <inheritdoc />
    public override DispatchEndpointConfiguration? CreateEndpointConfiguration(
        IMessagingConfigurationContext context,
        Uri address)
    {
        var segments = address.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries);

        if (address.Scheme == Transport.Schema
            && address.Host is ""
            && segments is ["replies"])
        {
            return new NatsDispatchEndpointConfiguration
            {
                Kind = DispatchEndpointKind.Reply,
                Subject = context.Naming.GetInstanceEndpoint(context.Host.InstanceId),
                Name = "Replies"
            };
        }

        if (NatsDestinations.TryResolveExplicit(Transport.Schema, address, out var subject))
        {
            return new NatsDispatchEndpointConfiguration
            {
                Subject = subject,
                Name = NatsAddress.SubjectSegment + "/" + subject
            };
        }

        return null;
    }

    /// <inheritdoc />
    public override ReceiveEndpointConfiguration CreateEndpointConfiguration(
        IMessagingConfigurationContext context,
        InboundRoute route)
    {
        if (route.Kind == InboundRouteKind.Reply)
        {
            var instanceEndpointName = context.Naming.GetInstanceEndpoint(context.Host.InstanceId);

            return new NatsReceiveEndpointConfiguration
            {
                Name = "Replies",
                ConsumerName = NatsNaming.ToDurableName(instanceEndpointName),
                FilterSubjects = [instanceEndpointName],
                IsTemporary = true,
                Kind = ReceiveEndpointKind.Reply,
                AutoProvision = true,

                // Without this the response arrives on the inbox subscription but is never
                // correlated back to the pending request, so the caller waits out its timeout.
                ReceiveMiddlewares = [ReplyReceiveMiddleware.Create()]
            };
        }

        var endpointName = context.Naming.GetReceiveEndpointName(route, ReceiveEndpointKind.Default);

        return new NatsReceiveEndpointConfiguration
        {
            Name = endpointName,
            ConsumerName = NatsNaming.ToDurableName(endpointName)
        };
    }

    /// <inheritdoc />
    public override void ConfigureEndpoint(
        IMessagingConfigurationContext context,
        ReceiveEndpointConfiguration configuration)
    {
        if (configuration is not NatsReceiveEndpointConfiguration natsConfiguration)
        {
            return;
        }

        natsConfiguration.ConsumerName ??= NatsNaming.ToDurableName(natsConfiguration.Name!);

        if (natsConfiguration is not { Kind: ReceiveEndpointKind.Default, Name: { } endpointName })
        {
            return;
        }

        var fault = natsConfiguration.Features.GetOrSet<ReceiveFaultEndpointFeature>();

        if (!fault.IsDisabled && fault.Address is null)
        {
            fault.Address = FaultAddress(context, endpointName, ReceiveEndpointKind.Error);
        }

        var skipped = natsConfiguration.Features.GetOrSet<ReceiveSkippedEndpointFeature>();

        if (!skipped.IsDisabled && skipped.Address is null)
        {
            skipped.Address = FaultAddress(context, endpointName, ReceiveEndpointKind.Skipped);
        }
    }

    private Uri FaultAddress(
        IMessagingConfigurationContext context,
        string endpointName,
        ReceiveEndpointKind kind)
        => new($"{Transport.Schema}:{NatsAddress.SubjectSegment}/"
            + context.Naming.GetReceiveEndpointName(endpointName, kind));

    /// <inheritdoc />
    public override void DiscoverTopology(
        IMessagingConfigurationContext context,
        ReceiveEndpoint endpoint,
        ReceiveEndpointConfiguration configuration)
    {
        if (configuration is not NatsReceiveEndpointConfiguration natsConfiguration)
        {
            return;
        }

        if (natsConfiguration.ConsumerName is null)
        {
            throw new InvalidOperationException("Consumer name is required.");
        }

        if (endpoint.Kind is ReceiveEndpointKind.Reply)
        {
            if (natsConfiguration.FilterSubjects.FirstOrDefault() is { } replySubject)
            {
                Topology.AddSubject(new NatsSubjectConfiguration
                {
                    Subject = replySubject,
                    IsCore = true,
                    Origin = TopologyOrigin.Endpoint
                });
            }

            return;
        }

        CollectFilterSubjects(context, endpoint, natsConfiguration);

        EnsureFaultSubject(natsConfiguration.Features.Get<ReceiveFaultEndpointFeature>()?.Address);
        EnsureFaultSubject(natsConfiguration.Features.Get<ReceiveSkippedEndpointFeature>()?.Address);

        // MaxConcurrency is deliberately not mapped onto MaxAckPending. It bounds how many messages
        // this process handles at once, whereas MaxAckPending is the server-side ceiling shared by
        // every instance reading the durable, and lowering it to one instance's concurrency would
        // starve the others.
        Topology.AddConsumer(new NatsConsumerConfiguration
        {
            Name = natsConfiguration.ConsumerName,
            StreamName = natsConfiguration.StreamName,
            FilterSubjects = natsConfiguration.FilterSubjects,
            AutoProvision = natsConfiguration.AutoProvision,
            Origin = TopologyOrigin.Endpoint
        });
    }

    /// <inheritdoc />
    public override void DiscoverTopology(
        IMessagingConfigurationContext context,
        DispatchEndpoint endpoint,
        DispatchEndpointConfiguration configuration)
    {
        if (configuration is not NatsDispatchEndpointConfiguration { Subject: { } subject } natsConfiguration)
        {
            return;
        }

        var isReply = endpoint.Kind is DispatchEndpointKind.Reply;

        Topology.AddSubject(new NatsSubjectConfiguration
        {
            Subject = subject,
            StreamName = isReply
                ? null
                : natsConfiguration.StreamName ?? Topology.FindStreamForSubject(subject)?.Name,
            IsCore = isReply,
            Origin = TopologyOrigin.Endpoint
        });
    }

    private void EnsureFaultSubject(Uri? address)
    {
        if (address is null
            || !NatsDestinations.TryResolveExplicit(Transport.Schema, address, out var subject))
        {
            return;
        }

        Topology.AddSubject(new NatsSubjectConfiguration
        {
            Subject = subject,
            StreamName = Topology.FindStreamForSubject(subject)?.Name,
            Origin = TopologyOrigin.Convention
        });
    }

    private void CollectFilterSubjects(
        IMessagingConfigurationContext context,
        ReceiveEndpoint endpoint,
        NatsReceiveEndpointConfiguration configuration)
    {
        foreach (var route in context.Router.GetInboundByEndpoint(endpoint))
        {
            if (route.Kind is InboundRouteKind.Reply || route.MessageType is not { } messageType)
            {
                continue;
            }

            // Deliberately independent of the route kind. The kind records how the handler was
            // registered, not how a sender dispatches, so deriving the subject from it would filter
            // the wrong one whenever an event handler is sent to, or a request handler published to.
            var subject = NatsDestinations.ResolveConvention(
                context.Naming,
                OutboundRouteKind.Publish,
                messageType);

            if (!configuration.FilterSubjects.Contains(subject))
            {
                configuration.FilterSubjects.Add(subject);
            }
        }
    }
}
