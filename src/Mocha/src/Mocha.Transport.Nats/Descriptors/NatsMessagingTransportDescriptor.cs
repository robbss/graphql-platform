using Mocha;
using Mocha.Features;
using Mocha.Middlewares;
using Mocha.Transport;
using NATS.Client.Core;

namespace Mocha.Transport.Nats;

/// <summary>
/// Fluent descriptor for configuring a NATS (JetStream) messaging transport.
/// </summary>
public sealed class NatsMessagingTransportDescriptor
    : MessagingTransportDescriptor<NatsTransportConfiguration>
    , INatsMessagingTransportDescriptor
{
    private readonly List<NatsReceiveEndpointDescriptor> _receiveEndpoints = [];
    private readonly List<NatsDispatchEndpointDescriptor> _dispatchEndpoints = [];

    public NatsMessagingTransportDescriptor(IMessagingSetupContext discoveryContext)
        : base(discoveryContext)
    {
        Configuration = new NatsTransportConfiguration();
    }

    protected internal override NatsTransportConfiguration Configuration { get; protected set; }

    public INatsMessagingTransportDescriptor Connection(Func<IServiceProvider, INatsConnectionProvider> provider)
    {
        Configuration.ConnectionProvider = provider;
        return this;
    }

    public INatsMessagingTransportDescriptor Host(string host, int port = 4222)
    {
        Configuration.ConnectionProvider = _ => new NatsConnectionProvider(new NatsOpts { Url = $"nats://{host}:{port}" });
        return this;
    }

    public INatsMessagingTransportDescriptor AutoProvision(bool autoProvision = true)
    {
        Configuration.AutoProvision = autoProvision;
        return this;
    }

    public INatsMessagingTransportDescriptor AddStream(string name, Action<INatsStreamDescriptor> configure)
    {
        var config = new NatsStreamConfiguration { Name = name };
        var descriptor = new NatsStreamDescriptor(config);
        configure(descriptor);
        Configuration.Streams.Add(config);
        return this;
    }

    public INatsMessagingTransportDescriptor AddConsumer(string name, string streamName, Action<INatsConsumerDescriptor> configure)
    {
        var config = new NatsConsumerConfiguration { Name = name, StreamName = streamName };
        var descriptor = new NatsConsumerDescriptor(config);
        configure(descriptor);
        Configuration.Consumers.Add(config);
        return this;
    }

    public INatsMessagingTransportDescriptor AddReceiveEndpoint(string consumerName, string streamName, Action<INatsReceiveEndpointDescriptor> configure)
    {
        var descriptor = new NatsReceiveEndpointDescriptor(Context, consumerName, streamName);
        configure(descriptor);
        _receiveEndpoints.Add(descriptor);
        return this;
    }

    public INatsMessagingTransportDescriptor AddDispatchEndpoint(string subject, Action<INatsDispatchEndpointDescriptor> configure)
    {
        var descriptor = new NatsDispatchEndpointDescriptor(Context, subject);
        configure(descriptor);
        _dispatchEndpoints.Add(descriptor);
        return this;
    }

    public NatsTransportConfiguration CreateConfiguration()
    {
        foreach (var endpoint in _receiveEndpoints)
        {
            Configuration.ReceiveEndpoints.Add(endpoint.CreateConfiguration());
        }

        foreach (var endpoint in _dispatchEndpoints)
        {
            Configuration.DispatchEndpoints.Add(endpoint.CreateConfiguration());
        }

        return Configuration;
    }
}

public sealed class NatsStreamDescriptor(NatsStreamConfiguration config) : INatsStreamDescriptor
{
    public INatsStreamDescriptor Subjects(params string[] subjects)
    {
        config.Subjects.AddRange(subjects);
        return this;
    }

    public INatsStreamDescriptor Storage(NATS.Client.JetStream.Models.StreamConfigStorage storage)
    {
        config.Storage = storage;
        return this;
    }

    public INatsStreamDescriptor Retention(NATS.Client.JetStream.Models.StreamConfigRetention retention)
    {
        config.Retention = retention;
        return this;
    }
}

public sealed class NatsConsumerDescriptor(NatsConsumerConfiguration config) : INatsConsumerDescriptor
{
    public INatsConsumerDescriptor FilterSubject(string filterSubject)
    {
        config.FilterSubject = filterSubject;
        return this;
    }

    public INatsConsumerDescriptor AckWait(TimeSpan ackWait)
    {
        config.AckWait = ackWait;
        return this;
    }

    public INatsConsumerDescriptor MaxDeliver(int maxDeliver)
    {
        config.MaxDeliver = maxDeliver;
        return this;
    }
}

public sealed class NatsReceiveEndpointDescriptor : ReceiveEndpointDescriptor<NatsReceiveEndpointConfiguration>, INatsReceiveEndpointDescriptor
{
    public NatsReceiveEndpointDescriptor(IMessagingConfigurationContext discoveryContext, string consumerName, string streamName)
        : base(discoveryContext)
    {
        Configuration = new NatsReceiveEndpointConfiguration
        {
            ConsumerName = consumerName,
            StreamName = streamName,
            Name = consumerName
        };
    }

    public INatsReceiveEndpointDescriptor Subject(string subject)
    {
        Configuration.Subject = subject;
        return this;
    }

    public INatsReceiveEndpointDescriptor MaxPrefetch(ushort maxPrefetch)
    {
        Configuration.MaxPrefetch = maxPrefetch;
        return this;
    }

    public NatsReceiveEndpointConfiguration CreateConfiguration() => Configuration;
}

public sealed class NatsDispatchEndpointDescriptor : DispatchEndpointDescriptor<NatsDispatchEndpointConfiguration>, INatsDispatchEndpointDescriptor
{
    public NatsDispatchEndpointDescriptor(IMessagingConfigurationContext discoveryContext, string subject)
        : base(discoveryContext)
    {
        Configuration = new NatsDispatchEndpointConfiguration
        {
            Subject = subject,
            Name = subject
        };
    }

    public INatsDispatchEndpointDescriptor StreamName(string streamName)
    {
        Configuration.StreamName = streamName;
        return this;
    }

    public NatsDispatchEndpointConfiguration CreateConfiguration() => Configuration;
}
