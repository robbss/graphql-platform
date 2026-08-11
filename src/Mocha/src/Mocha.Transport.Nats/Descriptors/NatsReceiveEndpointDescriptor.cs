using Mocha.Middlewares;

namespace Mocha.Transport.Nats;

/// <summary>
/// Default implementation of <see cref="INatsReceiveEndpointDescriptor"/>.
/// </summary>
internal sealed class NatsReceiveEndpointDescriptor
    : ReceiveEndpointDescriptor<NatsReceiveEndpointConfiguration>
    , INatsReceiveEndpointDescriptor
{
    private NatsReceiveEndpointDescriptor(IMessagingConfigurationContext context, string name)
        : base(context)
    {
        Configuration = new NatsReceiveEndpointConfiguration
        {
            Name = name,
            ConsumerName = NatsNaming.ToDurableName(name)
        };
    }

    /// <inheritdoc />
    protected internal override NatsReceiveEndpointConfiguration Configuration { get; protected set; }

    public static NatsReceiveEndpointDescriptor New(IMessagingConfigurationContext context, string name)
        => new(context, name);

    public NatsReceiveEndpointConfiguration CreateConfiguration() => Configuration;

    public new INatsReceiveEndpointDescriptor Handler<THandler>() where THandler : class, IHandler
    {
        base.Handler<THandler>();
        return this;
    }

    public new INatsReceiveEndpointDescriptor Handler(Type handlerType)
    {
        base.Handler(handlerType);
        return this;
    }

    public new INatsReceiveEndpointDescriptor Consumer<TConsumer>() where TConsumer : class, IConsumer
    {
        base.Consumer<TConsumer>();
        return this;
    }

    public new INatsReceiveEndpointDescriptor Consumer(Type consumerType)
    {
        base.Consumer(consumerType);
        return this;
    }

    public new INatsReceiveEndpointDescriptor Receives<TMessage>()
    {
        base.Receives<TMessage>();
        return this;
    }

    public new INatsReceiveEndpointDescriptor Receives(Type messageType)
    {
        base.Receives(messageType);
        return this;
    }

    public new INatsReceiveEndpointDescriptor MaxConcurrency(int maxConcurrency)
    {
        base.MaxConcurrency(maxConcurrency);
        return this;
    }

    public new INatsReceiveEndpointDescriptor UseReceive(
        ReceiveMiddlewareConfiguration configuration,
        string? before = null,
        string? after = null)
    {
        base.UseReceive(configuration, before, after);
        return this;
    }

    public INatsReceiveEndpointDescriptor FromStream(string streamName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamName);

        Configuration.StreamName = streamName;
        return this;
    }

    public INatsReceiveEndpointDescriptor ConsumerName(string consumerName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerName);

        Configuration.ConsumerName = NatsNaming.ToDurableName(consumerName);
        return this;
    }

    public INatsReceiveEndpointDescriptor Subject(string subject)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(subject);

        if (!Configuration.FilterSubjects.Contains(subject))
        {
            Configuration.FilterSubjects.Add(subject);
        }

        return this;
    }
}
