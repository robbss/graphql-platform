namespace Mocha.Transport.Nats;

/// <summary>
/// Represents a NATS JetStream stream topology resource.
/// </summary>
public sealed class NatsStream : TopologyResource<NatsStreamConfiguration>
{
    public string Name { get; private set; } = string.Empty;
    public new NatsStreamConfiguration Configuration => base.Configuration;

    protected override void OnInitialize(NatsStreamConfiguration configuration)
    {
        Name = configuration.Name;
        Address = new Uri(Topology.Address, $"stream/{Name}");
    }

    protected override void OnComplete(NatsStreamConfiguration configuration)
    {
    }
}

/// <summary>
/// Represents a NATS JetStream consumer topology resource.
/// </summary>
public sealed class NatsConsumer : TopologyResource<NatsConsumerConfiguration>
{
    public string Name { get; private set; } = string.Empty;
    public string StreamName { get; private set; } = string.Empty;
    public new NatsConsumerConfiguration Configuration => base.Configuration;

    protected override void OnInitialize(NatsConsumerConfiguration configuration)
    {
        Name = configuration.Name;
        StreamName = configuration.StreamName;
        Address = new Uri(Topology.Address, $"stream/{StreamName}/consumer/{Name}");
    }

    protected override void OnComplete(NatsConsumerConfiguration configuration)
    {
    }
}
