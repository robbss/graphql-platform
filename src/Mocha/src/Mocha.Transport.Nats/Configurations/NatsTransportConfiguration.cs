using Mocha;

namespace Mocha.Transport.Nats;

/// <summary>
/// Root configuration for a NATS (JetStream) messaging transport.
/// </summary>
public sealed class NatsTransportConfiguration : MessagingTransportConfiguration
{
    public const string DefaultName = "nats";
    public const string DefaultSchema = "nats";

    public NatsTransportConfiguration()
    {
        Name = DefaultName;
        Schema = DefaultSchema;
        RoutingStrategyFactory = static _ => new NatsRoutingStrategy();
    }

    /// <summary>
    /// Gets or sets a factory delegate for resolving the NATS connection provider.
    /// </summary>
    public Func<IServiceProvider, INatsConnectionProvider>? ConnectionProvider { get; set; }

    /// <summary>
    /// Gets or sets whether streams and consumers should be auto-provisioned on transport startup.
    /// </summary>
    public bool? AutoProvision { get; set; }

    /// <summary>
    /// Gets the list of JetStream stream configurations.
    /// </summary>
    public List<NatsStreamConfiguration> Streams { get; } = [];

    /// <summary>
    /// Gets the list of JetStream consumer configurations.
    /// </summary>
    public List<NatsConsumerConfiguration> Consumers { get; } = [];
}
