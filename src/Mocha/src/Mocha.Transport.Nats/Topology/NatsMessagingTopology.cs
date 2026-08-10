using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Mocha.Transport.Nats;

/// <summary>
/// Manages NATS JetStream topology declaration and provisioning.
/// </summary>
public sealed class NatsMessagingTopology(
    MessagingTransport transport,
    Uri rootAddress,
    bool autoProvision)
    : MessagingTopology(transport, rootAddress)
{
    private readonly List<NatsStream> _streams = [];
    private readonly List<NatsConsumer> _consumers = [];

    public bool AutoProvision { get; } = autoProvision;

    public IReadOnlyList<NatsStream> Streams => _streams;
    public IReadOnlyList<NatsConsumer> Consumers => _consumers;

    public NatsStream AddStream(NatsStreamConfiguration configuration)
    {
        configuration.Topology = this;
        var stream = new NatsStream();
        stream.Initialize(configuration);
        _streams.Add(stream);
        return stream;
    }

    public NatsConsumer AddConsumer(NatsConsumerConfiguration configuration)
    {
        configuration.Topology = this;
        var consumer = new NatsConsumer();
        consumer.Initialize(configuration);
        _consumers.Add(consumer);
        return consumer;
    }

    public async ValueTask AutoProvisionAsync(INatsJSContext jsContext, CancellationToken cancellationToken = default)
    {
        if (!AutoProvision)
        {
            return;
        }

        foreach (var stream in _streams)
        {
            var config = new StreamConfig
            {
                Name = stream.Name,
                Subjects = stream.Configuration.Subjects,
                Storage = stream.Configuration.Storage,
                Retention = stream.Configuration.Retention,
                MaxMsgs = stream.Configuration.MaxMsgs,
                MaxBytes = stream.Configuration.MaxBytes
            };

            await jsContext.CreateStreamAsync(config, cancellationToken).ConfigureAwait(false);
        }

        foreach (var consumer in _consumers)
        {
            var config = new ConsumerConfig
            {
                DurableName = consumer.Name,
                FilterSubject = consumer.Configuration.FilterSubject ?? string.Empty,
                DeliverPolicy = consumer.Configuration.DeliverPolicy,
                AckPolicy = consumer.Configuration.AckPolicy,
                AckWait = consumer.Configuration.AckWait,
                MaxDeliver = consumer.Configuration.MaxDeliver
            };

            await jsContext.CreateOrUpdateConsumerAsync(consumer.StreamName, config, cancellationToken).ConfigureAwait(false);
        }
    }
}
