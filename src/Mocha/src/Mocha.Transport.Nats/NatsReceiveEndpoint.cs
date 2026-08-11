using System.Threading.Channels;
using Mocha.Features;
using Mocha.Transport.Nats.Features;
using NATS.Client.Core;
using NATS.Client.JetStream;

namespace Mocha.Transport.Nats;

/// <summary>
/// NATS receive endpoint that consumes messages from a durable JetStream pull consumer.
/// </summary>
/// <param name="transport">The owning NATS transport instance.</param>
public sealed class NatsReceiveEndpoint(NatsMessagingTransport transport)
    : ReceiveEndpoint<NatsReceiveEndpointConfiguration>(transport)
{
    private static readonly INatsDeserialize<ReadOnlyMemory<byte>> s_deserializer =
        NatsRawSerializer<ReadOnlyMemory<byte>>.Default;

    private CancellationTokenSource? _stopping;
    private Task? _consumeLoop;
    private string? _replySubject;

    /// <summary>
    /// Gets the durable consumer this endpoint reads from, or <see langword="null"/> for reply
    /// endpoints, which subscribe over core NATS rather than JetStream.
    /// </summary>
    public NatsConsumer? Consumer { get; private set; }

    /// <inheritdoc />
    protected override void OnInitialize(
        IMessagingConfigurationContext context,
        NatsReceiveEndpointConfiguration configuration)
    {
        if (configuration.ConsumerName is null)
        {
            throw new InvalidOperationException("Consumer name is required.");
        }
    }

    /// <inheritdoc />
    protected override void OnComplete(
        IMessagingConfigurationContext context,
        NatsReceiveEndpointConfiguration configuration)
    {
        if (Kind is ReceiveEndpointKind.Reply)
        {
            _replySubject = configuration.FilterSubjects.FirstOrDefault()
                ?? throw new InvalidOperationException("The reply endpoint has no subject.");

            Address = new Uri($"{Transport.Schema}:{NatsAddress.SubjectSegment}/{_replySubject}");

            Source = ((NatsMessagingTopology)Transport.Topology)
                .Subjects.FirstOrDefault(s => s.Subject == _replySubject)
                ?? throw new InvalidOperationException($"Reply subject '{_replySubject}' not found.");

            return;
        }

        var topology = (NatsMessagingTopology)Transport.Topology;

        Consumer =
            topology.Consumers.FirstOrDefault(c => c.Name == configuration.ConsumerName)
            ?? throw new InvalidOperationException($"Consumer '{configuration.ConsumerName}' not found.");

        Source = Consumer;
    }

    /// <inheritdoc />
    protected override async ValueTask OnStartAsync(
        IMessagingRuntimeContext context,
        CancellationToken cancellationToken)
    {
        _stopping = new CancellationTokenSource();

        if (Kind is ReceiveEndpointKind.Reply)
        {
            _consumeLoop = SubscribeRepliesAsync(_replySubject!, _stopping.Token);
            return;
        }

        if (Consumer is not { StreamName: { } streamName } consumer)
        {
            return;
        }

        var jsConsumer = await transport.JetStream.GetConsumerAsync(
            streamName,
            consumer.Name,
            cancellationToken);

        _consumeLoop = ConsumeAsync(jsConsumer, _stopping.Token);
    }

    /// <inheritdoc />
    protected override async ValueTask OnStopAsync(
        IMessagingRuntimeContext context,
        CancellationToken cancellationToken)
    {
        if (_stopping is null)
        {
            return;
        }

        await _stopping.CancelAsync();

        if (_consumeLoop is not null)
        {
            try
            {
                // Bounded by the host's shutdown token: draining should finish in-flight work, not
                // hold shutdown open indefinitely behind a slow handler.
                await _consumeLoop.WaitAsync(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                // Expected: either the drain completed through cancellation, or shutdown ran out of
                // time and the remaining messages will be redelivered.
            }
        }

        _stopping.Dispose();
        _stopping = null;
        _consumeLoop = null;
    }

    private async Task SubscribeRepliesAsync(string subject, CancellationToken cancellationToken)
    {
        // Replies are correlated over a core subscription, so the connection-level DropNewest
        // default would silently discard responses under load. Wait applies back pressure instead.
        var options = new NatsSubOpts
        {
            ChannelOpts = new NatsSubChannelOpts { FullMode = BoundedChannelFullMode.Wait }
        };

        var messages = transport.Connection.Connection.SubscribeAsync(
            subject,
            queueGroup: null,
            s_deserializer,
            options,
            cancellationToken);

        await foreach (var message in messages)
        {
            await ExecuteAsync(
                static (context, state) =>
                {
                    var feature = context.Features.GetOrSet<NatsReceiveFeature>();
                    feature.Headers = state.Headers;
                    feature.Body = state.Data;
                },
                message,
                cancellationToken);
        }
    }

    private static int DeliveryCountOf(INatsJSMsg<ReadOnlyMemory<byte>> message)
    {
        var delivered = message.Metadata?.NumDelivered ?? 0;

        return delivered > int.MaxValue ? int.MaxValue : (int)delivered;
    }

    private async Task ConsumeAsync(INatsJSConsumer consumer, CancellationToken cancellationToken)
    {
        // DrainOnCancel turns stopping into a drain: no new messages are pulled, but everything
        // already buffered is still handled and acknowledged instead of being abandoned mid-flight.
        var options = new NatsJSConsumeOpts
        {
            MaxMsgs = (int)Math.Clamp(Consumer!.MaxAckPending, 1, int.MaxValue),
            DrainOnCancel = true
        };

        var ackProgressInterval = Consumer.AckProgressInterval;

        await foreach (var message in consumer.ConsumeAsync(s_deserializer, options, cancellationToken))
        {
            await ExecuteAsync(
                static (context, state) =>
                {
                    var feature = context.Features.GetOrSet<NatsReceiveFeature>();
                    feature.Message = state.Message;
                    feature.Headers = state.Message.Headers;
                    feature.Body = state.Message.Data;
                    feature.DeliveryCount = DeliveryCountOf(state.Message);
                    feature.AckProgressInterval = state.AckProgressInterval;
                },
                (Message: message, AckProgressInterval: ackProgressInterval),
                cancellationToken);
        }
    }
}
