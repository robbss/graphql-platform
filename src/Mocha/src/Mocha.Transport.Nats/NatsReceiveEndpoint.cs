using Microsoft.Extensions.Logging;
using Mocha.Features;
using Mocha.Transport.Nats.Features;
using NATS.Client.Core;
using NATS.Client.JetStream;

namespace Mocha.Transport.Nats;

/// <summary>
/// NATS receive endpoint that consumes messages from a JetStream consumer using pull-consumer iterations.
/// </summary>
/// <param name="transport">The owning NATS transport instance.</param>
public sealed class NatsReceiveEndpoint(NatsMessagingTransport transport)
    : ReceiveEndpoint<NatsReceiveEndpointConfiguration>(transport)
{
    private string _consumerName = string.Empty;
    private string _streamName = string.Empty;
    private ushort _maxPrefetch = 100;
    private CancellationTokenSource? _cts;
    private Task? _processingTask;

    protected override void OnInitialize(
        IMessagingConfigurationContext context,
        NatsReceiveEndpointConfiguration configuration)
    {
        _consumerName = configuration.ConsumerName ?? Name;
        _streamName = configuration.StreamName ?? "MOCHA_EVENTS";
        _maxPrefetch = configuration.MaxPrefetch;
    }

    protected override void OnComplete(
        IMessagingConfigurationContext context,
        NatsReceiveEndpointConfiguration configuration)
    {
        var topology = (NatsMessagingTopology)Transport.Topology;
        var consumer = topology.Consumers.FirstOrDefault(c => c.Name == _consumerName);
        if (consumer is not null)
        {
            Source = consumer;
        }
    }

    protected override ValueTask OnStartAsync(
        IMessagingRuntimeContext context,
        CancellationToken cancellationToken)
    {
        if (Transport is not NatsMessagingTransport natsTransport)
        {
            throw new InvalidOperationException("Transport is not NatsMessagingTransport");
        }

        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _processingTask = Task.Run(() => ConsumeLoopAsync(natsTransport, _cts.Token), _cts.Token);

        return ValueTask.CompletedTask;
    }

    private async Task ConsumeLoopAsync(NatsMessagingTransport natsTransport, CancellationToken cancellationToken)
    {
        try
        {
            var js = natsTransport.JSContext;
            var consumer = await js.GetConsumerAsync(_streamName, _consumerName, cancellationToken).ConfigureAwait(false);
            var opts = new NatsJSConsumeOpts { MaxMsgs = _maxPrefetch };

            await foreach (var msg in consumer.ConsumeAsync<byte[]>(opts: opts, cancellationToken: cancellationToken).ConfigureAwait(false))
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    break;
                }

                try
                {
                    var envelope = NatsMessageEnvelopeParser.Parse(msg.Headers, msg.Data);

                    await ExecuteAsync(
                        static (context, state) =>
                        {
                            context.Envelope = state.envelope;
                            var feature = context.Features.GetOrSet<NatsReceiveFeature>();
                            feature.Message = new NatsMsg<byte[]>(state.msg.Subject, state.msg.ReplyTo, 0, state.msg.Headers, state.msg.Data, null);
                        },
                        (msg, envelope),
                        cancellationToken).ConfigureAwait(false);

                    await msg.AckAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    await msg.NakAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Graceful shutdown
        }
        catch (Exception ex)
        {
            natsTransport.Logger.LogError(ex, "Error consuming from NATS JetStream consumer {ConsumerName} on stream {StreamName}", _consumerName, _streamName);
        }
    }

    protected override async ValueTask OnStopAsync(
        IMessagingRuntimeContext context,
        CancellationToken cancellationToken)
    {
        if (_cts is not null)
        {
            await _cts.CancelAsync().ConfigureAwait(false);
        }

        if (_processingTask is not null)
        {
            try
            {
                await _processingTask.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Ignored
            }
        }

        _cts?.Dispose();
        _cts = null;
    }
}
