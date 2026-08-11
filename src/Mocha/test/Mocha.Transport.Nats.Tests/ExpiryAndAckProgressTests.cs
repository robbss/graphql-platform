using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Mocha.Transport.Nats.Tests.Fixtures;
using Xunit;

namespace Mocha.Transport.Nats.Tests;

[Collection(JetStreamCollection.Name)]
public class ExpiryAndAckProgressTests(JetStreamFixture fixture)
{
    [Fact]
    public async Task An_Expired_Message_Is_Dropped_By_The_Server()
    {
        var cancellationToken = TestContext.Current.CancellationToken;

        Assert.SkipUnless(
            NatsServerCapabilities
                .FromServerVersion(fixture.Connection.ServerInfo?.Version)
                .SupportsMessageTtl,
            "Per-message TTL needs NATS server 2.11 or later.");

        await fixture.JetStream.CreateOrUpdateStreamAsync(
            new StreamConfig
            {
                Name = "E2E_TTL",
                Subjects = ["ttl-test.>"],
                Storage = StreamConfigStorage.Memory,
                AllowMsgTTL = true
            },
            cancellationToken);

        var headers = new NatsHeaders
        {
            { NatsScheduling.TtlHeader, NatsScheduling.ToTtlValue(TimeSpan.FromSeconds(1)) }
        };

        var ack = await fixture.JetStream.PublishAsync(
            "ttl-test.thing",
            new ReadOnlyMemory<byte>("payload"u8.ToArray()),
            NatsRawSerializer<ReadOnlyMemory<byte>>.Default,
            headers: headers,
            cancellationToken: cancellationToken);

        Assert.Null(ack.Error);
        Assert.Equal(1, await MessageCountAsync("E2E_TTL", cancellationToken));

        // The header the transport writes for MessageEnvelope.DeliverBy. The server, not the
        // transport, is what expires the message.
        var expired = await WaitForEmptyAsync("E2E_TTL", TimeSpan.FromSeconds(30), cancellationToken);

        Assert.True(expired, "The message outlived its TTL, so Nats-TTL was not applied.");
    }

    [Fact]
    public async Task Ack_Progress_Keeps_A_Slow_Handler_From_Being_Redelivered()
    {
        var cancellationToken = TestContext.Current.CancellationToken;

        await fixture.JetStream.CreateOrUpdateStreamAsync(
            new StreamConfig
            {
                Name = "E2E_ACKPROGRESS",
                Subjects = ["ack-progress.>"],
                Storage = StreamConfigStorage.Memory
            },
            cancellationToken);

        var ackWait = TimeSpan.FromSeconds(3);

        var consumer = await fixture.JetStream.CreateOrUpdateConsumerAsync(
            "E2E_ACKPROGRESS",
            new ConsumerConfig
            {
                Name = "ack_progress_reader",
                DurableName = "ack_progress_reader",
                AckPolicy = ConsumerConfigAckPolicy.Explicit,
                AckWait = ackWait
            },
            cancellationToken);

        await fixture.JetStream.PublishAsync(
            "ack-progress.slow",
            new ReadOnlyMemory<byte>("payload"u8.ToArray()),
            NatsRawSerializer<ReadOnlyMemory<byte>>.Default,
            cancellationToken: cancellationToken);

        var deliveries = 0;

        using var stopping = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        stopping.CancelAfter(TimeSpan.FromSeconds(30));

        try
        {
            await foreach (var message in consumer.ConsumeAsync(
                NatsRawSerializer<ReadOnlyMemory<byte>>.Default,
                new NatsJSConsumeOpts { MaxMsgs = 10 },
                stopping.Token))
            {
                deliveries++;

                // Hold the message for longer than AckWait while reporting progress, which is what
                // NatsAcknowledgementMiddleware does when AckProgressEvery is configured.
                var heartbeat = Task.Run(
                    async () =>
                    {
                        for (var i = 0; i < 4; i++)
                        {
                            await Task.Delay(ackWait / 2, stopping.Token);
                            await message.AckProgressAsync(cancellationToken: stopping.Token);
                        }
                    },
                    stopping.Token);

                await heartbeat;
                await message.AckAsync(cancellationToken: stopping.Token);

                break;
            }
        }
        catch (OperationCanceledException)
        {
            Assert.Fail("The consume loop did not complete inside the timeout.");
        }

        // The assertion: one delivery. Without progress reporting the deadline would have expired
        // mid-handler and JetStream would have delivered the message again.
        Assert.Equal(1, deliveries);
    }

    private async Task<long> MessageCountAsync(string streamName, CancellationToken cancellationToken)
    {
        var stream = await fixture.JetStream.GetStreamAsync(
            streamName,
            cancellationToken: cancellationToken);

        return stream.Info.State.Messages;
    }

    private async Task<bool> WaitForEmptyAsync(
        string streamName,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var deadline = DateTimeOffset.UtcNow.Add(timeout);

        while (DateTimeOffset.UtcNow < deadline)
        {
            if (await MessageCountAsync(streamName, cancellationToken) == 0)
            {
                return true;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
        }

        return false;
    }
}
