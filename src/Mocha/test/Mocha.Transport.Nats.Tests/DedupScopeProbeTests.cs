using Mocha.Transport.Nats.Tests.Fixtures;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Xunit;

namespace Mocha.Transport.Nats.Tests;

[Collection(JetStreamCollection.Name)]
public class DedupScopeProbeTests(JetStreamFixture fixture)
{
    [Fact]
    public async Task Deduplication_Is_Scoped_To_The_Stream_Not_The_Subject()
    {
        var cancellationToken = TestContext.Current.CancellationToken;

        await fixture.JetStream.CreateOrUpdateStreamAsync(
            new StreamConfig
            {
                Name = "PROBE_DEDUP_SCOPE",
                Subjects = ["probe-scope.a", "probe-scope.b"],
                Storage = StreamConfigStorage.Memory
            },
            cancellationToken);

        var first = await PublishAsync("probe-scope.a", "shared-id", cancellationToken);
        var second = await PublishAsync("probe-scope.b", "shared-id", cancellationToken);

        Assert.False(first.Duplicate);

        // If this is true, any republish of the same envelope inside one stream is silently
        // discarded, which is exactly what a dead-letter or fault republish is.
        Assert.True(
            second.Duplicate,
            "Deduplication is subject-scoped, so the stream-scoped theory is wrong.");

        var stream = await fixture.JetStream.GetStreamAsync(
            "PROBE_DEDUP_SCOPE",
            cancellationToken: cancellationToken);

        Assert.Equal(1, stream.Info.State.Messages);
    }

    [Fact]
    public async Task A_Stream_Left_Unconfigured_Still_Deduplicates()
    {
        var cancellationToken = TestContext.Current.CancellationToken;

        var created = await fixture.JetStream.CreateOrUpdateStreamAsync(
            new StreamConfig
            {
                Name = "PROBE_DEDUP_DEFAULT",
                Subjects = ["probe-default.>"],
                Storage = StreamConfigStorage.Memory,
                DuplicateWindow = TimeSpan.Zero
            },
            cancellationToken);

        // DuplicateWindow is serialized with JsonIgnoreCondition.WhenWritingDefault, so asking for
        // zero omits the field and the server applies its own default instead of disabling dedup.
        Assert.NotEqual(TimeSpan.Zero, created.Info.Config.DuplicateWindow);
    }

    private async Task<PubAckResponse> PublishAsync(
        string subject,
        string messageId,
        CancellationToken cancellationToken)
        => await fixture.JetStream.PublishAsync(
            subject,
            new ReadOnlyMemory<byte>("payload"u8.ToArray()),
            NatsRawSerializer<ReadOnlyMemory<byte>>.Default,
            headers: new NatsHeaders { { NatsMessageHeaders.DeduplicationKey, messageId } },
            cancellationToken: cancellationToken);
}
