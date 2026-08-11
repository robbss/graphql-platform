using Moq;
using NATS.Client.JetStream;
using Xunit;

namespace Mocha.Transport.Nats.Tests;

public class NatsStreamResolverTests
{
    private static INatsJSContext JetStreamReturning(params string[] streamNames)
    {
        var mock = new Mock<INatsJSContext>();

        mock.Setup(x => x.ListStreamNamesAsync(It.IsAny<string?>(), It.IsAny<CancellationToken>()))
            .Returns(ToAsyncEnumerable(streamNames));

        return mock.Object;
    }

    private static async IAsyncEnumerable<string> ToAsyncEnumerable(string[] values)
    {
        foreach (var value in values)
        {
            yield return value;
        }

        await Task.CompletedTask;
    }

    private static NatsMessagingTopology TopologyWithConsumer(params string[] subjects)
    {
        var topology = TestTopology.Create();

        topology.AddConsumer(new NatsConsumerConfiguration
        {
            Name = "order-service_order-created",
            FilterSubjects = [.. subjects]
        });

        return topology;
    }

    [Fact]
    public async Task Resolves_The_Stream_Capturing_The_Subject()
    {
        var topology = TopologyWithConsumer("order-service.order-created");

        await NatsStreamResolver.ResolveAsync(
            JetStreamReturning("ORDER_SERVICE"),
            topology,
            CancellationToken.None);

        Assert.Equal("ORDER_SERVICE", topology.Consumers[0].StreamName);
    }

    [Fact]
    public async Task Prefers_A_Locally_Declared_Stream_Over_Querying_The_Server()
    {
        var topology = TestTopology.Create();

        topology.AddStream(new NatsStreamConfiguration
        {
            Name = "ORDER_SERVICE",
            Subjects = ["order-service.>"]
        });

        topology.AddConsumer(new NatsConsumerConfiguration
        {
            Name = "order-service_order-created",
            FilterSubjects = ["order-service.order-created"]
        });

        var jetStream = new Mock<INatsJSContext>(MockBehavior.Strict);

        await NatsStreamResolver.ResolveAsync(jetStream.Object, topology, CancellationToken.None);

        Assert.Equal("ORDER_SERVICE", topology.Consumers[0].StreamName);
        jetStream.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task Leaves_An_Explicitly_Declared_Stream_Untouched()
    {
        var topology = TestTopology.Create();

        topology.AddConsumer(new NatsConsumerConfiguration
        {
            Name = "order-service_order-created",
            StreamName = "LEGACY_ORDERS",
            FilterSubjects = ["order-service.order-created"]
        });

        var jetStream = new Mock<INatsJSContext>(MockBehavior.Strict);

        await NatsStreamResolver.ResolveAsync(jetStream.Object, topology, CancellationToken.None);

        Assert.Equal("LEGACY_ORDERS", topology.Consumers[0].StreamName);
        jetStream.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task Fails_When_No_Stream_Captures_The_Subject()
    {
        var topology = TopologyWithConsumer("order-service.order-created");

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await NatsStreamResolver.ResolveAsync(
                JetStreamReturning(),
                topology,
                CancellationToken.None));

        Assert.Contains("No stream captures subject", exception.Message, StringComparison.Ordinal);
        Assert.Contains("FromStream", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Fails_When_Several_Streams_Capture_The_Subject()
    {
        var topology = TopologyWithConsumer("order-service.order-created");

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await NatsStreamResolver.ResolveAsync(
                JetStreamReturning("ORDER_SERVICE", "ORDER_ARCHIVE"),
                topology,
                CancellationToken.None));

        Assert.Contains("captured by 2 streams", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Fails_When_A_Consumer_Has_No_Subjects()
    {
        var topology = TopologyWithConsumer();

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await NatsStreamResolver.ResolveAsync(
                JetStreamReturning("ORDER_SERVICE"),
                topology,
                CancellationToken.None));

        Assert.Contains("has no subjects", exception.Message, StringComparison.Ordinal);
    }
}

public class NatsServerCapabilitiesTests
{
    [Theory]
    [InlineData("2.10.5", false, false)]
    [InlineData("2.11.0", true, false)]
    [InlineData("2.12.1", true, true)]
    [InlineData("v2.12.0-RC.3", true, true)]
    public void Version_Gates_Match_The_Documented_Server_Requirements(
        string version,
        bool ttl,
        bool schedules)
    {
        var capabilities = NatsServerCapabilities.FromServerVersion(version);

        Assert.Equal(ttl, capabilities.SupportsMessageTtl);
        Assert.Equal(schedules, capabilities.SupportsMessageSchedules);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("not-a-version")]
    public void An_Unknown_Version_Is_Treated_As_Capable(string? version)
    {
        var capabilities = NatsServerCapabilities.FromServerVersion(version);

        Assert.Null(capabilities.Version);
        Assert.True(capabilities.SupportsMessageTtl);
        Assert.True(capabilities.SupportsMessageSchedules);
    }
}
