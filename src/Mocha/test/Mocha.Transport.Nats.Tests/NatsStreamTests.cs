using Xunit;

namespace Mocha.Transport.Nats.Tests;

internal static class TestTopology
{
    public static NatsMessagingTopology Create() => new(
        new NatsMessagingTransport(static _ => { }),
        new Uri("nats://localhost:4222/"),
        autoProvision: true);
}

public class NatsStreamTests
{
    private static NatsStream CreateStream(NatsStreamConfiguration configuration)
        => TestTopology.Create().AddStream(configuration);

    [Fact]
    public void Initialize_Reads_The_Declared_Subjects()
    {
        var stream = CreateStream(new NatsStreamConfiguration
        {
            Name = "ORDER_SERVICE",
            Subjects = ["order-service.>"]
        });

        Assert.Equal("ORDER_SERVICE", stream.Name);
        Assert.Equal(["order-service.>"], stream.Subjects);
    }

    [Fact]
    public void Initialize_Defaults_Deduplication_To_Disabled()
    {
        var stream = CreateStream(new NatsStreamConfiguration { Name = "ORDER_SERVICE" });

        Assert.Equal(TimeSpan.Zero, stream.DuplicateWindow);
    }

    [Fact]
    public void Initialize_Rejects_A_Stream_Name_Containing_A_Dot()
    {
        var configuration = new NatsStreamConfiguration { Name = "order.service" };

        var exception = Assert.Throws<InvalidOperationException>(() => CreateStream(configuration));

        Assert.Contains("not a valid JetStream stream name", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Initialize_Requires_A_Name()
    {
        Assert.Throws<InvalidOperationException>(() => CreateStream(new NatsStreamConfiguration()));
    }
}

public class NatsConsumerTests
{
    private static NatsConsumer CreateConsumer(NatsConsumerConfiguration configuration)
        => TestTopology.Create().AddConsumer(configuration);

    [Fact]
    public void Initialize_Leaves_The_Stream_Unresolved_By_Default()
    {
        var consumer = CreateConsumer(new NatsConsumerConfiguration
        {
            Name = "order-service_order-created",
            FilterSubjects = ["order-service.order-created"]
        });

        Assert.Null(consumer.StreamName);
        Assert.Equal(["order-service.order-created"], consumer.FilterSubjects);
    }

    [Fact]
    public void Initialize_Keeps_An_Explicitly_Declared_Stream()
    {
        var consumer = CreateConsumer(new NatsConsumerConfiguration
        {
            Name = "order-service_order-created",
            StreamName = "ORDER_SERVICE"
        });

        Assert.Equal("ORDER_SERVICE", consumer.StreamName);
    }

    [Fact]
    public void Initialize_Rejects_A_Durable_Name_Containing_A_Dot()
    {
        var configuration = new NatsConsumerConfiguration { Name = "order-service.order-created" };

        var exception = Assert.Throws<InvalidOperationException>(() => CreateConsumer(configuration));

        Assert.Contains("not a valid JetStream consumer name", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ProvisionAsync_Fails_Clearly_When_The_Stream_Is_Unresolved()
    {
        var consumer = CreateConsumer(new NatsConsumerConfiguration
        {
            Name = "order-service_order-created",
            FilterSubjects = ["order-service.order-created"]
        });

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await consumer.ProvisionAsync(null!, CancellationToken.None));

        Assert.Contains("has not been resolved", exception.Message, StringComparison.Ordinal);
    }
}
