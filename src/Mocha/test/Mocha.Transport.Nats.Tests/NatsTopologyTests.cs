using Microsoft.Extensions.DependencyInjection;
using Mocha.Transport.Nats;
using Mocha.Transport.Nats.Tests.Helpers;
using Xunit;

namespace Mocha.Transport.Nats.Tests;

public class NatsTopologyTests
{
    [Fact]
    public void AddStream_Should_InitializeStreamResource_When_Configured()
    {
        // Arrange
        var services = new ServiceCollection();
        var builder = services.AddMessageBus();
        builder.AddNats(t => t.AddStream("EVENTS", s => s.Subjects("events.>")));
        var runtime = builder.BuildRuntime();

        var transport = runtime.Transports.OfType<NatsMessagingTransport>().Single();

        // Act
        var topology = (NatsMessagingTopology)transport.Topology;

        // Assert
        Assert.Single(topology.Streams);
        var stream = topology.Streams[0];
        Assert.Equal("EVENTS", stream.Name);
        Assert.Equal("nats://localhost:4222/stream/EVENTS", stream.Address.ToString());
    }

    [Fact]
    public void AddConsumer_Should_InitializeConsumerResource_When_Configured()
    {
        // Arrange
        var services = new ServiceCollection();
        var builder = services.AddMessageBus();
        builder.AddNats(t => t.AddConsumer("OrderProcessor", "EVENTS", c => c.FilterSubject("events.orders")));
        var runtime = builder.BuildRuntime();

        var transport = runtime.Transports.OfType<NatsMessagingTransport>().Single();

        // Act
        var topology = (NatsMessagingTopology)transport.Topology;

        // Assert
        Assert.Single(topology.Consumers);
        var consumer = topology.Consumers[0];
        Assert.Equal("OrderProcessor", consumer.Name);
        Assert.Equal("EVENTS", consumer.StreamName);
        Assert.Equal("nats://localhost:4222/stream/EVENTS/consumer/OrderProcessor", consumer.Address.ToString());
    }
}
