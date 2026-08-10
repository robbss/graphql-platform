using Mocha.Features;
using Moq;
using Xunit;

namespace Mocha.Transport.Nats.Tests;

public class NatsTransportDescriptorTests
{
    [Fact]
    public void Host_ShouldSetConnectionProvider_WhenHostAndPortConfigured()
    {
        // Arrange
        var contextMock = new Mock<IMessagingSetupContext>();
        var descriptor = new NatsMessagingTransportDescriptor(contextMock.Object);

        // Act
        descriptor.Host("nats-server", 4222);

        // Assert
        Assert.NotNull(descriptor.Configuration.ConnectionProvider);
        var provider = descriptor.Configuration.ConnectionProvider(Mock.Of<IServiceProvider>());
        Assert.Equal("nats-server", provider.Host);
        Assert.Equal(4222, provider.Port);
    }

    [Fact]
    public void AddStream_ShouldAddStreamConfiguration_WhenCalled()
    {
        // Arrange
        var contextMock = new Mock<IMessagingSetupContext>();
        var descriptor = new NatsMessagingTransportDescriptor(contextMock.Object);

        // Act
        descriptor.AddStream("ORDERS", s => s.Subjects("orders.>"));

        // Assert
        Assert.Single(descriptor.Configuration.Streams);
        var stream = descriptor.Configuration.Streams[0];
        Assert.Equal("ORDERS", stream.Name);
        Assert.Equal(new[] { "orders.>" }, stream.Subjects);
    }

    [Fact]
    public void AddConsumer_ShouldAddConsumerConfiguration_WhenCalled()
    {
        // Arrange
        var contextMock = new Mock<IMessagingSetupContext>();
        var descriptor = new NatsMessagingTransportDescriptor(contextMock.Object);

        // Act
        descriptor.AddConsumer("OrderProcessor", "ORDERS", c => c.FilterSubject("orders.created").MaxDeliver(3));

        // Assert
        Assert.Single(descriptor.Configuration.Consumers);
        var consumer = descriptor.Configuration.Consumers[0];
        Assert.Equal("OrderProcessor", consumer.Name);
        Assert.Equal("ORDERS", consumer.StreamName);
        Assert.Equal("orders.created", consumer.FilterSubject);
        Assert.Equal(3, consumer.MaxDeliver);
    }
}
