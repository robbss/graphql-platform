using Xunit;

namespace Mocha.Transport.Nats.Tests;

public class NatsAddressTests
{
    private static readonly Uri s_baseAddress = new("nats://localhost:4222/");

    [Fact]
    public void ForSubject_Builds_The_Expected_Address()
    {
        var address = NatsAddress.ForSubject(s_baseAddress, "ORDER_SERVICE", "order-service.order-created");

        Assert.Equal("nats://localhost:4222/ORDER_SERVICE/s/order-service.order-created", address.ToString());
    }

    [Fact]
    public void ForConsumer_Builds_The_Expected_Address()
    {
        var address = NatsAddress.ForConsumer(s_baseAddress, "ORDER_SERVICE", "order-service_order-created");

        Assert.Equal("nats://localhost:4222/ORDER_SERVICE/c/order-service_order-created", address.ToString());
    }

    [Fact]
    public void Subject_Address_Round_Trips()
    {
        var address = NatsAddress.ForSubject(s_baseAddress, "ORDER_SERVICE", "order-service.order-created");

        Assert.True(NatsAddress.TryParse(address, out var stream, out var kind, out var name));
        Assert.Equal("ORDER_SERVICE", stream);
        Assert.Equal(NatsAddress.SubjectSegment, kind);
        Assert.Equal("order-service.order-created", name);
    }

    [Fact]
    public void Consumer_Address_Round_Trips()
    {
        var address = NatsAddress.ForConsumer(s_baseAddress, "ORDER_SERVICE", "order-service_order-created");

        Assert.True(NatsAddress.TryParse(address, out var stream, out var kind, out var name));
        Assert.Equal("ORDER_SERVICE", stream);
        Assert.Equal(NatsAddress.ConsumerSegment, kind);
        Assert.Equal("order-service_order-created", name);
    }

    [Theory]
    [InlineData("nats://localhost:4222/ORDER_SERVICE")]
    [InlineData("nats://localhost:4222/ORDER_SERVICE/x/order-created")]
    [InlineData("nats://localhost:4222/ORDER_SERVICE/s/a/b")]
    public void TryParse_Rejects_Malformed_Addresses(string address)
    {
        Assert.False(NatsAddress.TryParse(new Uri(address), out _, out _, out _));
    }

    [Fact]
    public void TryParse_Rejects_Null()
    {
        Assert.False(NatsAddress.TryParse(null, out _, out _, out _));
    }
}
