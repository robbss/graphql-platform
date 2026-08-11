using Xunit;

namespace Mocha.Transport.Nats.Tests;

public class NatsDestinationsTests
{
    [Fact]
    public void TryResolveExplicit_Reads_A_Transport_Subject_Address()
    {
        var address = new Uri("nats://localhost:4222/ORDER_SERVICE/s/order-service.order-created");

        Assert.True(NatsDestinations.TryResolveExplicit("nats", address, out var subject));
        Assert.Equal("order-service.order-created", subject);
    }

    [Fact]
    public void TryResolveExplicit_Reads_A_Bare_Subject_Scheme()
    {
        var address = new Uri("subject:Order-Service.OrderCreated");

        Assert.True(NatsDestinations.TryResolveExplicit("nats", address, out var subject));
        Assert.Equal("Order-Service.OrderCreated", subject);
    }

    [Fact]
    public void TryResolveExplicit_Rejects_The_Authority_Form_Which_Would_Lose_Subject_Case()
    {
        var address = new Uri("subject://Order-Service.OrderCreated");

        Assert.Equal("order-service.ordercreated", address.Host);
        Assert.False(NatsDestinations.TryResolveExplicit("nats", address, out _));
    }

    [Fact]
    public void TryResolveExplicit_Reads_A_Schema_Relative_Subject()
    {
        var address = new Uri("nats:///s/order-service.order-created");

        Assert.True(NatsDestinations.TryResolveExplicit("nats", address, out var subject));
        Assert.Equal("order-service.order-created", subject);
    }

    [Fact]
    public void TryResolveExplicit_Rejects_A_Consumer_Address()
    {
        var address = new Uri("nats://localhost:4222/ORDER_SERVICE/c/order-service_order-created");

        Assert.False(NatsDestinations.TryResolveExplicit("nats", address, out _));
    }

    [Fact]
    public void TryResolveExplicit_Rejects_An_Unrelated_Scheme()
    {
        var address = new Uri("amqp://localhost/e/order-created");

        Assert.False(NatsDestinations.TryResolveExplicit("nats", address, out _));
    }
}
