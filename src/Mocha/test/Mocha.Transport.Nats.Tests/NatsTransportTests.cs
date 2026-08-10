using Microsoft.Extensions.DependencyInjection;
using Mocha.Transport.Nats;
using Mocha.Transport.Nats.Tests.Helpers;
using Xunit;

namespace Mocha.Transport.Nats.Tests;

public class NatsTransportTests
{
    [Fact]
    public void AddNats_Should_RegisterTransportInServiceCollection_When_Configured()
    {
        // Arrange
        var services = new ServiceCollection();
        var builder = services.AddMessageBus();
        builder.AddNats(t =>
        {
            t.Host("nats.example.com", 4222);
            t.AddStream("ORDERS", s => s.Subjects("orders.>"));
        });

        // Act
        var runtime = builder.BuildRuntime();

        // Assert
        Assert.NotNull(runtime);
        var natsTransport = runtime.Transports.OfType<NatsMessagingTransport>().SingleOrDefault();
        Assert.NotNull(natsTransport);
        Assert.Equal("nats", natsTransport.Schema);
    }

    [Fact]
    public void TryGetDispatchEndpoint_Should_ReturnFalse_When_UnknownAddressUsed()
    {
        // Arrange
        var services = new ServiceCollection();
        var builder = services.AddMessageBus();
        builder.AddNats(t => t.AddStream("ORDERS", s => s.Subjects("orders.>")));
        var runtime = builder.BuildRuntime();

        var transport = runtime.Transports.OfType<NatsMessagingTransport>().Single();

        // Act
        var found = transport.TryGetDispatchEndpoint(new Uri("nats://localhost:4222/stream/UNKNOWN"), out var endpoint);

        // Assert
        Assert.False(found);
        Assert.Null(endpoint);
    }
}
