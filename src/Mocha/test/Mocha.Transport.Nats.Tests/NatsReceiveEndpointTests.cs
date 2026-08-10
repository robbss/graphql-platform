using Microsoft.Extensions.DependencyInjection;
using Mocha.Transport.Nats.Tests.Helpers;
using Xunit;

namespace Mocha.Transport.Nats.Tests;

public class NatsReceiveEndpointTests
{
    [Fact]
    public void ReceiveEndpoint_Should_SetConsumerAndSourceResource_When_Configured()
    {
        // Arrange
        var services = new ServiceCollection();
        var builder = services.AddMessageBus();
        builder.AddNats(t =>
        {
            t.AddConsumer("OrderProcessor", "ORDERS", static _ => { });
            t.AddReceiveEndpoint("OrderProcessor", "ORDERS", static _ => { });
        });
        var runtime = builder.BuildRuntime();

        var transport = runtime.Transports.OfType<NatsMessagingTransport>().Single();

        // Act
        var endpoint = transport.ReceiveEndpoints.OfType<NatsReceiveEndpoint>().First(e => e.Name == "OrderProcessor");

        // Assert
        Assert.NotNull(endpoint);
        Assert.Equal("OrderProcessor", endpoint.Name);
        Assert.NotNull(endpoint.Source);
        Assert.IsType<NatsConsumer>(endpoint.Source);
        Assert.Equal("OrderProcessor", ((NatsConsumer)endpoint.Source).Name);
    }
}
