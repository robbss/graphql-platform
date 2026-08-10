using Microsoft.Extensions.DependencyInjection;
using Mocha.Transport.Nats.Tests.Helpers;
using Xunit;

namespace Mocha.Transport.Nats.Tests;

public class NatsDispatchEndpointTests
{
    [Fact]
    public void DispatchEndpoint_Should_SetSubjectAndDestinationResource_When_Configured()
    {
        // Arrange
        var services = new ServiceCollection();
        var builder = services.AddMessageBus();
        builder.AddNats(t =>
        {
            t.AddStream("ORDERS", static _ => { });
            t.AddDispatchEndpoint("orders.created", static e => e.StreamName("ORDERS"));
        });
        var runtime = builder.BuildRuntime();

        var transport = runtime.Transports.OfType<NatsMessagingTransport>().Single();

        // Act
        var endpoint = transport.DispatchEndpoints.OfType<NatsDispatchEndpoint>().First(e => e.Subject == "orders.created");

        // Assert
        Assert.NotNull(endpoint);
        Assert.Equal("orders.created", endpoint.Subject);
        Assert.NotNull(endpoint.Destination);
        Assert.IsType<NatsStream>(endpoint.Destination);
        Assert.Equal("ORDERS", ((NatsStream)endpoint.Destination).Name);
    }
}
