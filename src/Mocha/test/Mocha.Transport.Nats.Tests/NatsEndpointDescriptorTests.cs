using Moq;
using Xunit;

namespace Mocha.Transport.Nats.Tests;

public class NatsEndpointDescriptorTests
{
    private static NatsMessagingTransportDescriptor CreateDescriptor()
        => new(Mock.Of<IMessagingSetupContext>());

    [Fact]
    public void Endpoint_Derives_A_Durable_Name_From_The_Endpoint_Name()
    {
        var descriptor = CreateDescriptor();

        descriptor.Endpoint("order-service.order-created");

        var endpoint = Assert.Single(descriptor.CreateConfiguration().ReceiveEndpoints);
        var natsEndpoint = Assert.IsType<NatsReceiveEndpointConfiguration>(endpoint);

        Assert.Equal("order-service.order-created", natsEndpoint.Name);
        Assert.Equal("order-service_order-created", natsEndpoint.ConsumerName);
    }

    [Fact]
    public void Endpoint_Returns_The_Same_Declaration_For_A_Repeated_Name()
    {
        var descriptor = CreateDescriptor();

        descriptor.Endpoint("orders").Subject("order-service.order-created");
        descriptor.Endpoint("orders").Subject("order-service.order-cancelled");

        var endpoint = Assert.Single(descriptor.CreateConfiguration().ReceiveEndpoints);
        var natsEndpoint = Assert.IsType<NatsReceiveEndpointConfiguration>(endpoint);

        Assert.Equal(
            ["order-service.order-created", "order-service.order-cancelled"],
            natsEndpoint.FilterSubjects);
    }

    [Fact]
    public void FromStream_Pins_The_Endpoint_To_A_Stream()
    {
        var descriptor = CreateDescriptor();

        descriptor.Endpoint("orders").FromStream("ORDER_SERVICE").ConsumerName("orders.worker");

        var natsEndpoint = Assert.IsType<NatsReceiveEndpointConfiguration>(
            Assert.Single(descriptor.CreateConfiguration().ReceiveEndpoints));

        Assert.Equal("ORDER_SERVICE", natsEndpoint.StreamName);
        Assert.Equal("orders_worker", natsEndpoint.ConsumerName);
    }
}
