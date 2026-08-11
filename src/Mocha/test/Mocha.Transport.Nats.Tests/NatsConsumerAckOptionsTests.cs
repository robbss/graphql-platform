using Moq;
using Xunit;

namespace Mocha.Transport.Nats.Tests;

public class NatsConsumerAckOptionsTests
{
    [Fact]
    public void AckProgress_Is_Off_By_Default()
    {
        var consumer = TestTopology.Create().AddConsumer(new NatsConsumerConfiguration
        {
            Name = "order-service_order-created"
        });

        Assert.Null(consumer.AckProgressInterval);
    }

    [Fact]
    public void AckProgressEvery_Reaches_The_Consumer()
    {
        var descriptor = new NatsMessagingTransportDescriptor(Mock.Of<IMessagingSetupContext>());

        descriptor
            .DeclareConsumer("order-service_order-created")
            .AckWait(TimeSpan.FromSeconds(30))
            .AckProgressEvery(TimeSpan.FromSeconds(10));

        var configuration = Assert.Single(descriptor.CreateConfiguration().Consumers);
        var consumer = TestTopology.Create().AddConsumer(configuration);

        Assert.Equal(TimeSpan.FromSeconds(10), consumer.AckProgressInterval);
    }

    [Fact]
    public void MaxAckPending_Defaults_To_A_Bounded_Value()
    {
        var consumer = TestTopology.Create().AddConsumer(new NatsConsumerConfiguration
        {
            Name = "order-service_order-created"
        });

        Assert.Equal(1000, consumer.MaxAckPending);
    }
}
