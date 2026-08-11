using Moq;
using NATS.Client.JetStream.Models;
using Xunit;

namespace Mocha.Transport.Nats.Tests;

public class NatsMessagingTransportDescriptorTests
{
    private static NatsMessagingTransportDescriptor CreateDescriptor()
        => new(Mock.Of<IMessagingSetupContext>());

    [Fact]
    public void DeclareStream_Collects_Stream_Configuration()
    {
        var descriptor = CreateDescriptor();

        descriptor
            .DeclareStream("ORDER_SERVICE")
            .Subject("order-service.>")
            .Retention(StreamConfigRetention.Interest)
            .MaxAge(TimeSpan.FromDays(7))
            .DeduplicateWithin(TimeSpan.FromMinutes(2));

        var configuration = descriptor.CreateConfiguration();
        var stream = Assert.Single(configuration.Streams);

        Assert.Equal("ORDER_SERVICE", stream.Name);
        Assert.Equal(["order-service.>"], stream.Subjects);
        Assert.Equal(StreamConfigRetention.Interest, stream.Retention);
        Assert.Equal(TimeSpan.FromDays(7), stream.MaxAge);
        Assert.Equal(TimeSpan.FromMinutes(2), stream.DuplicateWindow);
    }

    [Fact]
    public void DeclareStream_Returns_The_Same_Declaration_For_A_Repeated_Name()
    {
        var descriptor = CreateDescriptor();

        descriptor.DeclareStream("ORDER_SERVICE").Subject("order-service.>");
        descriptor.DeclareStream("ORDER_SERVICE").Subject("order-service-legacy.>");

        var stream = Assert.Single(descriptor.CreateConfiguration().Streams);

        Assert.Equal(["order-service.>", "order-service-legacy.>"], stream.Subjects);
    }

    [Fact]
    public void DeclareConsumer_Collects_Consumer_Configuration()
    {
        var descriptor = CreateDescriptor();

        descriptor
            .DeclareConsumer("order-service_order-created")
            .Subject("order-service.order-created")
            .FromStream("ORDER_SERVICE")
            .AckWait(TimeSpan.FromSeconds(30))
            .MaxAckPending(500)
            .MaxDeliver(10)
            .Backoff(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5));

        var consumer = Assert.Single(descriptor.CreateConfiguration().Consumers);

        Assert.Equal("order-service_order-created", consumer.Name);
        Assert.Equal("ORDER_SERVICE", consumer.StreamName);
        Assert.Equal(["order-service.order-created"], consumer.FilterSubjects);
        Assert.Equal(TimeSpan.FromSeconds(30), consumer.AckWait);
        Assert.Equal(500, consumer.MaxAckPending);
        Assert.Equal(10, consumer.MaxDeliver);
        Assert.Equal([TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5)], consumer.Backoff);
    }

    [Fact]
    public void ServiceName_And_AutoProvision_Reach_The_Configuration()
    {
        var descriptor = CreateDescriptor();

        descriptor.ServiceName("order-service").AutoProvision(false);

        var configuration = descriptor.CreateConfiguration();

        Assert.Equal("order-service", configuration.ServiceName);
        Assert.False(configuration.AutoProvision);
    }

    [Fact]
    public void AddDefaults_Sets_The_Nats_Schema()
    {
        var descriptor = CreateDescriptor();

        descriptor.AddDefaults();

        Assert.Equal(
            NatsMessagingTransportDescriptorExtensions.DefaultSchema,
            descriptor.CreateConfiguration().Schema);
    }

    [Fact]
    public void Subject_Declarations_Are_Deduplicated()
    {
        var descriptor = CreateDescriptor();

        descriptor.DeclareStream("ORDER_SERVICE").Subject("order-service.>").Subject("order-service.>");

        Assert.Single(Assert.Single(descriptor.CreateConfiguration().Streams).Subjects!);
    }
}
