using Xunit;

namespace Mocha.Transport.Nats.Tests;

public class NatsNamingTests
{
    [Theory]
    [InlineData("order-service", "ORDER_SERVICE")]
    [InlineData("order.service", "ORDER_SERVICE")]
    [InlineData("OrderService", "ORDERSERVICE")]
    public void ToStreamName_UpperSnakeCases_The_Service_Name(string serviceName, string expected)
    {
        Assert.Equal(expected, NatsNaming.ToStreamName(serviceName));
    }

    [Fact]
    public void ToStreamSubjectFilter_Appends_A_Trailing_Wildcard()
    {
        Assert.Equal("order-service.>", NatsNaming.ToStreamSubjectFilter("order-service"));
    }

    [Theory]
    [InlineData("order-service.order-created", "order-service_order-created")]
    [InlineData("order-created_error", "order-created_error")]
    [InlineData("order-processing_dead-letter", "order-processing_dead-letter")]
    public void ToDurableName_Replaces_Dots_And_Keeps_Everything_Else(string endpointName, string expected)
    {
        Assert.Equal(expected, NatsNaming.ToDurableName(endpointName));
    }

    [Theory]
    [InlineData("order>created", "order_created")]
    [InlineData("order/created", "order_created")]
    [InlineData("order created", "order_created")]
    [InlineData("order>/created", "order__created")]
    public void ToDurableName_Replaces_Each_Illegal_Character(string endpointName, string expected)
    {
        Assert.Equal(expected, NatsNaming.ToDurableName(endpointName));
    }

    [Fact]
    public void Derived_Names_Are_Always_Valid_Names()
    {
        Assert.True(NatsNaming.IsValidName(NatsNaming.ToStreamName("order-service")));
        Assert.True(NatsNaming.IsValidName(NatsNaming.ToDurableName("order-service.order-created")));
    }

    [Theory]
    [InlineData("order-service", true)]
    [InlineData("order_service", true)]
    [InlineData("order.service", false)]
    [InlineData("order service", false)]
    [InlineData("order*", false)]
    [InlineData("order>", false)]
    [InlineData("order/service", false)]
    [InlineData("", false)]
    [InlineData(null, false)]
    public void IsValidName_Rejects_Characters_Nats_Forbids(string? name, bool expected)
    {
        Assert.Equal(expected, NatsNaming.IsValidName(name));
    }

    [Theory]
    [InlineData("order-service.order-created", true)]
    [InlineData("order-service.>", true)]
    [InlineData("order-service.*.created", true)]
    [InlineData("order-service..created", false)]
    [InlineData("order-service.>.created", false)]
    [InlineData("order service.created", false)]
    [InlineData("", false)]
    [InlineData(null, false)]
    public void IsValidSubject_Allows_Wildcards_But_Not_Empty_Tokens(string? subject, bool expected)
    {
        Assert.Equal(expected, NatsNaming.IsValidSubject(subject));
    }

    [Fact]
    public void Service_Prefixed_Names_Can_Exceed_The_Recommended_Length()
    {
        var durable = NatsNaming.ToDurableName("order-management-service.order-line-item-created");

        Assert.True(durable.Length > NatsNaming.RecommendedMaxNameLength);
    }
}
