using Xunit;

namespace Mocha.Transport.Nats.Tests;

public class SubjectMatcherTests
{
    [Theory]
    [InlineData("order-service.>", "order-service.order-created", true)]
    [InlineData("order-service.>", "order-service.orders.created", true)]
    [InlineData("order-service.>", "order-service", false)]
    [InlineData("order-service.*", "order-service.order-created", true)]
    [InlineData("order-service.*", "order-service.orders.created", false)]
    [InlineData("order-service.order-created", "order-service.order-created", true)]
    [InlineData("order-service.order-created", "order-service.order-updated", false)]
    [InlineData("*.order-created", "order-service.order-created", true)]
    [InlineData(">", "anything.at.all", true)]
    public void Matches_Follows_Nats_Wildcard_Rules(string filter, string subject, bool expected)
    {
        Assert.Equal(expected, SubjectMatcher.Matches(filter, subject));
    }

    [Fact]
    public void A_Longer_Subject_Does_Not_Match_A_Shorter_Filter()
    {
        Assert.False(SubjectMatcher.Matches("order-service.order-created", "order-service.order-created.v2"));
    }
}
