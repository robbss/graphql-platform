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
    [InlineData("order-service.order-created", "order-service.order-created.v2", false)]
    [InlineData("*.order-created", "order-service.order-created", true)]
    [InlineData(">", "anything.at.all", true)]
    public void Matches_Should_FollowNatsWildcardRules_When_GivenAFilterAndSubject(
        string filter,
        string subject,
        bool expected)
    {
        // act and assert
        Assert.Equal(expected, SubjectMatcher.Matches(filter, subject));
    }
}
