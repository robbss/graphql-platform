using Xunit;

namespace Mocha.Transport.Nats.Tests;

public class NatsSchedulingTests
{
    [Fact]
    public void The_Scheduling_Subject_Differs_From_The_Target()
    {
        const string target = "order-service.order-created";

        var scheduling = NatsScheduling.ToSchedulingSubject(target);

        Assert.NotEqual(target, scheduling);
        Assert.Equal("order-service.order-created._schedule", scheduling);
        Assert.True(NatsNaming.IsValidSubject(scheduling));
    }

    [Fact]
    public void A_One_Shot_Schedule_Uses_The_At_Form_In_Utc()
    {
        var deliverAt = new DateTimeOffset(2026, 8, 11, 11, 0, 0, TimeSpan.FromHours(2));

        var value = NatsScheduling.ToScheduleValue(deliverAt);

        Assert.StartsWith("@at ", value, StringComparison.Ordinal);
        Assert.Contains("2026-08-11T09:00:00", value, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(30, "30s")]
    [InlineData(90, "90s")]
    public void A_Time_To_Live_Is_Formatted_As_Seconds(int seconds, string expected)
    {
        Assert.Equal(expected, NatsScheduling.ToTtlValue(TimeSpan.FromSeconds(seconds)));
    }

    [Fact]
    public void A_Non_Positive_Time_To_Live_Never_Becomes_Zero()
    {
        Assert.Equal("1s", NatsScheduling.ToTtlValue(TimeSpan.FromSeconds(-5)));
    }
}
