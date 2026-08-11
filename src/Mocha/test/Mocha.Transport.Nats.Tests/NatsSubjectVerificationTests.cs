using Moq;
using NATS.Client.JetStream;
using Xunit;

namespace Mocha.Transport.Nats.Tests;

public class NatsSubjectVerificationTests
{
    private static INatsJSContext JetStreamReturning(params string[] streamNames)
    {
        var mock = new Mock<INatsJSContext>();

        mock.Setup(x => x.ListStreamNamesAsync(It.IsAny<string?>(), It.IsAny<CancellationToken>()))
            .Returns(ToAsyncEnumerable(streamNames));

        return mock.Object;
    }

    private static async IAsyncEnumerable<string> ToAsyncEnumerable(string[] values)
    {
        foreach (var value in values)
        {
            yield return value;
        }

        await Task.CompletedTask;
    }

    [Fact]
    public async Task Binds_A_Subject_Captured_By_A_Local_Stream()
    {
        var topology = TestTopology.Create();

        topology.AddStream(new NatsStreamConfiguration
        {
            Name = "ORDER_SERVICE",
            Subjects = ["order-service.>"]
        });

        topology.AddSubject(new NatsSubjectConfiguration
        {
            Subject = "order-service.order-created_error"
        });

        var jetStream = new Mock<INatsJSContext>(MockBehavior.Strict);

        await NatsStreamResolver.VerifySubjectsAsync(jetStream.Object, topology, CancellationToken.None);

        Assert.Equal("ORDER_SERVICE", topology.Subjects[0].StreamName);
        jetStream.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task Fails_At_Start_Up_When_No_Stream_Captures_An_Error_Subject()
    {
        var topology = TestTopology.Create();

        topology.AddStream(new NatsStreamConfiguration
        {
            Name = "ORDER_SERVICE",
            Subjects = ["order-service.>"]
        });

        topology.AddSubject(new NatsSubjectConfiguration { Subject = "order-created_error" });

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await NatsStreamResolver.VerifySubjectsAsync(
                JetStreamReturning(),
                topology,
                CancellationToken.None));

        Assert.Contains("order-created_error", exception.Message, StringComparison.Ordinal);
        Assert.Contains("times out waiting", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Binds_A_Subject_Captured_By_A_Remote_Stream()
    {
        var topology = TestTopology.Create();

        topology.AddSubject(new NatsSubjectConfiguration { Subject = "billing-service.invoice-raised" });

        await NatsStreamResolver.VerifySubjectsAsync(
            JetStreamReturning("BILLING_SERVICE"),
            topology,
            CancellationToken.None);

        Assert.Equal("BILLING_SERVICE", topology.Subjects[0].StreamName);
    }
}
