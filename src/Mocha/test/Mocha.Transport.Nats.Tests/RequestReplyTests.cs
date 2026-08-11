using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mocha.Transport.Nats.Tests.Fixtures;
using Xunit;

namespace Mocha.Transport.Nats.Tests;

public sealed record ProcessRefund(Guid RefundId, decimal Amount) : IEventRequest<RefundProcessed>;

public sealed record RefundProcessed(Guid RefundId, string Status);

public sealed class ProcessRefundHandler : IEventRequestHandler<ProcessRefund, RefundProcessed>
{
    public static readonly TaskCompletionSource<ProcessRefund> Invoked =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public ValueTask<RefundProcessed> HandleAsync(
        ProcessRefund message,
        CancellationToken cancellationToken)
    {
        Invoked.TrySetResult(message);

        return ValueTask.FromResult(new RefundProcessed(message.RefundId, "refunded"));
    }
}

[Collection(JetStreamCollection.Name)]
public class RequestReplyTests(JetStreamFixture fixture)
{
    [Fact]
    public async Task A_Request_Round_Trips_Through_A_Core_Nats_Reply()
    {
        var cancellationToken = TestContext.Current.CancellationToken;

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(fixture.Connection);
        builder.Services
            .AddMessageBus()
            .AddRequestHandler<ProcessRefundHandler>()
            .AddNats(nats => nats.ServiceName("e2e-refunds"));

        using var host = builder.Build();
        await host.StartAsync(cancellationToken);

        try
        {
            var refundId = Guid.NewGuid();

            var request = host.Services
                .GetRequiredService<IMessageBus>()
                .RequestAsync(new ProcessRefund(refundId, 49.99m), cancellationToken)
                .AsTask();

            // Separated so a failure says which half broke: the request never reaching the handler,
            // or the reply never getting back over the core subscription.
            var handled = await ProcessRefundHandler.Invoked.Task.WaitAsync(
                TimeSpan.FromSeconds(30),
                cancellationToken);

            Assert.Equal(refundId, handled.RefundId);

            var response = await request.WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);

            Assert.Equal(refundId, response.RefundId);
            Assert.Equal("refunded", response.Status);
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }

    [Fact]
    public async Task The_Reply_Subject_Stays_Out_Of_The_Stream()
    {
        var cancellationToken = TestContext.Current.CancellationToken;

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(fixture.Connection);
        builder.Services
            .AddMessageBus()
            .AddRequestHandler<ProcessRefundHandler>()
            .AddNats(nats => nats.ServiceName("e2e-refunds"));

        using var host = builder.Build();
        await host.StartAsync(cancellationToken);

        try
        {
            var topology = (NatsMessagingTopology)host.Services
                .GetRequiredService<IMessagingRuntime>()
                .Transports.OfType<NatsMessagingTransport>()
                .Single()
                .Topology;

            var replySubjects = topology.Subjects.Where(s => s.IsCore).Select(s => s.Subject).ToList();

            Assert.NotEmpty(replySubjects);

            // Reply inboxes are ephemeral. Capturing them in a stream would persist every response
            // for the stream's whole retention period.
            Assert.All(
                replySubjects,
                subject => Assert.DoesNotContain(
                    topology.Streams,
                    stream => stream.Subjects.Contains(subject)));
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }
}
