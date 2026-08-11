using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mocha.Transport.Nats.Tests.Fixtures;
using Xunit;

namespace Mocha.Transport.Nats.Tests;

public sealed record WorkItem(int Sequence);

public sealed class WorkItemHandler : IEventHandler<WorkItem>
{
    public static readonly ConcurrentBag<int> Handled = [];
    public static readonly TaskCompletionSource AllHandled =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public static int Expected { get; set; }

    public ValueTask HandleAsync(WorkItem message, CancellationToken cancellationToken)
    {
        Handled.Add(message.Sequence);

        if (Handled.Count >= Expected)
        {
            AllHandled.TrySetResult();
        }

        return ValueTask.CompletedTask;
    }
}

[Collection(JetStreamCollection.Name)]
public class CompetingConsumerTests(JetStreamFixture fixture)
{
    private const int MessageCount = 20;

    private IHost BuildInstance()
    {
        var builder = Host.CreateApplicationBuilder();

        builder.Services.AddSingleton(fixture.Connection);
        builder.Services
            .AddMessageBus()
            .AddEventHandler<WorkItemHandler>()
            .AddNats(nats => nats.ServiceName("e2e-workers"));

        return builder.Build();
    }

    [Fact]
    public async Task Two_Instances_Share_One_Durable_And_Each_Message_Is_Handled_Once()
    {
        var cancellationToken = TestContext.Current.CancellationToken;

        WorkItemHandler.Expected = MessageCount;

        using var first = BuildInstance();
        using var second = BuildInstance();

        await first.StartAsync(cancellationToken);
        await second.StartAsync(cancellationToken);

        try
        {
            var firstConsumers = ConsumerNames(first);
            var secondConsumers = ConsumerNames(second);

            // Competing consumption on JetStream is two instances pulling from the same durable, so
            // the durable name has to be derived identically on both rather than per instance.
            Assert.Equal(firstConsumers, secondConsumers);

            var bus = first.Services.GetRequiredService<IMessageBus>();

            for (var i = 0; i < MessageCount; i++)
            {
                await bus.PublishAsync(new WorkItem(i), cancellationToken);
            }

            await WorkItemHandler.AllHandled.Task.WaitAsync(
                TimeSpan.FromSeconds(60),
                cancellationToken);

            // Give any duplicate delivery a chance to show up before asserting exactly-once.
            await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);

            Assert.Equal(MessageCount, WorkItemHandler.Handled.Count);
            Assert.Equal(MessageCount, WorkItemHandler.Handled.Distinct().Count());
        }
        finally
        {
            await second.StopAsync(cancellationToken);
            await first.StopAsync(cancellationToken);
        }
    }

    private static List<string> ConsumerNames(IHost host)
        => [.. ((NatsMessagingTopology)host.Services
            .GetRequiredService<IMessagingRuntime>()
            .Transports.OfType<NatsMessagingTransport>()
            .Single()
            .Topology)
            .Consumers
            .Select(c => c.Name)
            .Order(StringComparer.Ordinal)];
}
