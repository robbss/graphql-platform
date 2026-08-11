using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mocha.Transport.Nats.Tests.Fixtures;
using Xunit;

namespace Mocha.Transport.Nats.Tests;

public sealed record OrderPlaced(Guid OrderId, string ProductName);

public sealed class OrderPlacedHandler : IEventHandler<OrderPlaced>
{
    public static readonly TaskCompletionSource<OrderPlaced> Received =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public ValueTask HandleAsync(OrderPlaced message, CancellationToken cancellationToken)
    {
        Received.TrySetResult(message);
        return ValueTask.CompletedTask;
    }
}

public sealed record StockChecked(Guid ItemId, int Attempt);

public sealed class StockCheckedHandler : IEventHandler<StockChecked>
{
    public static readonly ConcurrentQueue<int> Attempts = new();
    public static readonly TaskCompletionSource Succeeded =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public ValueTask HandleAsync(StockChecked message, CancellationToken cancellationToken)
    {
        Attempts.Enqueue(Attempts.Count + 1);

        if (Attempts.Count < 2)
        {
            throw new InvalidOperationException("Simulated transient failure.");
        }

        Succeeded.TrySetResult();
        return ValueTask.CompletedTask;
    }
}

public sealed record TopologyProbe(Guid Id);

public sealed class TopologyProbeHandler : IEventHandler<TopologyProbe>
{
    public ValueTask HandleAsync(TopologyProbe message, CancellationToken cancellationToken)
        => ValueTask.CompletedTask;
}

[Collection(JetStreamCollection.Name)]
public class EndToEndTests(JetStreamFixture fixture)
{
    private IHost BuildHost(string serviceName, Action<IMessageBusHostBuilder> configure)
    {
        var builder = Host.CreateApplicationBuilder();

        builder.Services.AddSingleton(fixture.Connection);

        var bus = builder.Services.AddMessageBus();

        configure(bus);

        bus.AddNats(nats => nats.ServiceName(serviceName));

        return builder.Build();
    }

    [Fact]
    public async Task A_Published_Event_Reaches_Its_Handler()
    {
        var cancellationToken = TestContext.Current.CancellationToken;

        using var host = BuildHost("e2e-orders", b => b.AddEventHandler<OrderPlacedHandler>());

        await host.StartAsync(cancellationToken);

        try
        {
            var published = new OrderPlaced(Guid.NewGuid(), "Mechanical Keyboard");

            await host.Services.GetRequiredService<IMessageBus>()
                .PublishAsync(published, cancellationToken);

            var received = await OrderPlacedHandler.Received.Task.WaitAsync(
                TimeSpan.FromSeconds(30),
                cancellationToken);

            Assert.Equal(published.OrderId, received.OrderId);
            Assert.Equal(published.ProductName, received.ProductName);
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }

    [Fact]
    public async Task A_Failing_Handler_Is_Retried_Until_It_Succeeds()
    {
        var cancellationToken = TestContext.Current.CancellationToken;

        // Redelivery has to be asked for. The default policy dead-letters a failing message rather
        // than returning it to the transport, so without this the handler is never called again.
        using var host = BuildHost("e2e-stock", b => b
            .AddEventHandler<StockCheckedHandler>()
            .AddResilience(policy => policy.Default().Retry(1).ThenRedeliver()));

        await host.StartAsync(cancellationToken);

        try
        {
            await host.Services.GetRequiredService<IMessageBus>()
                .PublishAsync(new StockChecked(Guid.NewGuid(), 1), cancellationToken);

            await StockCheckedHandler.Succeeded.Task.WaitAsync(
                TimeSpan.FromSeconds(60),
                cancellationToken);

            Assert.True(
                StockCheckedHandler.Attempts.Count >= 2,
                "The message should have been delivered more than once after the first failure.");
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }

    [Fact]
    public async Task The_Transport_Provisions_Its_Stream_And_Consumer()
    {
        var cancellationToken = TestContext.Current.CancellationToken;

        using var host = BuildHost("e2e-topology", b => b.AddEventHandler<TopologyProbeHandler>());

        await host.StartAsync(cancellationToken);

        try
        {
            var transport = host.Services
                .GetRequiredService<IMessagingRuntime>()
                .Transports.OfType<NatsMessagingTransport>()
                .Single();

            var topology = (NatsMessagingTopology)transport.Topology;

            var stream = Assert.Single(topology.Streams);

            Assert.All(topology.Consumers, c => Assert.Equal(stream.Name, c.StreamName));

            Assert.DoesNotContain(
                topology.Subjects.Where(s => s.IsCore).Select(s => s.Subject),
                subject => stream.Subjects.Contains(subject));

            var provisioned = await fixture.JetStream.GetStreamAsync(stream.Name, cancellationToken: cancellationToken);

            Assert.Equal(stream.Name, provisioned.Info.Config.Name);
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }
}
