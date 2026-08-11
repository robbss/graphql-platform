using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mocha.Transport.InMemory;
using Mocha.Transport.Nats.Tests.Fixtures;
using Xunit;

namespace Mocha.Transport.Nats.Tests;

public sealed record RoutedEvent(Guid Id);

public sealed class RoutedEventHandler : IEventHandler<RoutedEvent>
{
    public static readonly TaskCompletionSource<Guid> Received =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public ValueTask HandleAsync(RoutedEvent message, CancellationToken cancellationToken)
    {
        Received.TrySetResult(message.Id);
        return ValueTask.CompletedTask;
    }
}

[Collection(JetStreamCollection.Name)]
public class MultiTransportTests(JetStreamFixture fixture)
{
    [Fact]
    public async Task Nats_Still_Routes_When_Registered_Alongside_InMemory()
    {
        var cancellationToken = TestContext.Current.CancellationToken;

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(fixture.Connection);
        builder.Services
            .AddMessageBus()
            .AddEventHandler<RoutedEventHandler>()
            .AddNats(nats => nats.ServiceName("e2e-routing").IsDefaultTransport())
            .AddInMemory();

        using var host = builder.Build();
        await host.StartAsync(cancellationToken);

        try
        {
            var transports = host.Services.GetRequiredService<IMessagingRuntime>()
                .Transports.ToList();

            Assert.Equal(2, transports.Count);

            var nats = transports.OfType<NatsMessagingTransport>().Single();

            Assert.True(nats.IsDefaultTransport);

            var published = Guid.NewGuid();

            await host.Services.GetRequiredService<IMessageBus>()
                .PublishAsync(new RoutedEvent(published), cancellationToken);

            var received = await RoutedEventHandler.Received.Task.WaitAsync(
                TimeSpan.FromSeconds(30),
                cancellationToken);

            Assert.Equal(published, received);

            // The default transport should own the route, so the message must have gone through
            // JetStream rather than quietly falling back to the in-process transport.
            var stream = Assert.Single(((NatsMessagingTopology)nats.Topology).Streams);

            var info = await fixture.JetStream.GetStreamAsync(
                stream.Name,
                cancellationToken: cancellationToken);

            Assert.True(
                info.Info.State.Messages > 0,
                "The message did not reach the NATS stream, so it was routed elsewhere.");
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }

    [Fact]
    public async Task Registration_Order_Decides_Which_Transport_Claims_Handlers()
    {
        var cancellationToken = TestContext.Current.CancellationToken;

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(fixture.Connection);
        builder.Services
            .AddMessageBus()
            .AddEventHandler<ClaimedEventHandler>()
            .AddInMemory()
            .AddNats(nats => nats.ServiceName("e2e-claim-order").IsDefaultTransport());

        using var host = builder.Build();
        await host.StartAsync(cancellationToken);

        try
        {
            var nats = host.Services
                .GetRequiredService<IMessagingRuntime>()
                .Transports.OfType<NatsMessagingTransport>()
                .Single();

            // IsDefaultTransport only picks the fallback for an unrouted address. Convention-bound
            // handlers are claimed by whichever transport discovers them first, which is the one
            // registered first. Registering NATS after InMemory therefore leaves it with nothing to
            // publish, and no stream, even though it is marked as the default.
            Assert.True(nats.IsDefaultTransport);
            Assert.Empty(((NatsMessagingTopology)nats.Topology).Streams);
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }
}

public sealed record ClaimedEvent(Guid Id);

public sealed class ClaimedEventHandler : IEventHandler<ClaimedEvent>
{
    public ValueTask HandleAsync(ClaimedEvent message, CancellationToken cancellationToken)
        => ValueTask.CompletedTask;
}
