using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mocha.Transport.Nats.Tests.Fixtures;
using NATS.Client.Core;
using Xunit;

namespace Mocha.Transport.Nats.Tests;

public sealed record Heartbeat(int Sequence);

public sealed class HeartbeatHandler : IEventHandler<Heartbeat>
{
    public static readonly ConcurrentBag<int> Handled = [];

    public ValueTask HandleAsync(Heartbeat message, CancellationToken cancellationToken)
    {
        Handled.Add(message.Sequence);
        return ValueTask.CompletedTask;
    }
}

[Collection(JetStreamCollection.Name)]
public class ReconnectionTests(JetStreamFixture fixture)
{
    [Fact]
    public async Task Consumption_Resumes_After_The_Connection_Drops()
    {
        var cancellationToken = TestContext.Current.CancellationToken;

        // A dedicated connection: this test deliberately resets it, and doing that to the shared
        // fixture connection disturbs every other test's subscriptions.
        await using var connection = new NatsConnection(new NatsOpts
        {
            Url = fixture.ConnectionString,
            SubPendingChannelFullMode = System.Threading.Channels.BoundedChannelFullMode.Wait
        });

        await connection.ConnectAsync();

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<INatsConnection>(connection);
        builder.Services
            .AddMessageBus()
            .AddEventHandler<HeartbeatHandler>()
            .AddNats(nats => nats.ServiceName("e2e-reconnect"));

        using var host = builder.Build();
        await host.StartAsync(cancellationToken);

        try
        {
            var bus = host.Services.GetRequiredService<IMessageBus>();

            await bus.PublishAsync(new Heartbeat(1), cancellationToken);
            await WaitForAsync(1, cancellationToken);

            // NATS.Net owns reconnection, which is why this transport has no connection manager of
            // its own. Forcing a reconnect proves the consume loop survives it rather than silently
            // stopping and leaving the service alive but deaf.
            await connection.ReconnectAsync();

            await bus.PublishAsync(new Heartbeat(2), cancellationToken);

            var resumed = await WaitForAsync(2, cancellationToken);

            Assert.True(resumed, "Consumption did not resume after the connection was reset.");
            Assert.Contains(2, HeartbeatHandler.Handled);
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }

    private static async Task<bool> WaitForAsync(int sequence, CancellationToken cancellationToken)
    {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(60);

        while (DateTimeOffset.UtcNow < deadline)
        {
            if (HeartbeatHandler.Handled.Contains(sequence))
            {
                return true;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
        }

        return false;
    }
}
