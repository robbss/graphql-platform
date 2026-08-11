using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mocha.Transport.Nats.Tests.Fixtures;
using Xunit;

namespace Mocha.Transport.Nats.Tests;

public sealed record TracedEvent(Guid Id);

public sealed class TracedEventHandler : IEventHandler<TracedEvent>
{
    public static readonly TaskCompletionSource Received =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public ValueTask HandleAsync(TracedEvent message, CancellationToken cancellationToken)
    {
        Received.TrySetResult();
        return ValueTask.CompletedTask;
    }
}

[Collection(JetStreamCollection.Name)]
public class OpenTelemetryTests(JetStreamFixture fixture)
{
    [Fact]
    public async Task Only_The_Nats_Client_Emits_Spans()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var activities = new List<Activity>();

        using var listener = new ActivityListener
        {
            ShouldListenTo = _ => true,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = activities.Add
        };

        ActivitySource.AddActivityListener(listener);

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(fixture.Connection);
        builder.Services
            .AddMessageBus()
            .AddEventHandler<TracedEventHandler>()
            .AddNats(nats => nats.ServiceName("otel-service"));

        using var host = builder.Build();
        await host.StartAsync(cancellationToken);

        try
        {
            await host.Services.GetRequiredService<IMessageBus>()
                .PublishAsync(new TracedEvent(Guid.NewGuid()), cancellationToken);

            await TracedEventHandler.Received.Task.WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }

        var sources = activities
            .Select(a => a.Source.Name)
            .Distinct(StringComparer.Ordinal)
            .ToList();

        // The NATS client ships an always-registered "NATS.Net" ActivitySource, so its spans appear
        // for anything subscribing to every source the way this test does.
        Assert.Contains(sources, name => name.Equals("NATS.Net", StringComparison.Ordinal));

        // Mocha emits no spans of its own in this configuration, so the double instrumentation this
        // transport was designed to avoid does not currently occur. Asserted rather than assumed:
        // if a later Mocha version starts tracing, this fails and the ownership question has to be
        // answered deliberately instead of shipping duplicate spans around every message.
        Assert.DoesNotContain(sources, name => name.Contains("Mocha", StringComparison.OrdinalIgnoreCase));
    }
}
