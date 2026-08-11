using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mocha.Transport.Nats.Tests.Fixtures;
using Xunit;

namespace Mocha.Transport.Nats.Tests;

public sealed record TracedEvent(Guid Id);

[Collection(JetStreamCollection.Name)]
public class OpenTelemetryTests(JetStreamFixture fixture)
{
    [Fact]
    public async Task PublishAsync_Should_EmitOnlyNatsClientSpans_When_AnEventIsHandled()
    {
        // arrange
        // The NATS client ships an always-registered "NATS.Net" ActivitySource, and Mocha emits no
        // spans of its own here, so the double instrumentation this transport was designed to avoid
        // does not occur. Asserted rather than assumed: if a later Mocha version starts tracing, this
        // fails and the ownership question gets answered deliberately instead of shipping duplicate
        // spans around every message.
        var cancellationToken = TestContext.Current.CancellationToken;
        var recorder = new MessageRecorder();
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
        builder.Services.AddSingleton(recorder);
        builder.Services
            .AddMessageBus()
            .AddEventHandler<TracedEventHandler>()
            .AddNats(nats => nats.ServiceName("otel-service"));

        using var host = builder.Build();
        await host.StartAsync(cancellationToken);

        try
        {
            // act
            await host.Services.GetRequiredService<IMessageBus>()
                .PublishAsync(new TracedEvent(Guid.NewGuid()), cancellationToken);

            Assert.True(
                await recorder.WaitAsync(TimeSpan.FromSeconds(30)),
                "The handler did not receive the event.");
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }

        // assert
        var mochaSources = activities
            .Select(a => a.Source.Name)
            .Where(name => name.Contains("Mocha", StringComparison.OrdinalIgnoreCase))
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToList();

        Assert.Contains("NATS.Net", activities.Select(a => a.Source.Name));
        Assert.Equal([], mochaSources);
    }

    public sealed class TracedEventHandler(MessageRecorder recorder) : IEventHandler<TracedEvent>
    {
        public ValueTask HandleAsync(TracedEvent message, CancellationToken cancellationToken)
        {
            recorder.Record(message);
            return ValueTask.CompletedTask;
        }
    }
}
