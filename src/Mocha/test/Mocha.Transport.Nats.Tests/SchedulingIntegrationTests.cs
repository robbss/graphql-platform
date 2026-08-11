using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mocha.Transport.Nats.Tests.Fixtures;
using Xunit;

namespace Mocha.Transport.Nats.Tests;

public sealed record ReminderDue(Guid Id);

public sealed class ReminderDueHandler : IEventHandler<ReminderDue>
{
    public static readonly TaskCompletionSource<TimeSpan> Received =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public static readonly Stopwatch Clock = new();

    public ValueTask HandleAsync(ReminderDue message, CancellationToken cancellationToken)
    {
        Received.TrySetResult(Clock.Elapsed);
        return ValueTask.CompletedTask;
    }
}

[Collection(JetStreamCollection.Name)]
public class SchedulingIntegrationTests(JetStreamFixture fixture)
{
    private static readonly TimeSpan s_delay = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task A_Scheduled_Message_Is_Held_Until_It_Is_Due()
    {
        var cancellationToken = TestContext.Current.CancellationToken;

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(fixture.Connection);
        builder.Services
            .AddMessageBus()
            .AddEventHandler<ReminderDueHandler>()
            .AddNats(nats => nats.ServiceName("e2e-reminders").EnableScheduling());

        using var host = builder.Build();

        await host.StartAsync(cancellationToken);

        try
        {
            var transport = host.Services
                .GetRequiredService<IMessagingRuntime>()
                .Transports.OfType<NatsMessagingTransport>()
                .Single();

            Assert.SkipUnless(
                transport.Capabilities.SupportsMessageSchedules,
                "Message schedules need NATS server 2.12 or later; this server reports "
                + $"{transport.Capabilities.Version?.ToString() ?? "an unknown version"}.");

            var stream = Assert.Single(((NatsMessagingTopology)transport.Topology).Streams);

            // The server refuses a schedule whose target is the subject it arrived on, so enabling
            // scheduling has to add a distinct scheduling subject alongside each real one.
            Assert.Contains(
                stream.Subjects,
                s => s.EndsWith(NatsScheduling.SchedulingSuffix, StringComparison.Ordinal));

            Assert.True(stream.AllowMsgSchedules);

            ReminderDueHandler.Clock.Restart();

            await host.Services.GetRequiredService<IMessageBus>()
                .SchedulePublishAsync(
                    new ReminderDue(Guid.NewGuid()),
                    DateTimeOffset.UtcNow.Add(s_delay),
                    cancellationToken);

            var elapsed = await ReminderDueHandler.Received.Task.WaitAsync(
                s_delay + TimeSpan.FromSeconds(45),
                cancellationToken);

            // The point of the test: the broker held it, rather than it arriving straight away.
            Assert.True(
                elapsed >= s_delay - TimeSpan.FromSeconds(1),
                $"The scheduled message arrived after {elapsed}, before its {s_delay} delay.");
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }
}
