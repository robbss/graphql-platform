using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mocha.Transport.Nats.Tests.Fixtures;
using NATS.Client.JetStream.Models;
using Xunit;

namespace Mocha.Transport.Nats.Tests.Behaviors;

[Collection(JetStreamCollection.Name)]
public class ConventionStreamAdoptionTests(JetStreamFixture fixture)
{
    [Fact]
    public async Task DeclaredStream_Should_KeepTheSubjectsItAlreadyHolds_When_TheServerCapturesThem()
    {
        // arrange
        // Declaring the convention stream is the only way to set its retention, and a declaration is
        // authoritative: it does not rebase on the server's configuration the way a convention stream
        // does. So the subjects the convention pass contributes have to include the ones this stream
        // is itself already holding, or the update that applies the retention strips them.
        var cancellationToken = TestContext.Current.CancellationToken;
        const string serviceName = "adoption-service";
        const string streamName = "ADOPTION_SERVICE";
        const string faultSubject = "adoption-service.settlement-attempted_error";
        const string skippedSubject = "adoption-service.settlement-attempted_skipped";

        await fixture.JetStream.CreateStreamAsync(
            new StreamConfig(streamName, [faultSubject]) { NumReplicas = 1 },
            cancellationToken);

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(fixture.Connection);
        builder.Services
            .AddMessageBus()
            .AddEventHandler<SettlementAttemptedHandler>()
            .Host(host => host.ServiceName(serviceName))
            .AddNats(nats =>
            {
                nats.StreamName(serviceName);
                nats.DeclareStream(streamName).MaxAge(TimeSpan.FromHours(168));
            });

        using var host = builder.Build();

        try
        {
            // act
            // Before the fix this threw: the subject the server already held was treated as somebody
            // else's and left out, so the declaration wrote a stream without it and start-up failed
            // verifying that the service could publish its own faults.
            await host.StartAsync(cancellationToken);

            var stream = await fixture.JetStream.GetStreamAsync(streamName, cancellationToken: cancellationToken);

            // assert
            Assert.Contains(faultSubject, stream.Info.Config.Subjects!);
            Assert.Contains(skippedSubject, stream.Info.Config.Subjects!);
            Assert.Equal(TimeSpan.FromHours(168), stream.Info.Config.MaxAge);
        }
        finally
        {
            await host.StopAsync(cancellationToken);
            await fixture.JetStream.DeleteStreamAsync(streamName, cancellationToken);
        }
    }

    [Fact]
    public async Task DeclaredStream_Should_SurviveARestart_When_ItAlreadyProvisionedItsOwnSubjects()
    {
        // arrange
        // The first start has nothing to yield to and gets this right, so the defect only shows on the
        // one after it: the subjects are now on the server, and treating them as somebody else's left
        // the declaration with none of its own to write. Restarting is the whole reproduction.
        var cancellationToken = TestContext.Current.CancellationToken;
        const string serviceName = "restart-service";
        const string streamName = "RESTART_SERVICE";
        const string faultSubject = "restart-service.settlement-attempted_error";

        try
        {
            // act
            await StartAndStopAsync(serviceName, streamName, cancellationToken);
            await StartAndStopAsync(serviceName, streamName, cancellationToken);

            var stream = await fixture.JetStream.GetStreamAsync(
                streamName,
                cancellationToken: cancellationToken);

            // assert
            // Not the stream name as a literal subject, which is what an empty subject list becomes.
            Assert.Contains(faultSubject, stream.Info.Config.Subjects!);
            Assert.DoesNotContain(streamName, stream.Info.Config.Subjects!);
        }
        finally
        {
            await fixture.JetStream.DeleteStreamAsync(streamName, cancellationToken);
        }
    }

    private async Task StartAndStopAsync(
        string serviceName,
        string streamName,
        CancellationToken cancellationToken)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(fixture.Connection);
        builder.Services
            .AddMessageBus()
            .AddEventHandler<SettlementAttemptedHandler>()
            .Host(host => host.ServiceName(serviceName))
            .AddNats(nats =>
            {
                nats.StreamName(serviceName);
                nats.DeclareStream(streamName).MaxAge(TimeSpan.FromHours(168));
            });

        using var host = builder.Build();

        await host.StartAsync(cancellationToken);
        await host.StopAsync(cancellationToken);
    }

    public sealed record SettlementAttempted
    {
        public string? Reference { get; init; }
    }

    public sealed class SettlementAttemptedHandler : IEventHandler<SettlementAttempted>
    {
        public ValueTask HandleAsync(SettlementAttempted message, CancellationToken cancellationToken)
            => ValueTask.CompletedTask;
    }
}
