using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mocha.Transport.Nats.Tests.Fixtures;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Xunit;

namespace Mocha.Transport.Nats.Tests;

public sealed record PoisonMessage(Guid Id);

public sealed class PoisonMessageException(string message) : Exception(message);

public sealed class PoisonMessageHandler : IEventHandler<PoisonMessage>
{
    public static readonly TaskCompletionSource Attempted =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public ValueTask HandleAsync(PoisonMessage message, CancellationToken cancellationToken)
    {
        Attempted.TrySetResult();

        throw new PoisonMessageException("This message can never be handled.");
    }
}

[Collection(JetStreamCollection.Name)]
public class DeadLetterTests(JetStreamFixture fixture)
{
    private IHost BuildHost()
    {
        var builder = Host.CreateApplicationBuilder();

        builder.Services.AddSingleton(fixture.Connection);
        builder.Services
            .AddMessageBus()
            .AddEventHandler<PoisonMessageHandler>()
            .AddResilience(policy => policy.Default().Retry(1).ThenDeadLetter())
            .AddNats(nats => nats.ServiceName("e2e-poison"));

        return builder.Build();
    }

    [Fact]
    public async Task A_Permanently_Failing_Message_Lands_On_The_Error_Subject()
    {
        var cancellationToken = TestContext.Current.CancellationToken;

        using var host = BuildHost();
        await host.StartAsync(cancellationToken);

        try
        {
            var transport = host.Services
                .GetRequiredService<IMessagingRuntime>()
                .Transports.OfType<NatsMessagingTransport>()
                .Single();

            var topology = (NatsMessagingTopology)transport.Topology;
            var stream = Assert.Single(topology.Streams);

            var errorSubject = Assert.Single(
                topology.Subjects.Select(s => s.Subject),
                s => s.EndsWith("_error", StringComparison.Ordinal));

            // Publishing to a subject no stream captures does not fail fast on JetStream, it times
            // out waiting for an acknowledgement, so coverage is worth asserting directly.
            Assert.Contains(errorSubject, stream.Subjects);

            // Mocha's fault pipeline resolves the error endpoint by address, so this endpoint has to
            // exist for a faulted message to have anywhere to go.
            Assert.Contains(
                transport.DispatchEndpoints,
                e => e is NatsDispatchEndpoint { Subject.Subject: { } s } && s == errorSubject);

            await host.Services.GetRequiredService<IMessageBus>()
                .PublishAsync(new PoisonMessage(Guid.NewGuid()), cancellationToken);

            await PoisonMessageHandler.Attempted.Task.WaitAsync(
                TimeSpan.FromSeconds(30),
                cancellationToken);

            var delivered = await WaitForSubjectAsync(
                fixture.JetStream,
                stream.Name,
                errorSubject,
                TimeSpan.FromSeconds(30),
                cancellationToken);

            Assert.True(
                delivered > 0,
                $"The faulted message never reached '{errorSubject}'.");
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }

    /// <summary>
    /// Polls the stream's per-subject message counts until the subject holds a message.
    /// </summary>
    /// <remarks>
    /// Asserting on stream state rather than consuming keeps the test from competing with the
    /// transport's own durable consumer for the message.
    /// </remarks>
    private static async Task<long> WaitForSubjectAsync(
        INatsJSContext jetStream,
        string streamName,
        string subject,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var deadline = DateTimeOffset.UtcNow.Add(timeout);

        while (DateTimeOffset.UtcNow < deadline)
        {
            var stream = await jetStream.GetStreamAsync(
                streamName,
                new StreamInfoRequest { SubjectsFilter = ">" },
                cancellationToken);

            if (stream.Info.State.Subjects?.TryGetValue(subject, out var count) == true && count > 0)
            {
                return count;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
        }

        return 0;
    }
}
