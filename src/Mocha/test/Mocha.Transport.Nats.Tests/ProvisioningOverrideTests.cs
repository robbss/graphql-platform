using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mocha.Transport.Nats.Tests.Fixtures;
using NATS.Client.Core;
using NATS.Client.JetStream.Models;
using Xunit;

namespace Mocha.Transport.Nats.Tests;

public sealed record PreProvisioned(Guid Id);

public sealed class PreProvisionedHandler : IEventHandler<PreProvisioned>
{
    public static readonly TaskCompletionSource Received =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public ValueTask HandleAsync(PreProvisioned message, CancellationToken cancellationToken)
    {
        Received.TrySetResult();
        return ValueTask.CompletedTask;
    }
}

[Collection(JetStreamCollection.Name)]
public class ProvisioningOverrideTests(JetStreamFixture fixture)
{
    [Fact]
    public async Task An_Explicitly_Declared_Stream_Removes_The_Start_Up_Order_Dependency()
    {
        var cancellationToken = TestContext.Current.CancellationToken;

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(fixture.Connection);
        builder.Services
            .AddMessageBus()
            .AddEventHandler<PreProvisionedHandler>()
            .AddNats(nats => nats
                .ServiceName("e2e-declared")
                // Narrow subjects on purpose: JetStream rejects a stream whose subjects overlap an
                // existing one, so a wildcard here would collide with every other test's stream.
                .DeclareStream("E2E_DECLARED")
                    .Subject("mocha.transport.nats.tests.pre-provisioned")
                    .Subject("mocha.transport.nats.tests.pre-provisioned_error")
                    .Subject("mocha.transport.nats.tests.pre-provisioned_skipped")
                    .Storage(StreamConfigStorage.Memory));

        using var host = builder.Build();
        await host.StartAsync(cancellationToken);

        try
        {
            var topology = (NatsMessagingTopology)host.Services
                .GetRequiredService<IMessagingRuntime>()
                .Transports.OfType<NatsMessagingTransport>()
                .Single()
                .Topology;

            // A declared stream replaces the convention one entirely, so the subject wildcards have
            // to cover everything routing produces, including the error and skipped subjects.
            var stream = Assert.Single(topology.Streams);

            Assert.Equal("E2E_DECLARED", stream.Name);
            Assert.All(topology.Consumers, c => Assert.Equal("E2E_DECLARED", c.StreamName));

            await host.Services.GetRequiredService<IMessageBus>()
                .PublishAsync(new PreProvisioned(Guid.NewGuid()), cancellationToken);

            await PreProvisionedHandler.Received.Task.WaitAsync(
                TimeSpan.FromSeconds(30),
                cancellationToken);
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }

    [Fact]
    public async Task AutoProvision_False_Uses_A_Stream_Created_Outside_The_Transport()
    {
        var cancellationToken = TestContext.Current.CancellationToken;

        // Stand in for infrastructure managed by ops rather than by the application.
        await fixture.JetStream.CreateOrUpdateStreamAsync(
            new StreamConfig
            {
                Name = "E2E_EXTERNAL",
                Subjects = ["external-service.>"],
                Storage = StreamConfigStorage.Memory
            },
            cancellationToken);

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(fixture.Connection);
        builder.Services
            .AddMessageBus()
            .AddNats(nats => nats.ServiceName("e2e-external").AutoProvision(false));

        using var host = builder.Build();

        await host.StartAsync(cancellationToken);

        try
        {
            var transport = host.Services
                .GetRequiredService<IMessagingRuntime>()
                .Transports.OfType<NatsMessagingTransport>()
                .Single();

            // Nothing should have been created: with no handlers there is nothing to publish, and
            // auto-provisioning is off, so the transport must not touch the server's topology.
            Assert.Empty(((NatsMessagingTopology)transport.Topology).Streams);

            var external = await fixture.JetStream.GetStreamAsync(
                "E2E_EXTERNAL",
                cancellationToken: cancellationToken);

            Assert.Equal(["external-service.>"], external.Info.Config.Subjects);
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }

    [Fact]
    public async Task Deduplication_Drops_A_Repeated_Message_Id()
    {
        var cancellationToken = TestContext.Current.CancellationToken;

        await fixture.JetStream.CreateOrUpdateStreamAsync(
            new StreamConfig
            {
                Name = "E2E_DEDUP",
                Subjects = ["dedup-test.>"],
                Storage = StreamConfigStorage.Memory,
                DuplicateWindow = TimeSpan.FromMinutes(2)
            },
            cancellationToken);

        var headers = new NatsHeaders { { NatsMessageHeaders.DeduplicationKey, "fixed-message-id" } };

        var first = await fixture.JetStream.PublishAsync(
            "dedup-test.thing",
            new ReadOnlyMemory<byte>("one"u8.ToArray()),
            NatsRawSerializer<ReadOnlyMemory<byte>>.Default,
            headers: headers,
            cancellationToken: cancellationToken);

        var second = await fixture.JetStream.PublishAsync(
            "dedup-test.thing",
            new ReadOnlyMemory<byte>("two"u8.ToArray()),
            NatsRawSerializer<ReadOnlyMemory<byte>>.Default,
            headers: new NatsHeaders { { NatsMessageHeaders.DeduplicationKey, "fixed-message-id" } },
            cancellationToken: cancellationToken);

        Assert.False(first.Duplicate);

        // The header the transport always writes is what makes this work, and the reason a publish
        // reporting Duplicate has to be treated as success rather than as an error.
        Assert.True(second.Duplicate);

        var stream = await fixture.JetStream.GetStreamAsync(
            "E2E_DEDUP",
            cancellationToken: cancellationToken);

        Assert.Equal(1, stream.Info.State.Messages);
    }
}
