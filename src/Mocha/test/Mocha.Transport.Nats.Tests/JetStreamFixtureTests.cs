using Mocha.Transport.Nats.Tests.Fixtures;
using NATS.Client.JetStream.Models;
using Xunit;

namespace Mocha.Transport.Nats.Tests;

[Collection(JetStreamCollection.Name)]
public class JetStreamFixtureTests(JetStreamFixture fixture)
{
    [Fact]
    public async Task The_Container_Has_JetStream_Enabled()
    {
        var account = await fixture.JetStream.GetAccountInfoAsync(TestContext.Current.CancellationToken);

        Assert.NotNull(account);
    }

    [Fact]
    public async Task A_Stream_Can_Be_Provisioned_And_Found_By_Subject()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var topology = TestTopology.Create();

        var stream = topology.AddStream(new NatsStreamConfiguration
        {
            Name = "FIXTURE_SERVICE",
            Subjects = ["fixture-service.>"],
            Storage = StreamConfigStorage.Memory
        });

        await stream.ProvisionAsync(fixture.JetStream, cancellationToken);

        var matches = new List<string>();

        await foreach (var name in fixture.JetStream.ListStreamNamesAsync(
            "fixture-service.thing-happened",
            cancellationToken))
        {
            matches.Add(name);
        }

        Assert.Contains("FIXTURE_SERVICE", matches);
    }

    [Fact]
    public async Task A_Consumer_Can_Be_Provisioned_On_A_Resolved_Stream()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var topology = TestTopology.Create();

        var stream = topology.AddStream(new NatsStreamConfiguration
        {
            Name = "FIXTURE_CONSUMERS",
            Subjects = ["fixture-consumers.>"],
            Storage = StreamConfigStorage.Memory
        });

        await stream.ProvisionAsync(fixture.JetStream, cancellationToken);

        var consumer = topology.AddConsumer(new NatsConsumerConfiguration
        {
            Name = "fixture-consumers_thing-happened",
            FilterSubjects = ["fixture-consumers.thing-happened"]
        });

        await NatsStreamResolver.ResolveAsync(fixture.JetStream, topology, cancellationToken);

        Assert.Equal("FIXTURE_CONSUMERS", consumer.StreamName);

        await consumer.ProvisionAsync(fixture.JetStream, cancellationToken);

        var provisioned = await fixture.JetStream.GetConsumerAsync(
            "FIXTURE_CONSUMERS",
            "fixture-consumers_thing-happened",
            cancellationToken);

        Assert.Equal("fixture-consumers_thing-happened", provisioned.Info.Name);
    }
}
