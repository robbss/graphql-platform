using Squadron;
using Xunit;

namespace Mocha.Transport.Nats.Tests.Helpers;

public class MochaNatsResource : NatsResource;

public sealed class NatsFixture : IAsyncLifetime
{
    private readonly MochaNatsResource _resource = new();

    public async ValueTask InitializeAsync()
    {
        await _resource.InitializeAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _resource.DisposeAsync();
    }
}

[CollectionDefinition("Nats")]
public class NatsCollection : ICollectionFixture<NatsFixture>;
