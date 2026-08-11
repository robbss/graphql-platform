using Xunit;

namespace Mocha.Transport.Nats.Tests;

public class NatsConnectionProviderTests
{
    [Theory]
    [InlineData("nats://localhost:4222", "localhost", 4222)]
    [InlineData("nats://nats.internal:4333", "nats.internal", 4333)]
    [InlineData("localhost:4222", "localhost", 4222)]
    [InlineData("nats://localhost", "localhost", NatsConnectionProvider.DefaultPort)]
    public void ParseFirstServer_Handles_The_Common_Url_Shapes(string url, string host, int port)
    {
        var (parsedHost, parsedPort) = NatsConnectionProvider.ParseFirstServer(url);

        Assert.Equal(host, parsedHost);
        Assert.Equal(port, parsedPort);
    }

    [Fact]
    public void ParseFirstServer_Uses_Only_The_First_Server_In_A_Cluster()
    {
        var (host, port) = NatsConnectionProvider.ParseFirstServer(
            "nats://first:4222, nats://second:4223, nats://third:4224");

        Assert.Equal("first", host);
        Assert.Equal(4222, port);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void ParseFirstServer_Rejects_Empty_Urls(string url)
    {
        Assert.Throws<ArgumentException>(() => NatsConnectionProvider.ParseFirstServer(url));
    }
}
