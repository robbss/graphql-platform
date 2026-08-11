using Xunit;

namespace Mocha.Transport.Nats.Tests;

public class NatsMessagingTransportTests
{
    [Fact]
    public void Transport_Derives_From_MessagingTransport()
    {
        var transport = new NatsMessagingTransport(static _ => { });

        Assert.IsAssignableFrom<MessagingTransport>(transport);
    }

    [Fact]
    public void Configuration_Delegate_Is_Applied()
    {
        var applied = false;

        var transport = new NatsMessagingTransport(_ => applied = true);

        Assert.NotNull(transport);
        Assert.False(applied, "The delegate must not run before the bus initializes the transport.");
    }
}
