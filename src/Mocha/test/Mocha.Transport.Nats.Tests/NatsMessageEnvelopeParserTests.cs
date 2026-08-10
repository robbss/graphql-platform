using System.Text;
using Mocha.Middlewares;
using NATS.Client.Core;
using Xunit;

namespace Mocha.Transport.Nats.Tests;

public class NatsMessageEnvelopeParserTests
{
    [Fact]
    public void Parse_ShouldReturnEnvelopeWithMetadata_WhenHeadersAreProvided()
    {
        // Arrange
        var headers = new NatsHeaders
        {
            [NatsMessageHeaders.MessageId] = "msg-123",
            [NatsMessageHeaders.CorrelationId] = "corr-456",
            [NatsMessageHeaders.CausationId] = "cause-789",
            [NatsMessageHeaders.ContentType] = "application/json",
            [NatsMessageHeaders.MessageType] = "OrderCreatedEvent",
            ["custom-header"] = "custom-value"
        };

        byte[] body = Encoding.UTF8.GetBytes("{\"orderId\": 42}");

        // Act
        var envelope = NatsMessageEnvelopeParser.Parse(headers, body);

        // Assert
        Assert.Equal("msg-123", envelope.MessageId);
        Assert.Equal("corr-456", envelope.CorrelationId);
        Assert.Equal("cause-789", envelope.CausationId);
        Assert.Equal("application/json", envelope.ContentType);
        Assert.Equal("OrderCreatedEvent", envelope.MessageType);
        Assert.Equal(body, envelope.Body.ToArray());
        Assert.NotNull(envelope.Headers);
        Assert.True(envelope.Headers.TryGetValue("custom-header", out var customVal));
        Assert.Equal("custom-value", customVal);
    }
}
