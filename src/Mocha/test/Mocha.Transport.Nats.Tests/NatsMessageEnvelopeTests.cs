using System.Collections.Immutable;
using System.Text;
using Mocha.Middlewares;
using Xunit;

namespace Mocha.Transport.Nats.Tests;

public class NatsMessageEnvelopeTests
{
    private static MessageEnvelope CreateEnvelope()
    {
        var headers = new Headers();
        headers.Set("x-tenant", "acme");

        return new MessageEnvelope
        {
            MessageId = "01JQ8Z0000000000000000",
            CorrelationId = "correlation-1",
            ConversationId = "conversation-1",
            CausationId = "causation-1",
            SourceAddress = "nats://localhost:4222/ORDER_SERVICE/s/order-service.order-created",
            DestinationAddress = "nats://localhost:4222/ORDER_SERVICE/c/order-service_order-created",
            ResponseAddress = "_INBOX.abc123",
            FaultAddress = "nats://localhost:4222/ORDER_SERVICE/s/order-created_error",
            MessageType = "Contracts.OrderCreated",
            ContentType = "application/json",
            SentAt = new DateTimeOffset(2026, 8, 10, 12, 0, 0, TimeSpan.Zero),
            DeliverBy = new DateTimeOffset(2026, 8, 10, 12, 5, 0, TimeSpan.Zero),
            ScheduledTime = new DateTimeOffset(2026, 8, 10, 12, 1, 0, TimeSpan.Zero),
            EnclosedMessageTypes = ImmutableArray.Create("Contracts.OrderCreated", "Contracts.IOrderEvent"),
            Headers = headers,
            Body = Encoding.UTF8.GetBytes("""{"orderId":"1"}""")
        };
    }

    [Fact]
    public void Envelope_Round_Trips_Through_Nats_Headers()
    {
        var envelope = CreateEnvelope();

        var headers = NatsMessageHeadersWriter.Instance.Write(envelope);
        var parsed = NatsMessageEnvelopeParser.Instance.Parse(headers, envelope.Body, deliveryCount: 1);

        Assert.Equal(envelope.MessageId, parsed.MessageId);
        Assert.Equal(envelope.CorrelationId, parsed.CorrelationId);
        Assert.Equal(envelope.ConversationId, parsed.ConversationId);
        Assert.Equal(envelope.CausationId, parsed.CausationId);
        Assert.Equal(envelope.SourceAddress, parsed.SourceAddress);
        Assert.Equal(envelope.DestinationAddress, parsed.DestinationAddress);
        Assert.Equal(envelope.ResponseAddress, parsed.ResponseAddress);
        Assert.Equal(envelope.FaultAddress, parsed.FaultAddress);
        Assert.Equal(envelope.MessageType, parsed.MessageType);
        Assert.Equal(envelope.ContentType, parsed.ContentType);
        Assert.Equal(envelope.SentAt, parsed.SentAt);
        Assert.Equal(envelope.DeliverBy, parsed.DeliverBy);
        Assert.Equal(envelope.ScheduledTime, parsed.ScheduledTime);
        Assert.Equal(envelope.EnclosedMessageTypes, parsed.EnclosedMessageTypes);
        Assert.Equal(envelope.Body.ToArray(), parsed.Body.ToArray());
    }

    [Fact]
    public void MessageId_Travels_Separately_From_The_Dedup_Key()
    {
        var envelope = CreateEnvelope();

        var headers = NatsMessageHeadersWriter.Instance.Write(envelope);

        Assert.True(headers.TryGetLastValue(NatsMessageHeaders.MessageId, out var messageId));
        Assert.Equal(envelope.MessageId, messageId);

        // The dedup key is written by the dispatch endpoint, which qualifies it by destination
        // subject. Writing the bare identifier here would make any republish inside one stream
        // look like a duplicate.
        Assert.False(headers.ContainsKey(NatsMessageHeaders.DeduplicationKey));
    }

    [Fact]
    public void DeliveryCount_Comes_From_Metadata_Not_Headers()
    {
        var envelope = CreateEnvelope();

        var headers = NatsMessageHeadersWriter.Instance.Write(envelope);
        var parsed = NatsMessageEnvelopeParser.Instance.Parse(headers, envelope.Body, deliveryCount: 3);

        Assert.Equal(3, parsed.DeliveryCount);
    }

    [Fact]
    public void User_Headers_Round_Trip_Without_Transport_Keys()
    {
        var envelope = CreateEnvelope();

        var headers = NatsMessageHeadersWriter.Instance.Write(envelope);
        var parsed = NatsMessageEnvelopeParser.Instance.Parse(headers, envelope.Body, deliveryCount: 0);

        var parsedHeaders = Assert.IsType<Headers>(parsed.Headers);

        Assert.Equal("acme", parsedHeaders.GetValue("x-tenant"));
        Assert.False(parsedHeaders.ContainsKey("Nats-Msg-Id"));
        Assert.False(parsedHeaders.ContainsKey("x-message-type"));
    }

    [Fact]
    public void Absent_Optional_Fields_Stay_Null()
    {
        var envelope = new MessageEnvelope { Body = Encoding.UTF8.GetBytes("{}") };

        var headers = NatsMessageHeadersWriter.Instance.Write(envelope);
        var parsed = NatsMessageEnvelopeParser.Instance.Parse(headers, envelope.Body, deliveryCount: 0);

        Assert.Null(parsed.MessageId);
        Assert.Null(parsed.CorrelationId);
        Assert.Null(parsed.SentAt);
        Assert.Null(parsed.ScheduledTime);
        Assert.Empty(parsed.EnclosedMessageTypes ?? []);
    }

    [Fact]
    public void Multi_Line_Header_Values_Are_Flattened()
    {
        var headers = new Headers();
        headers.Set("fault-stack-trace", "at Handler.HandleAsync()\r\n   at Pipeline.InvokeAsync()");

        var envelope = new MessageEnvelope
        {
            Headers = headers,
            Body = Encoding.UTF8.GetBytes("{}")
        };

        var written = NatsMessageHeadersWriter.Instance.Write(envelope);

        Assert.True(written.TryGetLastValue("fault-stack-trace", out var value));
        Assert.DoesNotContain('\r', value!);
        Assert.DoesNotContain('\n', value!);
        Assert.Contains("at Handler.HandleAsync()", value, StringComparison.Ordinal);
    }

    [Fact]
    public void Each_Write_Returns_A_Fresh_Header_Instance()
    {
        var envelope = CreateEnvelope();

        var first = NatsMessageHeadersWriter.Instance.Write(envelope);
        var second = NatsMessageHeadersWriter.Instance.Write(envelope);

        Assert.NotSame(first, second);
    }
}
