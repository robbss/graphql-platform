using Mocha.Middlewares;
using NATS.Client.Core;

namespace Mocha.Transport.Nats;

/// <summary>
/// Parser for reconstructing a Mocha <see cref="MessageEnvelope"/> from NATS headers and payload.
/// </summary>
public static class NatsMessageEnvelopeParser
{
    public static MessageEnvelope Parse(NatsHeaders? headers, byte[]? data)
    {
        NatsMessageHeaders.TryGetHeaderValue(headers, NatsMessageHeaders.MessageId, out var id);
        NatsMessageHeaders.TryGetHeaderValue(headers, NatsMessageHeaders.CorrelationId, out var correlationId);
        NatsMessageHeaders.TryGetHeaderValue(headers, NatsMessageHeaders.CausationId, out var causationId);
        NatsMessageHeaders.TryGetHeaderValue(headers, NatsMessageHeaders.ContentType, out var contentType);
        NatsMessageHeaders.TryGetHeaderValue(headers, NatsMessageHeaders.MessageType, out var messageType);

        Headers parsedHeaders;

        if (headers?.Count > 0)
        {
            var result = new Headers(headers.Count);
            foreach (var kvp in headers)
            {
                if (!kvp.Key.StartsWith("mocha-", StringComparison.OrdinalIgnoreCase))
                {
                    if (kvp.Value.Count > 0 && kvp.Value[0] is { } val)
                    {
                        result.Set(kvp.Key, val);
                    }
                }
            }
            parsedHeaders = result;
        }
        else
        {
            parsedHeaders = Headers.Empty();
        }

        return new MessageEnvelope
        {
            MessageId = id,
            CorrelationId = correlationId,
            CausationId = causationId,
            ContentType = contentType,
            MessageType = messageType,
            Body = data ?? [],
            Headers = parsedHeaders
        };
    }
}
