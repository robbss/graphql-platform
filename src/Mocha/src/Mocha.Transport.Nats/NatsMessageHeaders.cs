using System.Diagnostics.CodeAnalysis;
using Mocha.Middlewares;
using NATS.Client.Core;

namespace Mocha.Transport.Nats;

/// <summary>
/// Constants and extension methods for NATS message header serialization and metadata mapping.
/// </summary>
public static class NatsMessageHeaders
{
    public const string MessageId = "mocha-message-id";
    public const string CorrelationId = "mocha-correlation-id";
    public const string CausationId = "mocha-causation-id";
    public const string ContentType = "mocha-content-type";
    public const string MessageType = "mocha-message-type";
    public const string SentTime = "mocha-sent-time";

    public static NatsHeaders ToNatsHeaders(MessageEnvelope envelope)
    {
        var headers = new NatsHeaders();

        if (envelope.MessageId is not null)
        {
            headers[MessageId] = envelope.MessageId;
        }

        if (envelope.CorrelationId is not null)
        {
            headers[CorrelationId] = envelope.CorrelationId;
        }

        if (envelope.CausationId is not null)
        {
            headers[CausationId] = envelope.CausationId;
        }

        if (envelope.ContentType is not null)
        {
            headers[ContentType] = envelope.ContentType;
        }

        if (envelope.MessageType is not null)
        {
            headers[MessageType] = envelope.MessageType;
        }

        headers[SentTime] = DateTimeOffset.UtcNow.ToString("O");

        if (envelope.Headers is not null)
        {
            foreach (var header in envelope.Headers)
            {
                if (header.Value is string strVal)
                {
                    headers[header.Key] = strVal;
                }
            }
        }

        return headers;
    }

    public static bool TryGetHeaderValue(NatsHeaders? headers, string key, [NotNullWhen(true)] out string? value)
    {
        value = null;
        if (headers is null)
        {
            return false;
        }

        if (headers.TryGetValue(key, out var stringValues) && stringValues.Count > 0)
        {
            value = stringValues[0];
            return value is not null;
        }

        return false;
    }
}
