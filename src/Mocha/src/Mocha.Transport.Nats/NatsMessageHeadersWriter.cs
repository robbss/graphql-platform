using System.Globalization;
using System.Text;
using Microsoft.Extensions.Primitives;
using Mocha.Middlewares;
using NATS.Client.Core;

namespace Mocha.Transport.Nats;

/// <summary>
/// Writes a <see cref="MessageEnvelope"/> into a <see cref="NatsHeaders"/> instance for publishing.
/// </summary>
internal sealed class NatsMessageHeadersWriter
{
    /// <summary>
    /// Shared singleton instance of the writer.
    /// </summary>
    public static readonly NatsMessageHeadersWriter Instance = new();

    /// <summary>
    /// Projects the envelope metadata and user-defined headers onto a fresh header collection.
    /// </summary>
    /// <param name="envelope">The envelope to write.</param>
    /// <returns>A new <see cref="NatsHeaders"/> instance owned by the caller.</returns>
    /// <remarks>
    /// A fresh instance is returned per call because NATS.Net 3.0 no longer makes headers read-only
    /// after publishing, so sharing one across concurrent publishes is unsafe.
    /// </remarks>
    public NatsHeaders Write(MessageEnvelope envelope)
    {
        var headers = new NatsHeaders();

        if (envelope.Headers is not null)
        {
            foreach (var header in envelope.Headers)
            {
                if (header.Value is null || NatsMessageHeaders.IsReserved(header.Key))
                {
                    continue;
                }

                headers.Add(header.Key, Sanitize(Format(header.Value)));
            }
        }

        Set(headers, NatsMessageHeaders.MessageId, envelope.MessageId);
        Set(headers, NatsMessageHeaders.CorrelationId, envelope.CorrelationId);
        Set(headers, NatsMessageHeaders.ConversationId, envelope.ConversationId);
        Set(headers, NatsMessageHeaders.CausationId, envelope.CausationId);
        Set(headers, NatsMessageHeaders.SourceAddress, envelope.SourceAddress);
        Set(headers, NatsMessageHeaders.DestinationAddress, envelope.DestinationAddress);
        Set(headers, NatsMessageHeaders.ResponseAddress, envelope.ResponseAddress);
        Set(headers, NatsMessageHeaders.FaultAddress, envelope.FaultAddress);
        Set(headers, NatsMessageHeaders.MessageType, envelope.MessageType);
        Set(headers, NatsMessageHeaders.ContentType, envelope.ContentType);
        Set(headers, NatsMessageHeaders.SentAt, Format(envelope.SentAt));
        Set(headers, NatsMessageHeaders.DeliverBy, Format(envelope.DeliverBy));
        Set(headers, NatsMessageHeaders.ScheduledTime, Format(envelope.ScheduledTime));

        if (envelope.EnclosedMessageTypes is { Length: > 0 } enclosedMessageTypes)
        {
            headers.Add(NatsMessageHeaders.EnclosedMessageTypes, new StringValues([.. enclosedMessageTypes]));
        }

        return headers;
    }

    private static void Set(NatsHeaders headers, string key, string? value)
    {
        if (!string.IsNullOrEmpty(value))
        {
            headers.Add(key, Sanitize(value));
        }
    }

    /// <summary>
    /// Replaces line breaks and control characters so a value is legal in a NATS header.
    /// </summary>
    /// <param name="value">The value to sanitize.</param>
    /// <returns>The value with line breaks and control characters collapsed to spaces.</returns>
    /// <remarks>
    /// NATS rejects header values containing CRLF, because the wire protocol is line-based. Mocha's
    /// fault middleware puts an exception stack trace in a header, which is multi-line, so without
    /// this every dead-lettered message would fail to publish. AMQP has no such restriction, which
    /// is why the RabbitMQ transport does not need it.
    /// </remarks>
    private static string Sanitize(string value)
    {
        var needsSanitizing = false;

        foreach (var character in value)
        {
            if (char.IsControl(character))
            {
                needsSanitizing = true;
                break;
            }
        }

        if (!needsSanitizing)
        {
            return value;
        }

        var builder = new StringBuilder(value.Length);

        foreach (var character in value)
        {
            builder.Append(char.IsControl(character) ? ' ' : character);
        }

        return builder.ToString();
    }

    private static string? Format(DateTimeOffset? value)
        => value?.ToString("O", CultureInfo.InvariantCulture);

    private static string Format(object value) => value switch
    {
        string text => text,
        DateTimeOffset dateTimeOffset => dateTimeOffset.ToString("O", CultureInfo.InvariantCulture),
        DateTime dateTime => new DateTimeOffset(dateTime).ToString("O", CultureInfo.InvariantCulture),
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? string.Empty
    };
}
