using System.Diagnostics.CodeAnalysis;
using static System.StringSplitOptions;

namespace Mocha.Transport.Nats;

/// <summary>
/// Resolves outbound routes to the subject they publish to.
/// </summary>
/// <remarks>
/// NATS has a single destination kind, so this collapses the exchange and queue distinction the
/// RabbitMQ transport has to make into one subject.
/// </remarks>
internal static class NatsDestinations
{
    /// <summary>
    /// Resolves the subject for an outbound route, honouring an explicit destination when present.
    /// </summary>
    /// <param name="schema">The transport schema.</param>
    /// <param name="naming">The bus naming conventions.</param>
    /// <param name="route">The outbound route.</param>
    /// <returns>The subject to publish to.</returns>
    public static string Resolve(string schema, IBusNamingConventions naming, OutboundRoute route)
    {
        if (route.HasExplicitDestination
            && route.Destination is { } destination
            && TryResolveExplicit(schema, destination, out var subject))
        {
            return subject;
        }

        return ResolveConvention(naming, route.Kind, route.MessageType);
    }

    /// <summary>
    /// Resolves the conventional subject for a message type.
    /// </summary>
    /// <param name="naming">The bus naming conventions.</param>
    /// <param name="kind">The outbound route kind.</param>
    /// <param name="messageType">The message type.</param>
    /// <returns>The subject to publish to.</returns>
    /// <remarks>
    /// Send and Publish converge on the same subject: subscribers select what they receive through
    /// consumer filters, so there is no need for the separate send and publish exchanges the
    /// RabbitMQ transport creates.
    /// </remarks>
    public static string ResolveConvention(
        IBusNamingConventions naming,
        OutboundRouteKind kind,
        MessageType messageType)
        => kind switch
        {
            OutboundRouteKind.Send => naming.GetSendEndpointName(messageType.RuntimeType),
            OutboundRouteKind.Publish => naming.GetPublishEndpointName(messageType.RuntimeType),
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null)
        };

    /// <summary>
    /// Attempts to resolve an explicitly configured destination address to a subject.
    /// </summary>
    /// <param name="schema">The transport schema.</param>
    /// <param name="destination">The destination address.</param>
    /// <param name="subject">The resolved subject, when resolution succeeds.</param>
    /// <returns><see langword="true"/> when the address maps to a subject.</returns>
    public static bool TryResolveExplicit(
        string schema,
        Uri destination,
        [NotNullWhen(true)] out string? subject)
    {
        if (NatsAddress.TryParse(destination, out _, out var kind, out var name)
            && kind == NatsAddress.SubjectSegment)
        {
            subject = name;
            return true;
        }

        var segments = destination.AbsolutePath.Split('/', RemoveEmptyEntries | TrimEntries);

        if (destination.Scheme is "subject" && segments.Length == 1)
        {
            subject = segments[0];
            return true;
        }

        if (destination.Scheme == schema && segments.Length == 2 && segments[0] == NatsAddress.SubjectSegment)
        {
            subject = segments[1];
            return true;
        }

        subject = null;
        return false;
    }
}
