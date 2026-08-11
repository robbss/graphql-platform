using NATS.Client.JetStream;

namespace Mocha.Transport.Nats;

/// <summary>
/// Binds each durable consumer to the stream that captures its subjects.
/// </summary>
/// <remarks>
/// A JetStream consumer has to be created on the stream capturing its subject, and that stream
/// belongs to the publishing service. RabbitMQ has no equivalent constraint, so this step exists to
/// keep subscribers declaring what they consume rather than where it lives.
/// </remarks>
internal static class NatsStreamResolver
{
    /// <summary>
    /// Resolves and binds the owning stream for every consumer that does not already have one.
    /// </summary>
    /// <param name="jetStream">The JetStream context used to query the server.</param>
    /// <param name="topology">The topology whose consumers are resolved.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public static async ValueTask ResolveAsync(
        INatsJSContext jetStream,
        NatsMessagingTopology topology,
        CancellationToken cancellationToken)
    {
        foreach (var consumer in topology.Consumers)
        {
            if (consumer.StreamName is not null)
            {
                continue;
            }

            var streamName = await ResolveStreamAsync(jetStream, topology, consumer, cancellationToken);

            consumer.BindToStream(streamName);
        }
    }

    /// <summary>
    /// Verifies that every subject the transport publishes to is captured by a stream, binding it
    /// to that stream.
    /// </summary>
    /// <param name="jetStream">The JetStream context used to query the server.</param>
    /// <param name="topology">The topology whose subjects are checked.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <remarks>
    /// Checked at start-up because a JetStream publish to an uncaptured subject does not fail
    /// immediately: it waits for an acknowledgement that never arrives and surfaces as a timeout,
    /// which is considerably harder to diagnose than a boot failure naming the subject.
    /// </remarks>
    public static async ValueTask VerifySubjectsAsync(
        INatsJSContext jetStream,
        NatsMessagingTopology topology,
        CancellationToken cancellationToken)
    {
        foreach (var subject in topology.Subjects)
        {
            if (subject.IsCore || subject.StreamName is not null)
            {
                continue;
            }

            if (topology.FindStreamForSubject(subject.Subject) is { } localStream)
            {
                subject.BindToStream(localStream.Name);
                continue;
            }

            var matches = await ListStreamsAsync(jetStream, subject.Subject, cancellationToken);

            if (matches.Count == 0)
            {
                throw new InvalidOperationException(
                    $"No stream captures subject '{subject.Subject}', which this service publishes "
                    + "to. Add it to a stream's subjects, or declare the stream that should capture "
                    + "it. Publishing to an uncaptured subject does not fail immediately, it times "
                    + "out waiting for an acknowledgement.");
            }

            subject.BindToStream(matches[0]);
        }
    }

    private static async ValueTask<string> ResolveStreamAsync(
        INatsJSContext jetStream,
        NatsMessagingTopology topology,
        NatsConsumer consumer,
        CancellationToken cancellationToken)
    {
        if (consumer.FilterSubjects.Length == 0)
        {
            throw new InvalidOperationException(
                $"Consumer '{consumer.Name}' has no subjects, so its stream cannot be resolved. "
                + "Declare the stream explicitly with FromStream.");
        }

        string? resolved = null;

        foreach (var subject in consumer.FilterSubjects)
        {
            var streamName =
                topology.FindStreamForSubject(subject)?.Name
                ?? await QueryStreamAsync(jetStream, consumer, subject, cancellationToken);

            if (resolved is null)
            {
                resolved = streamName;
                continue;
            }

            if (!string.Equals(resolved, streamName, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"Consumer '{consumer.Name}' subscribes to subjects captured by different "
                    + $"streams ('{resolved}' and '{streamName}'). A consumer can only read from a "
                    + "single stream, so split the handler or declare the stream with FromStream.");
            }
        }

        return resolved!;
    }

    private static async ValueTask<List<string>> ListStreamsAsync(
        INatsJSContext jetStream,
        string subject,
        CancellationToken cancellationToken)
    {
        var matches = new List<string>();

        await foreach (var name in jetStream.ListStreamNamesAsync(subject, cancellationToken))
        {
            matches.Add(name);
        }

        return matches;
    }

    private static async ValueTask<string> QueryStreamAsync(
        INatsJSContext jetStream,
        NatsConsumer consumer,
        string subject,
        CancellationToken cancellationToken)
    {
        var matches = await ListStreamsAsync(jetStream, subject, cancellationToken);

        if (matches.Count == 1)
        {
            return matches[0];
        }

        if (matches.Count == 0)
        {
            throw new InvalidOperationException(
                $"No stream captures subject '{subject}', required by consumer '{consumer.Name}'. "
                + "The publishing service may not have been deployed yet, or its stream was never "
                + "provisioned. Declare the stream with FromStream so this service provisions it "
                + "regardless of start-up order.");
        }

        throw new InvalidOperationException(
            $"Subject '{subject}', required by consumer '{consumer.Name}', is captured by "
            + $"{matches.Count} streams ({string.Join(", ", matches)}). Declare the intended stream "
            + "with FromStream.");
    }
}
