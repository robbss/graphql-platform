using NATS.Client.JetStream.Models;

namespace Mocha.Transport.Nats;

/// <summary>
/// Configuration options for a NATS JetStream stream topology object.
/// </summary>
public sealed class NatsStreamConfiguration : TopologyConfiguration
{
    /// <summary>
    /// Gets or sets the name of the stream.
    /// </summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the subjects associated with this stream.
    /// </summary>
    public List<string> Subjects { get; set; } = [];

    /// <summary>
    /// Gets or sets the storage type (File or Memory).
    /// </summary>
    public StreamConfigStorage Storage { get; set; } = StreamConfigStorage.File;

    /// <summary>
    /// Gets or sets the retention policy for messages in this stream.
    /// </summary>
    public StreamConfigRetention Retention { get; set; } = StreamConfigRetention.Limits;

    /// <summary>
    /// Gets or sets the maximum number of messages allowed in the stream.
    /// </summary>
    public long MaxMsgs { get; set; } = -1;

    /// <summary>
    /// Gets or sets the maximum age of messages in the stream.
    /// </summary>
    public TimeSpan? MaxAge { get; set; }

    /// <summary>
    /// Gets or sets the maximum message bytes allowed in the stream.
    /// </summary>
    public long MaxBytes { get; set; } = -1;
}
