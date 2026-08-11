using System.Collections.Immutable;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Mocha.Transport.Nats;

/// <summary>
/// Represents a JetStream stream, the coarse container that captures a service's subjects.
/// </summary>
public sealed class NatsStream : TopologyResource<NatsStreamConfiguration>, INatsResource
{
    /// <summary>
    /// Gets the name of this stream as declared in JetStream.
    /// </summary>
    public string Name { get; private set; } = null!;

    /// <summary>
    /// Gets the subjects this stream captures.
    /// </summary>
    public ImmutableArray<string> Subjects { get; private set; } = [];

    /// <inheritdoc />
    public bool? AutoProvision { get; private set; }

    /// <summary>
    /// Gets the deduplication window applied to the <c>Nats-Msg-Id</c> header.
    /// </summary>
    public TimeSpan DuplicateWindow { get; private set; }

    /// <summary>
    /// Gets a value indicating whether per-message TTL headers are honoured.
    /// </summary>
    public bool AllowMsgTtl { get; private set; }

    /// <summary>
    /// Gets a value indicating whether message scheduling is enabled.
    /// </summary>
    public bool AllowMsgSchedules { get; private set; }

    private StreamConfig _config = null!;

    /// <inheritdoc />
    protected override void OnInitialize(NatsStreamConfiguration configuration)
    {
        Name = configuration.Name ?? throw new InvalidOperationException("Stream name is required.");

        if (!NatsNaming.IsValidName(Name))
        {
            throw new InvalidOperationException(
                $"'{Name}' is not a valid JetStream stream name. Stream names cannot contain "
                + "'.', '*', '>', whitespace or path separators.");
        }

        Subjects = [.. configuration.Subjects ?? []];
        AutoProvision = configuration.AutoProvision;
        DuplicateWindow = configuration.DuplicateWindow ?? TimeSpan.Zero;
        AllowMsgTtl = configuration.AllowMsgTtl ?? false;
        AllowMsgSchedules = configuration.AllowMsgSchedules ?? false;

        _config = new StreamConfig
        {
            Name = Name,
            Subjects = [.. Subjects],
            Retention = configuration.Retention ?? StreamConfigRetention.Limits,
            Storage = configuration.Storage ?? StreamConfigStorage.File,
            DuplicateWindow = DuplicateWindow,
            AllowMsgTTL = AllowMsgTtl,
            AllowMsgSchedules = AllowMsgSchedules
        };

        if (configuration.MaxAge is { } maxAge)
        {
            _config.MaxAge = maxAge;
        }

        if (configuration.MaxMsgs is { } maxMsgs)
        {
            _config.MaxMsgs = maxMsgs;
        }

        if (configuration.MaxBytes is { } maxBytes)
        {
            _config.MaxBytes = maxBytes;
        }

        if (configuration.NumReplicas is { } numReplicas)
        {
            _config.NumReplicas = numReplicas;
        }
    }

    /// <inheritdoc />
    protected override void OnComplete(NatsStreamConfiguration configuration)
    {
        Address = NatsAddress.ForStream(Topology.Address, Name);
    }

    /// <inheritdoc />
    public async ValueTask ProvisionAsync(INatsJSContext context, CancellationToken cancellationToken)
    {
        await context.CreateOrUpdateStreamAsync(_config, cancellationToken);
    }
}
