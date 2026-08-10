using NATS.Client.Core;

namespace Mocha.Transport.Nats;

/// <summary>
/// Provider interface for resolving or creating a NATS connection.
/// </summary>
public interface INatsConnectionProvider
{
    /// <summary>
    /// Gets the NATS connection host URL.
    /// </summary>
    string Host { get; }

    /// <summary>
    /// Gets the NATS connection port.
    /// </summary>
    int Port { get; }

    /// <summary>
    /// Resolves or creates an <see cref="INatsConnection"/> instance.
    /// </summary>
    ValueTask<INatsConnection> GetConnectionAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Default NATS connection provider backed by <see cref="INatsConnection"/> resolved from DI or created via NatsOpts.
/// </summary>
public sealed class DefaultNatsConnectionProvider : INatsConnectionProvider
{
    private readonly INatsConnection? _connection;
    private readonly NatsOpts? _opts;

    public string Host { get; } = "localhost";
    public int Port { get; } = 4222;

    public DefaultNatsConnectionProvider(INatsConnection connection)
    {
        _connection = connection;
        if (!string.IsNullOrEmpty(connection.Opts.Url))
        {
            var uri = new Uri(connection.Opts.Url);
            Host = uri.Host;
            Port = uri.Port > 0 ? uri.Port : 4222;
        }
    }

    public DefaultNatsConnectionProvider(NatsOpts opts)
    {
        _opts = opts;
        if (!string.IsNullOrEmpty(opts.Url))
        {
            var uri = new Uri(opts.Url);
            Host = uri.Host;
            Port = uri.Port > 0 ? uri.Port : 4222;
        }
    }

    public ValueTask<INatsConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
    {
        if (_connection is not null)
        {
            return ValueTask.FromResult(_connection);
        }

        var conn = new NatsConnection(_opts ?? NatsOpts.Default);
        return ValueTask.FromResult<INatsConnection>(conn);
    }
}
