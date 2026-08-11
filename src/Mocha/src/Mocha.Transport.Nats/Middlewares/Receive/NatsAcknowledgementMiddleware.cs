using Mocha.Features;
using Mocha.Middlewares;
using Mocha.Transport.Nats.Features;
using NATS.Client.JetStream;

namespace Mocha.Transport.Nats.Middlewares;

/// <summary>
/// Settles each JetStream message according to how the receive pipeline finished.
/// </summary>
internal sealed class NatsAcknowledgementMiddleware
{
    private static readonly NatsAcknowledgementMiddleware s_instance = new();

    /// <summary>
    /// Runs the pipeline and acknowledges or negatively acknowledges the message.
    /// </summary>
    /// <param name="context">The receive context.</param>
    /// <param name="next">The next middleware.</param>
    public async ValueTask InvokeAsync(IReceiveContext context, ReceiveDelegate next)
    {
        var feature = context.Features.GetOrSet<NatsReceiveFeature>();

        if (feature.Message is not { } message)
        {
            // Core NATS delivery, as used by reply endpoints: nothing to settle.
            await next(context);
            return;
        }

        var cancellationToken = context.CancellationToken;

        using var progress = AckProgressReporter.Start(message, feature.AckProgressInterval, cancellationToken);

        try
        {
            await next(context);

            await message.AckAsync(cancellationToken: cancellationToken);
        }
        catch
        {
            // Settled without the pipeline token: when a handler fails because the host is shutting
            // down, the message should still be released for redelivery straight away rather than
            // waiting out AckWait.
            await message.NakAsync(cancellationToken: CancellationToken.None);
            throw;
        }
    }

    /// <summary>
    /// Creates the middleware configuration.
    /// </summary>
    /// <returns>The configuration.</returns>
    public static ReceiveMiddlewareConfiguration Create()
        => new(static (_, next) => ctx => s_instance.InvokeAsync(ctx, next), "NatsAcknowledgement");

    private sealed class AckProgressReporter : IDisposable
    {
        private readonly CancellationTokenSource _cancellation;

        private AckProgressReporter(CancellationTokenSource cancellation)
        {
            _cancellation = cancellation;
        }

        public static AckProgressReporter? Start(
            INatsJSMsg<ReadOnlyMemory<byte>> message,
            TimeSpan? interval,
            CancellationToken cancellationToken)
        {
            if (interval is not { } period || period <= TimeSpan.Zero)
            {
                return null;
            }

            var cancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            _ = ReportAsync(message, period, cancellation.Token);

            return new AckProgressReporter(cancellation);
        }

        public void Dispose()
        {
            _cancellation.Cancel();
            _cancellation.Dispose();
        }

        private static async Task ReportAsync(
            INatsJSMsg<ReadOnlyMemory<byte>> message,
            TimeSpan period,
            CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    await Task.Delay(period, cancellationToken);

                    await message.AckProgressAsync(cancellationToken: cancellationToken);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected: the handler finished, so there is no deadline left to extend.
            }
        }
    }
}
