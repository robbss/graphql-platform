using Microsoft.Extensions.DependencyInjection;

namespace Mocha.Transport.Nats.Tests.Helpers;

internal static class MessageBusHostBuilderTestExtensions
{
    public static MessagingRuntime BuildRuntime(this IMessageBusHostBuilder builder)
    {
        var provider = builder.Services.BuildServiceProvider();
        return (MessagingRuntime)provider.GetRequiredService<IMessagingRuntime>();
    }
}
