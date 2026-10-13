using RabbitMQ.Stream.Client;

namespace StreamBus;

public static class StreamBusErrorClassification
{
    public static bool IsFatal(Exception ex)
    {
        ArgumentNullException.ThrowIfNull(ex);
        return ex is ProtocolException
            || ex is SslException
            || ex is AuthMechanismNotSupportedException
            || IsAuthenticationFailure(ex);
    }

    private static bool IsAuthenticationFailure(Exception ex)
    {
        for (var current = ex; current is not null; current = current.InnerException)
        {
            if (current.GetType().Name.StartsWith("AuthenticationFailure", StringComparison.Ordinal))
                return true;
        }
        return false;
    }
}
