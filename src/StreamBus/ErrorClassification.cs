using RabbitMQ.Stream.Client;

namespace StreamBus;

/// <summary>
/// Classifies exceptions that cannot recover by retrying, so retry loops and
/// hand-off paths surface them immediately instead of retrying forever.
/// </summary>
public static class StreamBusErrorClassification
{
    /// <summary>
    /// Determines whether the exception is fatal: a protocol error, an SSL
    /// error, an unsupported authentication mechanism, or an authentication
    /// failure (including one nested in an inner chain).
    /// </summary>
    /// <param name="ex">The exception to classify.</param>
    /// <returns><see langword="true"/> when retrying cannot recover the failure.</returns>
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
