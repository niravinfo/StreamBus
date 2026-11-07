namespace StreamBus;

/// <summary>
/// Eager credential validation for <see cref="StreamBusOptions"/>; rethrows
/// the first credential error as a
/// <see cref="StreamBusConfigurationException"/>.
/// </summary>
public static class StreamBusCredentialValidator
{
    /// <summary>
    /// Validates that the bus credentials (<c>UserName</c>, <c>Password</c>,
    /// <c>Vhost</c>) are supplied, throwing on the first error.
    /// </summary>
    /// <param name="options">The bus connection options to validate.</param>
    /// <exception cref="StreamBusConfigurationException">A credential is missing or whitespace.</exception>
    public static void Validate(StreamBusOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        foreach (var error in options.ValidateCredentials())
            throw new StreamBusConfigurationException(error);
    }
}
