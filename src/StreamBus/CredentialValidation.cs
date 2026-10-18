namespace StreamBus;

public static class StreamBusCredentialValidator
{
    public static void Validate(StreamBusOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (string.IsNullOrWhiteSpace(options.UserName))
            throw new StreamBusConfigurationException("UserName must be supplied.");
        if (string.IsNullOrWhiteSpace(options.Password))
            throw new StreamBusConfigurationException("Password must be supplied.");
        if (string.IsNullOrWhiteSpace(options.Vhost))
            throw new StreamBusConfigurationException("Vhost must be supplied.");
    }
}
