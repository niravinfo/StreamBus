namespace StreamBus;

public static class StreamBusCredentialValidator
{
    public static void Validate(StreamBusOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        foreach (var error in options.ValidateCredentials())
            throw new StreamBusConfigurationException(error);
    }
}
