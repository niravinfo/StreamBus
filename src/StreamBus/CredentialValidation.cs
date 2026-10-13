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
        if (options.UserName == "guest")
            throw new StreamBusConfigurationException(
                "User 'guest' cannot be used by the stream client except over loopback: the client follows the broker's metadata to a non-loopback address and the broker rejects guest with AuthenticationFailureLoopback. Create a dedicated non-guest user and reconfigure UserName/Password.");
    }
}
