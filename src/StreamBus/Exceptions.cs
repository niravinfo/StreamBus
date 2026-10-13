namespace StreamBus;

public sealed class StreamBusFatalException : Exception
{
    public StreamBusFatalException(string message, Exception? inner = null)
        : base(message, inner) { }
}

public sealed class StreamBusTimeoutException : Exception
{
    public StreamBusTimeoutException(string message)
        : base(message) { }
}

public sealed class StreamBusConfigurationException : Exception
{
    public StreamBusConfigurationException(string message)
        : base(message) { }
}
