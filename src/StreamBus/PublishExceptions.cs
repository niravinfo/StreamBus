namespace StreamBus;

public sealed class StreamBusPublishException : StreamBusException
{
    public StreamBusPublishException(string message, Exception? inner = null)
        : base(message, inner) { }
}

public sealed class StreamBusPublishTransientException : StreamBusException
{
    public StreamBusPublishTransientException(string message, Exception? inner = null)
        : base(message, inner) { }
}

public sealed class StreamBusStreamNotFoundException : StreamBusException
{
    public string Stream { get; }
    public StreamBusStreamNotFoundException(string stream, Exception? inner = null)
        : base($"Stream '{stream}' does not exist (publishing does not auto-create streams).", inner)
    {
        Stream = stream;
    }
}

public sealed class StreamBusNotReadyException : StreamBusException
{
    public StreamBusNotReadyException(string message)
        : base(message) { }
}
