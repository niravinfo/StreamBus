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

public sealed class StreamConfigurationConflictException : Exception
{
    public StreamConfigurationConflictException(string stream, string expected, string actual)
        : base($"Stream '{stream}' already exists with a conflicting spec (expected {expected}; actual {actual}). Streams are immutable: delete and recreate the stream, or align the declaration.")
    {
        Stream = stream;
        Expected = expected;
        Actual = actual;
    }

    public string Stream { get; }
    public string Expected { get; }
    public string Actual { get; }
}
