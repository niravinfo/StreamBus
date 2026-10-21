namespace StreamBus;

// Internal transient signal used where the public surface already maps failures
// to StreamBusPublishTransientException (producer registry) or treats every
// non-cancellation attach exception as retryable (partition attach loop).
internal sealed class StreamBusTransientException : Exception
{
    public StreamBusTransientException(string message, Exception? inner = null)
        : base(message, inner) { }
}

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

// A startup block that waited on a consumer registration which cannot attach.
// Task 11 of the v2 plan re-bases this onto the shared StreamBusStartupException.
public sealed class StreamBusStartupBlockedException : Exception
{
    public StreamBusStartupBlockedException(string message, Exception? inner = null)
        : base(message, inner) { }
}
