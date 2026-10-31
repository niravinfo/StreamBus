namespace StreamBus;

/// <summary>
/// Common base for every StreamBus exception, so callers can catch the whole
/// library surface with one type. Retryability remains type-specific.
/// </summary>
public abstract class StreamBusException(string message, Exception? inner = null)
    : System.Exception(message, inner);

// Internal transient signal used where the public surface already maps failures
// to StreamBusPublishTransientException (producer registry) or treats every
// non-cancellation attach exception as retryable (partition attach loop).
internal sealed class StreamBusTransientException : StreamBusException
{
    public StreamBusTransientException(string message, Exception? inner = null)
        : base(message, inner) { }
}

// Internal contract violation in an effectively-once filter pipeline. It is
// rethrown past the EO in-handler retry catch so a programming error is not
// misclassified as a transient handler failure.
internal sealed class StreamBusFilterContractException : StreamBusException
{
    public StreamBusFilterContractException(string message)
        : base(message) { }
}

public sealed class StreamBusFatalException : StreamBusException
{
    public StreamBusFatalException(string message, Exception? inner = null)
        : base(message, inner) { }
}

public sealed class StreamBusTimeoutException : StreamBusException
{
    public StreamBusTimeoutException(string message)
        : base(message) { }
}

public sealed class StreamBusConfigurationException : StreamBusException
{
    public StreamBusConfigurationException(string message)
        : base(message) { }
}

public sealed class StreamConfigurationConflictException : StreamBusException
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
public sealed class StreamBusStartupBlockedException : StreamBusException
{
    public StreamBusStartupBlockedException(string message, Exception? inner = null)
        : base(message, inner) { }
}
