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

/// <summary>
/// A publish filter violated the filter contract by calling (or not calling)
/// <c>next()</c> exactly once. It surfaces raw from the publish pipeline so a
/// filter bug is not mistaken for a configuration error or retried as transient.
/// </summary>
public sealed class StreamBusFilterContractException(string message, Exception? inner = null)
    : StreamBusException(message, inner)
{
    /// <summary>
    /// Initializes the exception with a message.
    /// </summary>
    /// <param name="message">Description of the contract violation.</param>
    public StreamBusFilterContractException(string message)
        : this(message, null) { }
}

/// <summary>
/// The bus connection is in a fatal state; retrying cannot recover it.
/// Reconnect or operator action is required.
/// </summary>
public sealed class StreamBusFatalException : StreamBusException
{
    /// <summary>
    /// Initializes the exception with a message and optional inner exception.
    /// </summary>
    /// <param name="message">Description of the fatal condition.</param>
    /// <param name="inner">The exception that caused the failure, if any.</param>
    public StreamBusFatalException(string message, Exception? inner = null)
        : base(message, inner) { }
}

/// <summary>
/// A configured time budget expired; the outcome may be unknown, so only
/// retry when the operation is safe to repeat.
/// </summary>
public sealed class StreamBusTimeoutException : StreamBusException
{
    /// <summary>
    /// Initializes the exception with a message.
    /// </summary>
    /// <param name="message">Description of the expired budget.</param>
    public StreamBusTimeoutException(string message)
        : base(message) { }
}

/// <summary>
/// Invalid or contradictory bus configuration. Fix the configuration;
/// retrying will not help.
/// </summary>
public sealed class StreamBusConfigurationException : StreamBusException
{
    /// <summary>
    /// Initializes the exception with a message.
    /// </summary>
    /// <param name="message">Description of the configuration problem.</param>
    public StreamBusConfigurationException(string message)
        : base(message) { }
}

/// <summary>
/// An existing stream's broker-side specification disagrees with the
/// declaration. Streams are immutable: delete and recreate the stream, or
/// align the declaration.
/// </summary>
public sealed class StreamBusStreamConfigurationConflictException : StreamBusException
{
    /// <summary>
    /// Initializes the exception from the conflicting stream specification.
    /// </summary>
    /// <param name="stream">Name of the stream that already exists with a conflicting spec.</param>
    /// <param name="expected">The specification declared by StreamBus.</param>
    /// <param name="actual">The specification found on the broker.</param>
    public StreamBusStreamConfigurationConflictException(string stream, string expected, string actual)
        : base($"Stream '{stream}' already exists with a conflicting spec (expected {expected}; actual {actual}). Streams are immutable: delete and recreate the stream, or align the declaration.")
    {
        Stream = stream;
        Expected = expected;
        Actual = actual;
    }

    /// <summary>Name of the stream whose specification conflicts.</summary>
    public string Stream { get; }

    /// <summary>The stream specification declared by StreamBus.</summary>
    public string Expected { get; }

    /// <summary>The stream specification found on the broker.</summary>
    public string Actual { get; }
}

/// <summary>
/// A startup block that waited on a consumer registration which cannot attach.
/// Fix the consumer or its dependency; retrying will not help.
/// </summary>
public sealed class StreamBusStartupBlockedException : StreamBusException
{
    /// <summary>
    /// Initializes the exception with a message and optional inner exception.
    /// </summary>
    /// <param name="message">Description of the blocked startup.</param>
    /// <param name="inner">The exception returned by the registration, if any.</param>
    public StreamBusStartupBlockedException(string message, Exception? inner = null)
        : base(message, inner) { }
}
