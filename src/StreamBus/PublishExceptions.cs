namespace StreamBus;

/// <summary>
/// A publish was rejected, returned, or otherwise failed without a
/// transient classification. Do not blindly retry.
/// </summary>
public sealed class StreamBusPublishException : StreamBusException
{
    /// <summary>
    /// Initializes the exception with a message and optional inner exception.
    /// </summary>
    /// <param name="message">Description of the publish failure.</param>
    /// <param name="inner">The exception that caused the failure, if any.</param>
    public StreamBusPublishException(string message, Exception? inner = null)
        : base(message, inner) { }
}

/// <summary>
/// A publish failed transiently (e.g. the broker was unreachable) and the
/// producer was discarded. Retry the publish.
/// </summary>
public sealed class StreamBusPublishTransientException : StreamBusException
{
    /// <summary>
    /// Initializes the exception with a message and optional inner exception.
    /// </summary>
    /// <param name="message">Description of the transient failure.</param>
    /// <param name="inner">The exception that caused the failure, if any.</param>
    public StreamBusPublishTransientException(string message, Exception? inner = null)
        : base(message, inner) { }
}

/// <summary>
/// A referenced publish stream does not exist on the broker; publishing does
/// not auto-create streams. No runtime retry; a blocking startup attach may
/// retry while declarations converge.
/// </summary>
public sealed class StreamBusStreamNotFoundException : StreamBusException
{
    /// <summary>Name of the missing stream.</summary>
    public string Stream { get; }

    /// <summary>
    /// Initializes the exception for the given stream name.
    /// </summary>
    /// <param name="stream">Name of the stream that does not exist.</param>
    /// <param name="inner">The exception that caused the lookup failure, if any.</param>
    public StreamBusStreamNotFoundException(string stream, Exception? inner = null)
        : base($"Stream '{stream}' does not exist (publishing does not auto-create streams).", inner)
    {
        Stream = stream;
    }
}

/// <summary>
/// The bus is not ready for the operation yet. This is transient: retry once
/// the bus reaches the ready state.
/// </summary>
public sealed class StreamBusNotReadyException : StreamBusException
{
    /// <summary>
    /// Initializes the exception with a message.
    /// </summary>
    /// <param name="message">Description of the not-ready condition.</param>
    public StreamBusNotReadyException(string message)
        : base(message) { }
}
