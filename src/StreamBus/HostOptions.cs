namespace StreamBus;

/// <summary>
/// Host-wide start/stop behavior for StreamBus, mirroring MassTransit's
/// <c>BusOptions</c>. Configured through the options pattern; validated by
/// <see cref="ValidateStart"/> / <see cref="ValidateStop"/> before the hosted
/// service uses the values.
/// </summary>
public sealed class StreamBusHostOptions
{
    /// <summary><see langword="true"/> when host startup blocks until StreamBus connections and (per <c>WaitUntilConsumersAttached</c>) typed consumers are ready; default <see langword="false"/> (startup does not block, background failures are logged critically).</summary>
    public bool WaitUntilStarted { get; set; }

    /// <summary>Overall startup budget when <c>WaitUntilStarted</c> is set; a non-positive value throws <see cref="StreamBusConfigurationException"/>. Default <see langword="null"/> waits indefinitely.</summary>
    public TimeSpan? StartTimeout { get; set; }

    /// <summary>When blocking, also block until every typed consumer is attached (or fail when one fails); default <see langword="true"/>.</summary>
    public bool WaitUntilConsumersAttached { get; set; } = true;

    /// <summary>Overall shutdown budget; a non-positive value throws <see cref="StreamBusConfigurationException"/>. Default <see langword="null"/> waits indefinitely.</summary>
    public TimeSpan? StopTimeout { get; set; }

    /// <summary>Budget for stopping consumers during shutdown; must not exceed <see cref="StopTimeout"/> when both are set, and may be set on its own (default <see langword="null"/> waits indefinitely).</summary>
    public TimeSpan? ConsumerStopTimeout { get; set; }

    /// <summary>
    /// Validates the start-phase options: <see cref="StartTimeout"/> must be
    /// positive when set. Throws <see cref="StreamBusConfigurationException"/>
    /// otherwise.
    /// </summary>
    /// <param name="o">The options to validate; throws <see cref="ArgumentNullException"/> when null.</param>
    internal static void ValidateStart(StreamBusHostOptions o)
    {
        ArgumentNullException.ThrowIfNull(o);
        if (o.StartTimeout is { } start && start <= TimeSpan.Zero)
            throw new StreamBusConfigurationException("StartTimeout must be positive when set.");
    }

    /// <summary>
    /// Validates the stop-phase options: <see cref="StopTimeout"/> and
    /// <see cref="ConsumerStopTimeout"/> must be positive when set, and
    /// <see cref="ConsumerStopTimeout"/>, when set, must not exceed
    /// <see cref="StopTimeout"/> (it may be set without it).
    /// Throws <see cref="StreamBusConfigurationException"/> otherwise.
    /// </summary>
    /// <param name="o">The options to validate; throws <see cref="ArgumentNullException"/> when null.</param>
    internal static void ValidateStop(StreamBusHostOptions o)
    {
        ArgumentNullException.ThrowIfNull(o);
        if (o.StopTimeout is { } stop && stop <= TimeSpan.Zero)
            throw new StreamBusConfigurationException("StopTimeout must be positive when set.");
        if (o.ConsumerStopTimeout is { } consumer && consumer <= TimeSpan.Zero)
            throw new StreamBusConfigurationException("ConsumerStopTimeout must be positive when set.");
        if (o.ConsumerStopTimeout is { } consumerBudget && o.StopTimeout is { } limit && consumerBudget > limit)
            throw new StreamBusConfigurationException("ConsumerStopTimeout must not be greater than StopTimeout when both are set.");
    }
}
