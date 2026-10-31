using System.Text;

namespace StreamBus;

public sealed record EffectivelyOnceOptions
{
    public bool SourceIsSuperStream { get; init; }
    public bool TargetIsSuperStream { get; init; }
    public string? Reference { get; init; }
    public Func<ConsumeContext, string?>? TargetRouting { get; init; }

    /// <summary>Unbounded retry policy used while the result is confirmed.</summary>
    public RetryPolicy Retry { get; init; } = new();

    public ReadinessPolicy ReadinessPolicy { get; init; } = ReadinessPolicy.FailFastPolicy;
}

public sealed record PublishResultOptions
{
    public required string ProducerName { get; init; }
    public bool IsSuperStream { get; init; }
    public string? RoutingKey { get; init; }

    /// <summary>Unbounded retry policy used until the result publish is confirmed.</summary>
    public RetryPolicy Retry { get; init; } = new();
}

internal sealed record ResultWork
{
    public required string TargetStream { get; init; }
    public required bool TargetSuper { get; init; }
    public required ReadOnlyMemory<byte> Body { get; init; }
    public required ulong SourceOffset { get; init; }
    public required string? RoutingKey { get; init; }
    public required string ProducerName { get; init; }
    // Assigned once per logical result so retries of the same publish keep
    // the same AMQP identity instead of minting a new one per attempt.
    public required Guid MessageId { get; init; }
    public required DateTime SentTime { get; init; }
}

internal sealed record EffectivelyOnceConfig
{
    public required string TargetStream { get; init; }
    public required bool TargetIsSuperStream { get; init; }
    public required Func<ConsumeContext, string?> TargetRouting { get; init; }
    public required Func<ConsumeContext, Task<ReadOnlyMemory<byte>>> ResultHandler { get; init; }
    // Resume source of truth, injected by the registry (a func, not the Task-2
    // registry type, so this file compiles standalone in Task 1).
    // Args: (targetStream, targetSuper, producerName, ct) -> last publishing id, or null.
    public required Func<string, bool, string, CancellationToken, Task<ulong?>> ResumeQuery { get; init; }
}

internal static class ResultNaming
{
    public static string ProducerName(string sourcePartitionStream, string targetStream)
        => StreamBusNaming.EffectivelyOnceProducerName(sourcePartitionStream, targetStream);

    public static string ConsumerReference(Type busType, string sourceStream, string targetStream)
        => StreamBusNaming.EffectivelyOnceConsumerReference(busType, sourceStream, targetStream);

    public static ulong PublishingId(ulong sourceOffset) => sourceOffset;
}

internal static class ResultResume
{
    // Returns the greatest known last publishing id — the resume BASE, not the
    // resume point (Ruling R4): the shared stored+1 rule in OffsetResolution
    // derives lastId+1 exactly once. Both GetResumeOffsetAsync branches return
    // this base, so a super-target resume can never double-advance past an
    // unprocessed source offset (FR-6.5). Null when nothing is known.
    public static ulong? ResumeOffset(IEnumerable<ulong?> lastIds)
    {
        ArgumentNullException.ThrowIfNull(lastIds);
        ulong? max = null;
        foreach (var id in lastIds)
        {
            if (id is null) continue;
            max = max is null ? id.Value : Math.Max(max.Value, id.Value);
        }
        return max;
    }
}

internal static class EffectivelyOnceValidation
{
    public static void ValidateConsume(RetryPolicy retry, string prefix = "EffectivelyOnceOptions.Retry")
    {
        ArgumentNullException.ThrowIfNull(retry);
        if (retry.MaxAttempts is not null)
            throw new StreamBusConfigurationException($"{prefix}.MaxAttempts must be null for effectively-once (unbounded retry; the publish is retried until confirmed).");
        ValidateKnobs(retry.BaseDelay, retry.MaxDelay, retry.Multiplier, retry.MaxJitter, prefix);
    }

    public static void ValidatePublishResult(PublishResultOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.ProducerName);
        if (Encoding.UTF8.GetByteCount(options.ProducerName) > StreamBusLimits.MaxReferenceBytes)
            throw new StreamBusConfigurationException("PublishResultOptions.ProducerName exceeds the broker's 255-byte limit.");
        if (options.IsSuperStream && string.IsNullOrWhiteSpace(options.RoutingKey))
            throw new StreamBusConfigurationException("PublishResultOptions.RoutingKey is required for a super-stream result target.");
        ValidateConsume(options.Retry, "PublishResultOptions.Retry");
    }

    public static void ValidateKnobs(TimeSpan baseDelay, TimeSpan maxDelay, double multiplier, TimeSpan maxJitter, string prefix)
    {
        if (baseDelay <= TimeSpan.Zero)
            throw new StreamBusConfigurationException($"{prefix}.BaseDelay must be positive.");
        if (maxDelay <= TimeSpan.Zero)
            throw new StreamBusConfigurationException($"{prefix}.MaxDelay must be positive.");
        if (multiplier < 1.0 || !double.IsFinite(multiplier))
            throw new StreamBusConfigurationException($"{prefix}.Multiplier must be a finite number >= 1.");
        if (maxJitter < TimeSpan.Zero)
            throw new StreamBusConfigurationException($"{prefix}.MaxJitter must be non-negative.");
    }
}
