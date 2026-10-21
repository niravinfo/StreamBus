namespace StreamBus;

internal enum DeclaredStreamSource
{
    Endpoint,
    Publish,
    DeadLetter,
}

public sealed record DeclaredStream
{
    public required string Name { get; init; }
    public required bool IsSuper { get; init; }
    public int? Partitions { get; init; }
    public TimeSpan? MaxAge { get; init; }
    public ulong? MaxLengthBytes { get; init; }
    public int? MaxSegmentSizeBytes { get; init; }
    public StreamLeaderLocator? LeaderLocator { get; init; }
    internal DeclaredStreamSource Source { get; init; } = DeclaredStreamSource.Endpoint;
    internal string Detail { get; init; } = "";
    public string Description => Source switch
    {
        DeclaredStreamSource.Endpoint => $"endpoint '{Name}'",
        DeclaredStreamSource.Publish => string.IsNullOrWhiteSpace(Detail)
            ? $"publish registration for '{Name}'"
            : Detail,
        DeclaredStreamSource.DeadLetter => string.IsNullOrWhiteSpace(Detail)
            ? $"error-stream destination '{Name}'"
            : Detail,
        _ => Detail,
    };
}

internal static class DeclareSetBuilder
{
    public static IReadOnlyList<DeclaredStream> Build(
        IEnumerable<ReceiveEndpointDescriptor> endpoints,
        IEnumerable<PublishRegistration> publishes,
        string busName)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentNullException.ThrowIfNull(publishes);
        ArgumentException.ThrowIfNullOrWhiteSpace(busName);
        var result = new List<DeclaredStream>();
        var index = new Dictionary<string, int>(StringComparer.Ordinal);
        var superPublishWithoutPartitions = new Dictionary<string, PublishRegistration>(StringComparer.Ordinal);

        foreach (var endpoint in endpoints)
        {
            Merge(result, index,
                stream: endpoint.Stream,
                wantSuper: endpoint.IsSuperStream,
                partitions: endpoint.SuperPartitions,
                maxAge: endpoint.MaxAge,
                maxLengthBytes: endpoint.MaxLengthBytes,
                maxSegmentSizeBytes: endpoint.MaxSegmentSizeBytes,
                leaderLocator: endpoint.LeaderLocator,
                source: DeclaredStreamSource.Endpoint,
                describe: $"endpoint '{endpoint.Stream}'");
        }

        foreach (var group in publishes.GroupBy(p => p.MessageType))
        {
            var streams = group.Select(p => p.Stream).Distinct(StringComparer.Ordinal).ToList();
            if (streams.Count > 1)
                throw new StreamBusConfigurationException(
                    $"Message {group.Key.FullName} is registered for multiple streams on bus {busName} ('{string.Join("', '", streams)}'). Register each message type on exactly one stream per bus; publish to other streams with the explicit-stream PublishAsync<T>(stream, message, ...) overload.");
        }

        foreach (var publish in publishes)
        {
            var describe = $"publish {publish.MessageType.FullName}";
            if (index.TryGetValue(publish.Stream, out var position) && result[position].IsSuper != publish.IsSuper)
            {
                var knownSuper = result[position].IsSuper;
                throw new StreamBusConfigurationException(
                    $"Message {publish.MessageType.FullName} is registered with {(publish.IsSuper ? "PublishSuper" : "Publish")} for {(knownSuper ? "super" : "plain")} stream '{publish.Stream}'. Use {(knownSuper ? "PublishSuperAsync (routing key required)" : "PublishAsync (no routing key)")}.");
            }
            Merge(result, index,
                stream: publish.Stream,
                wantSuper: publish.IsSuper,
                partitions: publish.Partitions,
                maxAge: publish.MaxAge,
                maxLengthBytes: publish.MaxLengthBytes,
                maxSegmentSizeBytes: publish.MaxSegmentSizeBytes,
                leaderLocator: publish.LeaderLocator,
                source: DeclaredStreamSource.Publish,
                describe: describe);
            if (publish.IsSuper)
            {
                var entry = result[index[publish.Stream]];
                if (entry.Partitions is null)
                    superPublishWithoutPartitions.TryAdd(publish.Stream, publish);
                else
                    superPublishWithoutPartitions.Remove(publish.Stream);
            }
        }

        foreach (var endpoint in endpoints)
        {
            var destination = endpoint.DeadLetter;
            if (destination is null || !destination.IsStream)
                continue;
            Merge(result, index,
                stream: destination.Name,
                wantSuper: false,
                partitions: null,
                maxAge: destination.Spec?.MaxAge,
                maxLengthBytes: destination.Spec?.MaxLengthBytes,
                maxSegmentSizeBytes: destination.Spec?.MaxSegmentSizeBytes,
                leaderLocator: destination.Spec?.LeaderLocator,
                source: DeclaredStreamSource.DeadLetter,
                describe: $"error-stream destination '{destination.Name}' of endpoint '{endpoint.Stream}'");
        }

        foreach (var (stream, pending) in superPublishWithoutPartitions)
        {
            throw new StreamBusConfigurationException(
                $"Message {pending.MessageType.FullName} is registered with PublishSuper for super stream '{stream}' with no partitions. Set Partitions on the PublishSuper registration (s.Partitions(n)) or Partitions on a consuming endpoint.");
        }

        return result;
    }

    private static void Merge(
        List<DeclaredStream> result,
        Dictionary<string, int> index,
        string stream,
        bool wantSuper,
        int? partitions,
        TimeSpan? maxAge,
        ulong? maxLengthBytes,
        int? maxSegmentSizeBytes,
        StreamLeaderLocator? leaderLocator,
        DeclaredStreamSource source,
        string describe)
    {
        if (index.TryGetValue(stream, out var position))
        {
            var existing = result[position];
            if (existing.IsSuper != wantSuper)
            {
                if (existing.Source == DeclaredStreamSource.Endpoint && source == DeclaredStreamSource.Endpoint)
                {
                    throw new StreamConfigurationConflictException(stream,
                        $"endpoint '{stream}' expects {(wantSuper ? "a super stream" : "a plain stream")}",
                        $"another endpoint '{stream}' expects {(existing.IsSuper ? "a super stream" : "a plain stream")}");
                }
                throw new StreamConfigurationConflictException(stream,
                    $"{describe} expects {(wantSuper ? "a super stream" : "a plain stream")}",
                    $"{existing.Description} expects {(existing.IsSuper ? "a super stream" : "a plain stream")}");
            }
            var updated = existing;
            if (existing.Source == DeclaredStreamSource.Endpoint && source == DeclaredStreamSource.Endpoint)
                describe = $"another endpoint '{stream}'";
            if (partitions is { } incomingPartitions)
            {
                if (existing.Partitions is { } knownPartitions && knownPartitions != incomingPartitions)
                    throw new StreamConfigurationConflictException(stream,
                        $"{existing.Description} sets Partitions {knownPartitions}",
                        $"{describe} sets Partitions {incomingPartitions}");
                if (existing.Partitions is null)
                    updated = updated with { Partitions = incomingPartitions };
            }
            if (maxAge is { } incomingMaxAge)
            {
                if (existing.MaxAge is { } knownMaxAge && knownMaxAge != incomingMaxAge)
                    throw new StreamConfigurationConflictException(stream,
                        $"{existing.Description} sets MaxAge {Format(knownMaxAge)}",
                        $"{describe} sets MaxAge {Format(incomingMaxAge)}");
                if (existing.MaxAge is null)
                    updated = updated with { MaxAge = incomingMaxAge };
            }
            if (maxLengthBytes is { } incomingMaxLength)
            {
                if (existing.MaxLengthBytes is { } knownMaxLength && knownMaxLength != incomingMaxLength)
                    throw new StreamConfigurationConflictException(stream,
                        $"{existing.Description} sets MaxLengthBytes {knownMaxLength}",
                        $"{describe} sets MaxLengthBytes {incomingMaxLength}");
                if (existing.MaxLengthBytes is null)
                    updated = updated with { MaxLengthBytes = incomingMaxLength };
            }
            if (maxSegmentSizeBytes is { } incomingMaxSegment)
            {
                if (existing.MaxSegmentSizeBytes is { } knownMaxSegment && knownMaxSegment != incomingMaxSegment)
                    throw new StreamConfigurationConflictException(stream,
                        $"{existing.Description} sets MaxSegmentSizeBytes {knownMaxSegment}",
                        $"{describe} sets MaxSegmentSizeBytes {incomingMaxSegment}");
                if (existing.MaxSegmentSizeBytes is null)
                    updated = updated with { MaxSegmentSizeBytes = incomingMaxSegment };
            }
            if (leaderLocator is { } incomingLocator)
            {
                if (existing.LeaderLocator is { } knownLocator && knownLocator != incomingLocator)
                    throw new StreamConfigurationConflictException(stream,
                        $"{existing.Description} sets LeaderLocator {knownLocator}",
                        $"{describe} sets LeaderLocator {incomingLocator}");
                if (existing.LeaderLocator is null)
                    updated = updated with { LeaderLocator = incomingLocator };
            }
            result[position] = updated;
            return;
        }
        var created = new DeclaredStream
        {
            Name = stream,
            IsSuper = wantSuper,
            Partitions = partitions,
            MaxAge = maxAge,
            MaxLengthBytes = maxLengthBytes,
            MaxSegmentSizeBytes = maxSegmentSizeBytes,
            LeaderLocator = leaderLocator,
            Source = source,
            Detail = describe,
        };
        result.Add(created);
        index.Add(stream, result.Count - 1);
    }

    private static string Format(TimeSpan value) => value.ToString("c");

    public static void ValidateCallTarget(DeclaredStream? known, string stream, bool wantSuper)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        if (known is not null && known.IsSuper != wantSuper)
            throw new StreamBusConfigurationException(
                $"Explicit publish target '{stream}' is a known {(known.IsSuper ? "super" : "plain")} stream. Use {(known.IsSuper ? "PublishSuperAsync (routing key required)" : "PublishAsync (no routing key)")}.");
    }
}
