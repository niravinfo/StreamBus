namespace StreamBus;

public sealed record PublishRegistration
{
    public required Type BusMarker { get; init; }
    public required Type MessageType { get; init; }
    public required string Stream { get; init; }
    public required bool IsSuper { get; init; }
    public int? Partitions { get; init; }
    public TimeSpan? MaxAge { get; init; }
    public ulong? MaxLengthBytes { get; init; }
    public int? MaxSegmentSizeBytes { get; init; }
    public StreamLeaderLocator? LeaderLocator { get; init; }
}

public sealed class StreamBusPublishStore
{
    private readonly List<PublishRegistration> _registrations = new();

    public void Add(Type busMarker, Type messageType, string stream, bool isSuper, StreamOptions? spec = null)
    {
        ArgumentNullException.ThrowIfNull(busMarker);
        ArgumentNullException.ThrowIfNull(messageType);
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        StreamOptionsValidation.Validate(spec ?? new StreamOptions(), $"Publish '{stream}'", allowPartitions: isSuper);
        if (_registrations.Any(r => r.BusMarker == busMarker && r.MessageType == messageType && r.Stream == stream))
            throw new StreamBusConfigurationException($"Duplicate publish registration for {messageType.FullName} on stream '{stream}' (marker {busMarker.FullName}).");
        _registrations.Add(new PublishRegistration
        {
            BusMarker = busMarker,
            MessageType = messageType,
            Stream = stream,
            IsSuper = isSuper,
            Partitions = spec?.Partitions,
            MaxAge = spec?.MaxAge,
            MaxLengthBytes = spec?.MaxLengthBytes,
            MaxSegmentSizeBytes = spec?.MaxSegmentSizeBytes,
            LeaderLocator = spec?.LeaderLocator,
        });
    }

    public IReadOnlyList<PublishRegistration> Registrations => _registrations;
}

public sealed class StreamPublishRouter
{
    private readonly StreamBusPublishStore _store;

    public StreamPublishRouter(StreamBusPublishStore store)
    {
        ArgumentNullException.ThrowIfNull(store);
        _store = store;
    }

    public PublishRegistration Resolve(Type busMarker, Type messageType)
    {
        ArgumentNullException.ThrowIfNull(busMarker);
        ArgumentNullException.ThrowIfNull(messageType);
        var matches = _store.Registrations
            .Where(r => r.BusMarker == busMarker && r.MessageType == messageType)
            .ToList();
        if (matches.Count == 0)
            throw new StreamBusConfigurationException($"No publish registration for message {messageType.FullName} on bus {busMarker.FullName}. Register it with cfg.Publish<{messageType.Name}>(stream) or cfg.PublishSuper<{messageType.Name}>(stream).");
        if (matches.Count > 1)
        {
            var streams = string.Join("', '", matches.Select(m => m.Stream));
            throw new StreamBusConfigurationException($"Ambiguous publish registration for message {messageType.FullName} on bus {busMarker.FullName}: registered for streams '{streams}'. Publish with an explicit stream: PublishAsync<T>(stream, message, ...) or PublishSuperAsync<T>(stream, message, key, ...).");
        }
        return matches[0];
    }
}
