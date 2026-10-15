namespace StreamBus;

public sealed record DeclaredStream
{
    public required string Name { get; init; }
    public required bool IsSuper { get; init; }
    public int? Partitions { get; init; }
    public TimeSpan? MaxAge { get; init; }
    public ulong? MaxLengthBytes { get; init; }
    public int? MaxSegmentSizeBytes { get; init; }
    public StreamLeaderLocator? LeaderLocator { get; init; }
    public required string Source { get; init; }
}

internal static class DeclareSetBuilder
{
    public static IReadOnlyList<DeclaredStream> Build(
        IEnumerable<StreamDeclaration> explicitStreams,
        IEnumerable<SuperStreamDeclaration> explicitSupers,
        IEnumerable<ReceiveEndpointDescriptor> endpoints,
        IEnumerable<PublishRegistration> publishes,
        string busName)
    {
        ArgumentNullException.ThrowIfNull(explicitStreams);
        ArgumentNullException.ThrowIfNull(explicitSupers);
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentNullException.ThrowIfNull(publishes);
        ArgumentException.ThrowIfNullOrWhiteSpace(busName);
        var result = new List<DeclaredStream>();
        var explicitNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (var s in explicitStreams)
        {
            result.Add(new DeclaredStream { Name = s.Name, IsSuper = false, Source = "explicit" });
            explicitNames.Add(s.Name);
        }
        foreach (var s in explicitSupers)
        {
            result.Add(new DeclaredStream
            {
                Name = s.Name,
                IsSuper = true,
                Partitions = s.Partitions,
                MaxAge = s.MaxAge,
                MaxLengthBytes = s.MaxLengthBytes,
                MaxSegmentSizeBytes = s.MaxSegmentSizeBytes,
                LeaderLocator = s.LeaderLocator,
                Source = "explicit",
            });
            explicitNames.Add(s.Name);
        }
        foreach (var endpoint in endpoints)
        {
            if (explicitNames.Contains(endpoint.Stream))
            {
                var explicitIsSuper = explicitSupers.Any(s => s.Name == endpoint.Stream);
                if (explicitIsSuper != endpoint.IsSuperStream)
                    throw new StreamConfigurationConflictException(endpoint.Stream,
                        $"endpoint '{endpoint.Stream}' expects {(endpoint.IsSuperStream ? "a super stream" : "a plain stream")}",
                        $"explicit {(explicitIsSuper ? "super-stream" : "stream")} declaration");
                continue;
            }
            var existing = result.Find(d => d.Name == endpoint.Stream && d.Source == "endpoint");
            if (existing is not null)
            {
                if (existing.IsSuper != endpoint.IsSuperStream)
                    throw new StreamConfigurationConflictException(endpoint.Stream,
                        $"endpoint '{endpoint.Stream}' expects {(endpoint.IsSuperStream ? "a super stream" : "a plain stream")}",
                        $"another endpoint '{endpoint.Stream}' expects {(existing.IsSuper ? "a super stream" : "a plain stream")}");
                if (existing.Partitions is null && endpoint.SuperPartitions is not null)
                {
                    result[result.IndexOf(existing)] = existing with { Partitions = endpoint.SuperPartitions };
                }
                continue;
            }
            result.Add(new DeclaredStream
            {
                Name = endpoint.Stream,
                IsSuper = endpoint.IsSuperStream,
                Partitions = endpoint.SuperPartitions,
                MaxAge = endpoint.MaxAge,
                MaxLengthBytes = endpoint.MaxLengthBytes,
                MaxSegmentSizeBytes = endpoint.MaxSegmentSizeBytes,
                LeaderLocator = endpoint.LeaderLocator,
                Source = "endpoint",
            });
        }
        foreach (var publish in publishes)
        {
            var declaration = result.Find(d => d.Name == publish.Stream);
            if (declaration is not null)
            {
                if (declaration.IsSuper != publish.IsSuper)
                    throw new StreamBusConfigurationException(
                        $"Message {publish.MessageType.FullName} is registered with {(publish.IsSuper ? "PublishSuper" : "Publish")} for {(declaration.IsSuper ? "super" : "plain")} stream '{publish.Stream}'. Use {(declaration.IsSuper ? "PublishSuperAsync (routing key required)" : "PublishAsync (no routing key)")}.");
                continue;
            }
            if (publish.IsSuper)
                throw new StreamBusConfigurationException(
                    $"Message {publish.MessageType.FullName} is registered with PublishSuper for stream '{publish.Stream}' with no super-stream declaration. Declare it with DeclareSuperStream(\"{publish.Stream}\", partitions) or SetSuperStream on the consuming endpoint.");
            result.Add(new DeclaredStream { Name = publish.Stream, IsSuper = false, Source = "publish" });
        }
        return result;
    }

    public static void ValidateCallTarget(DeclaredStream? known, string stream, bool wantSuper)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        if (known is not null && known.IsSuper != wantSuper)
            throw new StreamBusConfigurationException(
                $"Explicit publish target '{stream}' is a known {(known.IsSuper ? "super" : "plain")} stream. Use {(known.IsSuper ? "PublishSuperAsync (routing key required)" : "PublishAsync (no routing key)")}.");
    }
}
