using System.Globalization;
using Microsoft.Extensions.Configuration;

namespace StreamBus;

internal sealed class SectionPrimitives
{
    public string Host { get; init; } = "127.0.0.1";
    public string Vhost { get; init; } = "/";
    public string? UserName { get; init; }
    public string? Password { get; init; }
    public int StreamPort { get; init; } = 5552;
    public int AmqpPort { get; init; } = 5672;
    public TimeSpan Heartbeat { get; init; } = TimeSpan.FromSeconds(60);
    public int ProducersPerConnection { get; init; } = 1;
    public int ConsumersPerConnection { get; init; } = 1;
}

internal static class StreamBusConfigurationBinding
{
    public static void Apply(StreamBusConfigurator configurator, IConfigurationSection section)
    {
        ArgumentNullException.ThrowIfNull(configurator);
        ArgumentNullException.ThrowIfNull(section);
        var errors = new List<string>();
        var path = section.Path;

        var primitives = new SectionPrimitives
        {
            Host = GetText(section, "Host", defaultValue: "127.0.0.1", errors) ?? "127.0.0.1",
            Vhost = GetText(section, "Vhost", defaultValue: "/", errors) ?? "/",
            UserName = GetText(section, "UserName", defaultValue: null, errors),
            Password = GetText(section, "Password", defaultValue: null, errors),
        };
        var streamPort = GetInt(section, "StreamPort", defaultValue: 5552, min: 1, max: 65535, errors);
        var amqpPort = GetInt(section, "AmqpPort", defaultValue: 5672, min: 1, max: 65535, errors);
        var heartbeatSeconds = GetDouble(section, "HeartbeatSeconds", defaultValue: 60, minExclusive: 0, errors);
        var connection = section.GetSection("Connection");
        var producers = GetInt(connection, "ProducersPerConnection", defaultValue: 1, min: 1, max: byte.MaxValue, errors);
        var consumers = GetInt(connection, "ConsumersPerConnection", defaultValue: 1, min: 1, max: byte.MaxValue, errors);

        var streams = BindStreamDeclarations(section, errors);
        var supers = BindSuperStreamDeclarations(section, errors);

        if (errors.Count > 0)
            throw new StreamBusConfigurationException(string.Join(Environment.NewLine, errors));

        configurator.ApplySectionPrimitives(new SectionPrimitives
        {
            Host = primitives.Host,
            Vhost = primitives.Vhost,
            UserName = primitives.UserName,
            Password = primitives.Password,
            StreamPort = streamPort,
            AmqpPort = amqpPort,
            Heartbeat = TimeSpan.FromSeconds(heartbeatSeconds),
            ProducersPerConnection = producers,
            ConsumersPerConnection = consumers,
        }, path);
        foreach (var stream in streams)
            configurator.DeclareStream(stream.Name, knobs => ApplyKnobs(knobs, stream));
        foreach (var super in supers)
            configurator.DeclareSuperStream(super.Name, super.Partitions, knobs => ApplyKnobs(knobs, super));
    }

    private static void ApplyKnobs(
        StreamDeclarationKnobsBuilder knobs,
        (string Name, TimeSpan? MaxAge, ulong? MaxLengthBytes, int? MaxSegmentSizeBytes, StreamLeaderLocator? Locator) parsed)
    {
        if (parsed.MaxAge is { } age) knobs.MaxAge(age);
        if (parsed.MaxLengthBytes is { } length) knobs.MaxLengthBytes(length);
        if (parsed.MaxSegmentSizeBytes is { } segment) knobs.MaxSegmentSizeBytes(segment);
        if (parsed.Locator is { } locator) knobs.LeaderLocator(locator);
    }

    private static void ApplyKnobs(
        StreamDeclarationKnobsBuilder knobs,
        (string Name, int Partitions, TimeSpan? MaxAge, ulong? MaxLengthBytes, int? MaxSegmentSizeBytes, StreamLeaderLocator? Locator) parsed)
        => ApplyKnobs(knobs, (parsed.Name, parsed.MaxAge, parsed.MaxLengthBytes, parsed.MaxSegmentSizeBytes, parsed.Locator));

    private static List<(string Name, TimeSpan? MaxAge, ulong? MaxLengthBytes, int? MaxSegmentSizeBytes, StreamLeaderLocator? Locator)> BindStreamDeclarations(
        IConfigurationSection section, List<string> errors)
        => BindDeclarations(section, "Streams", requirePartitions: false, errors).ConvertAll(e => (e.Name, e.MaxAge, e.MaxLengthBytes, e.MaxSegmentSizeBytes, e.Locator));

    private static List<(string Name, int Partitions, TimeSpan? MaxAge, ulong? MaxLengthBytes, int? MaxSegmentSizeBytes, StreamLeaderLocator? Locator)> BindSuperStreamDeclarations(
        IConfigurationSection section, List<string> errors)
    {
        var parsed = BindDeclarations(section, "SuperStreams", requirePartitions: true, errors);
        return parsed.ConvertAll(e => (e.Name, e.Partitions ?? 0, e.MaxAge, e.MaxLengthBytes, e.MaxSegmentSizeBytes, e.Locator));
    }

    private sealed record ParsedDeclaration(
        string Name,
        int? Partitions,
        TimeSpan? MaxAge,
        ulong? MaxLengthBytes,
        int? MaxSegmentSizeBytes,
        StreamLeaderLocator? Locator);

    private static List<ParsedDeclaration> BindDeclarations(
        IConfigurationSection section, string collection, bool requirePartitions, List<string> errors)
    {
        var result = new List<ParsedDeclaration>();
        var children = section.GetSection("Topology").GetSection(collection).GetChildren().ToList();
        for (var index = 0; index < children.Count; index++)
        {
            var child = children[index];
            var where = $"{section.Path}:Topology:{collection}:{index}";
            var entryErrors = 0;
            void Fail(string message) { errors.Add($"{where}: {message}"); entryErrors++; }
            var name = child.GetSection("Name").Value;
            if (string.IsNullOrWhiteSpace(name))
            {
                Fail("Name is required and must not be empty.");
                continue;
            }
            int? partitions = null;
            if (requirePartitions)
            {
                var raw = child.GetSection("Partitions").Value;
                if (raw is null)
                {
                    Fail("Partitions is required.");
                }
                else if (!int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) || parsed < 1)
                {
                    Fail($"Partitions '{raw}' must be an integer >= 1.");
                }
                else
                {
                    partitions = parsed;
                }
            }
            var maxAge = GetChildDouble(child, "MaxAgeDays", where, minExclusive: 0, errors, ref entryErrors, TimeSpan.FromDays);
            var maxLength = GetChildULong(child, "MaxLengthBytes", where, min: 1, errors, ref entryErrors);
            var maxSegment = GetChildInt(child, "MaxSegmentSizeBytes", where, min: 1, errors, ref entryErrors);
            StreamLeaderLocator? locator = null;
            var locatorRaw = child.GetSection("LeaderLocator").Value;
            if (locatorRaw is not null)
            {
                if (string.IsNullOrWhiteSpace(locatorRaw) || !Enum.TryParse<StreamLeaderLocator>(locatorRaw, ignoreCase: true, out var parsed) || !Enum.IsDefined(parsed))
                    Fail($"LeaderLocator '{locatorRaw}' is unknown. Expected one of: LeastLeaders, ClientLocal, Random.");
                else
                    locator = parsed;
            }
            if (entryErrors > 0) continue;
            result.Add(new ParsedDeclaration(name, partitions, maxAge, maxLength, maxSegment, locator));
        }
        return result;
    }

    private static TimeSpan? GetChildDouble(IConfigurationSection child, string key, string where, double minExclusive, List<string> errors, ref int entryErrors, Func<double, TimeSpan> convert)
    {
        var raw = child.GetSection(key).Value;
        if (raw is null) return null;
        if (!double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var value) || value <= minExclusive)
        {
            errors.Add($"{where}: {key} '{raw}' must be a number > {minExclusive.ToString(CultureInfo.InvariantCulture)}.");
            entryErrors++;
            return null;
        }
        return convert(value);
    }

    private static ulong? GetChildULong(IConfigurationSection child, string key, string where, ulong min, List<string> errors, ref int entryErrors)
    {
        var raw = child.GetSection(key).Value;
        if (raw is null) return null;
        if (!ulong.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value) || value < min)
        {
            errors.Add($"{where}: {key} '{raw}' must be an integer >= {min.ToString(CultureInfo.InvariantCulture)}.");
            entryErrors++;
            return null;
        }
        return value;
    }

    private static int? GetChildInt(IConfigurationSection child, string key, string where, int min, List<string> errors, ref int entryErrors)
    {
        var raw = child.GetSection(key).Value;
        if (raw is null) return null;
        if (!int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value) || value < min)
        {
            errors.Add($"{where}: {key} '{raw}' must be an integer >= {min.ToString(CultureInfo.InvariantCulture)}.");
            entryErrors++;
            return null;
        }
        return value;
    }

    private static string? GetText(IConfigurationSection section, string key, string? defaultValue, List<string> errors)
    {
        var raw = section.GetSection(key).Value;
        if (raw is null) return defaultValue;
        if (string.IsNullOrWhiteSpace(raw))
        {
            errors.Add($"{section.Path}:{key} must not be empty.");
            return defaultValue;
        }
        return raw;
    }

    private static int GetInt(IConfigurationSection section, string key, int defaultValue, int min, int max, List<string> errors)
    {
        var raw = section.GetSection(key).Value;
        if (raw is null) return defaultValue;
        if (!int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value) || value < min || value > max)
        {
            errors.Add($"{section.Path}:{key} '{raw}' must be an integer in {min}..{max}.");
            return defaultValue;
        }
        return value;
    }

    private static double GetDouble(IConfigurationSection section, string key, double defaultValue, double minExclusive, List<string> errors)
    {
        var raw = section.GetSection(key).Value;
        if (raw is null) return defaultValue;
        if (!double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var value) || value <= minExclusive)
        {
            errors.Add($"{section.Path}:{key} '{raw}' must be a number > {minExclusive.ToString(CultureInfo.InvariantCulture)}.");
            return defaultValue;
        }
        return value;
    }
}
