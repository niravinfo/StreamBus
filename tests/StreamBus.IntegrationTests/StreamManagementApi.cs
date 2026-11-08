using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace StreamBus.IntegrationTests;

internal sealed record StreamConnectionInfo(
    string Name,
    string Product,
    string UserProvidedName,
    string State);

internal sealed record StreamConsumerInfo(
    string Queue,
    string Reference,
    bool Active,
    string? ConnectionName);

/// <summary>
/// Thin management-API client for the chaos tests. Stream connections are
/// force-closed through HTTP; the management connection itself is not a stream
/// connection, so closing every stream connection never kills this client.
/// </summary>
internal sealed class StreamManagementApi : IDisposable
{
    private const string VhostSegment = "%2F";
    private static readonly string[] PrivilegedTags = ["administrator", "management"];

    private readonly HttpClient _http;

    private StreamManagementApi(HttpClient http, string user)
    {
        _http = http;
        User = user;
    }

    public string User { get; }

    public static async Task<StreamManagementApi> ConnectAsync(CancellationToken ct)
    {
        var baseUrl = Environment.GetEnvironmentVariable("STREAMBUS_MANAGEMENT_URL")
            ?? "http://127.0.0.1:15672";
        var failures = new List<string>();
        foreach (var (user, password) in Candidates())
        {
            var http = new HttpClient
            {
                BaseAddress = new Uri(baseUrl.TrimEnd('/') + "/"),
                Timeout = TimeSpan.FromSeconds(10),
            };
            http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(
                "Basic",
                Convert.ToBase64String(Encoding.UTF8.GetBytes($"{user}:{password}")));
            try
            {
                using var response = await http.GetAsync("api/whoami", ct);
                if (!response.IsSuccessStatusCode)
                {
                    failures.Add($"{user}: HTTP {(int)response.StatusCode}");
                    http.Dispose();
                    continue;
                }

                var body = await response.Content.ReadAsStringAsync(ct);
                using var document = JsonDocument.Parse(body);
                var tags = ReadTags(document.RootElement);
                if (tags.Any(tag => PrivilegedTags.Contains(tag, StringComparer.OrdinalIgnoreCase)))
                    return new StreamManagementApi(http, user);

                failures.Add($"{user}: tags [{string.Join(", ", tags)}] are not privileged");
                http.Dispose();
            }
            catch (Exception ex) when ((ex is HttpRequestException or TaskCanceledException)
                && !ct.IsCancellationRequested)
            {
                failures.Add($"{user}: {ex.GetType().Name}");
                http.Dispose();
            }
        }

        var detail = $"Tried {string.Join("; ", failures)}. Set STREAMBUS_MANAGEMENT_USER/PASSWORD (or STREAMBUS_USER/PASSWORD) to a RabbitMQ administrator.";
        if (IsCi())
            throw new InvalidOperationException("The RabbitMQ management API is unavailable. " + detail);
        Assert.Skip("RabbitMQ management API unavailable; chaos tests skipped. " + detail);
        throw new InvalidOperationException("unreachable");
    }

    public async Task<IReadOnlyList<StreamConnectionInfo>> GetStreamConnectionsAsync(CancellationToken ct)
    {
        var items = await GetArrayAsync($"api/stream/connections/{VhostSegment}", ct);
        return items
            .Where(item => string.Equals(GetString(item, "protocol"), "stream", StringComparison.Ordinal))
            .Select(item => new StreamConnectionInfo(
                GetString(item, "name"),
                GetObjectString(item, "client_properties", "product"),
                GetString(item, "user_provided_name"),
                GetString(item, "state")))
            .ToArray();
    }

    public async Task<IReadOnlyList<StreamConsumerInfo>> GetStreamConsumersAsync(CancellationToken ct)
    {
        var items = await GetArrayAsync($"api/stream/consumers/{VhostSegment}", ct);
        return items
            .Select(item => new StreamConsumerInfo(
                GetObjectString(item, "queue", "name"),
                GetObjectString(item, "properties", "name"),
                item.TryGetProperty("active", out var active) && active.ValueKind == JsonValueKind.True,
                GetConnectionName(item)))
            .ToArray();
    }

    /// <summary>Returns false when the connection disappeared before the DELETE.</summary>
    public async Task<bool> CloseStreamConnectionAsync(string connectionName, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionName);
        var url = $"api/stream/connections/{VhostSegment}/{Uri.EscapeDataString(connectionName)}";
        using var response = await _http.DeleteAsync(url, ct);
        if (response.StatusCode == HttpStatusCode.NotFound)
            return false;
        if (!response.IsSuccessStatusCode)
        {
            var body = await response.Content.ReadAsStringAsync(ct);
            throw new InvalidOperationException(
                $"Closing stream connection '{connectionName}' failed with HTTP {(int)response.StatusCode}: {body}");
        }
        return true;
    }

    public async Task<int> CloseAllStreamConnectionsAsync(CancellationToken ct)
    {
        var connections = await GetStreamConnectionsAsync(ct);
        var closed = 0;
        foreach (var connection in connections)
        {
            if (await CloseStreamConnectionAsync(connection.Name, ct))
                closed++;
        }
        return closed;
    }

    public static async Task<T> WaitForAsync<T>(
        Func<Task<T>> probe,
        Func<T, bool> ready,
        TimeSpan timeout,
        string what)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            var latest = await probe();
            if (ready(latest))
                return latest;
            if (DateTime.UtcNow >= deadline)
                Assert.Fail($"Timed out waiting for {what}. Last observation: {latest}");
            await Task.Delay(200, TestContext.Current.CancellationToken);
        }
    }

    public void Dispose() => _http.Dispose();

    private async Task<JsonElement[]> GetArrayAsync(string relativeUrl, CancellationToken ct)
    {
        using var response = await _http.GetAsync(relativeUrl, ct);
        var body = await response.Content.ReadAsStringAsync(ct);
        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException(
                $"GET {relativeUrl} failed with HTTP {(int)response.StatusCode}: {body}");
        }
        using var document = JsonDocument.Parse(body);
        if (document.RootElement.ValueKind != JsonValueKind.Array)
            throw new InvalidOperationException($"GET {relativeUrl} did not return a JSON array.");
        return document.RootElement.EnumerateArray().Select(item => item.Clone()).ToArray();
    }

    private static IEnumerable<(string User, string Password)> Candidates()
    {
        var seen = new HashSet<(string, string)>();
        if (Add(Environment.GetEnvironmentVariable("STREAMBUS_MANAGEMENT_USER"),
                Environment.GetEnvironmentVariable("STREAMBUS_MANAGEMENT_PASSWORD")))
            yield return (Environment.GetEnvironmentVariable("STREAMBUS_MANAGEMENT_USER")!,
                Environment.GetEnvironmentVariable("STREAMBUS_MANAGEMENT_PASSWORD")!);
        if (Add(Environment.GetEnvironmentVariable("STREAMBUS_USER"),
                Environment.GetEnvironmentVariable("STREAMBUS_PASSWORD")))
            yield return (Environment.GetEnvironmentVariable("STREAMBUS_USER")!,
                Environment.GetEnvironmentVariable("STREAMBUS_PASSWORD")!);
        if (Add("app", "password"))
            yield return ("app", "password");
        if (Add("guest", "guest"))
            yield return ("guest", "guest");

        bool Add(string? user, string? password)
        {
            if (string.IsNullOrWhiteSpace(user) || string.IsNullOrWhiteSpace(password))
                return false;
            return seen.Add((user, password));
        }
    }

    private static string[] ReadTags(JsonElement root)
        => root.TryGetProperty("tags", out var tags) && tags.ValueKind == JsonValueKind.Array
            ? tags.EnumerateArray()
                .Where(tag => tag.ValueKind == JsonValueKind.String)
                .Select(tag => tag.GetString() ?? "")
                .ToArray()
            : Array.Empty<string>();

    private static bool IsCi()
    {
        var value = Environment.GetEnvironmentVariable("CI");
        return string.Equals(value, "true", StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "1", StringComparison.Ordinal);
    }

    private static string GetString(JsonElement item, string property)
        => item.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString() ?? ""
            : "";

    private static string GetObjectString(JsonElement item, string objectProperty, string property)
        => item.TryGetProperty(objectProperty, out var nested) && nested.ValueKind == JsonValueKind.Object
            ? GetString(nested, property)
            : "";

    private static string? GetConnectionName(JsonElement item)
    {
        if (!item.TryGetProperty("connection_details", out var details)
            || details.ValueKind != JsonValueKind.Object)
            return null;
        var name = GetString(details, "name");
        return string.IsNullOrWhiteSpace(name) ? null : name;
    }
}
