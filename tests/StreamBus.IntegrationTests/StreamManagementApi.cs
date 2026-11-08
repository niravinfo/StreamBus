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

    private readonly HttpClient _http;

    private StreamManagementApi(HttpClient http, string user)
    {
        _http = http;
        User = user;
    }

    public string User { get; }

    public static async Task<StreamManagementApi> ConnectAsync(CancellationToken ct)
    {
        var handle = await TestBroker.StartAsync(ct);
        var http = new HttpClient
        {
            BaseAddress = new Uri($"http://{handle.Host}:{handle.ManagementPort}/"),
            Timeout = TimeSpan.FromSeconds(10),
        };
        http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(
            "Basic",
            Convert.ToBase64String(Encoding.UTF8.GetBytes($"{handle.Username}:{handle.Password}")));

        // The management plugin may still be warming up after the broker starts;
        // poll until it answers, but fail fast as a fixture error instead of
        // hanging until the CI job timeout if the port never comes up.
        using var pollCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        pollCts.CancelAfter(TimeSpan.FromSeconds(60));
        try
        {
            while (true)
            {
                try
                {
                    using var response = await http.GetAsync("api/whoami", pollCts.Token);
                    if (response.IsSuccessStatusCode)
                        return new StreamManagementApi(http, handle.Username);
                    if (response.StatusCode is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden)
                    {
                        http.Dispose();
                        throw new InvalidOperationException(
                            $"RabbitMQ management API rejected user '{handle.Username}' with HTTP {(int)response.StatusCode}.");
                    }
                }
                catch (Exception ex) when ((ex is HttpRequestException or TaskCanceledException)
                    && !pollCts.Token.IsCancellationRequested)
                {
                    // The management API may still be warming up; keep polling.
                }

                await Task.Delay(500, pollCts.Token);
            }
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            http.Dispose();
            throw new InvalidOperationException(
                $"RabbitMQ management API did not answer within 60s on {handle.Host}:{handle.ManagementPort}.", new TimeoutException());
        }
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
