using System.Collections.Concurrent;
using System.Globalization;
using System.Runtime.ExceptionServices;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace StreamBus;

internal sealed class StreamTopologyRegistry : IStreamTopology
{
    private readonly StreamBusConnectionSupervisor _supervisor;
    private readonly string _vhost;
    private readonly ILogger<StreamTopologyRegistry> _logger;
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _gates = new();
    private readonly ConcurrentDictionary<string, string> _declared = new();

    public StreamTopologyRegistry(StreamBusConnectionSupervisor supervisor, string vhost, ILogger<StreamTopologyRegistry>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(supervisor);
        ArgumentNullException.ThrowIfNull(vhost);
        _supervisor = supervisor;
        _vhost = vhost;
        _logger = logger ?? NullLogger<StreamTopologyRegistry>.Instance;
    }

    public string Vhost => _vhost;

    internal sealed record StreamExistence(bool SuperExists, bool PlainExists);

    internal async Task<StreamExistence> ProbeStreamAsync(string stream, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        var handle = await ReadyHandleAsync(stream, ct).ConfigureAwait(false);
        var super = await handle.SuperStreamExistsAsync(stream, ct).ConfigureAwait(false);
        var plain = await handle.StreamExistsAsync(stream, ct).ConfigureAwait(false);
        return new StreamExistence(super, plain);
    }

    public async Task EnsureStreamAsync(StreamDeclaration declaration, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(declaration);
        TopologyValidation.ValidateStream(declaration);
        await EnsureAsync(
            declaration,
            declaration.Name,
            Fingerprint.Plain(declaration),
            static (handle, name, token) => handle.StreamExistsAsync(name, token),
            static (handle, name, token) => handle.SuperStreamExistsAsync(name, token),
            static (handle, spec, token) => handle.CreateStreamAsync(TopologyMapping.ToStreamSpec(spec), token),
            ComparePlainAsync,
            "Stream",
            ct).ConfigureAwait(false);
    }

    public async Task EnsureSuperStreamAsync(SuperStreamDeclaration declaration, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(declaration);
        TopologyValidation.ValidateSuper(declaration);
        await EnsureAsync(
            declaration,
            declaration.Name,
            Fingerprint.Super(declaration),
            static (handle, name, token) => handle.SuperStreamExistsAsync(name, token),
            static (handle, name, token) => handle.StreamExistsAsync(name, token),
            static (handle, spec, token) => handle.CreateSuperStreamAsync(TopologyMapping.ToSuperStreamSpec(spec), token),
            CompareSuperAsync,
            "Super stream",
            ct).ConfigureAwait(false);
    }

    private async Task EnsureAsync<TSpec>(
        TSpec declaration,
        string name,
        string fingerprint,
        Func<IStreamSystemHandle, string, CancellationToken, Task<bool>> firstExists,
        Func<IStreamSystemHandle, string, CancellationToken, Task<bool>> secondExists,
        Func<IStreamSystemHandle, TSpec, CancellationToken, Task> create,
        Func<IStreamSystemHandle, TSpec, Exception?, CancellationToken, Task<Exception?>> compare,
        string label,
        CancellationToken ct)
    {
        var handle = await ReadyHandleAsync(name, ct).ConfigureAwait(false);
        var gate = _gates.GetOrAdd(name, static _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_declared.TryGetValue(name, out var known) && known == fingerprint)
                return;
            // Kind occupancy is decided in the compare path (single path covers
            // pre-existing and raced occupancy identically). The create gate checks
            // BOTH kinds: the broker allows plain-over-super coexistence (Ruling R6),
            // so attempting the create on a super-occupied name would silently enter
            // ambiguous broker state instead of throwing the kind conflict.
            Exception? createError = null;
            if (!await firstExists(handle, name, ct).ConfigureAwait(false)
                && !await secondExists(handle, name, ct).ConfigureAwait(false))
            {
                try
                {
                    await create(handle, declaration, ct).ConfigureAwait(false);
                    _declared[name] = fingerprint;
                    return;
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    createError = ex;
                }
            }
            var terminal = await compare(handle, declaration, createError, ct).ConfigureAwait(false);
            if (terminal is not null) ExceptionDispatchInfo.Capture(terminal).Throw();
            if (createError is not null)
                _logger.LogInformation(createError, "{Label} '{Stream}' was declared concurrently with an identical-enough spec; treating as success.", label, name);
            _declared[name] = fingerprint;
        }
        finally
        {
            gate.Release();
        }
    }

    // Re-probe + compare. Plain-stream argument compatibility is NOT readable via
    // the stream protocol, so the broker's create outcome is the only knob signal:
    // an identical re-declare always succeeds (Tasks 1–2 probes), hence a create
    // failure meeting an existing plain stream means the specs differed. A
    // pre-existing plain stream with no create attempt is success-with-warning
    // (order-dependent residual — keep declarations in sync across processes).
    private async Task<Exception?> ComparePlainAsync(IStreamSystemHandle handle, StreamDeclaration declaration, Exception? createError, CancellationToken ct)
    {
        if (await handle.SuperStreamExistsAsync(declaration.Name, ct).ConfigureAwait(false))
        {
            var partitions = await handle.QueryPartitionAsync(declaration.Name, ct).ConfigureAwait(false);
            return new StreamConfigurationConflictException(declaration.Name,
                TopologyDescribe.Expected(declaration),
                $"super stream with {partitions.Length} partitions");
        }
        if (await handle.StreamExistsAsync(declaration.Name, ct).ConfigureAwait(false))
        {
            if (createError is not null)
                return new StreamConfigurationConflictException(declaration.Name,
                    TopologyDescribe.Expected(declaration),
                    "plain stream with differing arguments (argument detail is not readable via the stream protocol)");
            _logger.LogWarning("Stream '{Stream}' already exists; argument-level compatibility is not readable via the stream protocol — treating as success. Keep declarations in sync across processes.", declaration.Name);
            return null;
        }
        if (createError is not null) return createError;
        return new StreamBusStreamNotFoundException(declaration.Name);
    }

    private async Task<Exception?> CompareSuperAsync(IStreamSystemHandle handle, SuperStreamDeclaration declaration, Exception? createError, CancellationToken ct)
    {
        if (await handle.SuperStreamExistsAsync(declaration.Name, ct).ConfigureAwait(false))
        {
            var partitions = await handle.QueryPartitionAsync(declaration.Name, ct).ConfigureAwait(false);
            if (partitions.Length != declaration.Partitions)
                return new StreamConfigurationConflictException(declaration.Name,
                    TopologyDescribe.Expected(declaration),
                    $"super stream with {partitions.Length} partitions");
            if (await handle.StreamExistsAsync(declaration.Name, ct).ConfigureAwait(false))
                return new StreamConfigurationConflictException(declaration.Name,
                    TopologyDescribe.Expected(declaration),
                    $"super stream with {partitions.Length} partitions coexisting with a plain stream of the same name");
            if (createError is not null)
                return new StreamConfigurationConflictException(declaration.Name,
                    TopologyDescribe.Expected(declaration),
                    $"super stream with {partitions.Length} partitions and differing arguments (argument detail is not readable via the stream protocol)");
            return null;
        }
        if (await handle.StreamExistsAsync(declaration.Name, ct).ConfigureAwait(false))
            return new StreamConfigurationConflictException(declaration.Name,
                TopologyDescribe.Expected(declaration),
                "plain stream");
        if (createError is not null) return createError;
        return new StreamBusStreamNotFoundException(declaration.Name);
    }

    private async Task<IStreamSystemHandle> ReadyHandleAsync(string stream, CancellationToken ct)
    {
        var readiness = await _supervisor.WaitUntilReadyAsync(ReadinessPolicy.FailFastPolicy, ct).ConfigureAwait(false);
        if (readiness == BusReadiness.Fatal)
            throw new StreamBusFatalException($"StreamBus for vhost '{_vhost}' is in Fatal state; topology declaration for '{stream}' refused.");
        if (readiness != BusReadiness.Ready)
            throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready ({readiness}); topology declaration for '{stream}' refused.");
        return _supervisor.CurrentHandle
            ?? throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready; topology declaration for '{stream}' refused.");
    }

    private static class Fingerprint
    {
        // Unit separator (U+001F, escaped explicitly — never a literal): stream
        // names cannot smuggle field boundaries through it.
        private const string Sep = "\u001F";
        public static string Plain(StreamDeclaration d)
            => string.Join(Sep, "plain", d.Name,
                d.MaxAge?.Ticks.ToString(CultureInfo.InvariantCulture) ?? "-",
                d.MaxLengthBytes?.ToString(CultureInfo.InvariantCulture) ?? "-",
                d.MaxSegmentSizeBytes?.ToString(CultureInfo.InvariantCulture) ?? "-",
                d.LeaderLocator.ToString());

        public static string Super(SuperStreamDeclaration d)
            => string.Join(Sep, "super", d.Name,
                d.Partitions.ToString(CultureInfo.InvariantCulture),
                d.MaxAge?.Ticks.ToString(CultureInfo.InvariantCulture) ?? "-",
                d.MaxLengthBytes?.ToString(CultureInfo.InvariantCulture) ?? "-",
                d.MaxSegmentSizeBytes?.ToString(CultureInfo.InvariantCulture) ?? "-",
                d.LeaderLocator.ToString());
    }
}
