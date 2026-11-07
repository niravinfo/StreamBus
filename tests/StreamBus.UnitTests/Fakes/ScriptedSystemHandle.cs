using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

/// <summary>
/// Stateless shared producer returned by <see cref="ScriptedSystemHandle"/> when a script
/// does not care about sends: every operation is a silent success.
/// </summary>
public sealed class NoopProducerHandle : IStreamPlainProducerHandle, IStreamSuperProducerHandle
{
    private NoopProducerHandle() { }

    /// <summary>The shared stateless instance.</summary>
    public static readonly NoopProducerHandle Instance = new();

    /// <inheritdoc />
    public Task SendAsync(ReadOnlyMemory<byte> body, PublishOptions options, CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public Task SendEnvelopeAsync(ReadOnlyMemory<byte> body, StreamMessageProperties properties, CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public Task PublishReplayAsync(ReadOnlyMemory<byte> body, ulong publishingId, PublishOptions options, CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>
/// Shared scripted fake for <see cref="IStreamSystemHandle"/>. Every member is virtual,
/// defaults are safe recording no-ops, and results flow from public scriptable properties,
/// so a test fake overrides or configures only what it actually scripts. For fakes whose
/// un-scripted members must throw instead, derive from
/// <see cref="StrictScriptedSystemHandle"/>; for the common consume-only script, derive
/// from <see cref="NeverReadyConsumerSystem"/>.
/// </summary>
public class ScriptedSystemHandle : IStreamSystemHandle
{
    /// <summary>Stream names passed to CreatePlainProducerAsync (and its with-reference default).</summary>
    public List<string> PlainStreams { get; } = new();

    /// <summary>Stream names passed to CreateSuperStreamProducerAsync (and its with-reference default).</summary>
    public List<string> SuperStreams { get; } = new();

    /// <summary>Stream names created through CreateStreamAsync.</summary>
    public List<string> CreatedStreamNames { get; } = new();

    /// <summary>Super-stream names created through CreateSuperStreamAsync.</summary>
    public List<string> CreatedSuperNames { get; } = new();

    /// <summary>Stream names probed through StreamExistsAsync and SuperStreamExistsAsync.</summary>
    public List<string> ExistenceChecks { get; } = new();

    /// <summary>Super-stream names passed to QueryPartitionAsync.</summary>
    public List<string> PartitionQueries { get; } = new();

    /// <summary>Offset queries received by TryQueryOffsetAsync (references are recorded even when null is returned).</summary>
    public List<(string Reference, string Stream)> OffsetQueries { get; } = new();

    /// <summary>Sequence queries received by QuerySequenceAsync.</summary>
    public List<(string Reference, string Stream)> SequenceQueries { get; } = new();

    /// <summary>Offset stores received by StoreSystemOffsetAsync.</summary>
    public List<(string Reference, string Stream, ulong Offset)> StoredOffsets { get; } = new();

    /// <summary>When set, replaces the default plain-producer creation (the default returns <see cref="PlainProducer"/>).</summary>
    public Func<string, CancellationToken, Task<IStreamPlainProducerHandle>>? OnCreatePlainProducer { get; set; }

    /// <summary>When set, replaces the default super-producer creation (the default returns <see cref="SuperProducer"/>).</summary>
    public Func<string, CancellationToken, Task<IStreamSuperProducerHandle>>? OnCreateSuperProducer { get; set; }

    /// <summary>When set, replaces the default StreamExistsAsync result.</summary>
    public Func<string, CancellationToken, Task<bool>>? StreamExistsFunc { get; set; }

    /// <summary>When set, replaces the default SuperStreamExistsAsync result.</summary>
    public Func<string, CancellationToken, Task<bool>>? SuperStreamExistsFunc { get; set; }

    /// <summary>When set, runs after CreateStreamAsync records the spec name (default: no-op).</summary>
    public Func<string, CancellationToken, Task>? OnCreateStream { get; set; }

    /// <summary>When set, runs after CreateSuperStreamAsync records the spec name (default: no-op).</summary>
    public Func<string, CancellationToken, Task>? OnCreateSuper { get; set; }

    /// <summary>Result of TryQueryOffsetAsync (default: none stored).</summary>
    public ulong? OffsetResult { get; set; }

    /// <summary>Result of QuerySequenceAsync.</summary>
    public ulong SequenceResult { get; set; }

    /// <summary>Result of StreamExistsAsync when <see cref="StreamExistsFunc"/> is unset.</summary>
    public Func<string, bool> StreamExistsResult { get; set; } = _ => true;

    /// <summary>Result of SuperStreamExistsAsync when <see cref="SuperStreamExistsFunc"/> is unset.</summary>
    public Func<string, bool> SuperStreamExistsResult { get; set; } = _ => false;

    /// <summary>Result of QueryPartitionAsync (default: the super-stream name itself).</summary>
    public Func<string, string[]> PartitionsResult { get; set; } = superStream => new[] { superStream };

    /// <summary>Handle returned by CreatePlainProducerAsync when <see cref="OnCreatePlainProducer"/> is unset.</summary>
    public IStreamPlainProducerHandle PlainProducer { get; set; } = NoopProducerHandle.Instance;

    /// <summary>Handle returned by CreateSuperStreamProducerAsync when <see cref="OnCreateSuperProducer"/> is unset.</summary>
    public IStreamSuperProducerHandle SuperProducer { get; set; } = NoopProducerHandle.Instance;

    /// <summary>When set, replaces the default raw-consumer creation (the default throws).</summary>
    public Func<RawConsumerConfig, CancellationToken, Task<RawConsumer>>? OnCreateRawConsumer { get; set; }

    /// <summary>Sets <see cref="IsClosed"/>; false until the script says otherwise.</summary>
    public virtual bool IsClosed { get; set; }

    /// <summary>Disposes nothing.</summary>
    public virtual ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    /// <summary>Records the stream and returns <see cref="PlainProducer"/> (or the <see cref="OnCreatePlainProducer"/> script).</summary>
    public virtual Task<IStreamPlainProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken cancellationToken)
    {
        lock (PlainStreams) PlainStreams.Add(stream);
        return OnCreatePlainProducer is { } create
            ? create(stream, cancellationToken)
            : Task.FromResult(PlainProducer);
    }

    /// <summary>Delegates to <see cref="CreatePlainProducerAsync"/> (reference-identity scripts override this).</summary>
    public virtual Task<IStreamPlainProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken cancellationToken)
        => CreatePlainProducerAsync(stream, cancellationToken);

    /// <summary>Records the stream and returns <see cref="SuperProducer"/> (or the <see cref="OnCreateSuperProducer"/> script).</summary>
    public virtual Task<IStreamSuperProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken cancellationToken)
    {
        lock (SuperStreams) SuperStreams.Add(stream);
        return OnCreateSuperProducer is { } create
            ? create(stream, cancellationToken)
            : Task.FromResult(SuperProducer);
    }

    /// <summary>Delegates to <see cref="CreateSuperStreamProducerAsync"/> (reference-identity scripts override this).</summary>
    public virtual Task<IStreamSuperProducerHandle> CreateSuperStreamProducerWithReferenceAsync(string stream, string reference, CancellationToken cancellationToken)
        => CreateSuperStreamProducerAsync(stream, cancellationToken);

    /// <summary>Records the query and returns <see cref="SequenceResult"/>.</summary>
    public virtual Task<ulong> QuerySequenceAsync(string reference, string stream, CancellationToken cancellationToken)
    {
        lock (SequenceQueries) SequenceQueries.Add((reference, stream));
        return Task.FromResult(SequenceResult);
    }

    /// <summary>Throws unless <see cref="OnCreateRawConsumer"/> scripts a consumer (tests inject a consumer factory instead).</summary>
    public virtual Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken cancellationToken)
        => OnCreateRawConsumer is { } create
            ? create(config, cancellationToken)
            : throw new NotSupportedException("ScriptedSystemHandle creates no consumers; inject a consumer factory instead.");

    /// <summary>Records the query and returns <see cref="OffsetResult"/>.</summary>
    public virtual Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken cancellationToken)
    {
        lock (OffsetQueries) OffsetQueries.Add((reference, stream));
        return Task.FromResult(OffsetResult);
    }

    /// <summary>Records the store and completes.</summary>
    public virtual Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken cancellationToken)
    {
        lock (StoredOffsets) StoredOffsets.Add((reference, stream, offset));
        return Task.CompletedTask;
    }

    /// <summary>Records the probe and returns <see cref="StreamExistsFunc"/>'s or <see cref="StreamExistsResult"/>'s answer.</summary>
    public virtual Task<bool> StreamExistsAsync(string stream, CancellationToken cancellationToken)
    {
        lock (ExistenceChecks) ExistenceChecks.Add(stream);
        return StreamExistsFunc is { } exists
            ? exists(stream, cancellationToken)
            : Task.FromResult(StreamExistsResult(stream));
    }

    /// <summary>Records the probe and returns <see cref="SuperStreamExistsFunc"/>'s or <see cref="SuperStreamExistsResult"/>'s answer.</summary>
    public virtual Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken cancellationToken)
    {
        lock (ExistenceChecks) ExistenceChecks.Add(superStream);
        return SuperStreamExistsFunc is { } exists
            ? exists(superStream, cancellationToken)
            : Task.FromResult(SuperStreamExistsResult(superStream));
    }

    /// <summary>Records the query and returns <see cref="PartitionsResult"/>.</summary>
    public virtual Task<string[]> QueryPartitionAsync(string superStream, CancellationToken cancellationToken)
    {
        lock (PartitionQueries) PartitionQueries.Add(superStream);
        return Task.FromResult(PartitionsResult(superStream));
    }

    /// <summary>Records the spec name, then runs <see cref="OnCreateStream"/> or completes.</summary>
    public virtual Task CreateStreamAsync(RabbitMQ.Stream.Client.StreamSpec spec, CancellationToken cancellationToken)
    {
        lock (CreatedStreamNames) CreatedStreamNames.Add(spec.Name);
        return OnCreateStream is { } create ? create(spec.Name, cancellationToken) : Task.CompletedTask;
    }

    /// <summary>Records the spec name, then runs <see cref="OnCreateSuper"/> or completes.</summary>
    public virtual Task CreateSuperStreamAsync(RabbitMQ.Stream.Client.PartitionsSuperStreamSpec spec, CancellationToken cancellationToken)
    {
        lock (CreatedSuperNames) CreatedSuperNames.Add(spec.Name);
        return OnCreateSuper is { } create ? create(spec.Name, cancellationToken) : Task.CompletedTask;
    }
}

/// <summary>
/// Strict variant of <see cref="ScriptedSystemHandle"/> for fakes where every member that is
/// not explicitly scripted must fail loudly: every producer, consumer, offset, topology, and
/// partition member throws <see cref="NotImplementedException"/>. <see cref="IsClosed"/> stays
/// settable and <see cref="DisposeAsync"/> stays a no-op so connection-supervisor scripts work
/// unchanged.
/// </summary>
public class StrictScriptedSystemHandle : ScriptedSystemHandle
{
    /// <summary>Throws: not scripted.</summary>
    public override Task<IStreamPlainProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken cancellationToken)
        => throw new NotImplementedException("StrictScriptedSystemHandle.CreatePlainProducerAsync was not scripted.");

    /// <summary>Throws: not scripted.</summary>
    public override Task<IStreamPlainProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken cancellationToken)
        => throw new NotImplementedException("StrictScriptedSystemHandle.CreatePlainProducerWithReferenceAsync was not scripted.");

    /// <summary>Throws: not scripted.</summary>
    public override Task<IStreamSuperProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken cancellationToken)
        => throw new NotImplementedException("StrictScriptedSystemHandle.CreateSuperStreamProducerAsync was not scripted.");

    /// <summary>Throws: not scripted.</summary>
    public override Task<IStreamSuperProducerHandle> CreateSuperStreamProducerWithReferenceAsync(string stream, string reference, CancellationToken cancellationToken)
        => throw new NotImplementedException("StrictScriptedSystemHandle.CreateSuperStreamProducerWithReferenceAsync was not scripted.");

    /// <summary>Throws: not scripted.</summary>
    public override Task<ulong> QuerySequenceAsync(string reference, string stream, CancellationToken cancellationToken)
        => throw new NotImplementedException("StrictScriptedSystemHandle.QuerySequenceAsync was not scripted.");

    /// <summary>Throws: not scripted.</summary>
    public override Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken cancellationToken)
        => throw new NotImplementedException("StrictScriptedSystemHandle.CreateRawConsumerAsync was not scripted.");

    /// <summary>Throws: not scripted.</summary>
    public override Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken cancellationToken)
        => throw new NotImplementedException("StrictScriptedSystemHandle.TryQueryOffsetAsync was not scripted.");

    /// <summary>Throws: not scripted.</summary>
    public override Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken cancellationToken)
        => throw new NotImplementedException("StrictScriptedSystemHandle.StoreSystemOffsetAsync was not scripted.");

    /// <summary>Throws: not scripted.</summary>
    public override Task<bool> StreamExistsAsync(string stream, CancellationToken cancellationToken)
        => throw new NotImplementedException("StrictScriptedSystemHandle.StreamExistsAsync was not scripted.");

    /// <summary>Throws: not scripted.</summary>
    public override Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken cancellationToken)
        => throw new NotImplementedException("StrictScriptedSystemHandle.SuperStreamExistsAsync was not scripted.");

    /// <summary>Throws: not scripted.</summary>
    public override Task<string[]> QueryPartitionAsync(string superStream, CancellationToken cancellationToken)
        => throw new NotImplementedException("StrictScriptedSystemHandle.QueryPartitionAsync was not scripted.");

    /// <summary>Throws: not scripted.</summary>
    public override Task CreateStreamAsync(RabbitMQ.Stream.Client.StreamSpec spec, CancellationToken cancellationToken)
        => throw new NotImplementedException("StrictScriptedSystemHandle.CreateStreamAsync was not scripted.");

    /// <summary>Throws: not scripted.</summary>
    public override Task CreateSuperStreamAsync(RabbitMQ.Stream.Client.PartitionsSuperStreamSpec spec, CancellationToken cancellationToken)
        => throw new NotImplementedException("StrictScriptedSystemHandle.CreateSuperStreamAsync was not scripted.");
}

/// <summary>
/// The common consume-side script shared by tests that inject their own consumer factory:
/// offset queries return null (recorded in <see cref="ScriptedSystemHandle.OffsetQueries"/>),
/// offset stores are silent no-ops, plain streams exist while super streams do not, partitions
/// mirror the super-stream name, and stream creates are no-ops. Producer creation, raw-consumer
/// creation, and sequence queries throw because those scripts never use them; each member stays
/// virtual so a test fake can override the exact pieces it needs.
/// </summary>
public class NeverReadyConsumerSystem : StrictScriptedSystemHandle
{
    /// <summary>Throws: these tests inject their own consumer factory.</summary>
    public override Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken cancellationToken)
        => throw new NotSupportedException("These tests inject their own consumer factory.");

    /// <summary>Records the query and reports no stored offset.</summary>
    public override Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken cancellationToken)
    {
        lock (OffsetQueries) OffsetQueries.Add((reference, stream));
        return Task.FromResult<ulong?>(null);
    }

    /// <summary>Records the store and completes.</summary>
    public override Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken cancellationToken)
    {
        lock (StoredOffsets) StoredOffsets.Add((reference, stream, offset));
        return Task.CompletedTask;
    }

    /// <summary>Reports that plain streams exist.</summary>
    public override Task<bool> StreamExistsAsync(string stream, CancellationToken cancellationToken) => Task.FromResult(true);

    /// <summary>Reports that super streams do not exist.</summary>
    public override Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken cancellationToken) => Task.FromResult(false);

    /// <summary>Reports the super-stream name as its own single partition.</summary>
    public override Task<string[]> QueryPartitionAsync(string superStream, CancellationToken cancellationToken)
        => Task.FromResult(new[] { superStream });

    /// <summary>Declares plain streams as a silent no-op.</summary>
    public override Task CreateStreamAsync(RabbitMQ.Stream.Client.StreamSpec spec, CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>Declares super streams as a silent no-op.</summary>
    public override Task CreateSuperStreamAsync(RabbitMQ.Stream.Client.PartitionsSuperStreamSpec spec, CancellationToken cancellationToken) => Task.CompletedTask;
}
