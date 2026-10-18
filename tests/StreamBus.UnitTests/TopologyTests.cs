using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

public sealed class TopologyTests
{
    [Fact]
    public void Locator_defaults_to_least_leaders()
    {
        Assert.Equal(StreamLeaderLocator.LeastLeaders, new StreamDeclaration { Name = "s" }.LeaderLocator);
        Assert.Equal(StreamLeaderLocator.LeastLeaders, new SuperStreamDeclaration { Name = "s", Partitions = 3 }.LeaderLocator);
        Assert.Equal(LeaderLocator.LeastLeaders, TopologyMapping.Map(StreamLeaderLocator.LeastLeaders));
    }

    [Fact]
    public void Locator_maps_all_values()
    {
        Assert.Equal(LeaderLocator.ClientLocal, TopologyMapping.Map(StreamLeaderLocator.ClientLocal));
        Assert.Equal(LeaderLocator.Random, TopologyMapping.Map(StreamLeaderLocator.Random));
    }

    [Fact]
    public void Default_spec_emits_no_retention_args()
    {
        // Args emission table (Task 1 probe values asserted here — update both if the client changes them):
        var spec = TopologyMapping.ToStreamSpec(new StreamDeclaration { Name = "s" });
        Assert.DoesNotContain("max-age", spec.Args.Keys);
        Assert.DoesNotContain("max-length-bytes", spec.Args.Keys);
        Assert.DoesNotContain("stream-max-segment-size-bytes", spec.Args.Keys);
        var knobbed = TopologyMapping.ToStreamSpec(new StreamDeclaration { Name = "s", MaxAge = TimeSpan.FromDays(7) });
        Assert.True(knobbed.Args.ContainsKey("max-age"));
        Assert.Equal("604800s", knobbed.Args["max-age"]);
    }

    [Fact]
    public void Knobs_pass_through_and_partitions_hold()
    {
        // PartitionsSuperStreamSpec knob properties are set-only, so passthrough
        // is observed through the emitted Args (same table as above).
        var spec = TopologyMapping.ToSuperStreamSpec(new SuperStreamDeclaration { Name = "ss", Partitions = 4, MaxAge = TimeSpan.FromHours(1), MaxLengthBytes = 1024, MaxSegmentSizeBytes = 4096, LeaderLocator = StreamLeaderLocator.Random });
        Assert.Equal("ss", spec.Name);
        Assert.Equal(4, spec.Partitions);
        Assert.Equal("3600s", spec.Args["max-age"]);
        Assert.Equal("1024", spec.Args["max-length-bytes"]);
        Assert.Equal("4096", spec.Args["stream-max-segment-size-bytes"]);
    }

    [Fact]
    public void Validation_rejects_bad_specs()
    {
        Assert.Throws<ArgumentException>(() => TopologyValidation.ValidateStream(new StreamDeclaration { Name = "  " }));
        Assert.Throws<StreamBusConfigurationException>(() => TopologyValidation.ValidateSuper(new SuperStreamDeclaration { Name = "s", Partitions = 0 }));
        Assert.Throws<StreamBusConfigurationException>(() => TopologyValidation.ValidateStream(new StreamDeclaration { Name = "s", MaxAge = TimeSpan.Zero }));
        Assert.Throws<StreamBusConfigurationException>(() => TopologyValidation.ValidateStream(new StreamDeclaration { Name = "s", MaxLengthBytes = 0 }));
        Assert.Throws<StreamBusConfigurationException>(() => TopologyValidation.ValidateStream(new StreamDeclaration { Name = "s", MaxSegmentSizeBytes = 0 }));
    }

    [Fact]
    public void Stream_spec_options_validate_at_registration()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            StreamOptionsValidation.Validate(new StreamOptions { Partitions = 0 }, "test", allowPartitions: true));
        Assert.Contains("Partitions", ex.Message);
        var plain = Assert.Throws<StreamBusConfigurationException>(() =>
            StreamOptionsValidation.Validate(new StreamOptions { Partitions = 3 }, "test", allowPartitions: false));
        Assert.Contains("no partitions", plain.Message);
    }

    private sealed class ScriptSystem : IStreamSystemHandle
    {
        public readonly List<string> CreatedStreams = new();
        public readonly List<string> CreatedSupers = new();
        public int ExistsProbes;
        public Func<string, CancellationToken, Task>? OnCreateStream;
        public Func<string, CancellationToken, Task>? OnCreateSuper;
        public Func<string, bool> StreamExists = _ => false;
        public Func<string, bool> SuperExists = _ => false;
        public Func<string, string[]> Partitions = _ => Array.Empty<string>();
        public bool IsClosed => false;
        public Task CreateStreamAsync(StreamSpec spec, CancellationToken ct)
        {
            lock (CreatedStreams) CreatedStreams.Add(spec.Name);
            if (OnCreateStream is not null) return OnCreateStream(spec.Name, ct);
            return Task.CompletedTask;
        }
        public Task CreateSuperStreamAsync(PartitionsSuperStreamSpec spec, CancellationToken ct)
        {
            lock (CreatedSupers) CreatedSupers.Add(spec.Name);
            if (OnCreateSuper is not null) return OnCreateSuper(spec.Name, ct);
            return Task.CompletedTask;
        }
        public Task<IStreamProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct) => throw new NotImplementedException("Topology tests declare only.");
        public Task<IStreamProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken ct) => throw new NotImplementedException("Topology tests declare only.");
        public Task<IStreamProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct) => throw new NotImplementedException("Topology tests declare only.");
        public Task<IStreamProducerHandle> CreateSuperStreamProducerWithReferenceAsync(string stream, string reference, CancellationToken ct) => throw new NotImplementedException("Topology tests declare only.");
        public Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken ct) => throw new NotImplementedException("Topology tests never attach.");
        public Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken ct) => Task.FromResult<ulong?>(null);
        public Task<ulong> QuerySequenceAsync(string reference, string stream, CancellationToken ct) => Task.FromResult(0UL);
        public Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken ct) => Task.CompletedTask;
        public Task<bool> StreamExistsAsync(string stream, CancellationToken ct) { Interlocked.Increment(ref ExistsProbes); return Task.FromResult(StreamExists(stream)); }
        public Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken ct) { Interlocked.Increment(ref ExistsProbes); return Task.FromResult(SuperExists(superStream)); }
        public Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct) => Task.FromResult(Partitions(superStream));
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class ReadyConnector : IStreamSystemConnector
    {
        private readonly ScriptSystem _handle;
        public ReadyConnector(ScriptSystem handle) => _handle = handle;
        public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct) => Task.FromResult<IStreamSystemHandle>(_handle);
    }

    private static (StreamTopologyRegistry Registry, ScriptSystem System) CreateRegistry()
    {
        var system = new ScriptSystem();
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        var supervisor = new StreamBusConnectionSupervisor(options, new ReadyConnector(system));
        supervisor.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        var readiness = supervisor.WaitUntilReadyAsync(ReadinessPolicy.Indefinitely, new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token).GetAwaiter().GetResult();
        Assert.Equal(BusReadiness.Ready, readiness);
        return (new StreamTopologyRegistry(supervisor, "/"), system);
    }

    [Fact]
    public async Task Repeat_same_spec_makes_no_further_broker_calls()
    {
        var (registry, system) = CreateRegistry();
        var ct = TestContext.Current.CancellationToken;
        var decl = new SuperStreamDeclaration { Name = "ss", Partitions = 3 };
        await registry.EnsureSuperStreamAsync(decl, ct);
        await registry.EnsureSuperStreamAsync(decl, ct);
        Assert.Single(system.CreatedSupers);
        var probesAfterFirst = system.ExistsProbes;
        await registry.EnsureSuperStreamAsync(decl, ct);
        Assert.Equal(probesAfterFirst, system.ExistsProbes);
    }

    [Fact]
    public async Task Four_concurrent_same_spec_issue_single_create()
    {
        var (registry, system) = CreateRegistry();
        var ct = TestContext.Current.CancellationToken;
        var decl = new SuperStreamDeclaration { Name = "ss", Partitions = 3 };
        await Task.WhenAll(Enumerable.Range(0, 4).Select(_ => registry.EnsureSuperStreamAsync(decl, ct)));
        Assert.Single(system.CreatedSupers);
    }

    [Fact]
    public async Task Preexisting_identical_spec_needs_no_create()
    {
        var (registry, system) = CreateRegistry();
        var ct = TestContext.Current.CancellationToken;
        system.SuperExists = _ => true;
        system.Partitions = _ => new[] { "ss-0", "ss-1", "ss-2" };
        await registry.EnsureSuperStreamAsync(new SuperStreamDeclaration { Name = "ss", Partitions = 3 }, ct);
        Assert.Empty(system.CreatedSupers);
    }

    [Fact]
    public async Task Raced_identical_create_succeeds()
    {
        var (registry, system) = CreateRegistry();
        var ct = TestContext.Current.CancellationToken;
        // Pre-checks see absent; the broker accepts the identical create (Tasks 1–2 probes).
        await registry.EnsureSuperStreamAsync(new SuperStreamDeclaration { Name = "ss", Partitions = 3 }, ct);
        Assert.Single(system.CreatedSupers);
    }

    [Fact]
    public async Task Create_rejection_with_matching_partitions_is_knob_conflict()
    {
        var (registry, system) = CreateRegistry();
        var ct = TestContext.Current.CancellationToken;
        // The registry's pre-check only attempts the create while the super looks
        // absent, so the first existence probe answers absent and later probes see
        // the raced-into-existence super with matching partitions.
        var superProbes = 0;
        system.SuperExists = _ => Interlocked.Increment(ref superProbes) > 1;
        system.OnCreateSuper = (_, _) => throw new InvalidOperationException("broker: PreconditionFailed");
        system.Partitions = _ => new[] { "ss-0", "ss-1", "ss-2" };
        var ex = await Assert.ThrowsAsync<StreamConfigurationConflictException>(() =>
            registry.EnsureSuperStreamAsync(new SuperStreamDeclaration { Name = "ss", Partitions = 3, MaxAge = TimeSpan.FromDays(1) }, ct));
        Assert.Contains("differing arguments", ex.Actual);
    }

    [Fact]
    public async Task Create_rejection_with_existing_plain_is_conflict()
    {
        var (registry, system) = CreateRegistry();
        var ct = TestContext.Current.CancellationToken;
        // Same absent-then-present sequencing as the super knob-conflict test: the
        // pre-check attempts the create (which the broker rejects), and the
        // re-probe then finds the plain stream, proving the specs differed.
        var streamProbes = 0;
        system.StreamExists = _ => Interlocked.Increment(ref streamProbes) > 1;
        system.OnCreateStream = (_, _) => throw new InvalidOperationException("broker: PreconditionFailed");
        var ex = await Assert.ThrowsAsync<StreamConfigurationConflictException>(() =>
            registry.EnsureStreamAsync(new StreamDeclaration { Name = "s", MaxAge = TimeSpan.FromDays(1) }, ct));
        Assert.Contains("differing arguments", ex.Actual);
    }

    [Fact]
    public async Task Coexisting_plain_and_super_conflicts()
    {
        var (registry, system) = CreateRegistry();
        var ct = TestContext.Current.CancellationToken;
        system.SuperExists = _ => true;
        system.StreamExists = _ => true;
        system.Partitions = _ => new[] { "ss-0", "ss-1", "ss-2" };
        var ex = await Assert.ThrowsAsync<StreamConfigurationConflictException>(() =>
            registry.EnsureSuperStreamAsync(new SuperStreamDeclaration { Name = "ss", Partitions = 3 }, ct));
        Assert.Contains("coexisting", ex.Actual);
    }

    [Fact]
    public async Task Partition_mismatch_throws_with_expected_and_actual()
    {
        var (registry, system) = CreateRegistry();
        var ct = TestContext.Current.CancellationToken;
        system.OnCreateSuper = (_, _) => throw new InvalidOperationException("broker: PreconditionFailed");
        system.SuperExists = _ => true;
        system.Partitions = _ => new[] { "ss-0", "ss-1", "ss-2" };
        var ex = await Assert.ThrowsAsync<StreamConfigurationConflictException>(() =>
            registry.EnsureSuperStreamAsync(new SuperStreamDeclaration { Name = "ss", Partitions = 5 }, ct));
        Assert.Contains("5", ex.Message);
        Assert.Contains("3", ex.Message);
        Assert.Contains("5", ex.Expected);
        Assert.Contains("3", ex.Actual);
    }

    [Fact]
    public async Task Kind_mismatch_throws()
    {
        var (registry, system) = CreateRegistry();
        var ct = TestContext.Current.CancellationToken;
        system.StreamExists = _ => true;
        var ex = await Assert.ThrowsAsync<StreamConfigurationConflictException>(() =>
            registry.EnsureSuperStreamAsync(new SuperStreamDeclaration { Name = "s", Partitions = 2 }, ct));
        Assert.Contains("plain stream", ex.Actual);
        // A plain Ensure over an occupying super must reach the compare path: the
        // create attempt has to fail (a successful create is success, per the
        // Task 1 PLAIN-ON-SUPER-NAME probe), so the rejection is scripted here.
        system.StreamExists = _ => false;
        system.SuperExists = _ => true;
        system.Partitions = _ => new[] { "s-0" };
        system.OnCreateStream = (_, _) => throw new InvalidOperationException("broker: PreconditionFailed");
        var ex2 = await Assert.ThrowsAsync<StreamConfigurationConflictException>(() =>
            registry.EnsureStreamAsync(new StreamDeclaration { Name = "s" }, ct));
        Assert.Contains("1", ex2.Actual);
    }

    [Fact]
    public async Task Create_failure_with_absent_stream_rethrows_original()
    {
        var (registry, system) = CreateRegistry();
        var ct = TestContext.Current.CancellationToken;
        system.OnCreateStream = (_, _) => throw new InvalidOperationException("broker: nope");
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            registry.EnsureStreamAsync(new StreamDeclaration { Name = "s" }, ct));
        Assert.Contains("nope", ex.Message);
    }

    [Fact]
    public async Task Preexisting_plain_stream_needs_no_create()
    {
        var (registry, system) = CreateRegistry();
        var ct = TestContext.Current.CancellationToken;
        system.StreamExists = _ => true;
        await registry.EnsureStreamAsync(new StreamDeclaration { Name = "s" }, ct);
        Assert.Empty(system.CreatedStreams);
    }
}
