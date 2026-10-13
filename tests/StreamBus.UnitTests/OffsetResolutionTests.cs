using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

sealed class ListLogger : ILogger
{
    public readonly List<string> Lines = new();
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
    public bool IsEnabled(LogLevel logLevel) => true;
    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        lock (Lines) Lines.Add($"{logLevel}: {formatter(state, exception)}");
    }
}

public sealed class OffsetResolutionTests
{
    private static (ListLogger Log, List<string> Branches) NewSink()
        => (new ListLogger(), new List<string>());

    [Fact]
    public void Always_modes_ignore_the_broker()
    {
        var (log, branches) = NewSink();
        Assert.IsType<OffsetTypeFirst>(OffsetResolution.ResolveOffset(ConsumeOffset.AlwaysFirst, 99, 50, "r", "p", log, branches.Add));
        Assert.IsType<OffsetTypeNext>(OffsetResolution.ResolveOffset(ConsumeOffset.AlwaysNext, 99, 50, "r", "p", log, branches.Add));
        Assert.Empty(branches);
    }

    [Theory]
    [InlineData(ConsumeOffset.ResumeOrFirst, "ResumeOrFirst", typeof(OffsetTypeFirst))]
    [InlineData(ConsumeOffset.ResumeOrNext, "ResumeOrNext", typeof(OffsetTypeNext))]
    public void No_stored_offset_falls_back(ConsumeOffset mode, string branch, Type expected)
    {
        var (log, branches) = NewSink();
        var result = OffsetResolution.ResolveOffset(mode, null, 0, "ref-A", "part-0", log, branches.Add);
        Assert.IsType(expected, result);
        Assert.Equal(new[] { branch }, branches);
        Assert.Contains(log.Lines, l => l.Contains("ref-A") && l.Contains("part-0") && l.Contains(branch));
    }

    [Fact]
    public void ResumeOrFail_without_stored_throws_naming_reference_and_partition()
    {
        var (log, branches) = NewSink();
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            OffsetResolution.ResolveOffset(ConsumeOffset.ResumeOrFail, null, 0, "ref-B", "part-2", log, branches.Add));
        Assert.Contains("ref-B", ex.Message);
        Assert.Contains("part-2", ex.Message);
    }

    [Fact]
    public void Stored_offset_resolves_to_next_and_broker_wins_over_local()
    {
        var (log, branches) = NewSink();
        var result = OffsetResolution.ResolveOffset(ConsumeOffset.ResumeOrFirst, 5, 9, "r", "p", log, branches.Add);
        Assert.Equal(new OffsetTypeOffset(6), result);
        Assert.Contains(log.Lines, l => l.Contains("9") && l.Contains("5"));
        Assert.Empty(branches);
    }

    [Fact]
    public async Task Query_failure_degrades_to_no_stored_branch()
    {
        var (log, branches) = NewSink();
        var result = await OffsetResolution.ResolveWithQueryAsync(
            ConsumeOffset.ResumeOrFirst, () => Task.FromException<ulong?>(new TimeoutException("blip")),
            0, "r", "p", log, branches.Add);
        Assert.IsType<OffsetTypeFirst>(result);
        Assert.Equal(new[] { "ResumeOrFirst" }, branches);
    }

    [Fact]
    public void Guard_rejects_non_monotonic_and_set_overwrites()
    {
        var guard = new MonotonicOffsetGuard();
        Assert.True(guard.TryAdmit("k", 4));
        Assert.False(guard.TryAdmit("k", 4));
        Assert.False(guard.TryAdmit("k", 2));
        Assert.True(guard.TryAdmit("k", 5));
        guard.Overwrite("k", 99);
        Assert.False(guard.TryAdmit("k", 50));
        Assert.True(guard.TryAdmit("k", 100));
    }

    [Fact]
    public void Derivation_is_stable_distinct_and_bounded()
    {
        var a1 = ReferenceDerivation.Derive(typeof(DefaultBus), "orders");
        var a2 = ReferenceDerivation.Derive(typeof(DefaultBus), "orders");
        Assert.Equal(a1, a2);
        Assert.StartsWith("streambus:", a1);
        Assert.NotEqual(a1, ReferenceDerivation.Derive(typeof(DefaultBus), "billing"));
        var longStream = new string('s', 300);
        var hashed = ReferenceDerivation.Derive(typeof(DefaultBus), longStream);
        Assert.True(System.Text.Encoding.UTF8.GetByteCount(hashed) <= 255);
        Assert.Throws<StreamBusConfigurationException>(() => ReferenceDerivation.Validate(new string('r', 300)));
        ReferenceDerivation.Validate(a1);
    }
}

public sealed class ConsumeValidationTests
{
    private static StreamConsumerRegistry UnstartedRegistry()
    {
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        var supervisor = new StreamBusConnectionSupervisor(options,
            new NeverReadyConnector());
        return new StreamConsumerRegistry(supervisor, "/", new StreamProducerRegistry(supervisor, "/"), null, new ResultProducerRegistry(supervisor, "/"));
    }

    private sealed class NeverReadyConnector : IStreamSystemConnector
    {
        public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
            => new TaskCompletionSource<IStreamSystemHandle>().Task;
    }

    private static Task<IStreamConsumer> Consume(StreamConsumerRegistry registry, string stream, ConsumeOptions options)
        => registry.ConsumeAsync(typeof(DefaultBus), stream, options, _ => Task.CompletedTask, TestContext.Current.CancellationToken);

    [Fact]
    public async Task Over_long_reference_throws_naming_limit()
    {
        var registry = UnstartedRegistry();
        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            Consume(registry, "s", new ConsumeOptions { Reference = new string('r', 300) }));
        Assert.Contains("255", ex.Message);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task Non_positive_every_messages_throws(int every)
    {
        var registry = UnstartedRegistry();
        await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            Consume(registry, "s", new ConsumeOptions { OffsetStore = new OffsetStoreOptions { EveryMessages = every } }));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task Non_positive_max_interval_throws(int seconds)
    {
        var registry = UnstartedRegistry();
        await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            Consume(registry, "s", new ConsumeOptions { OffsetStore = new OffsetStoreOptions { MaxInterval = TimeSpan.FromSeconds(seconds) } }));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task Null_or_empty_stream_throws(string? stream)
    {
        var registry = UnstartedRegistry();
        await Assert.ThrowsAnyAsync<ArgumentException>(() =>
            registry.ConsumeAsync(typeof(DefaultBus), stream!, new ConsumeOptions(), _ => Task.CompletedTask, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Null_handler_throws()
    {
        var registry = UnstartedRegistry();
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            registry.ConsumeAsync(typeof(DefaultBus), "s", new ConsumeOptions(), null!, TestContext.Current.CancellationToken));
    }

    private sealed class ListLoggerFactory : ILoggerFactory
    {
        public readonly ListLogger Logger = new();
        public void AddProvider(ILoggerProvider provider) { }
        public ILogger CreateLogger(string categoryName) => Logger;
        public void Dispose() { }
    }

    private static StreamConsumerRegistry RegistryWithLogging(ListLoggerFactory factory, StubSystemHandle handle)
    {
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        var supervisor = new StreamBusConnectionSupervisor(options, new ReadyConnector(handle));
        supervisor.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        return new StreamConsumerRegistry(supervisor, "/", new StreamProducerRegistry(supervisor, "/"), factory, new ResultProducerRegistry(supervisor, "/"));
    }

    private sealed class StubSystemHandle : IStreamSystemHandle
    {
        public bool Exists = true;
        public bool SuperExists;
        public bool IsClosed => false;
        public Task<IStreamProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct) => throw new NotImplementedException();
        public Task<IStreamProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken ct) => throw new NotImplementedException();
        public Task<IStreamProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct) => throw new NotImplementedException();
        public Task<IStreamProducerHandle> CreateSuperStreamProducerWithReferenceAsync(string stream, string reference, CancellationToken ct) => throw new NotImplementedException("StubSystemHandle never creates stable super producers.");
        public Task<ulong> QuerySequenceAsync(string reference, string stream, CancellationToken ct) => throw new NotImplementedException("StubSystemHandle never queries publishing sequences.");
        public Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken ct) => Task.FromResult<ulong?>(null);
        public Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken ct) => Task.CompletedTask;
        public Task<bool> StreamExistsAsync(string stream, CancellationToken ct) => Task.FromResult(Exists);
        public Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken ct) => Task.FromResult(SuperExists);
        public Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct) => Task.FromResult(new[] { superStream + "-0" });
        public Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken ct) => throw new NotImplementedException();
        public Task CreateStreamAsync(StreamSpec spec, CancellationToken ct) => throw new NotImplementedException("OffsetResolutionTests never declares topology.");
        public Task CreateSuperStreamAsync(PartitionsSuperStreamSpec spec, CancellationToken ct) => throw new NotImplementedException("OffsetResolutionTests never declares topology.");
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class ReadyConnector : IStreamSystemConnector
    {
        private readonly StubSystemHandle _handle;
        public ReadyConnector(StubSystemHandle handle) => _handle = handle;
        public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
            => Task.FromResult<IStreamSystemHandle>(_handle);
    }

    [Fact]
    public async Task Bus_level_set_audit_reaches_a_real_logger()
    {
        var factory = new ListLoggerFactory();
        var registry = RegistryWithLogging(factory, new StubSystemHandle());
        await registry.SetOffsetAsync("s", "grp", 0, 7, TestContext.Current.CancellationToken);
        Assert.Contains(factory.Logger.Lines, l => l.Contains("grp") && l.Contains("7"));
    }

    [Fact]
    public async Task Missing_stream_throws_not_found_on_all_entry_points()
    {
        var factory = new ListLoggerFactory();
        var handle = new StubSystemHandle { Exists = false, SuperExists = false };
        var registry = RegistryWithLogging(factory, handle);
        await Assert.ThrowsAsync<StreamBusStreamNotFoundException>(() =>
            registry.ConsumeAsync(typeof(DefaultBus), "missing", new ConsumeOptions(), _ => Task.CompletedTask, TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<StreamBusStreamNotFoundException>(() =>
            registry.GetOffsetAsync("missing", "grp", 0, TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<StreamBusStreamNotFoundException>(() =>
            registry.SetOffsetAsync("missing", "grp", 0, 7, TestContext.Current.CancellationToken));
    }
}
