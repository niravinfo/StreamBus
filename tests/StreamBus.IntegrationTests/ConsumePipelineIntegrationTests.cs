using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Stream.Client;

namespace StreamBus.IntegrationTests;

internal sealed record ConsumePipelineTenantMessage(string Name);

internal sealed class ConsumePipelineTenantScope
{
    public string? Tenant { get; set; }
}

internal sealed class ConsumePipelineTenantProbe
{
    public List<string?> Seen { get; } = new();
}

internal sealed class ConsumePipelineTenantFilter : IStreamConsumeFilter
{
    public Task OnConsume(ConsumeContext context, ConsumeNext next)
    {
        var scope = (ConsumePipelineTenantScope)context.Services.GetRequiredService(typeof(ConsumePipelineTenantScope));
        scope.Tenant = context.Headers.All.TryGetValue("tenant", out var value)
            ? value?.ToString()
            : null;
        context.SetPayload(scope.Tenant);
        return next();
    }
}

internal sealed class ConsumePipelineTenantConsumer(
    ConsumePipelineTenantScope scope,
    ConsumePipelineTenantProbe probe) : IConsumer<ConsumePipelineTenantMessage>
{
    public Task Consume(ConsumeContext<ConsumePipelineTenantMessage> context)
    {
        lock (probe.Seen) probe.Seen.Add(scope.Tenant);
        return Task.CompletedTask;
    }
}

internal sealed class ConsumePipelineHeaderThrowingFilter : IStreamConsumeFilter
{
    public Task OnConsume(ConsumeContext context, ConsumeNext next)
    {
        if (context.Headers.All.TryGetValue("tenant", out var tenant))
            context.SendHeaders.Set("tenant", tenant);
        throw new InvalidOperationException("pipeline boom");
    }
}

internal sealed class ConsumePipelineFilteringFilter : IStreamConsumeFilter
{
    public Task OnConsume(ConsumeContext context, ConsumeNext next)
        => Task.CompletedTask;
}

public sealed class ConsumePipelineIntegrationTests : IAsyncDisposable
{
    public ConsumePipelineIntegrationTests() => IntegrationLane.RequireBroker();

    private readonly List<string> _streams = new();

    private static async Task<StreamSystem> ConnectAdminAsync()
    {
        var handle = await TestBroker.StartAsync(TestContext.Current.CancellationToken);
        return await StreamSystem.Create(new StreamSystemConfig
        {
            UserName = handle.Username,
            Password = handle.Password,
            VirtualHost = "/",
            Endpoints = new List<System.Net.EndPoint> { new System.Net.IPEndPoint(System.Net.IPAddress.Parse(handle.Host), handle.StreamPort) },
        });
    }

    private static string Unique(string prefix) => $"{prefix}-{Guid.NewGuid():N}";

    // GitHub Action runners are 2-vCPU and the broker runs under Testcontainers,
    // so consume/store progress can legitimately take far longer there than on a
    // dev machine. Scale the in-test wait budget up when running under CI.
    private static readonly TimeSpan WaitTimeout =
        Environment.GetEnvironmentVariable("GITHUB_ACTIONS") == "true"
            ? TimeSpan.FromSeconds(120)
            : TimeSpan.FromSeconds(30);

    private static ReadOnlyMemory<byte> Body(string s) => System.Text.Encoding.UTF8.GetBytes(s);

    private static async Task CreateStreamAsync(StreamSystem admin, string stream)
        => await admin.CreateStream(new StreamSpec(stream));

    public async ValueTask DisposeAsync()
    {
        var admin = await ConnectAdminAsync();
        try
        {
            foreach (var stream in _streams)
            {
                try { await admin.DeleteStream(stream); } catch { }
            }
        }
        finally { await admin.DisposeAsync(); }
    }

    private static async Task WaitForAsync(Func<bool> done, TimeSpan timeout, string what, Func<string>? describeLast = null)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!done())
        {
            if (DateTime.UtcNow >= deadline) Assert.Fail(TimeoutMessage(what, describeLast));
            await Task.Delay(100, TestContext.Current.CancellationToken);
        }
    }

    private static async Task WaitForAsync(Func<Task<bool>> done, TimeSpan timeout, string what, Func<string>? describeLast = null)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!await done())
        {
            if (DateTime.UtcNow >= deadline) Assert.Fail(TimeoutMessage(what, describeLast));
            await Task.Delay(100, TestContext.Current.CancellationToken);
        }
    }

    private static string TimeoutMessage(string what, Func<string>? describeLast)
        => describeLast is null
            ? $"Timed out waiting for {what}."
            : $"Timed out waiting for {what}. Last observed: {describeLast()}.";

    /// <summary>
    /// Raw-reads the stream from offset 0 with an untracked consumer (no consumerGroup,
    /// no filters) and returns the offset of the last message it can see.
    /// </summary>
    /// <remarks>
    /// RabbitMQ interleaves offset-tracking records into the stream: every
    /// broker-side offset store for any tracked consumerGroup is materialized as a
    /// record that consumes a stream offset but is not readable. Message offsets
    /// are therefore NOT contiguous while consumers track offsets, so a test
    /// must discover the real last-message offset instead of assuming
    /// "N messages ⇒ last offset N-1".
    /// </remarks>
    private static async Task<ulong> LastPlayableMessageOffsetAsync(string stream, int minimumMessages)
    {
        var admin = await ConnectAdminAsync();
        try
        {
            var offsets = new List<ulong>();
            var receivedAny = new TaskCompletionSource();
            var consumer = await admin.CreateRawConsumer(new RawConsumerConfig(stream)
            {
                OffsetSpec = new OffsetTypeFirst(),
                MessageHandler = (_, context, message) =>
                {
                    lock (offsets)
                    {
                        offsets.Add(context.Offset);
                        if (offsets.Count >= minimumMessages) receivedAny.TrySetResult();
                    }
                    return Task.CompletedTask;
                },
            });
            await receivedAny.Task.WaitAsync(TimeSpan.FromSeconds(30));
            // Give a later chunk a beat to arrive (chunks are delivered progressively).
            await Task.Delay(500, TestContext.Current.CancellationToken);
            await consumer.Close();
            lock (offsets)
                return offsets.Max();
        }
        finally
        {
            await admin.DisposeAsync();
        }
    }

    private sealed class BusInstance : IAsyncDisposable
    {
        public required ServiceProvider Provider { get; init; }
        public required StreamBusHostedService Hosted { get; init; }
        public required IStreamBus Bus { get; init; }

        public static async Task<BusInstance> StartAsync(
            Action<IServiceCollection>? configureServices,
            Action<IStreamBusConfigurator> configureBus)
        {
            var handle = await TestBroker.StartAsync(TestContext.Current.CancellationToken);
            var services = new ServiceCollection();
            services.AddLogging();
            configureServices?.Invoke(services);
            services.AddStreamBus((_, cfg) =>
            {
                cfg.Host(handle.Host, "/", h => { h.Credentials(handle.Username, handle.Password); h.StreamPort(handle.StreamPort); });
                configureBus(cfg);
            });
            var provider = services.BuildServiceProvider();
            var hosted = provider.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
            var ct = TestContext.Current.CancellationToken;
            await hosted.StartAsync(ct);
            var bus = provider.GetRequiredService<IStreamBus>();
            Assert.Equal(BusReadiness.Ready, await bus.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30)), ct));
            return new BusInstance { Provider = provider, Hosted = hosted, Bus = bus };
        }

        public async ValueTask DisposeAsync()
        {
            try { await Hosted.StopAsync(CancellationToken.None); }
            finally { await Provider.DisposeAsync(); }
        }
    }

    [Fact]
    public async Task Tenant_filter_and_typed_consumer_share_one_per_message_scope()
    {
        var ct = TestContext.Current.CancellationToken;
        var stream = Unique("pipe-tenant");
        await using var admin = await ConnectAdminAsync();
        await CreateStreamAsync(admin, stream);
        _streams.Add(stream);

        var probe = new ConsumePipelineTenantProbe();
        await using var bus = await BusInstance.StartAsync(
            services =>
            {
                services.AddSingleton(probe);
                services.AddScoped<ConsumePipelineTenantScope>();
                services.AddTransient<ConsumePipelineTenantConsumer>();
            },
            cfg =>
            {
                cfg.UseConsumeFilter<ConsumePipelineTenantFilter>();
                cfg.ReceiveEndpoint(stream, e => e.Consumer<ConsumePipelineTenantConsumer>("tenant-grp"));
            });
        try
        {
            await bus.Bus.PublishRawAsync(
                stream,
                SystemTextJsonStreamSerializer.Default.Serialize(new ConsumePipelineTenantMessage("first")),
                new PublishOptions
                {
                    Properties = new StreamMessageProperties
                    {
                        Headers = new Dictionary<string, object?> { ["tenant"] = "acme" },
                    },
                },
                ct);

            await WaitForAsync(() => { lock (probe.Seen) return probe.Seen.Count == 1; }, TimeSpan.FromSeconds(30), "the tenant consumer");
            lock (probe.Seen) Assert.Equal(new[] { "acme" }, probe.Seen);
        }
        finally
        {
            await bus.DisposeAsync();
        }
    }

    [Fact]
    public async Task Exhaustion_envelope_carries_the_filter_output_headers()
    {
        var ct = TestContext.Current.CancellationToken;
        var source = Unique("pipe-exhaust");
        var errors = Unique("pipe-exhaust-errors");
        await using var admin = await ConnectAdminAsync();
        await CreateStreamAsync(admin, source);
        await CreateStreamAsync(admin, errors);
        _streams.Add(source);
        _streams.Add(errors);

        await using var bus = await BusInstance.StartAsync(
            configureServices: null,
            cfg => cfg.UseConsumeFilter<ConsumePipelineHeaderThrowingFilter>());
        var consumer = await bus.Bus.ConsumeAsync(source, new ConsumeOptions
        {
            ConsumerGroup = "exhaust-grp",
            StartOffset = StartOffset.AlwaysFirst,
            Retry = new RetryPolicy
            {
                MaxAttempts = 1,
                BaseDelay = TimeSpan.FromMilliseconds(10),
                MaxDelay = TimeSpan.FromMilliseconds(50),
                MaxJitter = TimeSpan.Zero,
            },
            ErrorStream = new ErrorStreamOptions { Stream = errors },
        }, _ => Task.CompletedTask, ct);

        try
        {
            await bus.Bus.PublishRawAsync(source, Body("hello"), new PublishOptions
            {
                Properties = new StreamMessageProperties
                {
                    Headers = new Dictionary<string, object?> { ["tenant"] = "acme" },
                },
            }, ct);

            var captured = new TaskCompletionSource<IReadOnlyDictionary<string, object>>(TaskCreationOptions.RunContinuationsAsynchronously);
            var probe = await admin.CreateRawConsumer(new RawConsumerConfig(errors)
            {
                Reference = $"exhaust-probe-{Guid.NewGuid():N}",
                OffsetSpec = new OffsetTypeFirst(),
                MessageHandler = (_, _, message) =>
                {
                    if (message.ApplicationProperties is { } properties)
                        captured.TrySetResult(properties);
                    return Task.CompletedTask;
                },
            });
            try
            {
                var properties = await captured.Task.WaitAsync(TimeSpan.FromSeconds(30), ct);
                Assert.Equal("acme", properties["tenant"]?.ToString());
                Assert.Equal("0", properties["streambus.error.source-offset"]?.ToString());
                Assert.Contains("pipeline boom", properties["streambus.error.exception-message"]?.ToString(), StringComparison.Ordinal);
                Assert.Equal(source, properties["streambus.error.source-stream"]?.ToString());
            }
            finally { probe.Dispose(); }
        }
        finally
        {
            await consumer.DisposeAsync();
        }
    }

    [Fact]
    public async Task Zero_next_advances_the_offset_once_and_does_not_redeliver_after_dispose()
    {
        var ct = TestContext.Current.CancellationToken;
        var stream = Unique("pipe-filtered");
        await using var admin = await ConnectAdminAsync();
        await CreateStreamAsync(admin, stream);
        _streams.Add(stream);

        await using var bus = await BusInstance.StartAsync(configureServices: null, _ => { });
        var filteringConsumer = await bus.Bus.ConsumeAsync(stream, new ConsumeOptions
        {
            ConsumerGroup = "filter-grp",
            StartOffset = StartOffset.AlwaysFirst,
            ConsumeFilters = new[] { ConsumeFilterRegistration.ForInstance(new ConsumePipelineFilteringFilter()) },
        }, _ => throw new InvalidOperationException("the handler must not run for a filtered message"), ct);

        try
        {
            for (var i = 0; i < 3; i++)
                await bus.Bus.PublishRawAsync(stream, Body($"filtered-{i}"), new PublishOptions(), ct);

            // RabbitMQ interleaves each broker-side offset store (ours included)
            // as a non-readable record that consumes a stream offset, so the
            // three messages may sit at 0,1,2 or at 0,1,3... depending on where
            // the tracking records landed. The invariant under test is: the last
            // FILTERED message's offset gets stored; redelivery stops after it.
            var lastMessageOffset = await LastPlayableMessageOffsetAsync(stream, minimumMessages: 3);
            ulong? stored = null;
            await WaitForAsync(
                async () => (stored = await bus.Bus.GetConsumerOffsetAsync(stream, "filter-grp", 0, ct)) == lastMessageOffset,
                WaitTimeout,
                $"the last filtered offset ({lastMessageOffset}) to be stored",
                () => stored is null ? "no stored offset" : $"stored offset {stored} (expected {lastMessageOffset})");
        }
        finally
        {
            await filteringConsumer.DisposeAsync();
        }

        var delivered = new List<string>();
        var resumed = await bus.Bus.ConsumeAsync(stream, new ConsumeOptions
        {
            ConsumerGroup = "filter-grp",
            StartOffset = StartOffset.ResumeOrFirst,
        }, ctx =>
        {
            lock (delivered) delivered.Add(System.Text.Encoding.UTF8.GetString(ctx.Body.Span));
            return Task.CompletedTask;
        }, ct);
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(2), ct);
            lock (delivered) Assert.Empty(delivered);

            await bus.Bus.PublishRawAsync(stream, Body("after-filter"), new PublishOptions(), ct);
            await WaitForAsync(() => { lock (delivered) return delivered.Count == 1; }, WaitTimeout, "the post-resume message");
            lock (delivered) Assert.Equal(new[] { "after-filter" }, delivered);
        }
        finally
        {
            await resumed.DisposeAsync();
        }
    }
}
