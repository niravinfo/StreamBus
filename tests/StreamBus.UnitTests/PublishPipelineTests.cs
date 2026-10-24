using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

public sealed record PipelineOrder(string Id);

public sealed class PipelineNoopFilter : IStreamPublishFilter
{
    public Task OnPublish(StreamPublishFilterContext context, PublishDelegate next, CancellationToken ct)
        => next(context);
}

public sealed class TenantPublishFilter : IStreamPublishFilter
{
    public Task OnPublish(StreamPublishFilterContext context, PublishDelegate next, CancellationToken ct)
    {
        context.Headers.Set("tenant", "acme");
        return next(context);
    }
}

public sealed class PublishPipelineTests
{
    [Fact]
    public async Task Filters_run_in_registration_order_and_unwind_in_reverse()
    {
        var log = new List<string>();
        var pipeline = new PublishPipeline(new IStreamPublishFilter[]
        {
            new RecordingFilter("A", log),
            new RecordingFilter("B", log),
            new RecordingFilter("C", log),
        });

        await pipeline.RunAsync(NewContext(), _ =>
        {
            log.Add("terminal");
            return Task.CompletedTask;
        }, TestContext.Current.CancellationToken);

        Assert.Equal(new[] { "A-in", "B-in", "C-in", "terminal", "C-out", "B-out", "A-out" }, log);
    }

    [Fact]
    public async Task Zero_next_is_rejected_as_a_configuration_error()
    {
        var pipeline = new PublishPipeline(new IStreamPublishFilter[] { new ZeroNextFilter() });

        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            pipeline.RunAsync(NewContext(), _ => Task.CompletedTask, TestContext.Current.CancellationToken));

        Assert.Contains(nameof(ZeroNextFilter), ex.Message, StringComparison.Ordinal);
        Assert.Contains("returned without calling next", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Double_next_is_rejected_as_a_configuration_error()
    {
        var pipeline = new PublishPipeline(new IStreamPublishFilter[] { new DoubleNextFilter() });
        var terminalCalls = 0;

        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            pipeline.RunAsync(NewContext(), _ =>
            {
                Interlocked.Increment(ref terminalCalls);
                return Task.CompletedTask;
            }, TestContext.Current.CancellationToken));

        Assert.Contains(nameof(DoubleNextFilter), ex.Message, StringComparison.Ordinal);
        Assert.Contains("invoked next 2 times", ex.Message, StringComparison.Ordinal);
        Assert.Equal(1, terminalCalls);
    }

    [Fact]
    public void Duplicate_filter_type_is_rejected_at_configuration_time()
    {
        var services = new ServiceCollection();
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus((_, cfg) =>
            {
                cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
                cfg.UsePublishFilter<PipelineNoopFilter>();
                cfg.UsePublishFilter<PipelineNoopFilter>();
            }));

        Assert.Contains(nameof(PipelineNoopFilter), ex.Message, StringComparison.Ordinal);
        Assert.Contains("already registered", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Header_writer_merges_call_level_properties_and_call_level_wins()
    {
        var callLevel = new StreamMessageProperties
        {
            MessageId = "m-1",
            Headers = new Dictionary<string, object?>
            {
                ["tenant"] = "call-level",
                ["call-only"] = true,
            },
        };
        var writer = new HeaderWriter(callLevel.Headers);
        writer.Set("tenant", "filter-level");
        writer.Set("filter-only", 7);

        var merged = writer.Materialize(callLevel);
        Assert.NotNull(merged);
        Assert.Equal("m-1", merged.MessageId);
        Assert.Equal("call-level", merged.Headers["tenant"]);
        Assert.Equal(true, merged.Headers["call-only"]);
        Assert.Equal(7, merged.Headers["filter-only"]);
    }

    [Fact]
    public async Task Header_enrichment_reaches_the_scripted_handle_properties()
    {
        await using var bus = await TestBus.StartAsync(cfg =>
        {
            cfg.Publish<PipelineOrder>("orders");
            cfg.UsePublishFilter<TenantPublishFilter>();
        }, TestContext.Current.CancellationToken);

        await bus.Bus.PublishAsync(new PipelineOrder("1"), TestContext.Current.CancellationToken);

        var captured = Assert.Single(bus.System.Publishes);
        Assert.NotNull(captured.Options.Properties);
        Assert.Equal("acme", captured.Options.Properties.Headers["tenant"]);
    }

    [Fact]
    public async Task Filter_exception_is_classified_as_a_publish_transient_failure()
    {
        var filter = new ThrowingFilter();
        await using var bus = await TestBus.StartAsync(cfg =>
        {
            cfg.Publish<PipelineOrder>("orders");
            cfg.UsePublishFilter(filter);
        }, TestContext.Current.CancellationToken);

        var ex = await Assert.ThrowsAsync<StreamBusPublishTransientException>(() =>
            bus.Bus.PublishAsync(new PipelineOrder("1"), TestContext.Current.CancellationToken));

        Assert.IsType<InvalidOperationException>(ex.InnerException);
        Assert.Equal(1, filter.Calls);
        Assert.Empty(bus.System.Publishes);
    }

    [Fact]
    public async Task Super_routing_and_body_cannot_be_replaced_by_a_filter_context()
    {
        await using var bus = await TestBus.StartAsync(cfg =>
        {
            cfg.PublishSuper<PipelineOrder>("events", s => s.Partitions(1));
            cfg.UsePublishFilter(new ContextSwapFilter());
        }, TestContext.Current.CancellationToken);

        await bus.Bus.PublishSuperAsync(new PipelineOrder("original"), "original-key", TestContext.Current.CancellationToken);

        var captured = Assert.Single(bus.System.Publishes);
        Assert.True(captured.Options.IsSuperStream);
        Assert.Equal("original-key", captured.Options.RoutingKey);
        Assert.Contains("original", System.Text.Encoding.UTF8.GetString(captured.Body), StringComparison.Ordinal);
        Assert.DoesNotContain("hacked", System.Text.Encoding.UTF8.GetString(captured.Body), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Raw_publish_does_not_run_publish_filters()
    {
        var filter = new CountingFilter();
        await using var bus = await TestBus.StartAsync(cfg => cfg.UsePublishFilter(filter), TestContext.Current.CancellationToken);

        await bus.Bus.PublishRawAsync("raw", new byte[] { 1, 2, 3 }, new PublishOptions(), TestContext.Current.CancellationToken);

        Assert.Equal(0, filter.Calls);
        var captured = Assert.Single(bus.System.Publishes);
        Assert.Equal(new byte[] { 1, 2, 3 }, captured.Body);
    }

    private static StreamPublishFilterContext NewContext() => new()
    {
        Stream = "orders",
        RoutingKey = null,
        Message = new PipelineOrder("1"),
        Headers = new HeaderWriter(),
        Bus = new StreamBusRegistrationView
        {
            Vhost = "/",
            Host = "localhost",
            StreamPort = 5552,
            AmqpPort = 5672,
            HeartbeatSeconds = 60,
        },
        Services = EmptyProvider.Instance,
    };

    private sealed class RecordingFilter(string name, List<string> log) : IStreamPublishFilter
    {
        public async Task OnPublish(StreamPublishFilterContext context, PublishDelegate next, CancellationToken ct)
        {
            log.Add($"{name}-in");
            await next(context);
            log.Add($"{name}-out");
        }
    }

    private sealed class ZeroNextFilter : IStreamPublishFilter
    {
        public Task OnPublish(StreamPublishFilterContext context, PublishDelegate next, CancellationToken ct)
            => Task.CompletedTask;
    }

    private sealed class DoubleNextFilter : IStreamPublishFilter
    {
        public async Task OnPublish(StreamPublishFilterContext context, PublishDelegate next, CancellationToken ct)
        {
            await next(context);
            await next(context);
        }
    }

    private sealed class ThrowingFilter : IStreamPublishFilter
    {
        public int Calls { get; private set; }

        public Task OnPublish(StreamPublishFilterContext context, PublishDelegate next, CancellationToken ct)
        {
            Calls++;
            throw new InvalidOperationException("filter boom");
        }
    }

    private sealed class CountingFilter : IStreamPublishFilter
    {
        public int Calls { get; private set; }

        public Task OnPublish(StreamPublishFilterContext context, PublishDelegate next, CancellationToken ct)
        {
            Calls++;
            return next(context);
        }
    }

    private sealed class ContextSwapFilter : IStreamPublishFilter
    {
        public Task OnPublish(StreamPublishFilterContext context, PublishDelegate next, CancellationToken ct)
        {
            var swapped = new StreamPublishFilterContext
            {
                Stream = context.Stream,
                RoutingKey = "hacked-key",
                Message = new PipelineOrder("hacked"),
                Headers = context.Headers,
                Bus = context.Bus,
                Services = context.Services,
            };
            return next(swapped);
        }
    }

    private sealed class EmptyProvider : IServiceProvider
    {
        public static readonly EmptyProvider Instance = new();
        public object? GetService(Type serviceType) => null;
    }

    private sealed class TestBus : IAsyncDisposable
    {
        public required ServiceProvider Provider { get; init; }
        public required IStreamBus Bus { get; init; }
        public required IHostedService Hosted { get; init; }
        public required CapturingSystem System { get; init; }

        public static async Task<TestBus> StartAsync(
            Action<IStreamBusConfigurator> configure,
            CancellationToken ct)
        {
            var services = new ServiceCollection();
            services.AddLogging();
            var system = new CapturingSystem();
            services.AddSingleton<IStreamSystemConnector>(new FakeConnector(system));
            services.AddStreamBus((_, cfg) =>
            {
                cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
                configure(cfg);
            });
            var provider = services.BuildServiceProvider();
            var hosted = provider.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
            await hosted.StartAsync(ct);
            return new TestBus
            {
                Provider = provider,
                Bus = provider.GetRequiredService<IStreamBus>(),
                Hosted = hosted,
                System = system,
            };
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                await Hosted.StopAsync(CancellationToken.None);
            }
            finally
            {
                await Provider.DisposeAsync();
            }
        }
    }

    private sealed class FakeConnector(CapturingSystem system) : IStreamSystemConnector
    {
        public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
            => Task.FromResult<IStreamSystemHandle>(system);
    }

    private sealed class CapturingSystem : IStreamSystemHandle
    {
        public readonly List<CapturedPublish> Publishes = new();

        public bool IsClosed => false;

        public Task<IStreamPlainProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct)
            => Task.FromResult<IStreamPlainProducerHandle>(new CapturingPlainHandle(stream, this));

        public Task<IStreamPlainProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken ct)
            => CreatePlainProducerAsync(stream, ct);

        public Task<IStreamSuperProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct)
            => Task.FromResult<IStreamSuperProducerHandle>(new CapturingSuperHandle(stream, this));

        public Task<IStreamSuperProducerHandle> CreateSuperStreamProducerWithReferenceAsync(string stream, string reference, CancellationToken ct)
            => CreateSuperStreamProducerAsync(stream, ct);

        public Task<ulong> QuerySequenceAsync(string reference, string stream, CancellationToken ct) => Task.FromResult(0UL);
        public Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken ct) => throw new NotSupportedException();
        public Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken ct) => Task.FromResult<ulong?>(0);
        public Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken ct) => Task.CompletedTask;
        public Task<bool> StreamExistsAsync(string stream, CancellationToken ct) => Task.FromResult(true);
        public Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken ct) => Task.FromResult(true);
        public Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct) => Task.FromResult(new[] { $"{superStream}-0" });
        public Task CreateStreamAsync(StreamSpec spec, CancellationToken ct) => Task.CompletedTask;
        public Task CreateSuperStreamAsync(PartitionsSuperStreamSpec spec, CancellationToken ct) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public void Add(string stream, ReadOnlyMemory<byte> body, PublishOptions options)
        {
            lock (Publishes) Publishes.Add(new CapturedPublish(stream, body.ToArray(), options));
        }
    }

    private sealed record CapturedPublish(string Stream, byte[] Body, PublishOptions Options);

    private class CapturingPlainHandle(string stream, CapturingSystem system) : IStreamPlainProducerHandle
    {
        public Task SendAsync(ReadOnlyMemory<byte> body, PublishOptions options, CancellationToken ct)
        {
            system.Add(stream, body, options);
            return Task.CompletedTask;
        }

        public Task SendEnvelopeAsync(
            ReadOnlyMemory<byte> body,
            PublishOptions options,
            IReadOnlyDictionary<string, object?> applicationProperties,
            string? messageId,
            string? correlationId,
            CancellationToken ct)
        {
            system.Add(stream, body, options);
            return Task.CompletedTask;
        }

        public Task PublishReplayAsync(ReadOnlyMemory<byte> body, ulong publishingId, PublishOptions options, CancellationToken ct)
        {
            system.Add(stream, body, options);
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class CapturingSuperHandle(string stream, CapturingSystem system)
        : CapturingPlainHandle(stream, system), IStreamSuperProducerHandle
    {
        public Task PublishWithIdAsync(ulong publishingId, ReadOnlyMemory<byte> body, PublishOptions options, CancellationToken ct)
            => SendAsync(body, options, ct);

        public Task PublishResultAsync(ReadOnlyMemory<byte> body, ulong publishingId, string? routingKey, CancellationToken ct)
            => SendAsync(body, new PublishOptions { RoutingKey = routingKey, IsSuperStream = true }, ct);
    }
}
