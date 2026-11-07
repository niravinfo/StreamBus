using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

public sealed record PipelineOrder(string Id);

public sealed record PipelineInvoice(string Id);

public abstract record PipelineBase(string Id);

public sealed record PipelineDerived(string Id) : PipelineBase(Id);

public readonly record struct PipelineStruct(int Id);

public sealed class TypedPublishProbe
{
    private int _calls;

    public int Calls => _calls;

    public void Record() => _calls++;
}

public sealed class SentContextProbe
{
    public PublishContext? Context { get; private set; }

    public void Record(PublishContext context) => Context = context;
}

public sealed class SentProbeFilter(SentContextProbe probe) : IStreamPublishFilter
{
    public Task OnPublish(PublishContext context, PublishNext next)
    {
        probe.Record(context);
        return next();
    }
}

public sealed class PublishContextProbe
{
    public int Calls { get; private set; }
    public object? Payload { get; private set; }

    public void Record(PublishContext context)
    {
        Calls++;
        Payload = context.Payload;
    }
}

public sealed class SharedPublishContextProbe
{
    public PublishContext? UniversalContext { get; private set; }
    public PublishContext<PipelineOrder>? TypedContext { get; private set; }

    public void RecordUniversal(PublishContext context) => UniversalContext = context;

    public void RecordTyped(PublishContext<PipelineOrder> context) => TypedContext = context;
}

public sealed class SharedUniversalPublishFilter(SharedPublishContextProbe probe) : IStreamPublishFilter
{
    public Task OnPublish(PublishContext context, PublishNext next)
    {
        probe.RecordUniversal(context);
        return next();
    }
}

public sealed class SharedTypedPublishFilter(SharedPublishContextProbe probe) : IStreamPublishFilter<PipelineOrder>
{
    public Task OnPublish(PublishContext<PipelineOrder> context, PublishNext next)
    {
        probe.RecordTyped(context);
        return next();
    }
}

public sealed class PublishPipelineLog
{
    private readonly List<string> _entries = new();

    public IReadOnlyList<string> Entries => _entries;

    public void Add(string entry) => _entries.Add(entry);

    public void Clear() => _entries.Clear();
}

public sealed class MultiMessagePublishProbe
{
    private int _instances;
    private int _calls;

    public int Instances => _instances;

    public int Calls => _calls;

    public void RecordInstance() => Interlocked.Increment(ref _instances);

    public void RecordCall() => Interlocked.Increment(ref _calls);
}

public sealed class MultiMessagePublishFilter : IStreamPublishFilter<PipelineOrder>, IStreamPublishFilter<PipelineInvoice>
{
    private readonly MultiMessagePublishProbe _probe;

    public MultiMessagePublishFilter(MultiMessagePublishProbe probe)
    {
        _probe = probe;
        probe.RecordInstance();
    }

    public Task OnPublish(
        PublishContext<PipelineOrder> context,
        PublishNext next)
    {
        _probe.RecordCall();
        return next();
    }

    public Task OnPublish(
        PublishContext<PipelineInvoice> context,
        PublishNext next)
    {
        _probe.RecordCall();
        return next();
    }
}

public sealed class OrderOnlyPublishFilter(TypedPublishProbe probe) : IStreamPublishFilter<PipelineOrder>
{
    public Task OnPublish(
        PublishContext<PipelineOrder> context,
        PublishNext next)
    {
        probe.Record();
        context.Headers.Set("order", context.Message.Id);
        return next();
    }
}

public sealed class ContextProbePublishFilter(PublishContextProbe probe) : IStreamPublishFilter
{
    public Task OnPublish(PublishContext context, PublishNext next)
    {
        probe.Record(context);
        return next();
    }
}

public sealed class PipelineLogAFilter(PublishPipelineLog log) : IStreamPublishFilter
{
    public async Task OnPublish(PublishContext context, PublishNext next)
    {
        log.Add("A-in");
        await next();
        log.Add("A-out");
    }
}

public sealed class PipelineLogCFilter(PublishPipelineLog log) : IStreamPublishFilter
{
    public async Task OnPublish(PublishContext context, PublishNext next)
    {
        log.Add("C-in");
        await next();
        log.Add("C-out");
    }
}

public sealed class OrderLogPublishFilter(PublishPipelineLog log) : IStreamPublishFilter<PipelineOrder>
{
    public async Task OnPublish(
        PublishContext<PipelineOrder> context,
        PublishNext next)
    {
        log.Add("typed-in");
        await next();
        log.Add("typed-out");
    }
}

public sealed class OpenGenericPublishFilter<T> : IStreamPublishFilter<T>
{
    public Task OnPublish(PublishContext<T> context, PublishNext next)
        => next();
}

public sealed class ClassOnlyPublishFilter<T> : IStreamPublishFilter<T> where T : class
{
    public Task OnPublish(PublishContext<T> context, PublishNext next)
        => next();
}

public sealed class PipelineNoopFilter : IStreamPublishFilter
{
    public Task OnPublish(PublishContext context, PublishNext next)
        => next();
}

public sealed class TenantPublishFilter : IStreamPublishFilter
{
    public Task OnPublish(PublishContext context, PublishNext next)
    {
        context.Headers.Set("tenant", "acme");
        return next();
    }
}

public sealed class RemoveTenantPublishFilter : IStreamPublishFilter
{
    public Task OnPublish(PublishContext context, PublishNext next)
    {
        context.Headers.Remove("tenant");
        return next();
    }
}

public sealed class ScopedPublishFilter : IStreamPublishFilter
{
    public Task OnPublish(PublishContext context, PublishNext next)
        => next();
}

public sealed class ScopedTypedPublishFilter : IStreamPublishFilter<PipelineOrder>
{
    public Task OnPublish(PublishContext<PipelineOrder> context, PublishNext next)
        => next();
}

public sealed class BlankContentTypeSerializer : IStreamSerializer
{
    public string ContentType => " ";

    public byte[] Serialize<T>(T message) => SystemTextJsonStreamSerializer.Default.Serialize(message);

    public T Deserialize<T>(ReadOnlyMemory<byte> body) => SystemTextJsonStreamSerializer.Default.Deserialize<T>(body);
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

        await pipeline.RunAsync(NewContext(), () =>
        {
            log.Add("terminal");
            return Task.CompletedTask;
        });

        Assert.Equal(new[] { "A-in", "B-in", "C-in", "terminal", "C-out", "B-out", "A-out" }, log);
    }

    [Fact]
    public async Task Zero_next_is_rejected_as_a_filter_contract_violation()
    {
        var pipeline = new PublishPipeline(new IStreamPublishFilter[] { new ZeroNextFilter() });

        var ex = await Assert.ThrowsAsync<StreamBusFilterContractException>(() =>
            pipeline.RunAsync(NewContext(), () => Task.CompletedTask));

        Assert.Contains(nameof(ZeroNextFilter), ex.Message, StringComparison.Ordinal);
        Assert.Contains("returned without calling next", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Double_next_is_rejected_as_a_filter_contract_violation()
    {
        var pipeline = new PublishPipeline(new IStreamPublishFilter[] { new DoubleNextFilter() });
        var terminalCalls = 0;

        var ex = await Assert.ThrowsAsync<StreamBusFilterContractException>(() =>
            pipeline.RunAsync(NewContext(), () =>
            {
                Interlocked.Increment(ref terminalCalls);
                return Task.CompletedTask;
            }));

        Assert.Contains(nameof(DoubleNextFilter), ex.Message, StringComparison.Ordinal);
        Assert.Contains("invoked next 2 times", ex.Message, StringComparison.Ordinal);
        Assert.Equal(1, terminalCalls);
    }

    [Fact]
    public async Task Fire_and_forget_next_is_rejected_as_a_filter_contract_violation()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = new PublishPipeline(new IStreamPublishFilter[] { new FireAndForgetNextFilter() });

        var ex = await Assert.ThrowsAsync<StreamBusFilterContractException>(() =>
            pipeline.RunAsync(NewContext(), () => release.Task));

        Assert.Contains(nameof(FireAndForgetNextFilter), ex.Message, StringComparison.Ordinal);
        Assert.Contains("before its continuation completed", ex.Message, StringComparison.Ordinal);
        Assert.False(release.Task.IsCompleted, "the escaped continuation must not be awaited by the pipeline");
        release.SetResult();
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
    public void Header_writer_merges_call_level_properties_and_filter_set_wins()
    {
        var callLevel = new StreamMessageProperties
        {
            MessageId = Guid.Parse("0f8fad5b-d9cb-469f-a165-70867728950e"),
            Headers = new Dictionary<string, object?>
            {
                ["tenant"] = "call-level",
                ["call-only"] = true,
            },
        };
        var writer = new StreamSendHeaders(callLevel.Headers, SystemTextJsonStreamSerializer.Default);
        writer.Set("tenant", "filter-level");
        writer.Set("filter-only", 7);

        var merged = writer.Materialize(callLevel);
        Assert.NotNull(merged);
        Assert.Equal(callLevel.MessageId, merged.MessageId);
        Assert.Equal("filter-level", merged.Headers["tenant"]);
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
    public async Task Filter_exception_surfaces_unwrapped_and_is_recorded_on_activity()
    {
        var filter = new ThrowingFilter();
        using var capture = StartCapturingPublishActivities("orders");
        try
        {
            await using var bus = await TestBus.StartAsync(cfg =>
            {
                cfg.Publish<PipelineOrder>("orders");
                cfg.UsePublishFilter(filter);
            }, TestContext.Current.CancellationToken);

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                bus.Bus.PublishAsync(new PipelineOrder("1"), TestContext.Current.CancellationToken));

            Assert.Equal("filter boom", ex.Message);
            Assert.Null(ex.InnerException);
            Assert.Equal(1, filter.Calls);
            Assert.Empty(bus.System.Publishes);
            AssertPublishActivityHasExceptionEvent(capture, typeof(InvalidOperationException).FullName!);
        }
        finally
        {
            capture.Dispose();
        }
    }

    [Fact]
    public async Task Foreign_cancelled_exception_from_filter_is_not_wrapped_as_transient()
    {
        using var filter = new ForeignCancelFilter();
        using var capture = StartCapturingPublishActivities("orders");
        try
        {
            await using var bus = await TestBus.StartAsync(cfg =>
            {
                cfg.Publish<PipelineOrder>("orders");
                cfg.UsePublishFilter(filter);
            }, TestContext.Current.CancellationToken);

            var ex = await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                bus.Bus.PublishAsync(new PipelineOrder("1"), TestContext.Current.CancellationToken));

            Assert.IsNotType<StreamBusPublishTransientException>(ex);
            Assert.Equal(filter.Own.Token, ex.CancellationToken);
            Assert.Empty(bus.System.Publishes);
            // Cancellation is never recorded as a failure on the publish activity.
            Assert.Null(GetPublishExceptionEvent(capture));
        }
        finally
        {
            capture.Dispose();
        }
    }

    [Fact]
    public async Task Transport_failure_without_filters_is_recorded_on_publish_activity()
    {
        using var capture = StartCapturingPublishActivities("orders");
        try
        {
            await using var bus = await TestBus.StartAsync(cfg =>
            {
                cfg.Publish<PipelineOrder>("orders");
            }, TestContext.Current.CancellationToken);
            bus.System.SendFailure = new IOException("transport boom");

            var ex = await Assert.ThrowsAsync<StreamBusPublishTransientException>(() =>
                bus.Bus.PublishAsync(new PipelineOrder("1"), TestContext.Current.CancellationToken));

            Assert.IsType<IOException>(ex.InnerException);
            AssertPublishActivityHasExceptionEvent(
                capture, typeof(StreamBusPublishTransientException).FullName!);
        }
        finally
        {
            capture.Dispose();
        }
    }

    [Fact]
    public async Task Filter_headers_do_not_change_routing_key_or_body()
    {
        await using var bus = await TestBus.StartAsync(cfg =>
        {
            cfg.PublishSuper<PipelineOrder>("events", s => s.Partitions(1));
            cfg.UsePublishFilter(new HeaderOnlyFilter());
        }, TestContext.Current.CancellationToken);

        await bus.Bus.PublishSuperAsync(new PipelineOrder("original"), "original-key", TestContext.Current.CancellationToken);

        var captured = Assert.Single(bus.System.Publishes);
        Assert.True(captured.Options.IsSuperStream);
        Assert.Equal("original-key", captured.Options.RoutingKey);
        Assert.Contains("original", System.Text.Encoding.UTF8.GetString(captured.Body), StringComparison.Ordinal);
        Assert.DoesNotContain("hacked", System.Text.Encoding.UTF8.GetString(captured.Body), StringComparison.Ordinal);
        Assert.NotNull(captured.Options.Properties);
        Assert.Equal("1", captured.Options.Properties.Headers["x-filter"]);
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

    [Fact]
    public async Task Typed_publish_filter_runs_only_for_its_message_type()
    {
        var probe = new TypedPublishProbe();
        await using var bus = await TestBus.StartAsync(cfg =>
        {
            cfg.Publish<PipelineOrder>("orders");
            cfg.Publish<PipelineInvoice>("invoices");
            cfg.UsePublishFilter<OrderOnlyPublishFilter>();
        }, TestContext.Current.CancellationToken, s => s.AddSingleton(probe));

        await bus.Bus.PublishAsync(new PipelineOrder("o-1"), TestContext.Current.CancellationToken);
        await bus.Bus.PublishAsync(new PipelineInvoice("i-1"), TestContext.Current.CancellationToken);

        var order = Assert.Single(bus.System.Publishes, p => p.Stream == "orders");
        var invoice = Assert.Single(bus.System.Publishes, p => p.Stream == "invoices");
        Assert.Equal("o-1", order.Options.Properties!.Headers["order"]);
        Assert.False(invoice.Options.Properties?.Headers.ContainsKey("order") ?? false);
        Assert.Equal(1, probe.Calls);
    }

    [Fact]
    public async Task Multi_interface_publish_filter_uses_one_instance_across_message_types()
    {
        var probe = new MultiMessagePublishProbe();
        await using var bus = await TestBus.StartAsync(cfg =>
        {
            cfg.Publish<PipelineOrder>("orders");
            cfg.Publish<PipelineInvoice>("invoices");
            cfg.UsePublishFilter<MultiMessagePublishFilter>();
        }, TestContext.Current.CancellationToken, s => s.AddSingleton(probe));

        await bus.Bus.PublishAsync(new PipelineOrder("o-1"), TestContext.Current.CancellationToken);
        await bus.Bus.PublishAsync(new PipelineInvoice("i-1"), TestContext.Current.CancellationToken);

        Assert.Equal(1, probe.Instances);
        Assert.Equal(2, probe.Calls);
        Assert.Equal(2, bus.System.Publishes.Count);
    }

    [Fact]
    public async Task Universal_and_typed_publish_filters_preserve_registration_order()
    {
        var log = new PublishPipelineLog();
        await using var bus = await TestBus.StartAsync(cfg =>
        {
            cfg.Publish<PipelineOrder>("orders");
            cfg.Publish<PipelineInvoice>("invoices");
            cfg.UsePublishFilter<PipelineLogAFilter>();
            cfg.UsePublishFilter<OrderLogPublishFilter>();
            cfg.UsePublishFilter<PipelineLogCFilter>();
        }, TestContext.Current.CancellationToken, s => s.AddSingleton(log));
        bus.System.Log = log;

        await bus.Bus.PublishAsync(new PipelineInvoice("i-1"), TestContext.Current.CancellationToken);

        Assert.Equal(new[] { "A-in", "C-in", "terminal", "C-out", "A-out" }, log.Entries);

        log.Clear();
        await bus.Bus.PublishAsync(new PipelineOrder("o-1"), TestContext.Current.CancellationToken);

        Assert.Equal(
            new[] { "A-in", "typed-in", "C-in", "terminal", "C-out", "typed-out", "A-out" },
            log.Entries);
    }

    [Fact]
    public async Task Universal_and_typed_publish_filters_share_one_context_instance()
    {
        var probe = new SharedPublishContextProbe();
        await using var bus = await TestBus.StartAsync(cfg =>
        {
            cfg.Publish<PipelineOrder>("orders");
            cfg.UsePublishFilter<SharedUniversalPublishFilter>();
            cfg.UsePublishFilter<SharedTypedPublishFilter>();
        }, TestContext.Current.CancellationToken, s => s.AddSingleton(probe));

        var message = new PipelineOrder("p-1");
        await bus.Bus.PublishAsync(message, TestContext.Current.CancellationToken);

        Assert.Same(probe.UniversalContext, probe.TypedContext);
        Assert.NotNull(probe.UniversalContext);
        Assert.Same(message, probe.UniversalContext.Payload);
        Assert.Equal("p-1", Assert.IsType<PipelineOrder>(probe.TypedContext!.Payload).Id);
    }

    [Fact]
    public async Task Publish_context_carries_generated_identity_and_sent_time()
    {
        var probe = new SentContextProbe();
        await using var bus = await TestBus.StartAsync(cfg =>
        {
            cfg.Publish<PipelineOrder>("orders");
            cfg.UsePublishFilter<SentProbeFilter>();
        }, TestContext.Current.CancellationToken, s => s.AddSingleton(probe));

        await bus.Bus.PublishAsync(new PipelineOrder("p-1"), TestContext.Current.CancellationToken);

        var context = Assert.IsType<PublishContext<PipelineOrder>>(probe.Context);
        Assert.NotEqual(Guid.Empty, context.MessageId);
        Assert.Null(context.CorrelationId);
        Assert.Equal(DateTimeKind.Utc, context.SentTime.Kind);
        Assert.Equal(0, context.SentTime.Ticks % TimeSpan.TicksPerMillisecond);

        var sent = Assert.Single(bus.System.Publishes);
        Assert.Equal(context.MessageId, sent.Options.Properties!.MessageId);
        Assert.Equal(context.SentTime, sent.Options.Properties.SentTime);
        Assert.Equal("application/json", sent.Options.Properties.ContentType);
    }

    [Fact]
    public async Task Supplied_message_id_and_sent_time_are_preserved()
    {
        var probe = new SentContextProbe();
        await using var bus = await TestBus.StartAsync(cfg =>
        {
            cfg.Publish<PipelineOrder>("orders");
            cfg.UsePublishFilter<SentProbeFilter>();
        }, TestContext.Current.CancellationToken, s => s.AddSingleton(probe));

        var messageId = Guid.NewGuid();
        var correlationId = Guid.NewGuid();
        var sentTime = new DateTime(2026, 10, 31, 9, 12, 3, 284, DateTimeKind.Utc);
        await bus.Bus.PublishAsync(
            new PipelineOrder("p-1"),
            new PublishOptions
            {
                Properties = new StreamMessageProperties
                {
                    MessageId = messageId,
                    CorrelationId = correlationId,
                    SentTime = sentTime,
                },
            },
            TestContext.Current.CancellationToken);

        var context = Assert.IsType<PublishContext<PipelineOrder>>(probe.Context);
        Assert.Equal(messageId, context.MessageId);
        Assert.Equal(correlationId, context.CorrelationId);
        Assert.Equal(sentTime, context.SentTime);

        var sent = Assert.Single(bus.System.Publishes);
        Assert.Equal(messageId, sent.Options.Properties!.MessageId);
        Assert.Equal(sentTime, sent.Options.Properties.SentTime);
    }

    [Fact]
    public async Task Publish_context_rejects_reserved_headers_from_options()
    {
        await using var bus = await TestBus.StartAsync(
            cfg => cfg.Publish<PipelineOrder>("orders"), TestContext.Current.CancellationToken);

        var options = new PublishOptions
        {
            Properties = new StreamMessageProperties
            {
                Headers = new Dictionary<string, object?> { ["streambus.message-id"] = "spoofed" },
            },
        };

        var ex = await Assert.ThrowsAsync<ArgumentException>(() =>
            bus.Bus.PublishAsync(new PipelineOrder("p-1"), options, TestContext.Current.CancellationToken));
        Assert.Contains("streambus.message-id", ex.Message, StringComparison.Ordinal);
        Assert.Empty(bus.System.Publishes);
    }

    [Fact]
    public async Task Publish_filter_can_remove_a_call_level_header()
    {
        await using var bus = await TestBus.StartAsync(cfg =>
        {
            cfg.Publish<PipelineOrder>("orders");
            cfg.UsePublishFilter<RemoveTenantPublishFilter>();
        }, TestContext.Current.CancellationToken);

        await bus.Bus.PublishAsync(
            new PipelineOrder("p-1"),
            new PublishOptions
            {
                Properties = new StreamMessageProperties
                {
                    Headers = new Dictionary<string, object?> { ["tenant"] = "call-level", ["keep"] = true },
                },
            },
            TestContext.Current.CancellationToken);

        var sent = Assert.Single(bus.System.Publishes);
        Assert.False(sent.Options.Properties!.Headers.ContainsKey("tenant"));
        Assert.Equal(true, sent.Options.Properties.Headers["keep"]);
    }

    [Fact]
    public async Task Blank_serializer_content_type_is_rejected_at_bus_construction()
    {
        await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            TestBus.StartAsync(
                cfg => cfg.Serializer(new BlankContentTypeSerializer()),
                TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Explicit_stream_publish_uses_the_declared_message_type()
    {
        var probe = new TypedPublishProbe();
        await using var bus = await TestBus.StartAsync(
            cfg => cfg.UsePublishFilter<OrderOnlyPublishFilter>(),
            TestContext.Current.CancellationToken,
            s => s.AddSingleton(probe));

        await bus.Bus.PublishAsync("manual", new PipelineOrder("o-2"), TestContext.Current.CancellationToken);
        await bus.Bus.PublishAsync("manual", new PipelineInvoice("i-2"), TestContext.Current.CancellationToken);

        Assert.Collection(
            bus.System.Publishes,
            order =>
            {
                Assert.Equal("manual", order.Stream);
                Assert.Equal("o-2", order.Options.Properties!.Headers["order"]);
            },
            invoice =>
            {
                Assert.Equal("manual", invoice.Stream);
                Assert.False(invoice.Options.Properties?.Headers.ContainsKey("order") ?? false);
            });
        Assert.Equal(1, probe.Calls);
    }

    [Fact]
    public async Task Publish_context_carries_the_published_payload()
    {
        var probe = new PublishContextProbe();
        await using var bus = await TestBus.StartAsync(cfg =>
        {
            cfg.Publish<PipelineBase>("bases");
            cfg.UsePublishFilter<ContextProbePublishFilter>();
        }, TestContext.Current.CancellationToken, s => s.AddSingleton(probe));

        await bus.Bus.PublishAsync<PipelineBase>(
            new PipelineDerived("d-1"),
            TestContext.Current.CancellationToken);

        Assert.Equal(1, probe.Calls);
        Assert.IsType<PipelineDerived>(probe.Payload);
        Assert.Single(bus.System.Publishes);
    }

    [Fact]
    public async Task Open_generic_constraint_violation_is_a_configuration_error()
    {
        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            TestBus.StartAsync(cfg =>
            {
                cfg.Publish<PipelineStruct>("structs");
                cfg.UsePublishFilter(typeof(ClassOnlyPublishFilter<>));
            }, TestContext.Current.CancellationToken));

        Assert.Contains(nameof(ClassOnlyPublishFilter<>), ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(PipelineStruct), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Unregistered_explicit_message_type_builds_its_pipeline_lazily()
    {
        await using var bus = await TestBus.StartAsync(
            cfg => cfg.UsePublishFilter(typeof(ClassOnlyPublishFilter<>)),
            TestContext.Current.CancellationToken);

        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            bus.Bus.PublishAsync("manual", new PipelineStruct(1), TestContext.Current.CancellationToken));

        Assert.Contains(nameof(ClassOnlyPublishFilter<>), ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(PipelineStruct), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Open_generic_and_closed_registration_collision_is_rejected_at_bus_creation()
    {
        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            TestBus.StartAsync(cfg =>
            {
                cfg.Publish<PipelineOrder>("orders");
                cfg.UsePublishFilter(typeof(OpenGenericPublishFilter<>));
                cfg.UsePublishFilter(typeof(OpenGenericPublishFilter<PipelineOrder>));
            }, TestContext.Current.CancellationToken));

        Assert.Contains("already registered on this bus", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Missing_filter_registration_fails_at_bus_creation()
    {
        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            TestBus.StartAsync(
                cfg =>
                {
                    cfg.Publish<PipelineOrder>("orders");
                    cfg.UsePublishFilter<TenantPublishFilter>();
                },
                TestContext.Current.CancellationToken,
                afterAddStreamBus: services => services.RemoveAll<TenantPublishFilter>()));

        Assert.Contains("could not be resolved from the root service provider", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Publish_filter_types_are_registered_as_singletons()
    {
        var services = new ServiceCollection();
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
            cfg.Publish<PipelineOrder>("orders");
            cfg.UsePublishFilter<TenantPublishFilter>();
            cfg.UsePublishFilter<OrderOnlyPublishFilter, PipelineOrder>();
        });

        var universal = services.Single(d => d.ServiceType == typeof(TenantPublishFilter));
        var typed = services.Single(d => d.ServiceType == typeof(OrderOnlyPublishFilter));
        Assert.Equal(ServiceLifetime.Singleton, universal.Lifetime);
        Assert.Equal(ServiceLifetime.Singleton, typed.Lifetime);
    }

    [Fact]
    public async Task Scoped_publish_filter_is_rejected_at_bus_construction()
    {
        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            TestBus.StartAsync(
                cfg =>
                {
                    cfg.Publish<PipelineOrder>("orders");
                    cfg.UsePublishFilter<ScopedPublishFilter>();
                },
                TestContext.Current.CancellationToken,
                configureServices: services => services.AddScoped<ScopedPublishFilter>()));

        Assert.Contains(nameof(ScopedPublishFilter), ex.Message, StringComparison.Ordinal);
        Assert.Contains("singleton", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Transient_publish_filter_is_rejected_at_bus_construction()
    {
        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            TestBus.StartAsync(
                cfg =>
                {
                    cfg.Publish<PipelineOrder>("orders");
                    cfg.UsePublishFilter<TenantPublishFilter>();
                },
                TestContext.Current.CancellationToken,
                configureServices: services => services.AddTransient<TenantPublishFilter>()));

        Assert.Contains(nameof(TenantPublishFilter), ex.Message, StringComparison.Ordinal);
        Assert.Contains("singleton", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Scoped_typed_publish_filter_is_rejected_at_bus_construction()
    {
        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            TestBus.StartAsync(
                cfg =>
                {
                    cfg.Publish<PipelineOrder>("orders");
                    cfg.UsePublishFilter<ScopedTypedPublishFilter, PipelineOrder>();
                },
                TestContext.Current.CancellationToken,
                configureServices: services => services.AddScoped<ScopedTypedPublishFilter>()));

        Assert.Contains(nameof(ScopedTypedPublishFilter), ex.Message, StringComparison.Ordinal);
        Assert.Contains("singleton", ex.Message, StringComparison.Ordinal);
    }

    private static PublishContext NewContext() => new()
    {
        Stream = "orders",
        RoutingKey = null,
        Payload = new PipelineOrder("1"),
        MessageId = Guid.NewGuid(),
        SentTime = DateTime.UtcNow,
        Headers = new StreamSendHeaders(null, SystemTextJsonStreamSerializer.Default),
        Services = EmptyProvider.Instance,
        CancellationToken = TestContext.Current.CancellationToken,
    };

    private sealed class RecordingFilter(string name, List<string> log) : IStreamPublishFilter
    {
        public async Task OnPublish(PublishContext context, PublishNext next)
        {
            log.Add($"{name}-in");
            await next();
            log.Add($"{name}-out");
        }
    }

    private sealed class ZeroNextFilter : IStreamPublishFilter
    {
        public Task OnPublish(PublishContext context, PublishNext next)
            => Task.CompletedTask;
    }

    private sealed class DoubleNextFilter : IStreamPublishFilter
    {
        public async Task OnPublish(PublishContext context, PublishNext next)
        {
            await next();
            await next();
        }
    }

    private sealed class FireAndForgetNextFilter : IStreamPublishFilter
    {
        public Task OnPublish(PublishContext context, PublishNext next)
        {
            _ = next();
            return Task.CompletedTask;
        }
    }

    private sealed class ThrowingFilter : IStreamPublishFilter
    {
        public int Calls { get; private set; }

        public Task OnPublish(PublishContext context, PublishNext next)
        {
            Calls++;
            throw new InvalidOperationException("filter boom");
        }
    }

    private sealed class ForeignCancelFilter : IStreamPublishFilter, IDisposable
    {
        public CancellationTokenSource Own { get; } = new();

        public Task OnPublish(PublishContext context, PublishNext next)
        {
            Own.Cancel();
            Own.Token.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public void Dispose() => Own.Dispose();
    }

    private sealed record PublishActivityCapture(
        List<Activity> Activities,
        string Stream,
        ActivityListener Listener) : IDisposable
    {
        public void Dispose() => Listener.Dispose();
    }

    private static PublishActivityCapture StartCapturingPublishActivities(string stream)
    {
        var activities = new List<Activity>();
        var source = StreamBusActivity.Source;
        var listener = new ActivityListener
        {
            ShouldListenTo = s => ReferenceEquals(s, source),
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => { lock (activities) activities.Add(activity); },
        };
        ActivitySource.AddActivityListener(listener);
        return new PublishActivityCapture(activities, stream, listener);
    }

    private static ActivityEvent? GetPublishExceptionEvent(PublishActivityCapture capture)
    {
        lock (capture.Activities)
        {
            // Scoped to this test's stream so unrelated publish activities
            // (startup, other test classes) cannot pollute the assertion.
            var publish = capture.Activities
                .LastOrDefault(a =>
                    a.OperationName == "streambus.publish"
                    && a.GetTagItem("streambus.stream") as string == capture.Stream);
            if (publish is null)
                return null;
            foreach (var e in publish.Events)
            {
                if (e.Name == "exception")
                    return e;
            }
            return null;
        }
    }

    private static void AssertPublishActivityHasExceptionEvent(
        PublishActivityCapture capture,
        string exceptionTypeName)
    {
        var exceptionEvent = GetPublishExceptionEvent(capture);
        Assert.NotNull(exceptionEvent);
        Assert.Equal(
            exceptionTypeName,
            exceptionEvent!.Value.Tags.FirstOrDefault(t => t.Key == "exception.type").Value as string);

        // OpenTelemetry semantic conventions: the exception event carries the
        // failure site via exception.stacktrace (exception.ToString()).
        var stackTrace = exceptionEvent!.Value.Tags.FirstOrDefault(t => t.Key == "exception.stacktrace").Value as string;
        Assert.False(string.IsNullOrWhiteSpace(stackTrace));
        Assert.Contains(exceptionTypeName, stackTrace, StringComparison.Ordinal);
    }

    private sealed class CountingFilter : IStreamPublishFilter
    {
        public int Calls { get; private set; }

        public Task OnPublish(PublishContext context, PublishNext next)
        {
            Calls++;
            return next();
        }
    }

    private sealed class HeaderOnlyFilter : IStreamPublishFilter
    {
        public Task OnPublish(PublishContext context, PublishNext next)
        {
            context.Headers.Set("x-filter", "1");
            return next();
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
            CancellationToken ct,
            Action<IServiceCollection>? configureServices = null,
            Action<IServiceCollection>? afterAddStreamBus = null)
        {
            var services = new ServiceCollection();
            services.AddLogging();
            configureServices?.Invoke(services);
            var system = new CapturingSystem();
            services.AddSingleton<IStreamSystemConnector>(new FakeConnector(system));
            services.AddStreamBus((_, cfg) =>
            {
                cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
                configure(cfg);
            });
            afterAddStreamBus?.Invoke(services);
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

    private sealed class CapturingSystem : ScriptedSystemHandle
    {
        public CapturingSystem()
        {
            OnCreatePlainProducer = (stream, _) =>
                Task.FromResult<IStreamPlainProducerHandle>(new CapturingPlainHandle(stream, this));
            OnCreateSuperProducer = (stream, _) =>
                Task.FromResult<IStreamSuperProducerHandle>(new CapturingSuperHandle(stream, this));
            OffsetResult = 0;
            SuperStreamExistsResult = _ => true;
            PartitionsResult = superStream => new[] { $"{superStream}-0" };
        }

        public readonly List<CapturedPublish> Publishes = new();
        public PublishPipelineLog? Log { get; set; }
        public Exception? SendFailure { get; set; }

        public override Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken ct)
            => throw new NotSupportedException();

        public void Add(string stream, ReadOnlyMemory<byte> body, PublishOptions options)
        {
            lock (Publishes) Publishes.Add(new CapturedPublish(stream, body.ToArray(), options));
            Log?.Add("terminal");
        }
    }

    private sealed record CapturedPublish(string Stream, byte[] Body, PublishOptions Options);

    private class CapturingPlainHandle(string stream, CapturingSystem system) : IStreamPlainProducerHandle
    {
        public Task SendAsync(ReadOnlyMemory<byte> body, PublishOptions options, CancellationToken ct)
        {
            if (system.SendFailure is { } failure)
                return Task.FromException(failure);
            system.Add(stream, body, options);
            return Task.CompletedTask;
        }

        public Task SendEnvelopeAsync(ReadOnlyMemory<byte> body, StreamMessageProperties properties, CancellationToken ct)
        {
            system.Add(stream, body, new PublishOptions());
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
    }
}
