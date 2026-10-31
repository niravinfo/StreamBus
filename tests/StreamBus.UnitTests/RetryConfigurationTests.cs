using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace StreamBus.UnitTests;

public sealed class RetryConfigurationTests
{
    private sealed record OrderEvent(string OrderId, int Sequence);

    private sealed class OrderConsumer : IConsumer<OrderEvent>
    {
        public Task Consume(ConsumeContext<OrderEvent> context) => Task.CompletedTask;
    }

    private static ConsumeOptions EndpointOptions(Action<IReceiveEndpointConfigurator> endpoint, Action<IStreamBusConfigurator>? bus = null)
    {
        var services = new ServiceCollection();
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
            bus?.Invoke(cfg);
            cfg.ReceiveEndpoint("orders", endpoint);
        });
        var sp = services.BuildServiceProvider();
        return Assert.Single(sp.GetRequiredService<StreamBusConsumerStore>().Registrations).Options;
    }

    private static RetryPolicy Schedule(Action<IRetryConfigurator> configure)
    {
        var retry = new RetryConfigurator();
        configure(retry);
        return ((IRetryConfigurator)retry).BuildPolicy();
    }

    private static TimeSpan DelayFor(RetryPolicy policy, int attempt)
        => RetryDelays.ResolveDelay(policy, attempt, new InvalidOperationException(), NullLogger.Instance);

    [Fact]
    public void Bounded_plus_stream_destination_hands_off_to_declared_stream()
    {
        var options = EndpointOptions(e =>
        {
            e.Consumer<OrderConsumer>("grp");
            e.UseMessageRetry(r => r.Intervals(TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(2)));
            e.DeadLetterToStream("audit-errors");
        });
        Assert.Equal(3, options.Retry.MaxAttempts);
        Assert.Equal("audit-errors", options.ErrorStream?.Stream);
        Assert.Null(options.DeadLetter);
        Assert.Null(options.ExhaustionCallback);
    }

    [Fact]
    public void Bounded_plus_callback_gives_callback_ownership()
    {
        var options = EndpointOptions(e =>
        {
            e.Consumer<OrderConsumer>("grp");
            e.UseMessageRetry(r => r.Immediate(5));
            e.OnExhausted((_, _) => Task.CompletedTask);
        });
        Assert.Equal(6, options.Retry.MaxAttempts);
        Assert.NotNull(options.ExhaustionCallback);
        Assert.Null(options.ErrorStream);
        Assert.Null(options.DeadLetter);
    }

    [Fact]
    public void Bounded_plus_neither_derives_classic_error_queue()
    {
        var options = EndpointOptions(e =>
        {
            e.Consumer<OrderConsumer>("grp");
            e.UseMessageRetry(r => r.Interval(3, TimeSpan.FromMilliseconds(100)));
        });
        Assert.Equal(4, options.Retry.MaxAttempts);
        Assert.Null(options.ErrorStream);
        Assert.NotNull(options.DeadLetter);
        Assert.Equal("orders_error", options.DeadLetter.Name);
        Assert.Equal(DeadLetterQueueType.ClassicQueue, options.DeadLetter!.QueueType);
    }

    [Fact]
    public void Unbounded_default_is_strict_forever()
    {
        var options = EndpointOptions(e => e.Consumer<OrderConsumer>("grp"));
        Assert.Null(options.Retry.MaxAttempts);
        Assert.Null(options.ErrorStream);
        Assert.Null(options.DeadLetter);
        Assert.Null(options.ExhaustionCallback);
    }

    [Fact]
    public void Unbounded_plus_destination_is_config_error()
    {
        var services = new ServiceCollection();
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus((_, cfg) =>
            {
                cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
                cfg.ReceiveEndpoint("orders", e =>
                {
                    e.Consumer<OrderConsumer>("grp");
                    e.DeadLetterToStream("audit-errors");
                });
            }));
        Assert.Contains("bound", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Unbounded_plus_callback_is_config_error()
    {
        var services = new ServiceCollection();
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus((_, cfg) =>
            {
                cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
                cfg.ReceiveEndpoint("orders", e =>
                {
                    e.Consumer<OrderConsumer>("grp");
                    e.OnExhausted((_, _) => Task.CompletedTask);
                });
            }));
        Assert.Contains("bound", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void None_hands_off_immediately_to_derived_default()
    {
        var options = EndpointOptions(e =>
        {
            e.Consumer<OrderConsumer>("grp");
            e.UseMessageRetry(r => r.None());
        });
        Assert.Equal(1, options.Retry.MaxAttempts);
        Assert.Equal("orders_error", options.DeadLetter?.Name);
    }

    [Fact]
    public void Callback_plus_destination_shadows_with_flag()
    {
        var options = EndpointOptions(e =>
        {
            e.Consumer<OrderConsumer>("grp");
            e.UseMessageRetry(r => r.Immediate(1));
            e.DeadLetterToStream("audit-errors");
            e.OnExhausted((_, _) => Task.CompletedTask);
        });
        Assert.NotNull(options.ExhaustionCallback);
        Assert.Null(options.ErrorStream);
        Assert.Equal("audit-errors", options.ShadowedDeadLetter?.Name);
    }

    [Fact]
    public void Endpoint_retry_overrides_global()
    {
        var options = EndpointOptions(
            e =>
            {
                e.Consumer<OrderConsumer>("grp");
                e.UseMessageRetry(r => r.Immediate(0));
            },
            cfg => cfg.UseMessageRetry(r => r.Intervals(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2))));
        Assert.Equal(1, options.Retry.MaxAttempts);
    }

    [Fact]
    public void Endpoint_inherits_global_when_unset()
    {
        var options = EndpointOptions(
            e => e.Consumer<OrderConsumer>("grp"),
            cfg => cfg.UseMessageRetry(r => r.Intervals(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(10))));
        Assert.Equal(4, options.Retry.MaxAttempts);
    }

    [Fact]
    public void Schedule_mappings_produce_exact_delays()
    {
        var immediate = Schedule(r => r.Immediate(2));
        Assert.Equal(3, immediate.MaxAttempts);
        Assert.Equal(TimeSpan.Zero, DelayFor(immediate, 1));
        Assert.Equal(TimeSpan.Zero, DelayFor(immediate, 3));

        var interval = Schedule(r => r.Interval(2, TimeSpan.FromMilliseconds(250)));
        Assert.Equal(3, interval.MaxAttempts);
        Assert.Equal(TimeSpan.FromMilliseconds(250), DelayFor(interval, 1));
        Assert.Equal(TimeSpan.FromMilliseconds(250), DelayFor(interval, 3));

        var intervals = Schedule(r => r.Intervals(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(200)));
        Assert.Equal(3, intervals.MaxAttempts);
        Assert.Equal(TimeSpan.FromMilliseconds(100), DelayFor(intervals, 1));
        Assert.Equal(TimeSpan.FromMilliseconds(200), DelayFor(intervals, 2));
        Assert.Equal(TimeSpan.FromMilliseconds(200), DelayFor(intervals, 9));

        var exponential = Schedule(r => r.Exponential(2, TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(10), 2.0));
        Assert.Equal(3, exponential.MaxAttempts);
        Assert.Equal(TimeSpan.FromMilliseconds(100), exponential.BaseDelay);
        Assert.Equal(TimeSpan.FromSeconds(10), exponential.MaxDelay);
        Assert.Equal(2.0, exponential.Multiplier);
        Assert.Equal(TimeSpan.Zero, exponential.MaxJitter);

        var none = Schedule(r => r.None());
        Assert.Equal(1, none.MaxAttempts);
    }

    [Fact]
    public void DeadLetterTo_requires_name_and_known_type()
    {
        var services = new ServiceCollection();
        Assert.ThrowsAny<ArgumentException>(() =>
            services.AddStreamBus((_, cfg) =>
            {
                cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
                cfg.ReceiveEndpoint("orders", e =>
                {
                    e.Consumer<OrderConsumer>("grp");
                    e.DeadLetterToStream("  ");
                });
            }));
    }

    private static ExhaustedContext CallbackContext(IServiceProvider services) => new()
    {
        Stream = "orders",
        Partition = 0,
        Offset = 7,
        Body = new ReadOnlyMemory<byte>(new byte[] { 1 }),
        Headers = new StreamHeaders(new Dictionary<string, object?>(), SystemTextJsonStreamSerializer.Default),
        Attempts = 3,
        Exception = new InvalidOperationException("boom"),
        FailedAt = DateTimeOffset.UtcNow,
        Services = services,
    };

    [Fact]
    public async Task Callback_return_commits_exactly_once()
    {
        await using var sp = new ServiceCollection().BuildServiceProvider();
        var commits = 0;
        var recycles = 0;
        var advanced = await ExhaustionCallbackRunner.RunAsync(
            (_, _) => Task.CompletedTask,
            CallbackContext(sp),
            () => { commits++; return Task.CompletedTask; },
            _ => recycles++,
            TestContext.Current.CancellationToken);
        Assert.True(advanced);
        Assert.Equal(1, commits);
        Assert.Equal(0, recycles);
    }

    [Fact]
    public async Task Callback_throw_recycles_without_committing()
    {
        await using var sp = new ServiceCollection().BuildServiceProvider();
        var commits = 0;
        Exception? recycled = null;
        var boom = new InvalidOperationException("callback boom");
        var advanced = await ExhaustionCallbackRunner.RunAsync(
            (_, _) => Task.FromException(boom),
            CallbackContext(sp),
            () => { commits++; return Task.CompletedTask; },
            ex => recycled = ex,
            TestContext.Current.CancellationToken);
        Assert.False(advanced);
        Assert.Equal(0, commits);
        Assert.Same(boom, recycled);
    }

    [Fact]
    public async Task Callback_cancellation_propagates()
    {
        await using var sp = new ServiceCollection().BuildServiceProvider();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var commits = 0;
        var recycles = 0;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            ExhaustionCallbackRunner.RunAsync(
                (_, ct) => Task.FromCanceled(ct),
                CallbackContext(sp),
                () => { commits++; return Task.CompletedTask; },
                _ => recycles++,
                cts.Token));
        Assert.Equal(0, commits);
        Assert.Equal(0, recycles);
    }

    [Fact]
    public async Task Exhausted_context_resolves_services()
    {
        var services = new ServiceCollection();
        services.AddSingleton("hello");
        await using var sp = services.BuildServiceProvider();
        var ctx = CallbackContext(sp);
        Assert.Equal("hello", ctx.GetService<string>());
        Assert.Throws<StreamBusConfigurationException>(() => ctx.GetService<int>());
    }
}
