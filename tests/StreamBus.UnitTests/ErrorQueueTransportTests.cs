using Microsoft.Extensions.DependencyInjection;

namespace StreamBus.UnitTests;

public sealed class ErrorQueueTransportTests
{
    private static StreamBusOptions Options() => new()
    {
        Vhost = "/",
        UserName = "app",
        Password = "secret",
    };

    private static ErrorEnvelope Envelope(ulong offset = 1) => new()
    {
        ErrorStream = "orders-errors",
        SourceStream = "orders",
        SourcePartition = 0,
        SourcePartitionStream = "orders",
        SourceOffset = offset,
        Body = new byte[] { 1, 2, 3 },
        ExceptionType = "System.InvalidOperationException",
        ExceptionMessage = "boom",
        ExceptionStack = "at Boom()",
        Attempts = 3,
        FailedAt = new DateTimeOffset(2026, 10, 15, 12, 0, 0, TimeSpan.Zero),
        MessageId = "m-1",
        CorrelationId = "c-1",
        RoutingValue = "o-1",
        ErrorRoutingValue = "orders",
    };

    private static DeadLetterOptions Queue(string name, TimeSpan? confirmTimeout = null) => new()
    {
        Name = name,
        QueueType = DeadLetterQueueType.ClassicQueue,
        QueueOptions = new ErrorQueueOptions
        {
            ConfirmTimeout = confirmTimeout ?? TimeSpan.FromSeconds(10),
        },
    };

    private static ErrorQueueTransport Transport(IErrorQueueConnection connection)
        => ErrorQueueTransport.ForTest(Options(), (_, _) => Task.FromResult(connection));

    private sealed class FakePublishChannel : IErrorQueuePublishChannel
    {
        public int PublishCount;
        public readonly List<TimeSpan> Timeouts = new();
        public Func<string, ErrorEnvelope, CancellationToken, Task>? Handler;
        public bool IsOpen { get; set; } = true;

        public async Task PublishAsync(string queue, ErrorEnvelope envelope, TimeSpan confirmTimeout, CancellationToken ct)
        {
            PublishCount++;
            Timeouts.Add(confirmTimeout);
            if (Handler is not null) await Handler(queue, envelope, ct);
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class FakeControlChannel : IErrorQueueControlChannel
    {
        public readonly Queue<ErrorQueueDelivery> Deliveries = new();
        public readonly List<ulong> Acked = new();
        public bool IsOpen { get; set; } = true;

        public Task DeclareAsync(string queue, IReadOnlyDictionary<string, object?> arguments, CancellationToken ct)
            => Task.CompletedTask;

        public Task DeclarePassiveAsync(string queue, CancellationToken ct) => Task.CompletedTask;

        public Task<ErrorQueueDelivery?> GetAsync(string queue, CancellationToken ct)
            => Task.FromResult(Deliveries.Count > 0 ? Deliveries.Dequeue() : null);

        public Task AckAsync(ulong deliveryTag, CancellationToken ct)
        {
            Acked.Add(deliveryTag);
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class FakeConnection : IErrorQueueConnection
    {
        public readonly List<FakePublishChannel> PublishChannels = new();
        public FakeControlChannel? ControlChannel;
        public int PublishChannelCreations;
        public bool IsOpen { get; set; } = true;

        public FakePublishChannel Current => PublishChannels[^1];

        public Task<IErrorQueuePublishChannel> CreatePublishChannelAsync(CancellationToken ct)
        {
            PublishChannelCreations++;
            var channel = new FakePublishChannel();
            PublishChannels.Add(channel);
            return Task.FromResult<IErrorQueuePublishChannel>(channel);
        }

        public Task<IErrorQueueControlChannel> CreateControlChannelAsync(CancellationToken ct)
            => Task.FromResult<IErrorQueueControlChannel>(
                ControlChannel ?? throw new InvalidOperationException("test did not configure a control channel"));

        public Task CloseAsync(CancellationToken ct)
        {
            IsOpen = false;
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    [Fact]
    public async Task Burst_of_2000_dead_letters_uses_one_publish_channel()
    {
        var connection = new FakeConnection();
        await using var transport = Transport(connection);
        var queue = Queue("orders-error");

        for (var i = 0; i < 2000; i++)
            await transport.PublishAsync(queue, Envelope((ulong)i), TestContext.Current.CancellationToken);

        Assert.Equal(1, connection.PublishChannelCreations);
        Assert.Equal(2000, connection.Current.PublishCount);
    }

    [Fact]
    public async Task Mid_burst_failure_leaves_the_remainder_unacked()
    {
        var connection = new FakeConnection
        {
            ControlChannel = new FakeControlChannel(),
        };
        var first = Envelope(1);
        var second = Envelope(2);
        var third = Envelope(3);
        connection.ControlChannel.Deliveries.Enqueue(new ErrorQueueDelivery(101, first.Body, first.ToHeaders()));
        connection.ControlChannel.Deliveries.Enqueue(new ErrorQueueDelivery(102, second.Body, second.ToHeaders()));
        connection.ControlChannel.Deliveries.Enqueue(new ErrorQueueDelivery(103, third.Body, third.ToHeaders()));
        await using var transport = Transport(connection);

        var republished = new List<ulong>();
        await Assert.ThrowsAsync<InvalidOperationException>(() => transport.DrainAsync(
            "orders-error",
            (envelope, _) =>
            {
                if (envelope.SourceOffset == 2) throw new InvalidOperationException("mid-burst shutdown");
                republished.Add(envelope.SourceOffset);
                return Task.CompletedTask;
            },
            TestContext.Current.CancellationToken));

        Assert.Equal(new ulong[] { 1 }, republished);
        Assert.Equal(new ulong[] { 101 }, connection.ControlChannel.Acked);
    }

    [Fact]
    public async Task Publish_channel_shutdown_recreates_the_channel_on_retry()
    {
        var connection = new FakeConnection();
        await using var transport = Transport(connection);
        await transport.PublishAsync(Queue("orders-error"), Envelope(), TestContext.Current.CancellationToken);
        connection.Current.Handler = (_, _, _) =>
        {
            connection.Current.IsOpen = false;
            throw new ObjectDisposedException("channel");
        };

        await Assert.ThrowsAsync<StreamBusTransientException>(() =>
            transport.PublishAsync(Queue("orders-error"), Envelope(), TestContext.Current.CancellationToken));

        await transport.PublishAsync(Queue("orders-error"), Envelope(2), TestContext.Current.CancellationToken);

        Assert.Equal(2, connection.PublishChannelCreations);
    }

    [Fact]
    public async Task Queue_confirm_timeout_flows_to_the_channel_and_must_be_positive()
    {
        var connection = new FakeConnection();
        await using var transport = Transport(connection);

        await transport.PublishAsync(Queue("default"), Envelope(), TestContext.Current.CancellationToken);
        await transport.PublishAsync(Queue("fast", TimeSpan.FromMilliseconds(250)), Envelope(2), TestContext.Current.CancellationToken);

        Assert.Equal(
            new[] { TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(250) },
            connection.Current.Timeouts);
        await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            transport.PublishAsync(Queue("invalid", TimeSpan.Zero), Envelope(3), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Publish_after_dispose_is_transient_not_object_disposed()
    {
        var connection = new FakeConnection();
        var transport = Transport(connection);
        await transport.DisposeAsync();

        var ex = await Assert.ThrowsAsync<StreamBusTransientException>(() =>
            transport.PublishAsync(Queue("orders-error"), Envelope(), TestContext.Current.CancellationToken));
        Assert.Contains("stopping", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    private sealed record OrderEvent(string Id);

    private sealed class OrderConsumer : IConsumer<OrderEvent>
    {
        public Task Consume(ConsumeContext<OrderEvent> context, CancellationToken ct) => Task.CompletedTask;
    }

    [Fact]
    public void DeadLetterToQueue_surfaces_the_confirm_timeout()
    {
        var services = new ServiceCollection();
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
            cfg.ReceiveEndpoint("orders", e =>
            {
                e.Consumer<OrderConsumer>("grp");
                e.UseMessageRetry(r => r.None());
                e.DeadLetterToQueue("orders-error", DeadLetterQueueType.ClassicQueue,
                    q => q.ConfirmTimeout(TimeSpan.FromMilliseconds(250)));
            });
        });

        var registration = Assert.Single(
            services.BuildServiceProvider().GetRequiredService<StreamBusConsumerStore>().Registrations);
        Assert.Equal(TimeSpan.FromMilliseconds(250), registration.Options.DeadLetter!.QueueOptions.ConfirmTimeout);

        Assert.Throws<StreamBusConfigurationException>(() =>
            new ServiceCollection().AddStreamBus((_, cfg) =>
            {
                cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
                cfg.ReceiveEndpoint("orders", e =>
                {
                    e.Consumer<OrderConsumer>("grp");
                    e.UseMessageRetry(r => r.None());
                    e.DeadLetterToQueue("orders-error", DeadLetterQueueType.ClassicQueue,
                        q => q.ConfirmTimeout(TimeSpan.Zero));
                });
            }));
    }
}
