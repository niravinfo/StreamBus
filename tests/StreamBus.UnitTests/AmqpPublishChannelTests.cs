using System.Diagnostics;
using System.Reflection;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace StreamBus.UnitTests;

public sealed class AmqpPublishChannelTests
{
    public class BlockingPublishProxy : DispatchProxy
    {
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod is null) return null;
            if (targetMethod.Name == "get_IsOpen") return true;
            if (targetMethod.Name.StartsWith("add_", StringComparison.Ordinal)
                || targetMethod.Name.StartsWith("remove_", StringComparison.Ordinal))
            {
                return null;
            }
            if (targetMethod.Name.StartsWith("BasicPublishAsync", StringComparison.Ordinal))
            {
                var ct = args?.OfType<CancellationToken>().LastOrDefault() ?? CancellationToken.None;
                return new ValueTask(Task.Delay(Timeout.InfiniteTimeSpan, ct));
            }
            if (targetMethod.ReturnType == typeof(ValueTask)) return ValueTask.CompletedTask;
            return targetMethod.ReturnType.IsValueType
                ? Activator.CreateInstance(targetMethod.ReturnType)
                : null;
        }
    }

    /// <summary>
    /// Fake confirming channel: capture the ack handler so the test decides
    /// which delivery tag the broker confirms. The publish sequence number
    /// handed to the next publish is a fixed literal (41).
    /// </summary>
    public class ConfirmingPublishProxy : DispatchProxy
    {
        public const ulong NextPublishSequenceNumber = 41;
        public AsyncEventHandler<BasicAckEventArgs>? AckHandler { get; private set; }
        public bool Published { get; private set; }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod is null) return null;
            if (targetMethod.Name == "get_IsOpen") return true;
            if (targetMethod.Name == "add_BasicAcksAsync")
            {
                AckHandler = (AsyncEventHandler<BasicAckEventArgs>)args![0]!;
                return null;
            }
            if (targetMethod.Name.StartsWith("add_", StringComparison.Ordinal)
                || targetMethod.Name.StartsWith("remove_", StringComparison.Ordinal))
            {
                return null;
            }
            if (targetMethod.Name == "GetNextPublishSequenceNumberAsync")
                return new ValueTask<ulong>(NextPublishSequenceNumber);
            if (targetMethod.Name.StartsWith("BasicPublishAsync", StringComparison.Ordinal))
            {
                Published = true;
                return new ValueTask(Task.CompletedTask);
            }
            if (targetMethod.ReturnType == typeof(ValueTask)) return ValueTask.CompletedTask;
            return targetMethod.ReturnType.IsValueType
                ? Activator.CreateInstance(targetMethod.ReturnType)
                : null;
        }
    }

    private static ErrorEnvelope Envelope() => new()
    {
        ErrorStream = "orders-errors",
        SourceStream = "orders",
        SourcePartition = 0,
        SourcePartitionStream = "orders",
        SourceOffset = 1,
        Body = new byte[] { 1, 2, 3 },
        ExceptionType = "System.InvalidOperationException",
        ExceptionMessage = "boom",
        ExceptionStack = "at Boom()",
        Attempts = 3,
        FailedAt = new DateTimeOffset(2026, 10, 15, 12, 0, 0, TimeSpan.Zero),
        MessageId = Guid.Parse("0f8fad5b-d9cb-469f-a165-70867728950e"),
        CorrelationId = Guid.Parse("6b1f5c1a-0d9e-4b6f-9a4e-1f2d3c4b5a69"),
        RoutingValue = "o-1",
    };

    [Fact]
    public async Task Confirm_budget_starts_before_the_publish_send()
    {
        var channel = DispatchProxy.Create<IChannel, BlockingPublishProxy>();
        var publish = new AmqpPublishChannel(channel);
        using var shutdown = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var started = Stopwatch.StartNew();

        await Assert.ThrowsAsync<StreamBusTimeoutException>(() =>
            publish.PublishAsync("orders-error", Envelope(), TimeSpan.FromMilliseconds(100), shutdown.Token));

        Assert.True(
            started.Elapsed < TimeSpan.FromSeconds(1.5),
            $"confirm budget did not bound the publish send (elapsed {started.Elapsed})");
    }

    [Fact]
    public async Task Ack_for_a_foreign_delivery_tag_does_not_confirm_the_publish()
    {
        var channel = DispatchProxy.Create<IChannel, ConfirmingPublishProxy>();
        var proxy = (ConfirmingPublishProxy)(object)channel;
        var publish = new AmqpPublishChannel(channel);

        var pending = publish.PublishAsync(
            "orders-error", Envelope(), TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);
        await WaitUntilProxyPublishedAsync(proxy);

        // A late ack from a previous publish carries a different delivery tag
        // and must not resolve this publish's confirm.
        await proxy.AckHandler!.Invoke(channel,
            new BasicAckEventArgs(ConfirmingPublishProxy.NextPublishSequenceNumber - 1, multiple: false, default));
        await Task.Delay(200, TestContext.Current.CancellationToken);
        Assert.False(pending.IsCompleted, "foreign-tag ack confirmed the publish");

        // Unconfirmed within the budget: the publish ends as an unknown-outcome
        // timeout — never a false confirm.
        await Assert.ThrowsAsync<StreamBusTimeoutException>(() => pending);
    }

    [Fact]
    public async Task Ack_for_the_matching_delivery_tag_confirms_the_publish()
    {
        var channel = DispatchProxy.Create<IChannel, ConfirmingPublishProxy>();
        var proxy = (ConfirmingPublishProxy)(object)channel;
        var publish = new AmqpPublishChannel(channel);

        var pending = publish.PublishAsync(
            "orders-error", Envelope(), TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await WaitUntilProxyPublishedAsync(proxy);
        await proxy.AckHandler!.Invoke(channel,
            new BasicAckEventArgs(ConfirmingPublishProxy.NextPublishSequenceNumber, multiple: false, default));

        await pending.WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
    }

    private static async Task WaitUntilProxyPublishedAsync(ConfirmingPublishProxy proxy)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        while (!proxy.Published)
        {
            await Task.Delay(10, cts.Token);
        }
    }
}
