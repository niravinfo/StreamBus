using System.Diagnostics;
using System.Reflection;
using RabbitMQ.Client;

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
        MessageId = "m-1",
        CorrelationId = "c-1",
        RoutingValue = "o-1",
        ErrorRoutingValue = "orders",
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
}
