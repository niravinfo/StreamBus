using RabbitMQ.Stream.Client;
using StreamBus;

namespace StreamBus.UnitTests;

/// <summary>
/// Pins the fail-fast guard around the <c>StreamSystem.CreateRawConsumer</c> result:
/// the client's contract returns <see cref="IConsumer"/>, but the partition loop needs
/// the concrete <see cref="RawConsumer"/> subclass (StoreOffset/Close). A client
/// version that returns a different concrete type must surface an actionable
/// <see cref="StreamBusConfigurationException"/>, not an opaque InvalidCastException
/// from inside the attach loop.
/// </summary>
public sealed class RawConsumerCastTests
{
    [Fact]
    public void Non_RawConsumer_result_fails_fast_with_actionable_configuration_error()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(
            () => RawConsumerCast.Require(new NotARawConsumer()));

        Assert.Contains(nameof(NotARawConsumer), ex.Message, StringComparison.Ordinal);
        Assert.Contains("RawConsumer", ex.Message, StringComparison.Ordinal);
        Assert.Contains("version", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Null_result_fails_fast_instead_of_throwing_NullReferenceException()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(() => RawConsumerCast.Require(null!));
        Assert.Contains("null", ex.Message, StringComparison.Ordinal);
    }

    private sealed class NotARawConsumer : IConsumer
    {
        public ConsumerInfo Info => new("not-a-raw-consumer-test-double", "stream", "ref", ["p"]);
        public Task<ResponseCode> Close() => Task.FromResult(ResponseCode.Ok);
        public void Dispose() { }
        public Task StoreOffset(ulong offset) => Task.CompletedTask;
    }
}
