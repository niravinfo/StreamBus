using RabbitMQ.Client;
using RabbitMQ.Client.Events;

// Analyzer noise inherent to a by-hand fake: unused field-like events and
// property stubs that return constants.
#pragma warning disable CS0067 // event is never used
#pragma warning disable CA1822 // member can be made static

namespace StreamBus.UnitTests;

// AmqpErrorQueueConnection wraps the client's IConnection behind the internal
// IErrorQueueConnection seam. ForTest fakes cover the seam, so the wrapper's own
// disposal delegation is only observable through a fake IConnection; every
// member outside disposal is inert.
public sealed class AmqpErrorQueueConnectionTests
{
    [Fact]
    public async Task DisposeAsync_disposes_the_underlying_connection_asynchronously()
    {
        var connection = new RecordingClientConnection();
        var wrapper = new AmqpErrorQueueConnection(connection);

        await wrapper.DisposeAsync();

        Assert.Equal(1, connection.DisposeAsyncCalls);
        Assert.Equal(0, connection.DisposeCalls);
    }

    private sealed class RecordingClientConnection : IConnection
    {
        public int DisposeCalls;
        public int DisposeAsyncCalls;

        public TimeSpan Heartbeat => TimeSpan.Zero;
        public bool IsOpen => true;
        public IProtocol Protocol => throw new NotSupportedException();
        public IDictionary<string, object?>? ServerProperties => throw new NotSupportedException();
        public IEnumerable<ShutdownReportEntry> ShutdownReport => throw new NotSupportedException();
        public string? ClientProvidedName => string.Empty;
        public AmqpTcpEndpoint Endpoint => throw new NotSupportedException();
        public int Port => 0;
        public int LocalPort => 0;
        public int RemotePort => 0;
        public ushort ChannelMax => 0;
        public uint FrameMax => 0;
        public IDictionary<string, object?> ClientProperties => throw new NotSupportedException();
        public ShutdownEventArgs? CloseReason => null;

        public event AsyncEventHandler<CallbackExceptionEventArgs>? CallbackExceptionAsync;
        public event AsyncEventHandler<ShutdownEventArgs>? ConnectionShutdownAsync;
        public event AsyncEventHandler<AsyncEventArgs>? RecoverySucceededAsync;
        public event AsyncEventHandler<ConnectionRecoveryErrorEventArgs>? ConnectionRecoveryErrorAsync;
        public event AsyncEventHandler<ConsumerTagChangedAfterRecoveryEventArgs>? ConsumerTagChangeAfterRecoveryAsync;
        public event AsyncEventHandler<QueueNameChangedAfterRecoveryEventArgs>? QueueNameChangedAfterRecoveryAsync;
        public event AsyncEventHandler<RecoveringConsumerEventArgs>? RecoveringConsumerAsync;
        public event AsyncEventHandler<ConnectionBlockedEventArgs>? ConnectionBlockedAsync;
        public event AsyncEventHandler<AsyncEventArgs>? ConnectionUnblockedAsync;

        public Task UpdateSecretAsync(string secret, string reason, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task CloseAsync(ushort reasonCode, string reasonText, TimeSpan timeout, bool abort, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public Task<IChannel> CreateChannelAsync(CreateChannelOptions? options = null, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public void Dispose() => DisposeCalls++;

        public ValueTask DisposeAsync()
        {
            DisposeAsyncCalls++;
            return ValueTask.CompletedTask;
        }
    }
}

#pragma warning restore CS0067
#pragma warning restore CA1822
