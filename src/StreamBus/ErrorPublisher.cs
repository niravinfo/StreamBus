using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace StreamBus;

internal sealed class ErrorPublisher
{
    private readonly StreamProducerRegistry _producers;
    private readonly ILogger<ErrorPublisher> _logger;
    private readonly ErrorQueueTransport? _queues;

    public ErrorPublisher(StreamProducerRegistry producers, ILogger<ErrorPublisher>? logger = null, ErrorQueueTransport? queues = null)
    {
        ArgumentNullException.ThrowIfNull(producers);
        _producers = producers;
        _logger = logger ?? NullLogger<ErrorPublisher>.Instance;
        _queues = queues;
    }

    public Task PublishHandoffAsync(string errorStream, ErrorEnvelope envelope, TimeSpan confirmTimeout, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(errorStream);
        ArgumentNullException.ThrowIfNull(envelope);
        LogSendHeaderCollisions(envelope);
        var properties = new StreamMessageProperties
        {
            MessageId = envelope.MessageId,
            CorrelationId = envelope.CorrelationId,
            SentTime = envelope.SentTime,
            Headers = envelope.ToApplicationProperties(),
        };
        return _producers.PublishEnvelopeAsync(errorStream, envelope.Body, properties, confirmTimeout, ct);
    }

    public Task PublishQueueHandoffAsync(DeadLetterOptions queue, ErrorEnvelope envelope, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(queue);
        ArgumentNullException.ThrowIfNull(envelope);
        if (_queues is null)
            throw new StreamBusConfigurationException("Queue dead-letter destinations require an error-queue transport (internal error).");
        LogSendHeaderCollisions(envelope);
        return _queues.PublishAsync(queue, envelope, ct);
    }

    private void LogSendHeaderCollisions(ErrorEnvelope envelope)
    {
        if (envelope.SendHeaders is null)
            return;
        var envelopeKeys = envelope.EnvelopeApplicationProperties();
        foreach (var key in envelope.SendHeaders.Keys)
        {
            if (envelopeKeys.ContainsKey(key))
            {
                _logger.LogWarning(
                    "Consume output header '{HeaderKey}' collides with error-envelope field '{EnvelopeKey}'; the envelope field wins.",
                    key, key);
            }
        }
    }

    // Orchestration with injectable transport (unit-testable without a broker):
    // retry the confirmed publish until it lands or ct fires, then run the
    // advance-store exactly once. Never advances past an unconfirmed handoff.
    public async Task HandOffAsync(
        ErrorEnvelope envelope,
        Func<ErrorEnvelope, CancellationToken, Task> publish,
        Func<Task> commitStore,
        Func<int, Exception, TimeSpan> delayFor,
        CancellationToken ct,
        TimeSpan? confirmTimeout = null)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        ArgumentNullException.ThrowIfNull(publish);
        ArgumentNullException.ThrowIfNull(commitStore);
        ArgumentNullException.ThrowIfNull(delayFor);
        var attempt = 0;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            attempt++;
            try
            {
                await publish(envelope, ct).ConfigureAwait(false);
                break;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "Error-stream handoff publish failed for {Source} offset {Offset} (attempt {Attempt}, timeout={Timeout}s); retrying without advancing.",
                    envelope.SourcePartitionStream, envelope.SourceOffset, attempt,
                    confirmTimeout?.TotalSeconds);
                var delay = delayFor(attempt, ex);
                if (delay < TimeSpan.Zero) delay = TimeSpan.Zero;
                await Task.Delay(delay, ct).ConfigureAwait(false);
            }
        }
        await commitStore().ConfigureAwait(false);
    }
}
