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

    public Task PublishHandoffAsync(string errorStream, ErrorEnvelope envelope, TimeSpan confirmTimeout, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(errorStream);
        ArgumentNullException.ThrowIfNull(envelope);
        LogSendHeaderCollisions(envelope);
        var properties = new StreamMessageProperties
        {
            MessageId = envelope.MessageId,
            CorrelationId = envelope.CorrelationId,
            SentTime = envelope.SentTime,
            ContentType = envelope.ContentType,
            Headers = envelope.ToApplicationProperties(),
        };
        return _producers.PublishEnvelopeAsync(errorStream, envelope.Body, properties, confirmTimeout, cancellationToken);
    }

    public Task PublishQueueHandoffAsync(DeadLetterOptions queue, ErrorEnvelope envelope, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(queue);
        ArgumentNullException.ThrowIfNull(envelope);
        if (_queues is null)
            throw new StreamBusConfigurationException("Queue dead-letter destinations require an error-queue transport (internal error).");
        LogSendHeaderCollisions(envelope);
        return _queues.PublishAsync(queue, envelope, cancellationToken);
    }

    private void LogSendHeaderCollisions(ErrorEnvelope envelope)
    {
        // EnvelopeApplicationProperties allocates the 14-entry metadata
        // dictionary; skip it entirely when no Warning sink is attached.
        if (envelope.SendHeaders is null || !_logger.IsEnabled(LogLevel.Warning))
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

    // Publish failures that can never be fixed by retrying the handoff: the
    // supervisor/AMQP transport latches fatal state (no in-process recovery) and
    // a configuration error is permanent. Transient and topology failures (a
    // missing error stream, not-ready, timeouts) keep retrying: the message must
    // never advance past an unconfirmed handoff.
    private static bool IsFatalHandoffFailure(Exception ex)
        => ex is StreamBusFatalException or StreamBusConfigurationException
            || StreamBusErrorClassification.IsFatal(ex);

    // Orchestration with injectable transport (unit-testable without a broker):
    // retry the confirmed publish until it lands or cancellationToken fires, then run the
    // advance-store exactly once. Never advances past an unconfirmed handoff.
    public async Task HandOffAsync(
        ErrorEnvelope envelope,
        Func<ErrorEnvelope, CancellationToken, Task> publish,
        Func<Task> commitStore,
        Func<int, Exception, TimeSpan> delayFor,
        CancellationToken cancellationToken,
        TimeSpan? confirmTimeout = null)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        ArgumentNullException.ThrowIfNull(publish);
        ArgumentNullException.ThrowIfNull(commitStore);
        ArgumentNullException.ThrowIfNull(delayFor);
        var attempt = 0;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            attempt++;
            try
            {
                await publish(envelope, cancellationToken).ConfigureAwait(false);
                break;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (IsFatalHandoffFailure(ex))
            {
                // A fatal destination failure cannot recover in-process (latched
                // connection/transport state or invalid configuration). Retrying
                // would wedge the partition forever with only warning logs; surface
                // it so the dispatch safety net records a failed attach.
                _logger.LogError(ex,
                    "Error-stream handoff publish failed fatally for {Source} offset {Offset} (attempt {Attempt}); surfacing instead of retrying.",
                    envelope.SourcePartitionStream, envelope.SourceOffset, attempt);
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
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }
        await commitStore().ConfigureAwait(false);
    }
}
