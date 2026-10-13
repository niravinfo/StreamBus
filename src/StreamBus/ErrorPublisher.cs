using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace StreamBus;

internal sealed class ErrorPublisher
{
    private readonly StreamProducerRegistry _producers;
    private readonly ILogger<ErrorPublisher> _logger;

    public ErrorPublisher(StreamProducerRegistry producers, ILogger<ErrorPublisher>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(producers);
        _producers = producers;
        _logger = logger ?? NullLogger<ErrorPublisher>.Instance;
    }

    public Task PublishHandoffAsync(string errorStream, ErrorEnvelope envelope, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(errorStream);
        ArgumentNullException.ThrowIfNull(envelope);
        return _producers.PublishEnvelopeAsync(
            errorStream, envelope.Body, routingKey: null,
            envelope.ToApplicationProperties(), envelope.MessageId, envelope.CorrelationId, ct);
    }

    // Orchestration with injectable transport (unit-testable without a broker):
    // retry the confirmed publish until it lands or ct fires, then run the
    // advance-store exactly once. Never advances past an unconfirmed handoff.
    public async Task HandOffAsync(
        ErrorEnvelope envelope,
        Func<ErrorEnvelope, CancellationToken, Task> publish,
        Func<Task> commitStore,
        Func<int, Exception, TimeSpan> delayFor,
        CancellationToken ct)
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
                    "Error-stream handoff publish failed for {Source} offset {Offset} (attempt {Attempt}); retrying without advancing.",
                    envelope.SourcePartitionStream, envelope.SourceOffset, attempt);
                var delay = delayFor(attempt, ex);
                if (delay < TimeSpan.Zero) delay = TimeSpan.Zero;
                await Task.Delay(delay, ct).ConfigureAwait(false);
            }
        }
        await commitStore().ConfigureAwait(false);
    }
}
