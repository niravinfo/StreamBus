using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace StreamBus;

internal sealed class ResultPublisher
{
    private readonly ResultProducerRegistry _results;
    private readonly ILogger<ResultPublisher> _logger;

    public ResultPublisher(ResultProducerRegistry results, ILogger<ResultPublisher>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(results);
        _results = results;
        _logger = logger ?? NullLogger<ResultPublisher>.Instance;
    }

    public Task PublishOnceAsync(ResultWork work, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(work);
        return _results.PublishResultAsync(
            work.TargetStream,
            work.TargetSuper,
            work.Body,
            work.SourceOffset,
            work.RoutingKey,
            work.ProducerName,
            ct,
            outputHeaders: null,
            work.MessageId,
            work.SentTime);
    }

    // Retry-forever (FR-6.5): a confirmed publish until it lands or ct fires.
    // There is deliberately no timeout and no silent skip. Injectable delay keeps
    // this unit-testable without a broker.
    public async Task PublishForeverAsync(ResultWork work, RetryPolicy retry, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(work);
        ArgumentNullException.ThrowIfNull(retry);
        var attempt = 0;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            attempt++;
            try
            {
                await PublishOnceAsync(work, ct).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                if (RetryDelays.ShouldLogRetryWarning(attempt))
                    _logger.LogWarning(ex,
                        "Effectively-once publish failed for target {Target} source offset {Offset} (attempt {Attempt}); retrying without advancing.",
                        work.TargetStream, work.SourceOffset, attempt);
                else
                    _logger.LogDebug(ex,
                        "Effectively-once publish failed for target {Target} source offset {Offset} (attempt {Attempt}); retrying without advancing.",
                        work.TargetStream, work.SourceOffset, attempt);
                var delay = RetryDelays.ResolveDelay(retry, attempt, ex, _logger);
                await Task.Delay(delay, ct).ConfigureAwait(false);
            }
        }
    }
}
