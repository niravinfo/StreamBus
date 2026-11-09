using RabbitMQ.Stream.Client;

namespace StreamBus;

/// <summary>
/// Builds the raw producer configs StreamBus uses for every publish path.
/// Publishing ids still correlate confirms; no producer name is assigned.
/// </summary>
internal static class ProducerConfigs
{
    public static RawProducerConfig Plain(string stream, ConfirmationTracker confirmations)
        => new(stream)
        {
            MessagesBufferSize = 100,
            ConfirmHandler = confirmations.Complete,
            ConnectionClosedHandler = _ =>
            {
                confirmations.OnConnectionLost();
                return Task.CompletedTask;
            },
        };

    public static RawSuperStreamProducerConfig Super(
        string stream,
        ConfirmationTracker confirmations,
        SuperStreamRouting routing)
        => new(stream)
        {
            MessagesBufferSize = 100,
            Routing = routing.Route,
            ConfirmHandler = confirmation => confirmations.Complete(confirmation.Item2),
            ConnectionClosedHandler = (_, _) =>
            {
                confirmations.OnConnectionLost();
                return Task.CompletedTask;
            },
        };
}
