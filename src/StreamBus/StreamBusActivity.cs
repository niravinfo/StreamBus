using System.Diagnostics;

namespace StreamBus;

internal static class StreamBusActivity
{
    internal static readonly ActivitySource Source = new("StreamBus", "2.0.0");

    internal static Activity? StartPublish(string stream, string? routingKey)
    {
        var activity = Source.StartActivity("streambus.publish", ActivityKind.Producer);
        if (activity is null)
            return null;
        activity.SetTag("streambus.stream", stream);
        if (routingKey is not null)
            activity.SetTag("streambus.routing_key", routingKey);
        return activity;
    }

    internal static void SetSize(Activity? activity, int size)
        => activity?.SetTag("streambus.size", size);

    internal static Activity? StartConsume(string stream, int partition, ulong offset, string? correlationId)
    {
        Activity? activity;
        if (correlationId is not null && ActivityContext.TryParse(correlationId, null, out var context))
        {
            activity = Source.StartActivity(
                "streambus.consume",
                ActivityKind.Consumer,
                default(ActivityContext),
                tags: null,
                links: new[] { new ActivityLink(context) });
        }
        else
        {
            activity = Source.StartActivity("streambus.consume", ActivityKind.Consumer);
        }

        if (activity is null)
            return null;
        activity.SetTag("streambus.stream", stream);
        activity.SetTag("streambus.partition", partition);
        activity.SetTag("streambus.offset", offset);
        return activity;
    }

    internal static void RecordException(Activity? activity, Exception exception)
    {
        if (activity is null)
            return;
        activity.AddEvent(new ActivityEvent("exception", tags: new ActivityTagsCollection
        {
            ["exception.type"] = exception.GetType().FullName,
            ["exception.message"] = exception.Message,
        }));
    }
}
