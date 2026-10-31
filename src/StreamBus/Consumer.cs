namespace StreamBus;

public interface IConsumer<T>
{
    Task Consume(ConsumeContext<T> context);
}
