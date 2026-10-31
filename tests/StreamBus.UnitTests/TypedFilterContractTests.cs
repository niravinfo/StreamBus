namespace StreamBus.UnitTests;

public sealed class TypedFilterContractTests
{
    [Fact]
    public async Task Typed_publish_adapter_names_the_user_filter_and_forwards_the_typed_message()
    {
        var filter = new ContractTypedPublishFilter();
        var adapter = new TypedPublishFilterAdapter<ContractDerived>(filter);
        var context = NewTypedPublishContext(new ContractDerived("d-1"));

        await adapter.OnPublish(context, () => Task.CompletedTask);

        Assert.Same(context, filter.SeenContext);
        Assert.Equal("d-1", filter.Seen!.Id);
        Assert.Equal(typeof(ContractTypedPublishFilter), FilterIdentity.TypeOf(adapter));
        Assert.True(context.Headers.TryGet("typed", out var value));
        Assert.Equal("d-1", value);
    }

    [Fact]
    public async Task Typed_consume_adapter_forwards_payload_and_typed_message()
    {
        var filter = new ContractTypedConsumeFilter();
        var adapter = new TypedConsumeFilterAdapter<ContractDerived>(filter);
        var context = NewTypedConsumeContext(new ContractDerived("d-1"));

        await adapter.OnConsume(context, () => Task.CompletedTask);

        Assert.Same(context, filter.SeenContext);
        Assert.Equal("d-1", filter.Seen!.Id);
        Assert.Equal("typed", context.GetPayload<string>());
        Assert.Equal(typeof(ContractTypedConsumeFilter), FilterIdentity.TypeOf(adapter));
    }

    [Fact]
    public void Typed_and_universal_consume_filters_share_one_instance()
    {
        var typedPublish = NewTypedPublishContext(new ContractDerived("d-1"));
        typedPublish.Headers.Set("shared", true);
        Assert.True(typedPublish.Headers.TryGet("shared", out _));
        Assert.Same(typedPublish.Payload, typedPublish.Message);

        var typed = NewTypedConsumeContext(new ContractDerived("d-1"));
        ConsumeContext universal = typed;
        typed.SendHeaders.Set("shared", true);
        typed.SetPayload("payload");
        Assert.True(universal.SendHeaders.TryGet("shared", out _));
        Assert.Equal("payload", typed.GetPayload<string>());
        Assert.Same(typed, universal);
        Assert.Same(typed.Headers, universal.Headers);
        Assert.Same(typed.SendHeaders, universal.SendHeaders);
        Assert.Same(typed.Services, universal.Services);
        Assert.Equal(typed.CancellationToken, universal.CancellationToken);
    }

    private static PublishContext<ContractDerived> NewTypedPublishContext(ContractDerived message) => new()
    {
        Stream = "contracts",
        RoutingKey = null,
        Payload = message,
        MessageId = Guid.NewGuid(),
        SentTime = DateTime.UtcNow,
        Headers = new StreamSendHeaders(null, SystemTextJsonStreamSerializer.Default),
        Services = EmptyProvider.Instance,
        CancellationToken = TestContext.Current.CancellationToken,
    };

    private static ConsumeContext<ContractDerived> NewTypedConsumeContext(ContractDerived message) => new()
    {
        Stream = "contracts",
        Partition = 0,
        Offset = 0,
        RoutingValue = null,
        Body = ReadOnlyMemory<byte>.Empty,
        Headers = new StreamHeaders(new Dictionary<string, object?>(), SystemTextJsonStreamSerializer.Default),
        SendHeaders = new StreamSendHeaders(null, SystemTextJsonStreamSerializer.Default),
        Message = message,
        Services = EmptyProvider.Instance,
        CancellationToken = TestContext.Current.CancellationToken,
    };

    private record ContractBase(string Id);

    private sealed record ContractDerived(string Id) : ContractBase(Id);

    private sealed class ContractTypedPublishFilter : IStreamPublishFilter<ContractDerived>
    {
        public ContractDerived? Seen { get; private set; }
        public PublishContext<ContractDerived>? SeenContext { get; private set; }

        public Task OnPublish(
            PublishContext<ContractDerived> context,
            PublishDelegate next)
        {
            SeenContext = context;
            Seen = context.Message;
            context.Headers.Set("typed", context.Message.Id);
            return next();
        }
    }

    private sealed class ContractTypedConsumeFilter : IStreamConsumeFilter<ContractDerived>
    {
        public ContractDerived? Seen { get; private set; }
        public ConsumeContext<ContractDerived>? SeenContext { get; private set; }

        public Task OnConsume(
            ConsumeContext<ContractDerived> context,
            ConsumeDelegate next)
        {
            SeenContext = context;
            Seen = context.Message;
            context.SetPayload("typed");
            return next();
        }
    }

    private sealed class EmptyProvider : IServiceProvider
    {
        public static readonly EmptyProvider Instance = new();

        public object? GetService(Type serviceType) => null;
    }
}
