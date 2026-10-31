namespace StreamBus.UnitTests;

public sealed class TypedFilterContractTests
{
    [Fact]
    public void Universal_publish_context_reports_declared_and_runtime_types()
    {
        var context = NewPublishContext(new ContractDerived("d-1"), declared: typeof(ContractBase));
        Assert.Equal(typeof(ContractBase), context.MessageType);
        Assert.Equal(typeof(ContractDerived), context.RuntimeMessageType);
    }

    [Fact]
    public async Task Typed_publish_adapter_names_the_user_filter_and_forwards_the_typed_message()
    {
        var filter = new ContractTypedPublishFilter();
        var adapter = new TypedPublishFilterAdapter<ContractDerived>(filter);
        var context = NewTypedPublishContext(new ContractDerived("d-1"));

        await adapter.OnPublish(context, () => Task.CompletedTask, TestContext.Current.CancellationToken);

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

        await adapter.OnConsume(context, () => Task.CompletedTask, TestContext.Current.CancellationToken);

        Assert.Same(context, filter.SeenContext);
        Assert.Equal("d-1", filter.Seen!.Id);
        Assert.Equal("typed", context.GetPayload<string>());
        Assert.Equal(typeof(ContractTypedConsumeFilter), FilterIdentity.TypeOf(adapter));
    }

    [Fact]
    public void Typed_contexts_share_headers_payloads_services_and_raw_context()
    {
        var typedPublish = NewTypedPublishContext(new ContractDerived("d-1"));
        typedPublish.Headers.Set("shared", true);
        Assert.True(typedPublish.Headers.TryGet("shared", out _));
        Assert.Same(typedPublish.Payload, typedPublish.Message);

        var typedConsume = NewTypedConsumeContext(new ContractDerived("d-1"));
        StreamConsumeFilterContext universalConsume = typedConsume;
        typedConsume.OutputHeaders.Set("shared", true);
        typedConsume.SetPayload("payload");
        Assert.True(universalConsume.OutputHeaders.TryGet("shared", out _));
        Assert.Equal("payload", universalConsume.GetPayload<string>());
        Assert.Equal(typeof(ContractDerived), universalConsume.MessageType);
        Assert.Same(typedConsume.Raw, universalConsume.Raw);
        Assert.Same(typedConsume.OutputHeaders, universalConsume.OutputHeaders);
        Assert.Same(typedConsume.Services, universalConsume.Services);
    }

    private static StreamPublishFilterContext NewPublishContext(object message, Type declared) => new()
    {
        Stream = "contracts",
        RoutingKey = null,
        Payload = message,
        MessageType = declared,
        Headers = new HeaderWriter(),
        Bus = NewBusView(),
        Services = EmptyProvider.Instance,
    };

    private static StreamPublishFilterContext<ContractDerived> NewTypedPublishContext(ContractDerived message) => new()
    {
        Stream = "contracts",
        RoutingKey = null,
        Payload = message,
        Headers = new HeaderWriter(),
        Bus = NewBusView(),
        Services = EmptyProvider.Instance,
    };

    private static StreamBusRegistrationView NewBusView() => new()
    {
        Vhost = "/",
        Host = "localhost",
        StreamPort = 5552,
        AmqpPort = 5672,
        HeartbeatSeconds = 60,
    };

    private static StreamConsumeFilterContext NewConsumeContext(object message) => new()
    {
        Raw = NewRawContext(),
        MessageType = message.GetType(),
        OutputHeaders = new HeaderWriter(),
        Services = EmptyProvider.Instance,
    };

    private static StreamConsumeFilterContext<ContractDerived> NewTypedConsumeContext(ContractDerived message) => new()
    {
        Raw = NewRawContext(),
        Message = message,
        OutputHeaders = new HeaderWriter(),
        Services = EmptyProvider.Instance,
    };

    private static ConsumeContext NewRawContext() => new()
    {
        Stream = "contracts",
        Partition = 0,
        Offset = 0,
        RoutingValue = null,
        Body = ReadOnlyMemory<byte>.Empty,
        Metadata = new MessageMetadata(),
        CancellationToken = TestContext.Current.CancellationToken,
    };

    private record ContractBase(string Id);

    private sealed record ContractDerived(string Id) : ContractBase(Id);

    private sealed class ContractTypedPublishFilter : IStreamPublishFilter<ContractDerived>
    {
        public ContractDerived? Seen { get; private set; }
        public StreamPublishFilterContext<ContractDerived>? SeenContext { get; private set; }

        public Task OnPublish(
            StreamPublishFilterContext<ContractDerived> context,
            PublishDelegate next,
            CancellationToken ct)
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
        public StreamConsumeFilterContext<ContractDerived>? SeenContext { get; private set; }

        public Task OnConsume(
            StreamConsumeFilterContext<ContractDerived> context,
            ConsumeDelegate next,
            CancellationToken ct)
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
