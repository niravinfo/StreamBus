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
        var inner = NewPublishContext(new ContractDerived("d-1"), declared: typeof(ContractDerived));

        await adapter.OnPublish(inner, _ => Task.CompletedTask, TestContext.Current.CancellationToken);

        Assert.Equal("d-1", filter.Seen!.Id);
        Assert.Equal(typeof(ContractTypedPublishFilter), FilterIdentity.TypeOf(adapter));
        Assert.True(inner.Headers.TryGet("typed", out var value));
        Assert.Equal("d-1", value);
    }

    [Fact]
    public async Task Typed_consume_adapter_forwards_skip_payload_and_typed_message()
    {
        var filter = new ContractTypedConsumeFilter();
        var adapter = new TypedConsumeFilterAdapter<ContractDerived>(filter);
        var inner = NewConsumeContext(new ContractDerived("d-1"));

        await adapter.OnConsume(inner, _ => Task.CompletedTask, TestContext.Current.CancellationToken);

        Assert.Equal("d-1", filter.Seen!.Id);
        Assert.True(adapter.CanSkip);
        Assert.True(inner.SkipHandler);
        Assert.Equal("typed", inner.GetPayload<string>());
        Assert.Equal(typeof(ContractTypedConsumeFilter), FilterIdentity.TypeOf(adapter));
    }

    [Fact]
    public void Typed_contexts_share_headers_payloads_skip_and_raw_context()
    {
        var innerPublish = NewPublishContext(new ContractDerived("d-1"), declared: typeof(ContractDerived));
        var typedPublish = new StreamPublishFilterContext<ContractDerived> { Inner = innerPublish };
        typedPublish.Headers.Set("shared", true);
        Assert.True(innerPublish.Headers.TryGet("shared", out _));
        Assert.Same(innerPublish.Message, typedPublish.Message);

        var innerConsume = NewConsumeContext(new ContractDerived("d-1"));
        var typedConsume = new StreamConsumeFilterContext<ContractDerived>
        {
            Inner = innerConsume,
            Message = new ContractDerived("d-1"),
        };
        typedConsume.SetPayload("payload");
        typedConsume.SkipHandler = true;
        Assert.Equal("payload", innerConsume.GetPayload<string>());
        Assert.True(innerConsume.SkipHandler);
        Assert.Same(innerConsume.Message, typedConsume.Raw);
        Assert.Same(innerConsume.OutputHeaders, typedConsume.OutputHeaders);
    }

    private static StreamPublishFilterContext NewPublishContext(object message, Type declared) => new()
    {
        Stream = "contracts",
        RoutingKey = null,
        Message = message,
        MessageType = declared,
        Headers = new HeaderWriter(),
        Bus = new StreamBusRegistrationView
        {
            Vhost = "/",
            Host = "localhost",
            StreamPort = 5552,
            AmqpPort = 5672,
            HeartbeatSeconds = 60,
        },
        Services = EmptyProvider.Instance,
    };

    private static StreamConsumeFilterContext NewConsumeContext(object message) => new()
    {
        Message = new ConsumeContext
        {
            Stream = "contracts",
            Partition = 0,
            Offset = 0,
            RoutingValue = null,
            Body = ReadOnlyMemory<byte>.Empty,
            Metadata = new MessageMetadata(),
            CancellationToken = TestContext.Current.CancellationToken,
        },
        MessageType = message.GetType(),
        TypedMessage = message,
        OutputHeaders = new HeaderWriter(),
        Services = EmptyProvider.Instance,
    };

    private record ContractBase(string Id);

    private sealed record ContractDerived(string Id) : ContractBase(Id);

    private sealed class ContractTypedPublishFilter : IStreamPublishFilter<ContractDerived>
    {
        public ContractDerived? Seen { get; private set; }

        public Task OnPublish(
            StreamPublishFilterContext<ContractDerived> context,
            PublishDelegate next,
            CancellationToken ct)
        {
            Seen = context.Message;
            context.Headers.Set("typed", context.Message.Id);
            return next(context.Inner);
        }
    }

    private sealed class ContractTypedConsumeFilter : IStreamConsumeFilter<ContractDerived>
    {
        public ContractDerived? Seen { get; private set; }

        public bool CanSkip => true;

        public Task OnConsume(
            StreamConsumeFilterContext<ContractDerived> context,
            ConsumeDelegate next,
            CancellationToken ct)
        {
            Seen = context.Message;
            context.SetPayload("typed");
            context.SkipHandler = true;
            return next(context.Inner);
        }
    }

    private sealed class EmptyProvider : IServiceProvider
    {
        public static readonly EmptyProvider Instance = new();

        public object? GetService(Type serviceType) => null;
    }
}
