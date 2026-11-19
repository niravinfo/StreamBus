using System.Runtime.CompilerServices;

namespace StreamBus.UnitTests;

public sealed class RuntimeAsyncCodegenTests
{
#if NET11_0
    [Fact]
    public void Net11Asset_is_compiled_with_runtime_async()
    {
        var method = typeof(global::StreamBus.StreamBus).GetMethod(nameof(global::StreamBus.StreamBus.PublishRawAsync));
        Assert.NotNull(method);
        // Runtime-async methods are not backed by a compiler-generated state machine.
        Assert.Empty(method!.GetCustomAttributes(typeof(AsyncStateMachineAttribute), inherit: false));
    }
#endif
}
