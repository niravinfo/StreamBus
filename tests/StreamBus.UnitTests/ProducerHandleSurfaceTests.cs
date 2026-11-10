using System.Reflection;

namespace StreamBus.UnitTests;

// IStreamSuperProducerHandle historically inherited
// IStreamPlainProducerHandle, bringing SendEnvelopeAsync onto the super
// surface. That method never stamps a routing key, so every call crashed
// with KeyNotFoundException from the client's Routing callback mid-send.
// The super handle must not expose it; misuse should be a compile error,
// not a runtime crash.
public sealed class ProducerHandleSurfaceTests
{
    private static MethodInfo? Method(Type type, string name)
    {
        var method = type.GetMethod(name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly);
        if (method is not null)
            return method;
        // Type.GetMethod does not surface members of inherited interfaces.
        return type.GetInterfaces()
            .Select(i => i.GetMethod(name, BindingFlags.Public | BindingFlags.Instance))
            .FirstOrDefault(m => m is not null);
    }

    [Fact]
    public void Super_handle_does_not_expose_SendEnvelopeAsync()
    {
        Assert.Null(Method(typeof(IStreamSuperProducerHandle), nameof(IStreamPlainProducerHandle.SendEnvelopeAsync)));
    }

    [Fact]
    public void Super_handle_keeps_its_routed_members()
    {
        Assert.NotNull(Method(typeof(IStreamSuperProducerHandle), nameof(IStreamProducerHandle.SendAsync)));
    }

    [Fact]
    public void Super_handle_is_not_a_plain_handle()
    {
        Assert.False(typeof(IStreamPlainProducerHandle).IsAssignableFrom(typeof(IStreamSuperProducerHandle)));
    }

    [Fact]
    public void Plain_handle_keeps_envelope_sends()
    {
        Assert.NotNull(Method(typeof(IStreamPlainProducerHandle), nameof(IStreamPlainProducerHandle.SendEnvelopeAsync)));
    }
}
