using System.Reflection;
using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

public sealed class ErrorClassificationTests
{
    [Fact]
    public void Protocol_ssl_and_auth_mechanism_errors_are_fatal()
    {
        Assert.True(StreamBusErrorClassification.IsFatal(new GenericProtocolException(ResponseCode.InternalError, "p")));
        Assert.True(StreamBusErrorClassification.IsFatal(new SslException("s")));
        Assert.True(StreamBusErrorClassification.IsFatal(new AuthMechanismNotSupportedException("a")));
    }

    [Fact]
    public void Tls_on_plaintext_port_handshake_failure_is_fatal()
    {
        // A TLS handshake against a plaintext port makes AuthenticateAsClientAsync
        // throw; the stream client translates that to a bare SslException (its
        // only constructor is message-only). The supervisor classifies exactly
        // that bare exception at its catch boundary, so a TLS misconfiguration
        // surfaces as fatal instead of being retried forever.
        var ex = new SslException("The SSL handshake failed. The remote host closed the connection.");
        Assert.True(StreamBusErrorClassification.IsFatal(ex));
    }

    [Fact]
    public void Network_errors_are_not_fatal()
    {
        Assert.False(StreamBusErrorClassification.IsFatal(new TimeoutException("t")));
        Assert.False(StreamBusErrorClassification.IsFatal(new System.Net.Sockets.SocketException(10061)));
        Assert.False(StreamBusErrorClassification.IsFatal(new System.IO.IOException("reset")));
        Assert.False(StreamBusErrorClassification.IsFatal(new StreamSystemInitialisationException("no endpoints could be reached")));
    }

    [Fact]
    public void Every_authentication_failure_type_in_the_client_is_fatal()
    {
        var authFailures = typeof(StreamSystem).Assembly.GetTypes()
            .Where(t => t.Name.StartsWith("AuthenticationFailure", StringComparison.Ordinal) && !t.IsAbstract)
            .ToList();
        Assert.NotEmpty(authFailures);
        foreach (var type in authFailures)
        {
            var instance = (Exception)Activator.CreateInstance(type, "auth")!;
            Assert.True(StreamBusErrorClassification.IsFatal(instance));
            Assert.True(StreamBusErrorClassification.IsFatal(new StreamBusFatalException("wrap", instance)));
        }
    }
}
