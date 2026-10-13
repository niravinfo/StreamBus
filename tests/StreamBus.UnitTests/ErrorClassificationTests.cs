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
