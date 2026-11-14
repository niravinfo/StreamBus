using System.Xml.Linq;

namespace ApiDocsGen.Tests;

public class XmlDocParserTests
{
    private static string FixturePath(string xml)
    {
        var path = Path.Combine(Path.GetTempPath(), $"apidocsgen-test-{Guid.NewGuid():N}.xml");
        File.WriteAllText(path, xml);
        return path;
    }

    private const string Fixture = """
        <?xml version="1.0"?>
        <doc>
          <assembly><name>StreamBus</name></assembly>
          <members>
            <member name="T:StreamBus.IConsumer`1">
              <summary>Consumes messages of type TMessage.</summary>
            </member>
            <member name="M:StreamBus.IConsumer`1.ConsumeAsync(StreamBus.ConsumeContext{``0})">
              <summary>Handles one message.</summary>
              <exception cref="T:StreamBus.StreamBusException">Thrown when the pipeline fails.</exception>
            </member>
            <member name="T:StreamBus.ReadinessPolicy">
              <summary>Readiness policy for startup blocking.</summary>
            </member>
            <member name="T:StreamBus.ReadinessPolicy.WaitUpTo">
              <summary>Waits up to a timeout.</summary>
            </member>
            <member name="T:StreamBus.StreamOptions">
              <summary>Declarative stream configuration.</summary>
            </member>
            <member name="P:StreamBus.StreamOptions.MaxAge">
              <summary>Retention age.</summary>
            </member>
            <member name="T:StreamBus.BusConnectionState">
              <summary>Connection states.</summary>
            </member>
            <member name="F:StreamBus.BusConnectionState.Connected">
              <summary>The connection is up.</summary>
            </member>
            <member name="T:StreamBus.Bus">
              <summary>The bus.</summary>
            </member>
            <member name="M:StreamBus.Bus.Empty()">
              <summary></summary>
            </member>
          </members>
        </doc>
        """;

    [Fact]
    public void ParsesGenericTypeAsTypeWithBacktickName()
    {
        var types = XmlDocParser.Parse(FixturePath(Fixture));
        var consumer = Assert.Single(types, t => t.FullName == "IConsumer`1");
        Assert.Equal("IConsumer<T>", consumer.Name);
        Assert.Equal("IConsumer-T", consumer.Slug);
        Assert.Equal("Consumes messages of type TMessage.", consumer.Summary);
    }

    [Fact]
    public void NestedTypeIsGroupedUnderParentType()
    {
        var types = XmlDocParser.Parse(FixturePath(Fixture));
        var policy = Assert.Single(types, t => t.FullName == "ReadinessPolicy");
        var nested = Assert.Single(policy.Members, m => m.Kind == ApiMemberKind.NestedType);
        Assert.Equal("WaitUpTo", nested.Name);
        Assert.Equal("Waits up to a timeout.", nested.Summary);
        Assert.DoesNotContain(types, t => t.FullName == "ReadinessPolicy.WaitUpTo");
    }

    [Fact]
    public void ParsesMethodSignatureWithGenericParameters()
    {
        var types = XmlDocParser.Parse(FixturePath(Fixture));
        var consumer = Assert.Single(types, t => t.FullName == "IConsumer`1");
        var method = Assert.Single(consumer.Members, m => m.Name == "ConsumeAsync");
        Assert.Equal(ApiMemberKind.Method, method.Kind);
        Assert.Contains("ConsumeAsync", method.Signature);
        Assert.Contains("ConsumeContext<T>", method.Signature);
        Assert.DoesNotContain("``0", method.Signature);
        var ex = Assert.Single(method.Exceptions);
        Assert.Equal("StreamBusException", ex.TypeName);
        Assert.Equal("Thrown when the pipeline fails.", ex.Description);
    }

    [Fact]
    public void ParsesPropertiesAndFieldsWithKinds()
    {
        var types = XmlDocParser.Parse(FixturePath(Fixture));
        var options = Assert.Single(types, t => t.FullName == "StreamOptions");
        var property = Assert.Single(options.Members, m => m.Name == "MaxAge");
        Assert.Equal(ApiMemberKind.Property, property.Kind);

        var state = Assert.Single(types, t => t.FullName == "BusConnectionState");
        var field = Assert.Single(state.Members, m => m.Name == "Connected");
        Assert.Equal(ApiMemberKind.Field, field.Kind);
    }

    [Fact]
    public void MemberWithEmptySummaryYieldsEmptySummaryNotNull()
    {
        var types = XmlDocParser.Parse(FixturePath(Fixture));
        var bus = Assert.Single(types, t => t.FullName == "Bus");
        var method = Assert.Single(bus.Members, m => m.Name == "Empty");
        Assert.Equal(string.Empty, method.Summary);
    }
}
