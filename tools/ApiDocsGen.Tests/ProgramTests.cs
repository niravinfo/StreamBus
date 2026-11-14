namespace ApiDocsGen.Tests;

public class ProgramTests
{
    private static readonly string Fixture = """
        <?xml version="1.0"?>
        <doc>
          <assembly><name>StreamBus</name></assembly>
          <members>
            <member name="T:StreamBus.IConsumer`1">
              <summary>Consumes messages.</summary>
            </member>
            <member name="M:StreamBus.IConsumer`1.ConsumeAsync(StreamBus.ConsumeContext{``0})">
              <summary>Handles one message.</summary>
            </member>
          </members>
        </doc>
        """;

    private static string CreateInputDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"apidocsgen-cli-{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "StreamBus.xml"), Fixture);
        return dir;
    }

    [Fact]
    public void WritesPagesAndClearsStaleOutput()
    {
        var inputDir = CreateInputDir();
        var outDir = Path.Combine(Path.GetTempPath(), $"apidocsgen-out-{Guid.NewGuid():N}");
        Directory.CreateDirectory(outDir);
        var stale = Path.Combine(outDir, "Stale.mdx");
        File.WriteAllText(stale, "old content");

        var exit = Program.Main([Path.Combine(inputDir, "StreamBus.xml"), outDir]);

        Assert.Equal(0, exit);
        Assert.True(File.Exists(Path.Combine(outDir, "IConsumer-T.mdx")));
        Assert.False(File.Exists(stale));
    }

    [Fact]
    public void SecondRunWithSameInputIsByteIdentical()
    {
        var inputDir = CreateInputDir();
        var outDir = Path.Combine(Path.GetTempPath(), $"apidocsgen-out-{Guid.NewGuid():N}");

        Assert.Equal(0, Program.Main([Path.Combine(inputDir, "StreamBus.xml"), outDir]));
        var firstContent = File.ReadAllBytes(Path.Combine(outDir, "IConsumer-T.mdx"));

        // The second run must delete and regenerate the same bytes.
        Assert.Equal(0, Program.Main([Path.Combine(inputDir, "StreamBus.xml"), outDir]));
        var secondContent = File.ReadAllBytes(Path.Combine(outDir, "IConsumer-T.mdx"));

        Assert.Equal(firstContent, secondContent);
    }

    [Fact]
    public void WrongArgumentCountFails()
    {
        Assert.Equal(1, Program.Main([]));
        Assert.Equal(1, Program.Main(["only-one"]));
    }

    [Fact]
    public void MissingXmlFileFails()
    {
        var outDir = Path.Combine(Path.GetTempPath(), $"apidocsgen-out-{Guid.NewGuid():N}");
        Assert.Equal(1, Program.Main(["/nonexistent/StreamBus.xml", outDir]));
    }
}
