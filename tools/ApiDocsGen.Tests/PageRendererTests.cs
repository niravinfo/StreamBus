namespace ApiDocsGen.Tests;

public class PageRendererTests
{
    private static readonly string[] KnownSlugs = ["StreamBusException", "IConsumer-T"];

    private static ApiType Fixture() => new(
        FullName: "IConsumer`1",
        Name: "IConsumer<T>",
        Slug: "IConsumer-T",
        Kind: ApiTypeKind.Interface,
        Summary: "Consumes messages of type TMessage.",
        Remarks: "One consumer instance per message type.",
        Members:
        [
            new ApiMember("ConsumeAsync", "ConsumeAsync(ConsumeContext<T> context)", ApiMemberKind.Method,
                "Handles one message.", "", [new ApiExceptionRef("StreamBusException", "Thrown when the pipeline fails.")]),
            new ApiMember("MaxAge", "MaxAge", ApiMemberKind.Property,
                "Retention age.", "Optional.", []),
            new ApiMember("Connected", "Connected", ApiMemberKind.Field,
                "", "", []),
        ]);

    [Fact]
    public void PageStartsWithFrontmatterTitle()
    {
        var output = PageRenderer.Render(Fixture(), KnownSlugs);
        Assert.StartsWith("---\ntitle: IConsumer<T>\n---\n", output);
    }

    [Fact]
    public void GenericAngleBracketsOnlyAppearInsideCodeFences()
    {
        var output = PageRenderer.Render(Fixture(), KnownSlugs);
        var inFence = false;
        var frontmatterDelimiters = 0;
        foreach (var line in output.Split('\n'))
        {
            if (frontmatterDelimiters < 2)
            {
                if (line.TrimEnd().StartsWith("---", StringComparison.Ordinal))
                {
                    frontmatterDelimiters++;
                }

                continue;
            }

            if (line.TrimStart().StartsWith("```", StringComparison.Ordinal))
            {
                inFence = !inFence;
                continue;
            }

            if (!inFence)
            {
                Assert.DoesNotContain("<", line);
            }
        }
    }

    [Fact]
    public void MembersGroupUnderKindHeadingsWithFencedSignatures()
    {
        var output = PageRenderer.Render(Fixture(), KnownSlugs);
        Assert.Contains("\n## Methods\n", output);
        Assert.Contains("\n## Properties\n", output);
        Assert.Contains("\n## Fields\n", output);
        Assert.Contains("```csharp\nConsumeAsync(ConsumeContext<T> context)\n```", output);
    }

    [Fact]
    public void ExceptionsRenderAsBulletedList()
    {
        var output = PageRenderer.Render(Fixture(), KnownSlugs);
        Assert.Contains("- [StreamBusException](StreamBusException) — Thrown when the pipeline fails.", output);
    }

    [Fact]
    public void EmptySummaryRendersFallback()
    {
        var output = PageRenderer.Render(Fixture(), KnownSlugs);
        Assert.Contains("No description.", output);
    }

    [Fact]
    public void AngleBracketsInBodyTextAreEscaped()
    {
        var type = Fixture() with
        {
            FullName = "RetryConfig",
            Name = "RetryConfig",
            Slug = "RetryConfig",
            Kind = ApiTypeKind.Class,
            Summary = "The consumer class must implement IConsumer<T> for one message type.",
        };
        var output = PageRenderer.Render(type, KnownSlugs);
        Assert.Contains("IConsumer&lt;T&gt;", output);
        Assert.DoesNotContain("IConsumer<T>", output);
    }

    [Fact]
    public void CurlyBracesInBodyTextAreEscaped()
    {
        var type = Fixture() with
        {
            FullName = "RetryConfig",
            Name = "RetryConfig",
            Slug = "RetryConfig",
            Kind = ApiTypeKind.Class,
            Summary = "Dead letters go to {stream}_error by default.",
        };
        var output = PageRenderer.Render(type, KnownSlugs);
        Assert.Contains("&#123;stream&#125;_error", output);
        Assert.DoesNotContain("{stream}", output);
    }

    [Fact]
    public void ExceptionLinksOnlyToKnownTypePages()
    {
        var withKnown = Fixture() with
        {
            FullName = "RetryConfig",
            Name = "RetryConfig",
            Slug = "RetryConfig",
            Kind = ApiTypeKind.Class,
            Members =
            [
                new ApiMember("Fails", "Fails()", ApiMemberKind.Method,
                    "Throws.", "",
                [
                    new ApiExceptionRef("StreamBusException", "Pipeline failure."),
                    new ApiExceptionRef("InvalidOperationException", "Bad argument."),
                ]),
            ],
        };
        var output = PageRenderer.Render(withKnown, ["StreamBusException", "RetryConfig"]);
        Assert.Contains("[StreamBusException](StreamBusException) — Pipeline failure.", output);
        Assert.Contains("- InvalidOperationException — Bad argument.", output);
        Assert.DoesNotContain("[InvalidOperationException]", output);
    }

    [Fact]
    public void TitleIsSanitizedForYaml()
    {
        var weird = Fixture() with { Name = "A|B:C" };
        var output = PageRenderer.Render(weird, KnownSlugs);
        Assert.StartsWith("---\ntitle: A", output);
        Assert.DoesNotContain("|", output[..output.IndexOf('\n', 5)]);
    }
}
