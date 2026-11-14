namespace ApiDocsGen;

public static class Program
{
    public static int Main(string[] args)
    {
        const string usage = "Usage: ApiDocsGen <streambus.xml> <output-dir>";
        if (args.Length != 2)
        {
            Console.Error.WriteLine(usage);
            return 1;
        }

        var xmlPath = args[0];
        var outputDir = args[1];
        try
        {
            var types = XmlDocParser.Parse(xmlPath);

            Directory.CreateDirectory(outputDir);

            // Clear stale output so renamed or removed members never leave
            // orphan pages behind; generation then repopulates the directory.
            foreach (var stale in Directory.EnumerateFiles(outputDir, "*.mdx"))
            {
                File.Delete(stale);
            }

            foreach (var type in types)
            {
                File.WriteAllText(Path.Combine(outputDir, type.Slug + ".mdx"), PageRenderer.Render(type));
            }

            Console.WriteLine($"Generated {types.Count} pages");
            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"ApiDocsGen failed: {ex.Message}");
            return 1;
        }
    }
}
