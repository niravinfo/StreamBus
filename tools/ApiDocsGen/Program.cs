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

        Console.Error.WriteLine("Page rendering is implemented in a later commit.");
        return 0;
    }
}
