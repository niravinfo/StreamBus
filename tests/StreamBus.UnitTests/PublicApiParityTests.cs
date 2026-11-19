#if NET11_0
using System.Reflection;
using System.Runtime.Loader;

namespace StreamBus.UnitTests;

public sealed class PublicApiParityTests
{
    [Fact]
    public void Net10_and_net11_assets_expose_the_same_public_api()
    {
        var net10Path = FindNet10Asset();
        var context = new AssemblyLoadContext("streambus-net10-parity", isCollectible: true);
        context.Resolving += (_, name) => AssemblyLoadContext.Default.LoadFromAssemblyName(name);
        Assembly net10;
        using (var stream = File.OpenRead(net10Path))
            net10 = context.LoadFromStream(stream);

        var net11 = typeof(StreamBusHostOptions).Assembly;
        Assert.Equal(Surface(net11), Surface(net10));
    }

    private static string[] Surface(Assembly assembly) => assembly.GetExportedTypes()
        .SelectMany(type => new[] { $"TYPE::{type.FullName}" }
            .Concat(type.GetMembers(
                    BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
                .Select(member => $"{type.FullName}::{member.MemberType}::{member}")))
        .OrderBy(entry => entry, StringComparer.Ordinal)
        .ToArray();

    private static string FindNet10Asset()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
            foreach (var config in new[] { "Release", "Debug" })
            {
                var candidate = Path.Combine(dir.FullName, "src", "StreamBus", "bin", config, "net10.0", "StreamBus.dll");
                if (File.Exists(candidate)) return candidate;
            }
        throw new FileNotFoundException("Could not locate the net10.0 StreamBus asset; build the solution before running this test.");
    }
}
#endif
