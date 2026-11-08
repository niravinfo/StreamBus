namespace StreamBus.IntegrationTests;

/// <summary>
/// The chaos tests force-close every stream connection on the shared broker.
/// A non-parallel collection keeps them from killing connections under other
/// integration tests while still letting the rest of the suite run in parallel.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class ChaosCollection
{
    public const string Name = "chaos";
}
