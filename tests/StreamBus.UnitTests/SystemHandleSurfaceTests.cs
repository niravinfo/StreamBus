using System.Reflection;

namespace StreamBus.UnitTests;

public sealed class SystemHandleSurfaceTests
{
    [Theory]
    [InlineData("CreatePlainProducerWithReferenceAsync")]
    [InlineData("CreateSuperStreamProducerWithReferenceAsync")]
    [InlineData("QuerySequenceAsync")]
    public void Removed_producer_reference_members_are_absent(string name)
        => Assert.Null(typeof(IStreamSystemHandle).GetMethod(
            name,
            BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly));

    [Fact]
    public void Offset_members_remain_for_consumer_groups()
    {
        Assert.NotNull(typeof(IStreamSystemHandle).GetMethod(
            nameof(IStreamSystemHandle.TryQueryOffsetAsync),
            BindingFlags.Public | BindingFlags.Instance));
        Assert.NotNull(typeof(IStreamSystemHandle).GetMethod(
            nameof(IStreamSystemHandle.StoreSystemOffsetAsync),
            BindingFlags.Public | BindingFlags.Instance));
    }
}
