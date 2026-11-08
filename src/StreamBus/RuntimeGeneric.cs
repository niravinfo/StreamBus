using System.Diagnostics.CodeAnalysis;

namespace StreamBus;

/// <summary>
/// Closes a generic type from a runtime <see cref="Type"/>.
/// </summary>
/// <remarks>
/// This is the single reflection-based closing point for the typed
/// filter/context/consumer machinery. Registrations that pin their message type
/// at the call site (<c>UsePublishFilter&lt;TFilter, TMessage&gt;</c> /
/// <c>UseConsumeFilter&lt;TFilter, TMessage&gt;</c>) bypass it entirely. The
/// remaining type-based and open-generic registrations cannot be statically
/// preserved by a trimmer or NativeAOT, so the public entry points that opt into
/// them carry <see cref="RequiresUnreferencedCodeAttribute"/> and
/// <see cref="RequiresDynamicCodeAttribute"/>.
/// </remarks>
internal static class RuntimeGeneric
{
    [RequiresUnreferencedCode(TrimMessage)]
    [RequiresDynamicCode(AotMessage)]
    public static TInterface Create<TInterface>(Type openGenericDefinition, params Type[] typeArguments)
        where TInterface : class
    {
        ArgumentNullException.ThrowIfNull(openGenericDefinition);
        ArgumentNullException.ThrowIfNull(typeArguments);
        return (TInterface)Activator.CreateInstance(
            openGenericDefinition.MakeGenericType(typeArguments))!;
    }

    [RequiresUnreferencedCode(TrimMessage)]
    [RequiresDynamicCode(AotMessage)]
    public static TInterface Create<TInterface>(
        Type openGenericDefinition,
        Type[] typeArguments,
        object argument)
        where TInterface : class
    {
        ArgumentNullException.ThrowIfNull(openGenericDefinition);
        ArgumentNullException.ThrowIfNull(typeArguments);
        ArgumentNullException.ThrowIfNull(argument);
        return (TInterface)Activator.CreateInstance(
            openGenericDefinition.MakeGenericType(typeArguments),
            argument)!;
    }

    internal const string TrimMessage =
        "Closes a generic type from a runtime Type. The trimmer cannot preserve the closed instantiation.";

    internal const string AotMessage =
        "Closes a generic type from a runtime Type. The closed instantiation may not exist in a native image.";

    internal const DynamicallyAccessedMemberTypes FilterMembers =
        DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.Interfaces;
}
