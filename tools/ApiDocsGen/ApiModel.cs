namespace ApiDocsGen;

public enum ApiMemberKind
{
    Method,
    Property,
    Field,
    NestedType,
}

public enum ApiTypeKind
{
    Class,
    Interface,
    Enum,
    Unknown,
}

/// <summary>An &lt;exception cref="..."&gt; entry attached to a member.</summary>
public sealed record ApiExceptionRef(string TypeName, string Description);

/// <summary>A method, property, field, or nested type documented on a type's page.</summary>
public sealed record ApiMember(
    string Name,
    string Signature,
    ApiMemberKind Kind,
    string Summary,
    string Remarks,
    IReadOnlyList<ApiExceptionRef> Exceptions);

/// <summary>A public type with all its documented members.</summary>
public sealed record ApiType(
    string FullName,
    string Name,
    string Slug,
    ApiTypeKind Kind,
    string Summary,
    string Remarks,
    IReadOnlyList<ApiMember> Members);
