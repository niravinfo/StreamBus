using System.Text;
using System.Xml.Linq;

namespace ApiDocsGen;

/// <summary>
/// Parses a .NET XML documentation file into a list of <see cref="ApiType"/>
/// models, one per public type, with members attached.
/// </summary>
public static partial class XmlDocParser
{
    private const string RootNamespace = "StreamBus.";

    private static readonly Dictionary<string, string> KeywordTypes = new()
    {
        ["System.String"] = "string",
        ["System.Int32"] = "int",
        ["System.Int64"] = "long",
        ["System.Boolean"] = "bool",
        ["System.Double"] = "double",
        ["System.Single"] = "float",
        ["System.Decimal"] = "decimal",
        ["System.Byte"] = "byte",
        ["System.Object"] = "object",
        ["System.Void"] = "void",
    };

    public static IReadOnlyList<ApiType> Parse(string xmlPath)
    {
        var doc = XDocument.Load(xmlPath, LoadOptions.None);
        if (doc.Root is null)
        {
            throw new InvalidOperationException($"The XML documentation file '{xmlPath}' has no root element.");
        }

        // fullName (root namespace stripped, e.g. "IConsumer`1") -> mutable build state
        var types = new Dictionary<string, Builder>(StringComparer.Ordinal);

        var members = (doc.Root.Element("members")?.Elements("member") ?? []).ToList();

        // Two passes: type entries first (whatever their file order) so every
        // member finds its declaring page, then methods/properties/fields.
        foreach (var member in members)
        {
            var id = member.Attribute("name")?.Value ?? string.Empty;
            if (id.Length > 1 && id[0] == 'T' && id[1] == ':')
            {
                AddMember(types, id, member);
            }
        }

        foreach (var member in members)
        {
            var id = member.Attribute("name")?.Value ?? string.Empty;
            if (id.Length <= 1 || id[0] == 'T' || id[1] != ':')
            {
                continue;
            }

            AddMember(types, id, member);
        }

        return [.. types.Values
            .Where(b => b.IsType)
            .OrderBy(b => b.FullName, StringComparer.Ordinal)
            .Select(b => b.Build())];
    }

    private static void AddMember(Dictionary<string, Builder> types, string memberId, XElement element)
    {
        if (memberId.Length < 2 || memberId[1] != ':')
        {
            return;
        }

        if (memberId.Contains("k__BackingField") || memberId.Contains(".op_"))
        {
            return;
        }

        var kindChar = memberId[0];
        var rawName = memberId[2..];
        if (kindChar == 'T')
        {
            var (fullName, name, slug) = ParseTypePortion(rawName);
            var dot = fullName.LastIndexOf('.');
            if (dot < 0)
            {
                var builder = GetOrAddBuilder(types, fullName, isType: true);
                builder.DeclaredName = name;
                builder.Slug = slug;
                builder.Summary ??= Extract(element, "summary");
                builder.Remarks ??= Extract(element, "remarks");
                return;
            }

            // Nested type: attach to the topmost ancestor type's page, not
            // as a standalone page of its own.
            var nestedLeaf = fullName[(dot + 1)..];
            var parentPath = fullName[..dot];
            if (TryAscend(types, ref parentPath, out var prefix, out var parent))
            {
                var nestedSummary = Extract(element, "summary");
                var nestedRemarks = Extract(element, "remarks");
                parent.Members.Add(new ApiMember(prefix + nestedLeaf, nestedLeaf, ApiMemberKind.NestedType, nestedSummary, nestedRemarks, []));
            }

            return;
        }

        if (kindChar is not ('M' or 'P' or 'F'))
        {
            return;
        }

        // Split the declaring path from the member name. Methods end in (...):
        // the declaring path is everything before the last dot ahead of '('.
        // Properties and fields have no parameter list; the last dotted
        // segment is the member name.
        string declaringPath;
        string memberName;
        if (kindChar == 'M')
        {
            var openParen = rawName.IndexOf('(');
            var head = openParen >= 0 ? rawName[..openParen] : rawName;
            var lastDot = head.LastIndexOf('.');
            declaringPath = head[..lastDot];
            memberName = head[(lastDot + 1)..];
        }
        else
        {
            var lastDot = rawName.LastIndexOf('.');
            var memberLeaf = rawName[(lastDot + 1)..];
            if (memberLeaf == "Item")
            {
                return;
            }

            declaringPath = rawName[..lastDot];
            memberName = memberLeaf;
        }

        (declaringPath, memberName) = (NormalizeRoot(declaringPath), NormalizeRoot(memberName));

        // Walk up to the nearest ancestor type that is a page of its own
        // (top-level type); nested and parent type names prefix the member.
        if (!TryAscend(types, ref declaringPath, out var namePrefix, out var target))
        {
            return;
        }

        if (namePrefix.Length > 0)
        {
            memberName = namePrefix + "." + memberName;
        }

        var summary = Extract(element, "summary");
        var remarks = Extract(element, "remarks");
        var exceptions = element.Elements("exception")
            .Select(ex => new ApiExceptionRef(DisplaySimpleName(ex.Attribute("cref")?.Value ?? string.Empty), InnerText(ex)))
            .ToList();

        var signature = kindChar == 'M'
            ? RenderMethodSignature(memberName, rawName, types, declaringPath)
            : memberName;

        var kind = ToKind(kindChar);
        target.Members.Add(new ApiMember(memberName, signature, kind, summary, remarks, exceptions));
        target.OnlyFields &= kind == ApiMemberKind.Field;
    }

    private static ApiMemberKind ToKind(char kindChar) => kindChar switch
    {
        'M' => ApiMemberKind.Method,
        'P' => ApiMemberKind.Property,
        _ => ApiMemberKind.Field,
    };

    private static string RenderMethodSignature(string memberName, string rawName, Dictionary<string, Builder> types, string declaringPath)
    {
        var head = rawName[..rawName.IndexOf('(')];
        var openParen = rawName.IndexOf('(');
        var paramText = openParen >= 0 ? rawName[(openParen + 1)..].TrimEnd(')') : string.Empty;

        // Method-level generic arity: trailing ``N on the method name.
        var arity = 0;
        var plainName = memberName;
        var tick = memberName.LastIndexOf("``", StringComparison.Ordinal);
        if (tick > 0 && int.TryParse(memberName[(tick + 2)..], out arity))
        {
            plainName = memberName[..tick];
        }

        var sb = new StringBuilder();
        if (memberName.Contains('.'))
        {
            // Member of a nested type: keep the nested prefix visible.
            sb.Append(memberName).Append('(');
        }
        else
        {
            sb.Append(plainName);
            if (arity > 0)
            {
                sb.Append('<');
                sb.Append(string.Join(", ", Enumerable.Range(0, arity).Select(TypeParamName)));
                sb.Append('>');
            }

            sb.Append('(');
        }

        if (paramText.Length > 0)
        {
            sb.Append(string.Join(", ", paramText.Split(',').Select(RenderTypeName)));
        }

        sb.Append(')');
        return sb.ToString();
    }

    private static string RenderTypeName(string crefType)
    {
        crefType = crefType.Trim();

        // ``N : a type parameter of the method or the declaring type (the
        // generated XML uses the same form for both).
        if (crefType.StartsWith("``", StringComparison.Ordinal) && int.TryParse(crefType[2..], out var index))
        {
            return TypeParamName(index);
        }

        // `0-style references (type parameter of the declaring type).
        if (crefType.StartsWith('`') && int.TryParse(crefType[1..], out index))
        {
            return TypeParamName(index);
        }

        if (KeywordTypes.TryGetValue(crefType, out var keyword))
        {
            return keyword;
        }

        // Generic instance types brace their arguments: List{System.String}.
        var genericArity = crefType.IndexOf('`');
        if (genericArity > 0 && int.TryParse(crefType[(genericArity + 1)..], out var arity) && arity > 0)
        {
            // Find the brace after the arity digits, if present.
            var rest = crefType[(genericArity + 1)..];
            var digitEnd = rest.TakeWhile(char.IsDigit).Count();
            var brace = crefType.IndexOf('{', genericArity);
            if (brace > 0)
            {
                var inner = crefType[(brace + 1)..].TrimEnd('}');
                var rendered = string.Join(", ", SplitTopLevel(inner).Select(RenderTypeName));
                return DisplaySimpleName(crefType[..brace]) + "<" + rendered + ">";
            }

            _ = digitEnd;
            return DisplaySimpleName(crefType[..genericArity]) + "<" + new string(',', arity - 1) + ">";
        }

        var braceOpen = crefType.IndexOf('{');
        if (braceOpen >= 0)
        {
            var inner = crefType[(braceOpen + 1)..].TrimEnd('}');
            return DisplaySimpleName(crefType[..braceOpen]) + "<" + string.Join(", ", SplitTopLevel(inner).Select(RenderTypeName)) + ">";
        }

        return DisplaySimpleName(crefType);
    }

    private static IEnumerable<string> SplitTopLevel(string list)
    {
        var depth = 0;
        var start = 0;
        for (var i = 0; i < list.Length; i++)
        {
            if (list[i] is '{' or '<')
            {
                depth++;
            }
            else if (list[i] is '}' or '>')
            {
                depth--;
            }
            else if (list[i] == ',' && depth == 0)
            {
                yield return list[start..i].Trim();
                start = i + 1;
            }
        }

        yield return list[start..].Trim();
    }

    private static string TypeParamName(int index) => index == 0 ? "T" : "T" + index.ToString(System.Globalization.CultureInfo.InvariantCulture);

    private static int CountTypeArity(string fullName)
    {
        var tick = fullName.IndexOf('`');
        return tick > 0 && int.TryParse(fullName[(tick + 1)..], out var arity) ? arity : 0;
    }

    private static string DisplaySimpleName(string cref)
    {
        // cref values arrive as "T:StreamBus.StreamBusException" or plain names.
        if (cref.Contains(':'))
        {
            cref = cref[(cref.IndexOf(':') + 1)..];
        }

        cref = NormalizeRoot(cref);
        return cref.Replace('`', '-');
    }

    private static string NormalizeRoot(string name) =>
        name.StartsWith(RootNamespace, StringComparison.Ordinal)
            ? name[RootNamespace.Length..]
            : name;

    private static Builder GetOrAddBuilder(Dictionary<string, Builder> types, string fullName, bool isType)
    {
        if (!types.TryGetValue(fullName, out var builder))
        {
            builder = new Builder(fullName) { IsType = isType };
            types[fullName] = builder;
        }
        else if (isType)
        {
            builder.IsType = true;
        }

        return builder;
    }

    /// <summary>
    /// Walks <paramref name="path"/> up (stripping the last dotted segment at
    /// each step) until an existing builder that is a documented type
    /// (a page of its own) is found. On success, <paramref name="path"/> is
    /// set to the builder's full name, <paramref name="prefix"/> carries the
    /// stripped segments (e.g. "WaitUpTo."), and <paramref name="builder"/>
    /// is the attach target.
    /// </summary>
    private static bool TryAscend(Dictionary<string, Builder> types, ref string path, out string prefix, out Builder builder)
    {
        prefix = string.Empty;
        while (true)
        {
            if (types.TryGetValue(path, out var candidate) && candidate.IsType)
            {
                builder = candidate;
                return true;
            }

            var dot = path.LastIndexOf('.');
            if (dot < 0)
            {
                break;
            }

            prefix = path[(dot + 1)..] + (prefix.Length > 0 ? "." + prefix : string.Empty);
            path = path[..dot];
        }

        builder = null!;
        return false;
    }

    private static (string FullName, string Name, string Slug) ParseTypePortion(string rawName)
    {
        var fullName = NormalizeRoot(rawName);
        var arity = CountTypeArity(rawName);
        var displayName = arity > 0
            ? fullName[..fullName.IndexOf('`')] + "<" + string.Join(", ", Enumerable.Range(0, arity).Select(TypeParamName)) + ">"
            : fullName;
        return (fullName, displayName, Slugify(displayName));
    }

    public static string Slugify(string name)
    {
        var sb = new StringBuilder(name.Length);
        foreach (var ch in name)
        {
            if (ch is '`' or '<' or '>' or ',' or '.' or '{' or '}' or ' ' or '(' or ')')
            {
                // Collapse duplicate separators: the previous branch already
                // emitted one.
                if (sb.Length > 0 && sb[^1] != '-')
                {
                    sb.Append('-');
                }
            }
            else
            {
                sb.Append(ch);
            }
        }

        return sb.ToString().TrimEnd('-');
    }

    private static string Extract(XElement element, string tag)
    {
        var value = element.Element(tag);
        return value is null ? string.Empty : InnerText(value);
    }

    private static string InnerText(XElement element)
    {
        var text = string.Concat(element.Nodes().OfType<XText>().Select(t => t.Value)).Trim();
        return WhitespaceRegex().Replace(text, " ");
    }

    [System.Text.RegularExpressions.GeneratedRegex(@"\s+")]
    private static partial System.Text.RegularExpressions.Regex WhitespaceRegex();

    private sealed class Builder(string fullName)
    {
        public string FullName { get; } = fullName;

        public bool IsType { get; set; }

        public string DeclaredName { get; set; } = fullName;

        public string Slug { get; set; } = Slugify(fullName);

        public string? Summary { get; set; }

        public string? Remarks { get; set; }

        public List<ApiMember> Members { get; } = [];

        public bool OnlyFields { get; set; } = true;

        public ApiType Build()
        {
            var kind = Kind();
            return new ApiType(FullName, DeclaredName, Slug, kind, Summary ?? string.Empty, Remarks ?? string.Empty, Members);
        }

        private ApiTypeKind Kind()
        {
            if (OnlyFields && Members.Count > 0)
            {
                return ApiTypeKind.Enum;
            }

            if (Members.Exists(m => m.Kind is ApiMemberKind.Method or ApiMemberKind.Property)
                && DeclaredName.Length > 1
                && DeclaredName[0] == 'I'
                && char.IsUpper(DeclaredName[1]))
            {
                return ApiTypeKind.Interface;
            }

            return ApiTypeKind.Class;
        }
    }
}
