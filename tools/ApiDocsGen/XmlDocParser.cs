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

        var kind = ToKind(kindChar);

        // Display name: strip method generic arity backticks, and show
        // constructors as "constructor".
        if (kind == ApiMemberKind.Method)
        {
            var arityTick = memberName.LastIndexOf("``", StringComparison.Ordinal);
            if (arityTick > 0)
            {
                memberName = memberName[..arityTick];
            }

            if (memberName.EndsWith("#ctor", StringComparison.Ordinal))
            {
                memberName = memberName[..^"#ctor".Length] + "constructor";
            }
        }

        var signature = kindChar == 'M'
            ? RenderMethodSignature(memberName, rawName)
            : memberName;

        target.Members.Add(new ApiMember(memberName, signature, kind, summary, remarks, exceptions));
        target.OnlyFields &= kind == ApiMemberKind.Field;
    }

    private static ApiMemberKind ToKind(char kindChar) => kindChar switch
    {
        'M' => ApiMemberKind.Method,
        'P' => ApiMemberKind.Property,
        _ => ApiMemberKind.Field,
    };

    private static string RenderMethodSignature(string memberName, string rawName)
    {
        // Method-level generic arity lives on the raw XML name
        // (e.g. "GetService``1"), before display normalization strips it.
        var openParen = rawName.IndexOf('(');
        var paramText = openParen >= 0 ? rawName[(openParen + 1)..].TrimEnd(')') : string.Empty;
        var head = openParen >= 0 ? rawName[..openParen] : rawName;
        var rawLeaf = head[(head.LastIndexOf('.') + 1)..];
        var arity = 0;
        var tick = rawLeaf.LastIndexOf("``", StringComparison.Ordinal);
        if (tick > 0)
        {
            _ = int.TryParse(rawLeaf[(tick + 2)..], out arity);
        }

        var sb = new StringBuilder();
        if (memberName.Contains('.'))
        {
            // Member of a nested type: keep the nested prefix visible.
            sb.Append(memberName).Append('(');
        }
        else
        {
            sb.Append(memberName);
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
        if (cref.StartsWith("System.", StringComparison.Ordinal))
        {
            cref = cref["System.".Length..];
        }

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
        var text = RenderNodes(element).Trim();
        return WhitespaceRegex().Replace(text, " ");
    }

    /// <summary>
    /// Renders documentation children inline: text nodes pass through, while
    /// see/paramref/typeparamref references are replaced by their simple
    /// names so no cref markup leaks into the page text.
    /// </summary>
    private static string RenderNodes(XElement element)
    {
        var sb = new StringBuilder();
        foreach (var node in element.Nodes())
        {
            switch (node)
            {
                case XText text:
                    sb.Append(text.Value);
                    break;
                case XElement inline when inline.Name.LocalName is "see" or "seealso":
                    sb.Append(DisplaySimpleName(inline.Attribute("cref")?.Value ?? inline.Attribute("langword")?.Value ?? string.Empty));
                    break;
                case XElement inline when inline.Name.LocalName is "paramref" or "typeparamref":
                    sb.Append(inline.Attribute("name")?.Value ?? string.Empty);
                    break;
                case XElement inline when inline.Name.LocalName == "c":
                    sb.Append(inline.Value);
                    break;
                case XElement nested:
                    sb.Append(string.Concat(nested.Nodes().OfType<XText>().Select(t => t.Value)));
                    break;
                default:
                    break;
            }
        }

        return sb.ToString();
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

            // The XML carries no interface flag; the codebase's interface
            // convention is the I-prefixed name.
            if (DeclaredName.Length > 1 && DeclaredName[0] == 'I' && char.IsUpper(DeclaredName[1]))
            {
                return ApiTypeKind.Interface;
            }

            return ApiTypeKind.Class;
        }
    }
}
