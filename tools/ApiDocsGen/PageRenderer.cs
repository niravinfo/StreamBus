using System.Text;

namespace ApiDocsGen;

/// <summary>Renders an <see cref="ApiType"/> model to a Starlight .mdx page.</summary>
public static class PageRenderer
{
    public static string Render(ApiType type, IReadOnlyCollection<string> knownTypeSlugs)
    {
        var sb = new StringBuilder();

        sb.Append("---\ntitle: ").Append(SanitizeTitle(type.Name)).Append("\n---\n\n");
        sb.Append(EscapeBody(type.Summary.Length > 0 ? type.Summary : "No description.")).Append("\n\n");

        sb.Append("```csharp\n").Append(RenderDeclaration(type)).Append("\n```\n\n");

        if (type.Remarks.Length > 0)
        {
            sb.Append("## Remarks\n\n").Append(EscapeBody(type.Remarks)).Append("\n\n");
        }

        AppendSection(sb, "## Methods", type, ApiMemberKind.Method, knownTypeSlugs);
        AppendSection(sb, "## Properties", type, ApiMemberKind.Property, knownTypeSlugs);
        AppendSection(sb, "## Fields", type, ApiMemberKind.Field, knownTypeSlugs);
        AppendSection(sb, "## Nested types", type, ApiMemberKind.NestedType, knownTypeSlugs);

        return sb.ToString();
    }

    private static void AppendSection(StringBuilder sb, string heading, ApiType type, ApiMemberKind kind, IReadOnlyCollection<string> knownTypeSlugs)
    {
        var members = type.Members
            .Where(m => m.Kind == kind)
            .OrderBy(m => m.Name, StringComparer.Ordinal)
            .ToList();
        if (members.Count == 0)
        {
            return;
        }

        sb.Append(heading).Append("\n\n");
        foreach (var member in members)
        {
            sb.Append("### ").Append(member.Name).Append("\n\n");
            sb.Append(EscapeBody(member.Summary.Length > 0 ? member.Summary : "No description."))
              .Append("\n\n");
            sb.Append("```csharp\n").Append(member.Signature).Append("\n```\n\n");

            if (member.Remarks.Length > 0)
            {
                sb.Append(EscapeBody(member.Remarks)).Append("\n\n");
            }

            if (member.Exceptions.Count > 0)
            {
                foreach (var exception in member.Exceptions)
                {
                    var slug = XmlDocParser.Slugify(exception.TypeName);
                    // Only the library's own types have generated pages; BCL
                    // exceptions link nowhere, so they stay plain text.
                    sb.Append("- ");
                    if (knownTypeSlugs.Contains(slug))
                    {
                        sb.Append('[').Append(exception.TypeName).Append("](").Append(slug).Append(')');
                    }
                    else
                    {
                        sb.Append(exception.TypeName);
                    }

                    sb.Append(" — ").Append(EscapeBody(exception.Description)).Append('\n');
                }

                sb.Append('\n');
            }
        }
    }

    private static string RenderDeclaration(ApiType type)
    {
        var keyword = type.Kind switch
        {
            ApiTypeKind.Interface => "public interface",
            ApiTypeKind.Enum => "public enum",
            _ => "public class",
        };
        return keyword + " " + type.Name;
    }

    /// <summary>Replaces YAML-hostile characters in frontmatter titles.</summary>
    private static string SanitizeTitle(string title) => title.Replace('|', '-').Replace(':', '-');

    /// <summary>
    /// Escapes angle brackets in body text so MDX does not parse them as
    /// JSX. Content inside code fences is left untouched.
    /// </summary>
    private static string EscapeBody(string text) =>
        text.Replace("<", "&lt;").Replace(">", "&gt;").Replace("{", "&#123;").Replace("}", "&#125;");
}
