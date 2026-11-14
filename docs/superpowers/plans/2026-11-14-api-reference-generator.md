# API Reference Generator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generate a Starlight API reference (one page per public type) from the compiled `StreamBus.xml` doc file, wired into the docs site build and CI.

**Architecture:** A zero-dependency C# console tool (`tools/ApiDocsGen`) parses the XML doc file with `System.Xml.Linq`, builds a type/member model, and emits one `.mdx` page per public type into `docs/src/content/docs/api/`. The docs site sidebar auto-generates the API Reference section from that directory, and the Pages CI workflow builds the library and runs the generator before the Astro build. No DocFX; no npm dependencies (decision in design doc §10, second session).

**Tech Stack:** .NET 10 console app (System.Xml.Linq), xUnit, Astro/Starlight autogenerate sidebar, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-11-11-documentation-rewrite-design.md` — §5 (superseded approach), §10 third-session decision "generator script consuming the compiled StreamBus.xml into Starlight pages (lightweight P6; DocFX not adopted)"; decision log entry "Still open: XML→Starlight API reference generator".

## Global Constraints

- `GenerateDocumentationFile` is already `true` in `src/StreamBus/StreamBus.csproj`; current coverage is 515/516 members with non-empty summaries — **no doc-comment authoring pass is in scope**.
- Generator must emit deterministic output (same XML → byte-identical files) so diffs and CI caching behave.
- Generator must escape MDX-hostile text (generics like `IConsumer<T>`, angle brackets in crefs) — all signatures go inside fenced code blocks.
- Output pages are `.mdx` with Starlight frontmatter (`title`); slugs must be filesystem-safe (no backticks, parentheses, or commas in filenames).
- Insignificant members are skipped: `P:...Item`, compiler-generated (name contains `k__BackingField` or `(` after `..`), nested private types. Everything else in `T:`/`M:`/`P:`/`F:` is emitted.
- The docs site must keep building: `pnpm build` in `docs/` must succeed with the generated directory in place.

## Review Focus

- Generic type/method names in XML use ``` ``1 ``` backtick arity — a naive splitter corrupts `IConsumer`1` into `IConsumer'1`; members with generic *parameters* like `M:StreamBus.Bus.StreamBus.IStreamBus`1.Publish``1(...)` combine both. Tests must pin decomposition.
- Cref-encoded operator/parameter syntax (`System.String`, ``` `0 ``` type references, parens) must be rendered as readable C#, not raw cref — `Publish``1(StreamBus.IStreamSendHeaders)` must not appear on the page.
- Rerunning the generator after a type is renamed must not leave the stale page behind (deleted-member pages are orphans that break the sidebar). Emission must clear the output directory.
- A member whose `<summary>` is empty (1 exists today) must render with a "No description." body, not an empty section or a crash.
- Starlight frontmatter titles containing `|` or `:` break YAML — titles must be sanitized.

---

### Task 1: XML parsing + member-name decomposition core

**Files:**
- Create: `tools/ApiDocsGen/ApiDocsGen.csproj`
- Create: `tools/ApiDocsGen/ApiModel.cs`
- Create: `tools/ApiDocsGen/XmlDocParser.cs`
- Test: `tools/ApiDocsGen.Tests/ApiDocsGen.Tests.csproj`, `tools/ApiDocsGen.Tests/XmlDocParserTests.cs`

**Interfaces:**
- Produces: `XmlDocParser.Parse(string xmlPath) -> IReadOnlyList<ApiType>`; `ApiType { string Name (C# name, e.g. "IConsumer<T>"), string Slug, string Summary, string Remarks, List<ApiMember> Members, ApiTypeKind Kind }`; `ApiMember { string Signature, string Summary, string Remarks, IReadOnlyList<string> Exceptions }`.
- Consumes: nothing.

- [ ] **Step 1: Create both csproj files and the failing tests**

`ApiDocsGen.csproj`: net10.0, `OutputType Exe`, no packages. Tests: xUnit + `Microsoft.NET.Test.Sdk`. Tests parse an inline fixture string (write to temp file):

```csharp
[Fact] ParseTypeMember()          // T: member → ApiType.Name "IConsumer`1" → C# "IConsumer<T>"
[Fact] NestedTypeGetsOwnName()    // T:StreamBus.ReadinessPolicy.WaitUpTo → nested member of ReadinessPolicy
[Fact] ParseMethodWithParams()    // M:StreamBus.Bus.Publish``1(System.String) → Signature contains "Publish" and "string"
[Fact] ParsePropertyAndEnum()     // P:..., F:... produce the right member kinds
[Fact] EmptySummary()             // member with empty summary yields Summary "" (Task 3 renders fallback)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `dotnet test tools/ApiDocsGen.Tests`
Expected: compile FAIL — `XmlDocParser` not defined.

- [ ] **Step 3: Implement `XmlDocParser` + `ApiModel`**

Model rules: `T:` entries become `ApiType`s (generic arity `` `n `` → `<T>`, `<TKey, TValue>` style); nested types are grouped under their parent type as members of the same page. `M:`/`P:`/`F:` attach to the owning type by name prefix; arity backticks are decomposed before parameter parsing; cref parameter types are mapped to C# keyword forms (`System.String`→`string`, `` `0 ``→`T`) — keep this mapping in a small static dictionary/table inside `XmlDocParser`. Skip `Item` properties, `op_` members, and `k__BackingField`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test tools/ApiDocsGen.Tests`
Expected: all 5 PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/ApiDocsGen tools/ApiDocsGen.Tests
git commit -m "feat: add XML doc parser core for the API reference generator"
```

### Task 2: Page rendering + CLI emission

**Files:**
- Create: `tools/ApiDocsGen/PageRenderer.cs`
- Create: `tools/ApiDocsGen/Program.cs`
- Test: `tools/ApiDocsGen.Tests/PageRendererTests.cs`, `tools/ApiDocsGen.Tests/ProgramTests.cs`

**Interfaces:**
- Consumes: `ApiType`/`ApiMember` from Task 1.
- Produces: `PageRenderer.Render(ApiType) -> string` (full `.mdx` file content); CLI `dotnet run --project tools/ApiDocsGen -- <xmlPath> <outputDir>` exits 0 and writes pages, clearing `outputDir` first.

- [ ] **Step 1: Write failing renderer tests**

Assertions (fixture `ApiType` from Task 1's parser):
- output starts with frontmatter: `---\ntitle: IConsumer<T>\n---`
- generic name appears escaped inside a code fence, never as bare `<T>` outside one (	assert: no line outside fences contains `<`)
- all members listed under `## Methods` / `## Properties` / `## Fields` headings, each signature fenced
- `<exception cref="...">` entries render as a bulleted "Exceptions" list
- empty `Summary` renders `No description.`

- [ ] **Step 2: Run tests to verify they fail**

Run: `dotnet test tools/ApiDocsGen.Tests`
Expected: FAIL — `PageRenderer` not defined.

- [ ] **Step 3: Implement `PageRenderer` and `Program`**

Render order: frontmatter → type summary → syntax fence (declaration line) → Remarks → member sections sorted alphabetically. `Program`: parse args; if `outputDir` exists, delete `*.mdx` inside it; render every type; write `<Slug>.mdx` where slug = type name with `` ` ``/`<`/`>`/`,`/`.` replaced by `-` (e.g. `IConsumer-T.mdx`); print `Generated N pages` to stdout; exit 0. Exit 1 with message to stderr on bad args or XML load failure.

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test tools/ApiDocsGen.Tests`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/ApiDocsGen tools/ApiDocsGen.Tests
git commit -m "feat: render Starlight API reference pages from the XML doc model"
```

### Task 3: Run against the real assembly + wire into the site

**Files:**
- Modify: `docs/astro.config.mjs` (sidebar)
- Modify: `README.md` (only if it links to the docs — add nothing new if it already points at the site)
- Modify: `docs/superpowers/specs/2026-11-11-documentation-rewrite-design.md` (decision log entry)

**Interfaces:**
- Consumes: CLI from Task 2; `docs/src/content/docs/api/` output directory.

- [ ] **Step 1: Build and run the generator against the real XML**

Run: `dotnet build src/StreamBus -v q && dotnet run --project tools/ApiDocsGen -- src/StreamBus/bin/Debug/net10.0/StreamBus.xml docs/src/content/docs/api`
Expected: stdout `Generated ~86 pages` (top-level `T:` count from the audit); directory populated.

- [ ] **Step 2: Add sidebar autogenerate group**

In `docs/astro.config.mjs`, append after the "In production" group:

```js
{
  label: 'API reference',
  autogenerate: { directory: 'api' },
},
```

- [ ] **Step 3: Verify the site builds and spot-check pages**

Run: `pnpm build` (in `docs/`)
Expected: build succeeds, page count ≈ 16 + generated pages. Open `docs/dist/StreamBus/api/istreambus/index.html` in a browser: title, member sections, and code fences render correctly; sidebar lists `IConsumer<T>` (or its slugified title) without raw backticks.

- [ ] **Step 4: Add the decision-log entry (P6 complete) and commit generated output**

Append to §9 of the design doc: API reference shipped as generator, tool path, output path, page count. Commit generated pages together with the config change (generated files are checked in — the Pages workflow in Task 4 also regenerates them as a freshness check).

```bash
git add docs/astro.config.mjs docs/src/content/docs/api docs/superpowers/specs/2026-11-11-documentation-rewrite-design.md
git commit -m "docs: generated API reference section wired into the sidebar"
```

### Task 4: CI wiring + regeneration guard

**Files:**
- Modify: `.github/workflows/docs.yml`

- [ ] **Step 1: Add .NET build + generator steps before the Astro build**

Insert after checkout, before "Install dependencies" (order does not matter vs pnpm; keep the XML build first so a failure fails fast):

```yaml
      - name: Setup .NET
        uses: actions/setup-dotnet@v4
        with:
          dotnet-version: 10.0.x
      - name: Build library XML docs
        run: dotnet build src/StreamBus -v q
      - name: Generate API reference
        run: dotnet run --project tools/ApiDocsGen -- src/StreamBus/bin/Debug/net10.0/StreamBus.xml docs/src/content/docs/api
```

Because generation is deterministic and Task 3 committed the output, generation in CI doubles as a staleness check: use `git diff --exit-code docs/src/content/docs/api` after the step to fail when checked-in pages drift from the XML.

Add that diff line as a separate workflow step:

```yaml
      - name: Check API reference freshness
        run: git diff --exit-code -- docs/src/content/docs/api
```

- [ ] **Step 2: Verify locally**

Re-run generation twice; second run produces no `git diff` output (determinism proof). Then `git diff --exit-code` passes.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/docs.yml
git commit -m "ci: build XML docs and regenerate API reference in the Pages workflow"
```

## Self-review

Coverage: §10 open item "XML→Starlight API reference generator" → Tasks 1–3; CI/hosting concern (§6, broken builds fail) → Task 4; no doc-comment authoring needed (audit: 515/516 summaries). Review-focus items pinned: backtick decomposition (Task 1 tests), cref rendering (Task 1+2), stale pages (Task 2 clear-dir + tests), empty summary fallback (Task 2), YAML title sanitation (Task 2). Proportion: plan ≈ spec remainder.
