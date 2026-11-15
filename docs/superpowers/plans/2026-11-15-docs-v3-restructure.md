# StreamBus Docs v3 — Concept-first Site Rewrite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the StreamLight/Starlight docs site into the three-tier concept-first structure (Concepts → Guides → Configuration, 20 pages + splash) defined in spec §9.

**Architecture:** New pages are written under new slugs (`concepts/`, `guides/`, `configuration/`) while old pages stay untouched; one cutover task then swaps the sidebar, rewrites the splash hero and README links, and deletes the 14 old pages. Every C# snippet is verified by a recreated snippet-harness console project; the site build is verified per tier and fully at cutover.

**Tech Stack:** Astro + Starlight (`docs/`, pnpm), MDX pages, .NET 10 console snippet harness referencing `src/StreamBus`.

**Spec:** `docs/superpowers/specs/2026-11-15-docs-structure-v3-design.md` — the page-by-page content lives in spec §9 (C1–C7, G1–G9, Cfg1–Cfg5), editorial rules in §3, quality bar in §5, old-content mapping in §2. **This plan does not repeat page content; each task executes one §9 page spec.** Cross-cutting conventions (running example `OrderPlaced`/`orders`/group `billing`, file-path-labeled code blocks, FAQ sections on guides, omissions lists) are in spec §9's preamble and apply to every task.

## Global Constraints

- Site base path is `/StreamBus` (hero links hardcode it, e.g. `/StreamBus/concepts/introduction/`).
- Working branch: `nirav/feature/FixDocs` (the docs already live here; continue on it).
- Pages are MDX (`.mdx`) under `docs/src/content/docs/`; Starlight `Tabs`/`TabItem` available and UI imports come from `@astrojs/starlight/components`.
- No fake APIs: the only Planned note is `Handle<T>`/`Ignore<T>` in C7 per spec §6 — wording verbatim from the spec, no compilable-looking snippet.
- Every public C# snippet must compile against `src/StreamBus` and pass `AddStreamBus` registration validation via the harness (spec §5). Micro-snippets inside prose (interface signatures, single expressions) are exempt.
- Procedure for every harness run: pages contribute one method `Verify<Page>` calling each registration snippet; a page's publish/consume **runtime** awaits must use fake continuation bodies — only registration validation runs, no broker.
- `localhost` and `(context, cfg)` lambda style everywhere; never `rabbitmq.internal`, never `_` discards (spec decision log 2026-11-14).
- Commit style: small commits per page, imperative subject (repo uses `feat:`/`docs:`/`fix:` prefixes).
- Do not modify anything under `docs/superpowers/` except adding this plan and new specs.

## Review Focus

Content failure modes the spec implies but per-page verification won't catch; each is pinned to its owning task's steps:

1. **Snippets that can't compile as extracted** (fragment referencing symbols a page never defines) — pinned: every task's extract step requires naming hidden context in `// ...` lines when the on-page excerpt elides usings/types, and the harness refuses to add hidden definitions beyond usings.
2. **Tier repetition** (same concept or the full `AddStreamBus` block on two pages) — pinned to Task 24's repetition audit (grep + read pass; full block allowed only in `guides/get-started`).
3. **Broken cross-links after slug change** — pinned to Task 23's link-verification step.
4. **MDX syntax hazards in prose** (raw angle brackets like `IConsumer<OrderPlaced>` outside code fences) — pinned to per-tier `pnpm build` steps (Tasks 8, 17, 22) rather than per page.
5. **Stale old-page facts carried into new pages** (e.g. claiming manual offset control defaults, wrong `Exponential` signature) — pinned: each task's page-writing step requires the fact-check against `src/StreamBus` for every behavior sentence salvaged from a deleted page (mapping in spec §2).

---

### Task 0: Commit the design doc

**Files:**
- Modify: (git index only) `docs/superpowers/specs/2026-11-15-docs-structure-v3-design.md`

- [ ] **Step 1: Commit the already-written spec file**

The file was written this session and is currently uncommitted (`AM` in git status).

Run: `git add docs/superpowers/specs/2026-11-15-docs-structure-v3-design.md && git commit -m "docs: add docs v3 concept-first structure design"`

---

### Task 1: Recreate the snippet harness

**Files:**
- Create: `tests/StreamBus.DocsSnippets/StreamBus.DocsSnippets.csproj`
- Create: `tests/StreamBus.DocsSnippets/Program.cs`

**Interfaces:**
- Produces: a runnable harness invoked as `dotnet run --project tests/StreamBus.DocsSnippets` printing `ALL SNIPPET REGISTRATIONS VALID` when every page method registers successfully; per-page verification methods `Verify<Page>()` (e.g. `VerifyGetStarted()`, `VerifyScaleOut()`) added by later tasks; failed registration prints the page name and throws. One shared file `Usings.cs` (implicit usings ns) plus one file per task group is acceptable; a single `Program.cs` with regions per page is preferred while total snippets < 40.

- [ ] **Step 1: Create the csproj**

net10.0 `Exe` project referencing `../../src/StreamBus/StreamBus.csproj` plus `Microsoft.Extensions.Hosting`; no test framework needed (registration-time validation is the check).

- [ ] **Step 2: Create Program.cs with the harness skeleton**

`Main` calls each existing `Verify*` method in sequence, wrapped so a failure prints `FAILED: <page> : <message>` and sets a non-zero exit; on all-success prints `ALL SNIPPET REGISTRATIONS VALID`. Seed with `VerifyHarness Smoke()` proving the pattern: one `AddStreamBus` registration with `cfg.Host("localhost", "/", h => h.Credentials("guest", "guest"))`, built and validated.

- [ ] **Step 3: Run and verify green on the seed**

Run: `dotnet run --project tests/StreamBus.DocsSnippets`
Expected: `ALL SNIPPET REGISTRATIONS VALID`.

- [ ] **Step 4: Commit**

`git add tests/StreamBus.DocsSnippets && git commit -m "test: recreate docs snippet harness"`

---

### Tasks 2–8: Concept pages (write in this order)

Each task follows the same **five-step pattern**, differing only in page; steps are written out once here and apply to all seven:

**Files (respective task):**
- Create: `docs/src/content/docs/concepts/introduction.mdx` (Task 2), `concepts/streams.mdx` (3), `concepts/messages.mdx` (4), `concepts/producers.mdx` (5), `concepts/consumers.mdx` (6), `concepts/middleware.mdx` (7), `concepts/exceptions.mdx` (8)

- [ ] **Step 1: Write the page exactly per its spec §9 block (C1–C7)**

Frontmatter: `title` and a `description` of one sentence. Every behavior sentence salvaged from a legacy page (mapping in spec §2) must be re-verified against `src/StreamBus` before writing (e.g. C7's unbounded-default and C6's filter contracts are verified facts from this session's source reads — cite files in doubt). Omissions listed in each §9 block are hard boundaries: no application-scale code on concepts pages.

- [ ] **Step 2: Extract snippets into the harness**

Add snippets from the page (micro-prose snippets exempt) to `Program.cs` as `Verify<Page>()`. Definitions the page's excerpt elides must appear as visible `// ...` comment lines on the page — the harness may add only usings, never hidden types.

- [ ] **Step 3: Run the harness**

Run: `dotnet run --project tests/StreamBus.DocsSnippets`
Expected: `ALL SNIPPET REGISTRATIONS VALID`.

- [ ] **Step 4: Commit**

`git add docs/src/content/docs/concepts/<page>.mdx tests/StreamBus.DocsSnippets && git commit -m "docs: concept page <title>"`

---

### Task 8a (part of Task 8's commit): the Planned exception-filtering note

- [ ] In `concepts/exceptions.mdx`, the transient-vs-poison section carries the Planned note **verbatim from spec §6** (blockquote, `:::note[Planned — exception filtering]`), with no code snippet and no promise of a release number. Content otherwise documents today's behavior faithfully.

---

### Task 8b: Tier build check — Concepts

- [ ] Run `pnpm --dir docs build`
Expected: build succeeds; the new `concepts/*` pages are built even though the sidebar doesn't list them yet (Starlight builds all content pages as drafts only if configured — if they are built, proceed; if Starlight skips unlisted pages, note it — sidebar wiring happens in Task 23 and the real page-count check happens there).

- [ ] Commit any fixes with `fix: concepts tier build nits`

---

### Task 9: Guide — Get started

**Files:**
- Create: `docs/src/content/docs/guides/get-started.mdx`

**Interfaces:**
- Produces: `guides/get-started/` slug, later linked from `concepts/introduction` (skip-to-code callout, Task 2) and the splash hero (Task 23).

- [ ] **Step 1: Write the page per spec §9.G1** — merge of Installation + Quickstart; the `Program.cs` section must be one complete file-path-labeled block (label it `Program.cs`), reusing the verified Quickstart program verbatim (including `OrderPlaced`, `OrderConsumer`, `OrderPublisher`); keep plugin-enabling Tabs, the line-up walkthrough, the 4-step startup story, and the Ctrl+C offsets-resume beat.

- [ ] **Step 2: Verbatim-build check** — create a scratch console project (under `C:\Users\TGSUser227\AppData\Local\Temp\opencode\docs-scratch`) referencing `src/StreamBus`, whose `Program.cs` is the page's `Program.cs` block verbatim.

Run: scratch `dotnet build` → Expected: Build succeeded.

- [ ] **Step 3: Add `VerifyGetStarted()` to the harness and run** → `ALL SNIPPET REGISTRATIONS VALID`.

- [ ] **Step 4: Commit** — `docs: guide get started`

---

### Tasks 10–17: Remaining guides (this order)

Files: Create `docs/src/content/docs/guides/` … `publish-an-event.mdx` (10), `consume-events.mdx` (11), `handle-failures.mdx` (12), `scale-out.mdx` (13), `partition-a-stream.mdx` (14), `use-multiple-buses.mdx` (15), `replay-a-stream.mdx` (16), `add-cross-cutting-logic.mdx` (17).

- [ ] Per task, the same five steps as the Concepts pattern (spec §9.G2–G9; FAQ sections mandatory; `Verify*` harness method; commit `docs: guide <slug>`). Task-specific notes:

- **Task 14** (`partition-a-stream`): partition-count/agreement content is salvaged from legacy `topology.mdx` + `publishers.mdx` — verify `Partitions(n)`, `PublishSuperAsync` signatures, and the broker-count-wins rule in `src/StreamBus` first; share-partition-read wording points to Task 16's *why/when* owner (G6 owns the *how*).
- **Task 15** (`use-multiple-buses`): the DI failure-modes section is the verified source truth from this session (`StreamBusConfigurator.cs:1124-1127`, `Registry.cs`, `Bus.cs:131,135-136`): constructor injection of an unregistered marker throws `InvalidOperationException`; `GetService`/optional-default yields null; plain `IStreamBus` requires the non-generic `AddStreamBus`. No speculation.
- **Task 16** (`replay-a-stream`): `SetConsumerOffsetAsync`/`GetConsumerOffsetAsync`, `StartOffset` values, and `bus.Topology.Ensure*Async` verified in `Bus.cs` and `ConsumeOptions.cs` first; adopts the rewind-hazard callout from legacy `consumers.mdx` lines on offset overwrite.
- **Task 12** (`handle-failures`): all exhaustion semantics and the schedule table are already-fact-checked salvage from legacy `error-handling.mdx`; preserves `ErrorEnvelope` usage block.

---

### Task 17b: Tier build check — Guides

- [ ] `pnpm --dir docs build` → green, or fixes committed as `fix: guides tier build nits`.

---

### Tasks 18–22: Configuration pages

Files: Create `docs/src/content/docs/configuration/host-and-connections.mdx` (18), `endpoint-options.mdx` (19), `retry-and-dead-letters.mdx` (20), `observability.mdx` (21), `transport-security.mdx` (22).

- [ ] Per task, same five-step pattern (spec §9.Cfg1–Cfg5; every table keeps a Defaults column; links back to owning concept page; no teaching prose).

- **Task 18**: content is salvage from legacy `configuration.mdx` + `lifecycle.mdx` tables (host options, readiness policies, `BusConnectionState`) — verify option names in `src/StreamBus/Hosting.cs` and `Configuration/` first.
- **Task 19**: includes stream-spec knobs + agreement summary moved from legacy `topology.mdx`; full rules live in guide `partition-a-stream` — this page keeps only the reference table.
- **Task 21/22**: near-verbatim salvage of legacy `observability.mdx` / `transport-security.mdx` (spec says closest-to-target); change frontmatter only; verify metric names in `src/StreamBus` (`StreamBusMetrics`) once.
- **Task 22 commit doubles as the tier build check**: add `pnpm --dir docs build` before committing.

---

### Task 23: Cutover — sidebar, hero, deletions, README

**Files:**
- Modify: `docs/astro.config.mjs` (sidebar)
- Modify: `docs/src/content/docs/index.mdx` (hero cards + both hero links)
- Modify: `README.md` (doc links)
- Delete: the 14 legacy `.mdx` files in `docs/src/content/docs/` root (introduction, installation, quickstart, architecture, configuration, topology, lifecycle, publishers, consumers, error-handling, transport-security, observability, multiple-buses, middleware)

**Interfaces:**
- Produces: final sidebar — three groups in order: `Concepts` (concepts/introduction → streams → messages → producers → consumers → middleware → exceptions), `Guides` (guides/get-started → publish-an-event → consume-events → handle-failures → scale-out → partition-a-stream → use-multiple-buses → replay-a-stream → add-cross-cutting-logic), `Configuration` (configuration/host-and-connections → endpoint-options → retry-and-dead-letters → observability → transport-security).

- [ ] **Step 1: Replace the sidebar** in `docs/astro.config.mjs` with the structure above (labels `Concepts`, `Guides`, `Configuration`).

- [ ] **Step 2: Rewrite the splash** — hero `Get Started` link → `/StreamBus/concepts/introduction/`; second action → `guides/get-started/`; card grid trimmed to stay non-duplicative with Concepts per spec §2 (four or fewer cards, none repeating concept-page sentences).

- [ ] **Step 3: Delete the 14 legacy pages** (`git rm`). No redirect stubs (spec decision 2026-11-15).

- [ ] **Step 4: Update README links** to the new slugs (README currently points at old page slugs — update each occurrence; keep badges/quickstart as-is).

- [ ] **Step 5: Build the whole site**

Run: `pnpm --dir docs build`
Expected: 21 pages (splash + 20 content). If Starlight refuses unlisted pages elsewhere, any listing `docs/guides/*` artifacts from Tasks 8b/17b confirmed listed now.

- [ ] **Step 6: Link check** — grep the built output plus source `.mdx` for `](../` / `](/StreamBus/` references: every relative link resolves to an existing slug; zero references to any deleted slug anywhere in `docs/src`, `README.md`.

- [ ] **Step 7: Commit** — `docs: three-tier cutover (closes the v3 restructure)`

---

### Task 24: Final QA — editorial rules + repetition audit

**Files:**
- Modify: any page failing audit (as fixes)

- [ ] **Step 1: Repetition audit (Review Focus #2)** — for each tier boundary: no full `AddStreamBus` block outside `guides/get-started`; each concept taught once (grep candidate duplications: "at-least-once", "single-active", "cursor", "back-pressure" — occurrences must link, not re-teach). Fix violations by shortening to a link.

- [ ] **Step 2: Editorial-rules pass (spec §3)** — one read-through of all 20 pages checking: problem-first/definition-first openers, snippet justification sentence, no "In this article" style, FAQ present on all 9 guides, omissions respected, Planned note present exactly once (C7).

- [ ] **Step 3: Fresh-eyes review** — dispatch a reviewer subagent: "You are a .NET developer who has never heard of RabbitMQ Streams. Read the built starting at concepts/introduction: report (a) any point you were confused, (b) any page teaching API before concept, (c) any missing must-know before writing code." `pnpm --dir docs preview` (background) for the reviewer if needed, or hand it the mdx files.

- [ ] **Step 4: Full re-verify** — `dotnet run --project tests/StreamBus.DocsSnippets` → `ALL SNIPPET REGISTRATIONS VALID`; `pnpm --dir docs build` → green, 21 pages.

- [ ] **Step 5: Commit** — `docs: v3 editorial QA pass` — and report completion per superpowers:finishing a development branch when merging is decided.
