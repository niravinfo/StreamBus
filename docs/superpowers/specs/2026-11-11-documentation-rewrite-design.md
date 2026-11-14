# StreamBus Documentation Rewrite — Design & Tracking

**Status:** In progress — substantially implemented on branch `nirav/feature/FixDocs`
**Date:** 2026-11-11 (updated after discovering the branch state)

> **2026-11-11 correction:** This file was first written while on the wrong
> branch (`main`), which still had the old `docs/guide/*.md` layout. The real
> work is on `nirav/feature/FixDocs`. The original proposal below is kept for
> history; §8 below now reflects the actual shipped state of the branch.
**Purpose:** This file is the single source of truth for the documentation
rewrite. It is tracked in the repo (not in chat) so that multiple agents,
devices, and sessions stay in sync. Every finalized decision is recorded here
with a date. Any agent resuming this work MUST read this file first and append
new decisions at the bottom of the Decision Log rather than re-litigating
settled items.

---

## 1. Why

StreamBus's current docs (`docs/guide/*.md`) grew feature-by-feature alongside
the code. They assume the reader already understands the library's model,
don't cover every public capability, and don't follow the conventions of
standard .NET library documentation (Microsoft Learn style).

Goal: documentation written for a developer who knows RabbitMQ streams at a
casal level at best and knows **nothing** about StreamBus — from "what is
this?" to a running app in minutes, with every public functionality of the
library shown with a working example.

## 2. Finalized decisions (2026-11-11, user-confirmed)

| # | Decision |
|---|----------|
| D1 | All existing user-facing docs are **deleted**: `docs/guide/` (8 files) and `docs/llms.txt`. Rewritten from scratch; not edited in place. |
| D2 | Detailed documentation lives in a **Starlight** (Astro) docs website in the repo. |
| D3 | `README.md` stays and carries only: badges, a short high-level description, install snippet, the 90% quickstart, and links to the Starlight site. |
| D4 | Style target: **.NET API docs / Microsoft Learn conventions** and popular-library-docs standards (like MassTransit's docs): task-oriented pages, progressive disclosure (Get started → Fundamentals → Feature pages → API reference), every code sample complete and runnable. |
| D5 | Target reader **knows nothing about the library**; pages must introduce concepts before using them. Every functionality of the library must be shown. |
| D6 | Internal records are **kept** (not user-facing, out of scope for the rewrite): `docs/superpowers/`, `docs/adr/`, acceptance/requirements/design docs. |

## 3. Proposed website structure

Site lives at `docs/website/` (Astro + Starlight). Sidebar:

```
🏠 Home
├── Getting Started
│   ├── Introduction        (what StreamBus is, why it exists, when to use it)
│   ├── Installation        (NuGet, prerequisites: .NET 10, RabbitMQ 4.x, ports)
│   ├── Quickstart          (the 90% case, runnable end-to-end, explained line by line)
│   └── Tutorial: a complete app (publish + consume + retry + dead letter, built up step by step)
├── Fundamentals
│   ├── Architecture        (client library, buses, endpoints, connection supervision)
│   ├── Configuration Model (explicit fluent config, no binder, validation at Build)
│   ├── Hosting & Lifecycle (blocking/non-blocking start, readiness, StreamBusHostOptions)
│   ├── Serialization       (JSON per bus, typed messages)
│   └── Topology            (declarations, super streams, retention knobs, adopt rule)
├── Publishing
│   ├── Typed publish       (Publish<T>, route table, message/correlation IDs, headers)
│   ├── Super streams       (PublishSuper<T>, routing keys)
│   └── Escape hatches      (raw bytes publish)
├── Consuming
│   ├── Endpoints & groups  (ReceiveEndpoint, required consumer groups, SAC vs super partitions)
│   ├── Consumers           (IConsumer<T>, ConsumeContext features)
│   ├── Raw consume & offsets (manual offset get/set)
│   └── Middleware         (ordered consume filters)
├── Error Handling
│   ├── Retry               (global + per-endpoint schedules, escape hatch, bounded/unbounded)
│   ├── Exhaustion & dead letters (stream/queue destinations, {stream}_error default, OnExhausted)
│   └── Exceptions reference (exception hierarchy table)
├── Advanced
│   ├── Multi-bus            (marker buses, shared consumers)
│   ├── TLS                  (StreamBus TLS options)
│   ├── Publish middleware
│   └── Producers at scale   (anonymous producers, dedup behavior)
├── Observability
│   ├── Health checks        (per-registration health, aggregation)
│   ├── Metrics              (StreamBusMetrics names/semantics)
│   └── Logging              (log events, categories)
└── API Reference            (generated from XML doc comments — see §5)
```

Page template (MS Learn style): one-sentence summary ("In this article, you
learn to…"), prerequisite callouts, complete runnable C# samples, notes/warnings
for gotchas, and cross-links instead of repeating content.

## 4. README.md (after rewrite)

Badges (build, NuGet), 2–3 sentence positioning, install snippet, the 90%
quickstart (kept as-is — it's already good), a short feature bullet list where
each bullet links into the Starlight site, and a prominent link to the hosted
site. No deep content — details live on the site.

## 5. API reference — proposal (my call, correctable)

Microsoft-Learn-style API reference should be **generated from XML `///` doc
comments** on the public API (`AddStreamBus`, configurators, `IConsumer<T>`,
`ConsumeContext<T>`, retry/dead-letter APIs, options types, exceptions,
metrics names, health names). Tool: **DocFX** in a `docs/api` step consuming
the compiled assembly's XML doc file, linked from Starlight's API Reference
page. Consequences:

- Public API gets /// comments as part of this work (source of truth in code).
- Interim state until DocFX is wired: the API Reference section lists pages
  hand-written for the ~15 most important types, marked as generated-later.

## 6. CI / hosting (my call, correctable)

- `docs/website` builds in CI (npm build) so broken links/samples fail the build.
- Hosting: GitHub Pages on the `niravinfo/StreamBus` repo (`/StreamBus`
  subpath or custom domain) — final URL recorded here when set.

## 7. Open questions

- OQ1: Target URL for the hosted site (GitHub Pages path vs custom domain)?
- OQ2: Are the 0.2.0 released docs replaced wholesale, or do we keep the
  `docs/` markdown published anywhere (e.g. a `/docs` branch) during transition?
- OQ3: Is the historical design/acceptance content (currently linked from
  nothing after the guide deletion) ever needed by end users, or internal-only
  forever? (Assumed internal-only, D6.)

## 8. Actual state on branch `nirav/feature/FixDocs` (2026-11-11)

Already done (commits `e8d1534` → `a0a952f`):

- **D2 done**: Starlight site at `docs/`, GitHub Pages deploy wired
  (`https://niravinfo.github.io/StreamBus`, base path matches repo).
- **D1 done**: legacy guides, ADRs, process docs, and `llms.txt` removed
  (commit `d5692ca`).
- **D3 done**: README redesigned, badges kept, MassTransit scrubbed from
  package metadata/comments (commit `f3c444d`).
- **Site content** (`docs/src/content/docs/`): `index.mdx` (hero) +
  `guides/` — getting-started, configuration, publishing, consuming,
  errors, topology, multi-bus, hosting, observability, tls, changelog.
  Sidebar in `docs/astro.config.mjs`: Getting Started → Core → Advanced.
- **Review fixes applied** (`a0a952f`): ConsumeOptions property name,
  readiness policy names, metric tags, DLQ target registration.

Still open (carry-over of the original phases):

| Phase | Scope | Status |
|---|---|---|
| P6 | XML `///` comments on public API + generated API reference | Not started |
| P7 | Verify site CI build + Pages deploy after latest commits | To verify |
| — | Review pass of all 12 pages for zero-knowledge-reader tone | To verify |


## 9. Decision Log (append-only; newest at the bottom)

- 2026-11-11: D1–D6 finalized with the user (delete and rewrite, Starlight,
  README high-level + badges, MS Learn style, zero-knowledge reader, internal
  docs kept).
- 2026-11-11: Discovered the rewrite was already implemented on
  `nirav/feature/FixDocs` (earlier sessions). Website, all 12 content pages,
  README, legacy-doc deletion, and one review-fixes pass are committed
  (HEAD `a0a952f`). Remaining: API reference from XML comments (P6),
  verifying site CI/deploy, and a fresh-eyes review pass for
  zero-knowledge-reader tone.
- 2026-11-14: User rejected internal/jargon wording ("Typed Publish",
  "Escape hatches") and bare gerund menu names ("Publishing", "Consuming").
  Navigation must use standard concept-noun naming (MassTransit/NServiceBus/
  MS Learn prior art). **Adopted Option A (concept-noun scheme):** sidebar and
  page renames — Publishing→Producers, Consuming→Consumers, Errors &
  Retry→Error handling, Multi-bus→Multiple buses, TLS→Transport security;
  in-page: "Typed Publish" card→"Message publishing", "Escape hatches"→
  "Publishing raw messages", "Raw consume and manual offsets"→
  "Consuming raw messages"/"Manual offset control". Topology, Hosting,
  Observability kept.
- 2026-11-14: Full review-and-rewrite pass executed against the actual
  public API. Fixed in the pages: getting-started used the deleted
  `AddStreamBusHosting` API (0.2.0 changelog documents its removal) —
  replaced with the real options-pattern hosting; configuration stopped
  repeating the quickstart; error-handling had the wrong `Exponential`
  signature (`(count, base, max, multiplier)` → real
  `(retryCount, minDelay, maxDelay)`) and referenced the deleted
  `StreamBusStartupBlockedException`; observability health data keys were
  missing the `bus:`/`reg:` prefixes. Verification method: every C#
  snippet was lifted into a scratch project referencing src/StreamBus,
  compiled (`dotnet build`), and executed at registration time (AddStreamBus
  validates eagerly) — all pass. Remaining open: P6 (DocFX-generated API
  reference from the now-written XML comments) and the changelog lacks
  entries for releases after 0.2.0.

## 10. 2026-11-14 restart: fresh page set (user-confirmed)

User rejected patching the old flat `guides/` list; all Astro content
files were deleted and the information architecture re-decided from the
reader's perspective, phase by phase:

```
Home (splash)
├── Get started
│   ├── Introduction    — what/why/why-not; no code
│   ├── Installation    — NuGet, prerequisites, local broker
│   └── Quickstart      — one runnable program + what happens at startup
├── Fundamentals
│   ├── Architecture    — streams mental model + StreamBus's shape
│   ├── Configuration   — AddStreamBus, Host, validation model, serializer
│   ├── Topology        — declarations, agreement rules, adopt rule, runtime API
│   └── Lifecycle       — hosting options + readiness + supervision, one story
├── Messaging
│   ├── Publishing      — typed publish, budgets, properties, super streams,
│   │                     raw publish (section)
│   ├── Consumers       — endpoints, groups, scale-out, raw consume (section)
│   └── Error handling  — schedules, exhaustion, envelope, exceptions
└── In production
    ├── Transport security
    ├── Observability
    ├── Multiple buses
    └── Middleware      — shared filter contract for both pipelines
```

Decisions made with the user (2026-11-14, second session):
- Four-phase progressive structure approved (spec §3 sidebar superseded).
- Tutorial page: dropped; Quickstart stays small, Messaging pages carry depth.
- Streams mental model (offsets, groups, at-least-once) lives in
  Fundamentals/Architecture, not in Introduction.
- Raw publish/consume are sections inside Publishing/Consumers, not pages.
- Changelog page dropped for now (only covers 0.2.0; package is 0.4.0-beta.1).
- Middleware is one shared page; Publishing/Consumers keep short sections.
- API reference: user wants it; plan is a generator script consuming the
  compiled StreamBus.xml into Starlight pages (lightweight P6; DocFX not
  adopted). Second phase, after the content pages.
- Quality bar: every C# snippet must compile and pass AddStreamBus
  registration validation against src/StreamBus (snippet harness method
  retained from the first pass).
- 2026-11-14 (third session): Implemented §10. All 15 pages written fresh:
  index (splash), introduction, installation, quickstart, architecture,
  configuration, topology, lifecycle, publishing, consumers, error-handling,
  transport-security, observability, multiple-buses, middleware.
  Changelog dropped per decision above; README links updated to new slugs.
  Verification: quickstart Program.cs extracted verbatim from the page and
  built as a real project (Build succeeded); every other C# snippet
  compiles and passes AddStreamBus registration validation (harness:
  ALL SNIPPET REGISTRATIONS VALID); all 60 distinct API calls in pages
  exist in source; no broken relative links; Starlight builds 16 pages.
  Corrections caught during writing: raw-consume offsets auto-store per
  OffsetStore cadence (earlier draft wrongly said manual); rabbitmqadmin
  AMQP publish into a stream was unverified guesswork and was removed —
  quickstart is now fully self-contained (publish + consume in-process).
  Still open: XML→Starlight API reference generator (two-phase plan
  agreed), and a visual review pass in the browser.
- 2026-11-14 (review round): Style corrections per user feedback: (1) all
  "In this article, you learn..." blog-style openers removed — pages open
  with substance; (2) Quickstart heading "The program" → "Build the app";
  (3) Installation no longer tells users how to run a broker — it documents
  enabling the stream plugins (`rabbitmq_stream`, `rabbitmq_stream_management`)
  with Linux/Windows Tabs (Starlight supports them; verified in the build);
  the prior "streams are enabled out of the box" claim was wrong and is gone
  (ground truth: tests/StreamBus.IntegrationTests/TestBroker.cs + RabbitMQ docs);
  (4) lambda signature is consistently `(context, cfg)` everywhere — never
  the `_` discard; (5) `rabbitmq.internal` examples replaced with
  `localhost` everywhere. Re-verified after each change: snippet harness
  (ALL SNIPPET REGISTRATIONS VALID), verbatim quickstart build
  (succeeded), API-existence scan (60 calls, none unknown), Starlight
  build (tabs render, no intros).
- 2026-11-14 (P6 shipped): API reference implemented as a lightweight
  generator (DocFX rejected: it builds a second, unstyled site next to
  Starlight and its pipeline buys nothing at 86 types). Tool:
  `tools/ApiDocsGen` (zero-dependency .NET console app; xUnit tests in
  `tools/ApiDocsGen.Tests`), parses the compiled `src/StreamBus` XML doc
  file and writes one Starlight .mdx page per public top-level type (83
  pages; nested types like `ReadinessPolicy.WaitUpTo` are sections on
  their parent's page) into `docs/src/content/docs/api/`, sidebar via
  Starlight `autogenerate` ("API reference" group). Output is committed
  and deterministic; CI regenerates and diffs it as a freshness check.
  All 515 documented members render (MDX-hazard escaping for `<T>`
  generics and `{stream}` braces); Starlight builds 99 pages. Remaining
  open: visual browser review of the whole site (including new API pages).
- 2026-11-14 (P6 reversal, user decision): the generated API reference
  section was removed at the user's request ("I don't like it at all").
  Removed: `tools/ApiDocsGen` (+ tests), `docs/src/content/docs/api/`
  (83 generated pages), the "API reference" sidebar group, and the CI
  generate/freshness steps in `docs.yml`. The site returns to 16 pages.
  Prior implementation lives in git history
  (`235d1c8`..`95e8fe3`) if an API reference is ever revisited.

