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
