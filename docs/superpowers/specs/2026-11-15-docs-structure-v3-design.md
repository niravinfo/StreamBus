# StreamBus Docs v3 — Concept-first Restructure & Rewrite

**Status:** Approved direction (2026-11-15, conversation with author)
**Supersedes:** the §10 (2026-11-14) page set in `2026-11-11-documentation-rewrite-design.md`
**Scope:** documentation only. The `Handle<T>`/`Ignore<T>` retry-filtering API is
**out of scope** and will be built later; its doc pages get an explicitly-marked
"Planned" section only (see §6).

## 1. Intent and why

The 2026-11-14 rewrite produced accurate, compiling, API-verified pages — but
the *shape* is wrong. A deep review of MassTransit's docs (masstransit.io,
2026-11-15) showed why:

- MassTransit runs a strict three-tier IA — **Concepts → Guides →
  Configuration** — and says so explicitly: concepts give vocabulary and mental
  models; guides build working patterns; configuration applies production
  options. Our 16 pages are one genre: an API tour per feature area.
- Their code samples are **problem-first** (a deadlock scenario before the
  retry config). Ours are API-first: one snippet per public method, ordered by
  the class's member list. We document *what exists*; users need *what to do*.
- They teach the broker unprompted (a full "what is an exchange" mini-lesson
  inside the RabbitMQ transport page; queue → exchange → queue message-flow
  walkthroughs). Our users typically don't know what a RabbitMQ stream *is*,
  how it differs from a queue, or why at-least-once changes their handler
  design.
- They give doctrine and negative guidance (naming tense for commands vs
  events, "messages contain state not behavior", "do NOT change this
  default"). We state behavior but never take positions.
- They answer real user questions as FAQ-shaped judgment sections. We answer
  them incidentally, scattered.
- We repeat ourselves: the same `AddStreamBus` block appears on ~5 pages;
  Introduction / hero / Architecture / Configuration overlap heavily.

Goals:

- New users can go from "what even is a stream?" to a running app, and be
  *educated about RabbitMQ Streams* on the way (queue vs stream, offsets,
  groups, at-least-once, when NOT to use a stream).
- Every page has one genre and one job; nothing repeats across tiers.
- Every code snippet is justified by a user problem, and compiles against
  `src/StreamBus` (snippet harness retained).

Non-goals:

- Building any library feature (including exception filtering).
- API reference pages (user rejected generated API reference 2026-11-14).
- Brokers-on-the-web/comparative marketing content; changelog page.

Anti-goals:

- API-inventory pages ("here is every method on IStreamBus in order").
- Fake documentation of unimplemented APIs. Anything not real today is marked
  "Planned" and has no code snippet implying it works.

## 2. Approved structure (2026-11-15)

Three sidebar groups, in this order. The **first page of the entire site** is
Concepts/Introduction; the splash hero's "Get Started" button links there.

```
Concepts
├── Introduction       ← FIRST PAGE OF THE SITE; hero "Get Started" target
├── Streams            — queue vs stream, offsets & groups, at-least-once,
│                        super streams, when NOT to use a stream
├── Messages           — commands vs events, naming tense doctrine,
│                        state-not-behavior, MessageId dedup under at-least-once
├── Producers          — what publishing means: registration→route, broker
│                        confirmation, at-least-once on publishes, partitions
├── Consumers          — groups as cursors: same group = scale-out, different
│                        groups = independent feeds, SAC, per-delivery scope
├── Middleware         — pipelines both directions, what filters can/can't
│                        change, skip-vs-forward contracts
└── Exceptions         — failure on a log (no per-message ack/log), transient
                         vs poison doctrine, current retry semantics, error
                         envelope, and a marked "Planned: Handle<T>/Ignore<T>"

Guides
├── Get started        — plugin enable (tabs), package, one runnable app
│                        (merges current Installation + Quickstart)
├── Publish an event
├── Consume events
├── Handle failures
├── Scale out
├── Partition a stream (super streams) — routed publish + partitioned consume
│                                      as one coherent end-to-end story
├── Use multiple buses — marker types, DI resolution model, per-bus route
│                        tables, shared consumers, missing-bus DI exceptions
│                        (promoted from Configuration: majority use case)
├── Replay a stream
└── Add cross-cutting logic (filters)

Configuration
├── Host & connections
├── Endpoint options
├── Retry & dead letters
├── Observability
└── Transport security
```

Page-count outcome: 20 content pages from 14 (the splash `index.mdx` stays).
Unnecessary standalone pages removed: Introduction (reborn as the concept-tier
introduction), Installation + Quickstart (merged into Get started),
Architecture (split across the concept tier), Lifecycle, Topology (content
distributed). Concept pages are added where the MassTransit conversation showed
the gap (Streams, Messages, Producers, Consumers, Middleware, Exceptions).
Multiple buses is promoted to a Guide (majority use case), so Configuration
shrinks to five pages. Old slugs are simply deleted — no redirect stubs: the
library is pre-public, the only inbound links are our own README, updated in
the same change.

### Where old content goes (mapping)

| Old page | Destination |
|---|---|
| index.mdx hero | Kept; "Get Started" button → `concepts/introduction`; second card set trimmed to stay non-duplicative |
| introduction.mdx | Concepts/Introduction (orienting "what is this + learning path + skip-to-code link") |
| installation.mdx + quickstart.mdx | Guides/Get started |
| architecture.mdx | Split: streams-vs-queues + offsets + at-least-once + super streams → Concepts/Streams; failure-on-a-log → Concepts/Exceptions; startup/readiness/supervision → Configuration/Host & connections + Concepts/Introduction briefly |
| configuration.mdx | Configuration/Host & connections (strip quickstart duplicates) |
| topology.mdx | Specs/agreement tables → Configuration/Endpoint options + Publish guide; adopt rule, runtime declarations, partition reads → Guides/Replay a stream + Get started |
| lifecycle.mdx | Startup/shutdown options → Configuration/Host & connections; readiness policies → Configuration + Consumers guide; state table → Concepts/Streams or Introduction briefly |
| publishers.mdx | Concepts/Producers (the why) + Guides/Publish an event (the how + all options) |
| consumers.mdx | Concepts/Consumers + Guides/Consume events + Guides/Replay a stream (manual offsets) |
| error-handling.mdx | Concepts/Exceptions (why) + Guides/Handle failures (how) + Configuration/Retry & dead letters (tables) |
| middleware.mdx | Concepts/Middleware + Guides/Add cross-cutting logic |
| observability.mdx | Configuration/Observability (mostly as-is) |
| transport-security.mdx | Configuration/Transport security (mostly as-is) |
| multiple-buses.mdx | Guides/Use multiple buses (promoted: majority use case) |
| (none) | Guides/Partition a stream — new guide for super streams end-to-end |

## 3. Editorial rules (apply to every page)

1. **Problem-first**: every Guide opens with the reader's problem or
   scenario; every Concepts page opens with a plain-English definition of the
   domain concept before any API. Never open with the library's taxonomy.
2. **Snippet justification**: a sentence naming the need before every code
   block. Delete snippets that exist only because the method exists.
3. **Educate the domain inline**: queue-vs-stream comparison table (with a
   diagram) in Concepts/Streams; one educational callout at first use of a
   broker concept anywhere else.
4. **Doctrine & negative guidance**: naming doctrine for streams, groups,
   and messages (commands verb-noun, events past-tense; groups named for the
   consuming service); explicit "do not do X" warnings explaining *why*
   (e.g., disable SAC cross-process = offset regressions).
5. **FAQ/judgment sections** on guides (How many partitions? Two apps share a
   group? Retention? Can two services read one stream?).
6. **Zero repetition across tiers**: a concept is taught once (Concepts), a
   task shown once (Guides), a table of options lives once
   (Configuration). Cross-links instead of restating. The full
   `AddStreamBus` block appears in full exactly once (Get started);
   elsewhere snippets elide with `// ...` or show fragments.
7. **Fresh scenario per guide**, but a shared message vocabulary
   (`OrderPlaced`-style domain events) so cross-links feel coherent.
8. **Keep what already works**: eager-validation story, tables with defaults,
   verified-compiling snippets (harness), the "never guesses" voice, no
   blog-style "In this article" openers.
9. **Planned features**: any unimplemented capability (only
   `Handle<T>`/`Ignore<T>` for now) appears under an explicit
   **"Planned"** callout, no compilable-looking snippet of the future API,
   and a plain description of the intended semantics.

## 4. Pages that need new writing vs salvage

- **Write from scratch:** Concepts/Introduction, Streams, Messages, Producers,
  Consumers, Middleware, Exceptions — these don't exist in the needed genre
  today; and Guides/Use multiple buses, which must be rewritten from the
  resolution-model-first framing (marker type → bus, message type → stream)
  rather than salvaged from the current reference-style page.
- **Rewrite problem-first:** Get started (merge, re-frame), Publish an event,
  Consume events, Handle failures, Scale out (assembles scattered group/SAC
  content), Replay a stream (assembles scattered offset content), Add
  cross-cutting logic.
- **Salvage lightly:** the five remaining Configuration pages — restructure into
  reference tables + defaults, strip duplicated teaching, link back to
  Concepts. Observability, Transport security are closest to target already.

## 5. Quality bar / verification

- Every C# snippet: compiles against `src/StreamBus` and passes
  `AddStreamBus` registration validation (existing snippet-harness method,
  `tests/StreamBus.DocsSnippets`).
- Get-started `Program.cs` extracted verbatim and built as a real project.
- Concept pages get a reviewer pass explicitly checking: does each page teach
  before it shows API? Would a user who has never heard of a RabbitMQ stream
  follow it?
- Each cross-tier claim checked once: no content repeats between Concepts,
  Guides, Configuration.
- Starlight builds all pages; no broken links; sidebar order correct;
  hero links updated.

Approach (settled): full three-tier rewrite (Option A from 2026-11-15
conversation). Retrofit (B) and merge-only (C) were rejected — the shape, not
the wording, is the problem.

## 6. Exceptions: current behavior + Planned exception filtering

Current (must be documented accurately, today):

- Every thrown exception enters the endpoint's schedule — there is no
  exception-type filtering. Retries are unbounded by default, which means a
  non-retryable exception parks its partition forever unless a bounded
  schedule + hand-off is configured. The Exceptions concept page and Handle
  failures guide must teach this consequence explicitly and give the
  current mitigation (bounded schedule + dead-letter; or `OnExhausted`/a
  consume filter that decides, with its caveats).

Planned (to add now, as a *what's coming* note):

> **Planned — exception filtering.** A future release adds
> `Handle<TException>` / `Ignore<TException>` on the retry configurator so a
> schedule can retry transient exceptions while immediately handing off
> poison ones. Semantics (intended): a handled type retries; an ignored
> type exhausts on first failure and goes to the configured hand-off; with
> none configured, today's behavior (all exceptions retry) continues.

When the API ships, this section is replaced by real docs; no code snippet
until then.

## 7. Constraints

- Site is Starlight (`docs/`), deploy via existing GitHub Pages workflow;
  hero links bypass slug normalization and are hardcoded to the base path
  (`/StreamBus/concepts/introduction/`) — update them along with the
  sidebar.
- Content files use MDX with `.mdx` extension; Starlight Tabs supported and
  used for OS-specific commands.
- No page count target beyond the approved structure; do not split a page
  merely for length — depth belongs inside Guide sections.
- README links to the new slugs must be updated.

## 8. Decisions log

- 2026-11-15: Full three-tier rewrite approved (Option A over retrofit/merge).
- 2026-11-15: Concepts menu ≈ MassTransit's: Introduction, Streams (our
  transport/architecture equivalent), Messages, Producers, Consumers,
  Middleware, Exceptions.
- 2026-11-15: Concepts/Introduction is the first page of the site; hero
  "Get Started" button targets it (with a "skip to code" link into Get started
  for impatient readers).
- 2026-11-15: `Handle<T>`/`Ignore<T>` retry filtering deferred — build
  later; docs carry an explicitly-marked Planned note only.
- 2026-11-15: No Buses/Architecture concept page; bus shape covered by
  Concepts/Introduction + Configuration/Host & connections.
- 2026-11-15: Fresh per-guide scenarios with shared message vocabulary (not
  one persistent end-to-end app).
- 2026-11-15 (builder-check round): Super streams get their own guide —
  "Partition a stream (super streams)" — rather than being split across
  Publish/Consume/Scale-out; the concept tier keeps only the what.
- 2026-11-15: Multiple buses promoted from Configuration to the Guide
  "Use multiple buses"  the majority-use-case call. It must teach the
  resolution model — *marker type = which bus (DI-typed, `IStreamBus<TBus>`), message type = which stream on that bus* — plus the
  failure modes verified in source: injecting an unregistered marker throws
  `InvalidOperationException` ("Unable to resolve service…") when the
  consuming service is first constructed (constructor injection never
  yields null); `GetService`/optional-with-default yield null; injecting
  plain `IStreamBus` when only marker buses are registered fails the same
  way, because only the non-generic `AddStreamBus` registers it.
  FAQ section: "I injected a bus and got Unable to resolve service — why?".
- 2026-11-15: Old URL slugs are deleted with no redirect stubs — the library
  is not yet public and the README (updated in the same change) is the only
  inbound link source. One big-bang cutover on the docs branch: all pages
  written, sidebar swapped, old pages deleted in one pass; no mixed-genre
  intermediate deploy.

## 9. Per-page content specification (2026-11-15)

Derived from the section-by-section MassTransit analysis (Concepts/Intro,
Messages, Consumers, Producers, Middleware, Exceptions, Transports; Bus
Configuration; the message-contract guide). Cross-cutting conventions for
every spec below: running-example vocabulary is the order/billing domain
already used in the current pages (`OrderPlaced`, `orders`, group
`billing`, `OrderConsumer`); concepts pages carry no application-scale code
and end with an on-path link; guides label code blocks with file paths and
finish with an FAQ section; configuration pages open with 1–2 sentences of
scope, then option tables with a Defaults column, and link back to the
concept instead of re-teaching it. Deliberate omissions are listed per page
so writers stop at them.

MassTransit's runner note: they have no getting-started page at all
(Concepts → Guides → Configuration). We keep Get started — a single-transport
newcomer library needs the concrete runnable step — but it is the *only*
page allowed to contain a full application.

### Concepts

**C1. Introduction** (first page of the site; hero target)
- Thesis opener (2 paragraphs): what StreamBus is, in one bold sentence,
  then the "understand the model before code" pitch (adapt MassTransit's
  "before you start writing any code… understand" framing).
- **Understand the concepts before you code** — a 6-item bullet list naming
  what a newcomer must grasp: streams vs queues; offsets and consumer
  groups; at-least-once and idempotence; what makes a good message;
  what a consumer actually is here; what happens when a handler throws.
- Card grid linking the six concept pages, one sentence per card.
- Skip-to-code callout at top: "want to just run something? → Get started".
- **The learning path**: numbered Concepts → Guides → Configuration, plus
  one paragraph on how StreamBus maps to `Microsoft.Extensions.Hosting`
  and why that makes it familiar ("a background service you already know
  how to run").
- Omissions: no API names beyond `AddStreamBus`, no install steps, no
  feature list (that's the splash page's job).

**C2. Streams**
- Opens with the doctrine paragraph (adapt MassTransit's dispatch-vs-stream
  passage): a stream is an append-only log; producers append, no message is
  removed on read; consumers track their position. This is the page's most
  important passage.
- **What a stream is** — offsets, retention by age/size (not by
  consumption), non-destructive repeated reads.
- **Streams vs queues** — comparison table (aspect | queue | stream):
  message lifecycle, ack model, delivery, ordering, the scaling unit,
  retry model, best-fit workloads; plus a simple diagram (producer →
  log → cursors). Unlearning callout for AMQP users ("two things to
  unlearn", kept from current Architecture — it's our best passage).
- **Offsets and consumer groups** — the cursor model: same group shares a
  cursor, different groups are independent readers; stored-offset cadence
  and the redelivered tail.
- **At-least-once** — caution callout: duplicates are normal and expected,
  handlers must be idempotent, `MessageId` is there to dedup.
- **Super streams** — one paragraph what (partitions by routing key);
  how lives in the guide.
- **When you should not use a stream** — honest aside (MassTransit-style):
  request/reply, per-message routing, delayed scheduling → use a
  queue-based tool; link out without embarrassment.
- Omissions: no StreamBus code at all on this page.

**C3. Messages**
- Definition before code (MassTransit's message page pattern): a message is
  something that happened (event) — on a stream it is a fact to be appended.
  Two paragraphs, no names yet.
- **Naming** — tense doctrine with example vocabulary: events in past
  tense (`OrderPlaced`, `OrderBilled`); the ordered-lifecycle example
  (OrderPlaced → OrderBilled → OrderShipped); naming the stream too
  (`orders`, `order-lifecycle`).
- **Design** — state, not behavior; records with `get; init`; the
  inheritance anti-pattern doctrine, adapted honestly: our endpoints bind
  one message type, so base-class tricks don't route here at all ("say no"
  verdict retained); JSON serialization note and namespace/contract
  agreement across services — bold callout that the shape is a contract
  shared by every service reading the stream.
- **Envelope and headers** — the one big table on the page:
  Property | Stamped by | Meaning for `MessageId` (always),
  `CorrelationId` (never invented), `SentTime` (always UTC now),
  `ContentType` (from serializer), user headers (AMQP application
  properties + allowed value types, reserved `streambus.*` keys).
- **Deduplicating under at-least-once** — scenarios where redelivery hits
  the same consumer; `MessageId` for a dedup window/cache.
- Guidance bullet list closing the page.
- Omissions: serializer configuration (→ Configuration/Host & connections),
  header-writing `PublishOptions` syntax (→ Guide).

**C4. Producers**
- Definition: publishing means *appending a fact to a stream*; contrast
  with MassTransit's send/publish dichotomy — on a stream there is one
  verb; the routing decision was already made at registration ("the route
  table is your declaration of truth").
- **Registered routes** — type → stream resolution; ambiguity rejected at
  registration; kind mismatch (plain vs super) named at registration.
- **Confirmation** — what the returned task means (broker-confirmed append),
  consequences for awaits and for at-least-once on the publish side (no
  producer name → no broker dedup, plan for duplicates on retry).
- **Partition choice** — routing key selects the partition; for entities,
  "same key, same partition, same order" doctrine — order in streams comes
  only from the routing key.
- Where to get a bus: pointer list (DI injection types — link Use multiple
  buses) rather than MassTransit's 4-source priority list.
- Omissions: budgets/options (→ Guide + Configuration), raw publish
  (→ Guide), super-stream mechanics (→ Guide).

**C5. Consumers**
- Plain-English open (MassTransit pattern: start from the noun): a
  consumer reads a stream, one message type at a time, at the position its
  group last stored.
- **Groups are cursors, not just labels** — the identity we require
  (why StreamBus never derives it) and the failure model rewritten the
  MassTransit "lock/lifecycle" narrative in our semantics (scope → resolve
  → `Consume` → store-offset-or-retry — there is no per-message ack/lock,
  so offset-store cadence is the checkpoint).
- **Single-active consumer & scale** — per-plane truth: plain stream SAC
  adds failover not throughput; super stream partitions spread across
  instances; different groups each get everything.
- Consume stages mini-timeline; minimal `IConsumer<T>` contract view
  with the running example; per-delivery scope rationale.
- Omissions: endpoint option table (→ Configuration/Endpoint options),
  scenarios and examples (→ Guide/Consume events).

**C6. Middleware**
- Definition: behavior around every publish and delivery; ASP.NET
  Core middleware analogy (MassTransit's opener, retained).
- **Built-in first** — table of the stock pipeline stages in both
  directions (publish: filters → serialize → confirm; consume: filters →
  deserialize → handler, with retry sitting inside the endpoint), and the
  composed sample teaching *ordering* (bus-wide → endpoint filters,
  publish serial before wire).
- **The two pipelines** details: publish runs pre-serialization, consume
  wraps the handler.
- **The next contract differences** — publish: exactly-once-and-await;
  consume: at-most-once, and a legitimate skip (offset still advances);
  "when in doubt: `return await next()`" doctrine.
- **What filters can change** (headers, payloads) **and never** (stream,
  routing and destination — "routing is a registration decision, not a
  middleware decision");
- Payload passing between filters and into the consumer:
  `SetPayload<T>`/`GetPayload<T>` — few small snippets only.
- "See for yourself how to write one → Guide" close — the custom-filter
  how to stays a Guide here like MassTransit's "Create a middleware filter".
- Question-form headings ("What is a filter?" style) acceptable here.

**C7. Exceptions**
- Disarming opener in the MassTransit tone: your handler will throw —
  against a log, the broker does nothing but keep delivering, so StreamBus
  handles it explicitly.
- **What happens by default** — numbered list (throw → retry schedule →
  exhaustion → hand-off → offset advances); today's unbounded default
  honored with its hazard spelled out ("a poison exception retried
  forever parks a partition — bound your schedules").
- **Transient vs poison** — doctrine: deadlock/network/transient-calls
  deserve retries; validation/bug exceptions never fix themselves — with
  today's mitigation callout (bounded schedule + hand-off) and a clearly
  marked **Planned** note: `Handle<T>` / `Ignore<T>` exception filtering
  is coming; no compilable-looking snippet, just intended semantics
  (§6 above).
- **Pointer sections** (4–6 sentences, then a link, MassTransit's
  retry/redelivery pattern): schedules (→ Guide/Handle failures +
  Configuration/Retry & dead letters), exhaustion hand-off modes,
  `OnExhausted` — each one pointer-style.
- **The error envelope** — what travels on a dead letter
  (`streambus.error.*` + original headers intact, nothing repackaged).
- **Operating on failures** — where dead letters land (stream vs queue
  tradeoff: queues are visible in the management UI), drain → fix →
  re-publish recovery with fresh ids; state responsibility boundary
  (StreamBus ends at the hand-off).
- Exception-hierarchy mini-table (reused from current error-handling).

### Guides

**G1. Get started** (merges Installation + Quickstart — the only full-app page)
- Prerequisites table (.NET 10, RabbitMQ 4.x, non-guest user) kept.
- Enable stream plugins — Linux/Windows tabs kept; async-plugin-callout kept.
- Install package.
- **Build the app** — the current Quickstart `Program.cs` verbatim
  (registration + publisher worker + consumer), now one file-path-labeled
  block, followed by the expected console output, plus the valuable
  *how-the-pieces-line-up* walkthrough (`cfg.Publish<T>("orders")`,
  endpoint, group, retry, `WaitUntilStarted`), and the 4-step
  *what-happens-at-startup* numbered story, both salvaged from the current
  page.
- Ctrl+C/resume offsets beat ("run it again: the offsets continue" — kept).
- Where to go next: one link per concept page.
- Omissions: TLS, more-than-one-broker, hosting details (→ Configuration).

**G2. Publish an event**
- Scenario open: "your checkout service recorded OrderPlaced; other teams
  consume it".
- Declare + publish: the basic pair (already a snippet in current
  Publishers — reframed with the need stated first).
- **Add context to a message** — properties/headers for a need (tenant,
  correlation), the allowed value types, filter precedence note.
- **Tighten your budgets** — the readiness/confirm knobs with the
  timeout-outcome-unknown caution (kept, problem-framed).
- **When the consumer isn't .NET** — raw publish; also explicit-target
  publish (one type, several streams) with the
  `StreamBusStreamNotFoundException`-no-autocreate warning.
- FAQ: — what happens to messages published before any consumer exists
  (retention makes waiting harmless — call this out, MassTransit's
  "published before the queue exists" warning inverted into good news,
  our strongest queue-vs-stream teaching moment); — when do I need
  `ConfirmTimeout`; — why isn't there a publish-to-many-streams operator.
- Omits super streams (→ G6, top-of-page pointer note).

**G3. Consume events**
- Scenario open: the billing service subscribes to `orders`.
- Endpoint registration + `IConsumer<T>` + group naming doctrine (name it
  after the consuming service, not the stream).
- **The delivery, close-up** — `ConsumeContext<T>` tour: coordinates, ids,
  headers, `Services` scope, cancellation.
- **A second team wants the same events** — different group on the same
  stream, worked example (this is where the "different groups = independent
  readers" doctrine lands in practice).
- **Throwing means scheduling** — one paragraph + link to G4.
- FAQ: can two instances of my service share the group? (→ Scale out);
  what happens on a crash mid-batch (redelivered tail);
  where does the first message come from (StartOffset semantics, one-line
  table pointer).

**G4. Handle failures**
- Scenario open (MassTransit's retry opener adapted): "a deadlock, a busy
  downstream service — usually clears on the second attempt".
- Bounded schedule + dead-letter arrival at `{stream}_error` queue shown
  in the real management-UI (MassTransit's screenshot teach-by-ops —
  stream destination in the management stream page).
- **Schedules that fit the failure** — keyword-brief Immediate/Interval/
  Intervals/Exponential/None table (kept from current page) with the
  "attempts are totals" note.
- **When attempts run out** — exhaustion matrix summary, destination
  choice doctrine (queue if ops drains it, stream if it feeds a replay
  pipeline), `OnExhausted` for full control.
- **Inspectable failures** — error envelope keys, the reading snippet.
- **Recover after a fix** — drain → republish as new messages with fresh
  ids; explicit state boundary prose.
- Doctrine callout — poison won't retry away; today's mitigation
  (bounded) + the Planned filtering pointer back to C7.
- FAQ: why not retry forever (the parked partition); why doesn't stopping
  retry on one message delay the rest (it does — that's back-pressure,
  by design).

**G5. Scale out**
- Scenario open: "two instances of billing deploy tomorrow".
- Plain stream + SAC: failover only, why (one cursor).
- Super stream path: partitions distribute; route-side prerequisite
  (→ G6).
- The do-not-disable-SAC warning kept (cross-process duplicates and
  offset regressions).
- **Finding your partition count** — doctrine paragraph + numbers in the
  FAQ style of MassTransit's "How Much Concurrency Should I Allow?":
  start from the slowest handler's throughput; more partitions ≠ more
  order per entity; counts can't change later (broker rules; link G6).
- FAQ: how do I decide group names (per-service rule); does adding
  instances change delivery guarantees; what if a partition's consumer
  dies (SAC, redelivered tail).

**G6. Partition a stream (super streams)** — new, end-to-end
- Scenario open: "orders outgrew one log; per-entity order must survive".
- `PublishSuper` registration + spec (partitions) + routed publish with
  routing key; identity-mapping doctrine (tenant-id, order-id as key).
- Endpoint attach on the super stream; group spread across instances.
- **Reading one partition** for ops/replay (`orders-super-1` physical
  stream, raw consume pointer).
- Agreement/conflict + existing-count-rules summary from current Topology
  (declaration union; mismatch names both; broker count wins once created).
- FAQ: how many partitions; what breaks if the key logic changes
  (ordering moves — per-entity only within a key); can a plain-stream
  consumer read a super stream (no — kind conflicts).

**G7. Use multiple buses** (promoted, resolution-first)
- Scenario open: "orders and billing live on different brokers/vhosts".
- Marker types; two-bus registration layout (current page's walkthrough
  kept).
- **How resolution works — the doctrine paragraph**: marker type selects
  the bus (DI-typed, `IStreamBus<TBus>`), message type selects the stream
  *on that bus*; per-bus route tables; escalation from a wrong marker
  fails loudly at DI.
- **Injecting a bus** — each constructor-injection shape + the two
  failure modes verified in source, phrased as doctrine: unregistered
  marker → `InvalidOperationException` ("Unable to resolve service…")
  when the consumer is first constructed; plain `IStreamBus` requires the
  non-generic `AddStreamBus` call; `GetService`/defaults can yield null —
  standard DI semantics.
- **Consumers and health, per bus** — shared consumer classes with
  distinct groups; `bus:{vhost}|{busType}` health rows for attribution.
- FAQ: "I injected a bus and got Unable to resolve service — why?" (the
  table: which overload registers which service type); same vhost twice
  (conflict throw / first-wins); same type on two buses (independent
  routes).

**G8. Replay a stream**
- Scenario open: "the bug is fixed; reprocess last week's orders" — today
  this is buried across three pages; this guide gives it a home.
- **Replay into a new group** — fresh group with `StartOffset.AlwaysFirst`
  for verification-style sampling.
- **Rewind an existing group** — `SetConsumerOffsetAsync` to a target
  offset (`GetConsumerOffsetAsync` first), replication-warning callout
  (rewinding re-delivers and moves the *whole* group's cursor).
- **Dangerous but available** doctrine: reprocess into a bounded `UseRetry`
  endpoint so the replay itself gets the failure pipeline.
- **Read partitions directly** — super-stream physical-name inspection
  (pointer wording shared with G6, single owner: G6 owns the how, this
  page owns the *why/when*).
- Runtime topology declarations (`bus.Topology.Ensure*Async`) as the
  operator/after-startup tool, salvaged here from current Topology.
- FAQ: is replay safe with live consumers (yes — separate group, separate
  cursor); why did some messages replay twice (at-least-once — expected);
  why did some messages replay twice (at-least-once — expected);
  how do I replay only failed ones (dead-letter correlation).

**G9. Add cross-cutting logic (filters)**
- Scenario open: "every published message needs the tenant id stamped —
  without copy-pasting it into every consumer/producer".
- Publish filter end to end (TenantFilter), then a consume filter
  (selective handling/enrichment), registered bus-wide and per-endpoint.
- The `next` contract on both sides with the one-sentence statement of
  differences (back-link to C6 for theory — concepts own the
  doctrine, this page owns the typing).
- What a filter can/can't change — practical list (routing fixed);
  `SetPayload<T>` hand-off into the consumer.
- Pointer to runnable examples in `examples/` (existing).
- FAQ: singleton-scoping constraints (thread-safety, per-message scope
  via `context.Services`); double-registration throw (why);
  typed vs universal and the trimming/AOT note (kept, condensed).

### Configuration

All five: opener sentence(s) of scope → option tables with Defaults column
→ cross-links back to the owning concept; no Application-scale code, no concept teaching. 

**Cfg1. Host & connections** — connection options table (kept from current
Configuration), serializer switching, TLS handoff (→ Cfg5), the
hosted-service/startup/shutdown options (`StreamBusHostOptions` +
`ReadinessPolicy` table + `BusConnectionState` table — salvaged verbatim
from Lifecycle/Architecture, minus the prose teaching, which stayed in
Concepts/Introduction & Streams). 

**Cfg2. Endpoint options** — the `SingleActiveConsumer`/`StartOffset`/
`OffsetStore`/`Readiness` table (kept), stream-spec knobs grouped here
(`MaxAge`/`MaxLengthBytes`/segment/leader — moved from Configuration+
Topology agreement tables), agreement/adoption conflict rules summary
(the full rules live in G6; this is the reference table).

**Cfg3. Retry & dead letters** — schedule methods table, `RetryPolicy`
record fields, exhaustion config matrix and destination options
(`DeadLetterToStream/Queue` options incl. `ErrorPublishTimeout`), the
exception-hierarchy reference table.

**Cfg4. Observability** — kept nearly as-is (health data keys incl. `bus:`/
`reg:` prefixes, metrics table, spans, logs expectations); re-framed
opening: what a collector needs vs. what the health check means.

**Cfg5. Transport security** — kept nearly as-is (TLS options table,
hostname-verification warning); integration-test TLS reference pointer.
