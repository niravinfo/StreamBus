# ADR-5: Exception hierarchy

- **Status:** Accepted
- **Date:** 2026-10-21
- **Supersedes:** none

## Context

StreamBus exceptions were a flat set of unrelated sealed types. Callers who
wanted to handle any StreamBus failure had to enumerate every type, and new
leaf types silently escaped existing catch blocks. The leaf types themselves
carry useful meaning (configuration, not found, conflict, transient publish,
timeout, and so on), so replacing them would be a larger break than necessary.

## Decision

Add `StreamBusException` as the common base for every StreamBus exception in
the assembly, including the internal transient type and the publish and
configuration conflict types. Leaf types and their existing messages remain
unchanged, as do the classification rules that map transient failures to
retry and fatal failures to propagation.

Catching `StreamBusException` therefore catches every StreamBus-originated
failure, while catching a leaf type keeps the previous precision.

## Alternatives considered

- **Marker interface (`IStreamBusError`).** Offers no pattern-matching benefit
  over a base class, cannot carry shared behavior, and is semantically weaker
  than an exception hierarchy.
- **Replace leaves with a small set of new types.** A much larger breaking
  change for callers that already match on specific failures.

## Consequences

- New StreamBus exception types must derive from the base; the test
  `ExceptionHierarchyTests` pins this for every sealed exception type in the
  assembly.
- Callers can add one catch-all without losing the ability to handle specific
  failures.
- Existing type-based retry classification is untouched.

## Evidence

- `ExceptionHierarchyTests`
- Existing retry, publish, topology, and configuration tests remain green with
  unchanged leaf-type assertions.
