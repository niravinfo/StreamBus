# ADR-5: Exception hierarchy

- **Status:** Accepted
- **Date:** 2026-10-21
- **Supersedes:** none

## Context

StreamBus exceptions were a flat set of unrelated sealed types. Callers who
wanted to handle any StreamBus failure had to enumerate every type, and new
leaf types silently escaped existing catch blocks — yet the leaf types carry
useful meaning, so replacing them would be a larger break than necessary.

## Decision

`StreamBusException` is the common base for every StreamBus exception in the
assembly, including internal transient types and the publish and
configuration conflict types. Leaf types, their messages, and the retry
classification rules (transient → retry, fatal → propagate) are unchanged.

Catching `StreamBusException` catches every StreamBus-originated failure;
catching a leaf type keeps the previous precision.

Alternatives rejected: a marker interface (no pattern-matching benefit, weaker
semantics) and collapsing the leaves into a few new types (a much larger
breaking change for callers matching specific failures).

## Consequences

- New StreamBus exception types must derive from the base; a unit test pins
  this for every sealed exception type in the assembly.
- Callers can add one catch-all without losing specific-failure handling.
