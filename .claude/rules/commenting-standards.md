---
paths:
  - "**/*.go"
  - "**/*.proto"
---

# CockroachDB Commenting Standards

## Formatting

- Block comments (standalone line): full sentences, capitalized, punctuated.
- Inline comments (end of line): lowercase, terse, no terminal punctuation.

## Placement

- **Data structures**: purpose, lifecycle, invariants at the declaration.
  Document which code initializes and accesses fields, when they become
  obsolete. For fields guarded by mutexes, state which lock protects them.
- **Struct fields**: purpose, lifecycle, and usage of each field.
- **Functions**: inputs, outputs, contract, return value semantics for each
  combination including non-error cases. Do not re-document struct details.
- **Interfaces/APIs**: document method contracts, blocking behavior, and
  call sequences between methods.
- **Algorithms**: inside function bodies. Explain logic, phases, and
  non-obvious details. Focus on "why," not "what." Use phase comments to
  separate distinct processing stages.
- **Overviews/design**: understandable with zero prior knowledge of the code.
  Explain concepts/abstractions, show how pieces fit together. Define new
  terms immediately. Illustrate with examples.
- **Protobuf messages/enums**: document each value with context on when it
  applies and how it relates to the overall state machine or protocol.

## Writing Principles

- Use ASCII diagrams for architecture, request flow, and state machines.
- Formal, impersonal tone. "The processor stops" not "we stop the processor."
- Document invariants explicitly with an `Invariant:` marker. State contracts
  (preconditions, postconditions, return value semantics) on methods.
- Document locking: mutex ordering, which locks a method holds or requires
  (use `REQUIRES:`), and exceptions to the ordering.
- Field comments: state access rules (which goroutine/lock guards the field),
  mutation lifecycle, and ownership.
- Use temporal language ("before," "during," "once," "after") for lifecycles
  and guarantee boundaries.
- Introduce concepts progressively: simple case first, define terms on first
  use, differentiate easily confused concepts.
- Prefer concrete examples (values, state snapshots, before/after) over
  abstract descriptions.
- Document design rationale: why this approach, what tradeoffs, when to revisit.
- Surface edge cases and caveats explicitly ("However,"). Do not bury them.
- Use `NB:` for qualifications, `NOTE:` for design context.
- Document known races: the race, why fixing is worse, recovery mechanism.
- Document call-ordering constraints on interface methods (how many times,
  what order).
- Document backward compatibility: how old and new versions interact during
  rolling upgrades.

When writing substantial comments (type/package/interface docs, algorithm
explanations), read `.claude/docs/commenting-guide.md` for detailed examples.

## Comment Maintenance

- Add explanations when you discover valuable missing knowledge.
- Fix factually incorrect comments immediately (treat as bugs).
- Fix grammar/spelling that significantly impairs reading.
- Avoid cosmetic-only changes. Prefix grammar nits with "nit:" in reviews.
