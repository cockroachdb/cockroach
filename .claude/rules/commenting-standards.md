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

## Comment Maintenance

- Add explanations when you discover valuable missing knowledge.
- Fix factually incorrect comments immediately (treat as bugs).
- Fix grammar/spelling that significantly impairs reading.
- Avoid cosmetic-only changes. Prefix grammar nits with "nit:" in reviews.
