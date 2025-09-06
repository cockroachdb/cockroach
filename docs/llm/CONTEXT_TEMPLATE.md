# Purpose
A compact, high-signal reference that lets LLMs reason confidently about this domain without re-deriving fundamentals from code. Prioritize invariants, cross-domain boundaries, and triage playbooks over long prose or code dumps.

## Length targets
- Default doc length: 800–1,200 words.
- System-level docs: up to ~1,800 words.
- Per-package OVERVIEWs: 500–900 words.
- Keep "At a glance" ~200–300 words.
- Prefer links/relative paths over code dumps; keep examples tiny.

## At a glance (keep ~200–300 words)
- **Component index**: service → responsibilities, owner, repo path, primary entrypoints.
- **Ownership & boundaries matrix**: allowed interactions, forbidden couplings.
- **Key entry points & types**: 8–15 items with relative file paths.
- **Critical invariants**: Safety properties and why they hold.
- **Request/data flow**: 1–2 ASCII diagrams for read/write paths or primary flows.

## Deep dive (bullet-first; target 800–1,200 words total; up to ~1,800 for system-level docs; 500–900 for per-package OVERVIEWs)
1) **Architecture & control flow**: Main loops, stages, and when/where work is done.
2) **Data model & protos**: Important structs/protos, lifecycles, versioning rules.
3) **Concurrency & resource controls**: Locks/latches/monitors/tokens; backpressure.
4) **Failure modes & recovery**: Timeouts, retries, idempotence, partial failure.
5) **Observability & triage**: Key metrics, traces, logs; step-by-step debug recipes.
6) **Interoperability**: Exact seams to other subsystems (link their OVERVIEWs).
  - When referencing another context file, consider adding a backreference in the target file (e.g., a brief “See also” link) if it materially helps bidirectional navigation or clarifies ownership boundaries.
7) **Mixed-version / upgrades**: Gates, migrations, compatibility pitfalls.
8) **Configuration & feature gates**: Env vars, flags, settings, feature switches; defaults and file anchors.
9) **Background jobs & schedules**: Cron/queues; producers/consumers; where scheduled and handled.
10) **Edge cases & gotchas**: Known sharp edges, pathological patterns, test knobs.
11) **Examples**: 1–2 worked examples (inputs → outputs), tiny code citations by path.
12) **References**: Minimal links to in-tree docs, RFCs, or external docs.
13) **Glossary**: 8–15 terms with short, precise definitions.
14) **Search keys & synonyms**: Canonical symbol names and common synonyms for fast retrieval.

**Style**: Bullet-first, short paragraphs, explicit file paths, minimal quoting.
**Don’t**: Restate whole files, duplicate docs, or include stale external info.
