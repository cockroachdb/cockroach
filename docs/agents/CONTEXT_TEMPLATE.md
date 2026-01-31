# Authoring guidance (do **not** include in AGENTS.md)
- Create a compact, high-signal reference that helps LLMs reason confidently about this domain without re-deriving fundamentals from code.
- Prioritize invariants, cross-domain boundaries, and triage playbooks over long prose or code dumps.
- Individual AGENTS.md files should begin with "At a glance".
- Prefer links/paths over code dumps; keep examples tiny.
- Always use repo-relative paths, not absolute or file-relative.
- Do not include sections that are low-signal. If and only if sections are skipped, add a note at the end listing intentionally omitted sections.

## Length targets
- Default doc length: 800–1,200 words.
- System-level docs: up to ~1,800 words.
- Per-package OVERVIEWs: 500–900 words.
- Keep "At a glance" ~200–300 words.

## At a glance (keep ~200–300 words)
_AGENTS.md files start with this section._
- **Components**: service → responsibilities, owner, repo path, primary code entrypoints.
- **See also**: listing relevant cross-domain AGENTS.md docs only (no code paths).
- **Ownership & boundaries**: allowed interactions, forbidden couplings.
- **Key entry points & types**: 3–15 items with relative file paths.
- **Critical invariants**: Safety properties and why they hold.
- **Request/data flow**: 1–2 brief summaries or ASCII diagrams of read/write paths or primary flows.

## Deep dive (bullet-first; target 800–1,200 words total; up to ~1,800 for system-level docs; 500–900 for per-package OVERVIEWs; number sequentially)
1) **Architecture & control flow**: Main loops, stages, and when/where work is done.
2) **Data model & protos**: Important structs/protos, lifecycles, versioning rules.
3) **Concurrency & resource controls**: Locks/latches/monitors/tokens; backpressure.
4) **Failure modes & recovery**: Timeouts, retries, idempotence, partial failure.
5) **Observability & triage**: Key metrics, traces, logs; step-by-step debug recipes.
6) **Interoperability**: Exact seams to other subsystems (descriptive only; do not include links here).
7) **Mixed-version / upgrades**: Gates, migrations, compatibility pitfalls.
8) **Configuration & feature gates**: Env vars, flags, settings, feature switches; defaults and file anchors.
9) **Background jobs & schedules**: Cron/queues; producers/consumers; where scheduled and handled.
10) **Edge cases & gotchas**: Known sharp edges, pathological patterns, test knobs.
11) **Examples**: 1–2 worked examples (inputs → outputs), tiny code citations by path.
12) **References**: Minimal documentation links (in-tree, RFCs, or external). No code.
13) **Glossary**: 8–15 terms with short, precise definitions.
14) **Search keys & synonyms**: Canonical symbol names and common synonyms for fast retrieval.

**Style**: Bullet-first, short paragraphs, explicit file paths, minimal quoting.
**Don’t**: Restate whole files, duplicate docs, or include stale external info.
