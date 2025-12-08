---
name: commit-helper
description: Help create git commits and PRs with properly formatted messages and release notes following CockroachDB conventions. Use when committing changes or creating pull requests.
---

# CockroachDB Commit Helper

Help the user create properly formatted commit messages and release notes that follow CockroachDB conventions.

## Workflow

1. **Analyze the changes**: Run `git diff --staged` or `git diff` to understand what was modified
2. **Determine the package prefix**: Identify the primary package affected
3. **Ask the user** for:
   - Issue/epic number if not already known
   - Whether release notes are needed and what category fits best
4. **Write the subject line**: Imperative mood, no period, under 72 characters
5. **Write the body**: Explain the before/after, why the change was needed
6. **Add issue references**: Include Resolves/Epic as appropriate
7. **Write the release note**: Clear, user-focused description of the change
8. **Create the commit** using the properly formatted message

## Commit Message Structure

**Basic Format:**
```
package: imperative title without period

Detailed explanation of what changed, why it changed, and
how it impacts users. Explain the problem that existed
before and how this commit solves it.

Include context about alternate approaches considered and
any side effects or consequences.

Resolves: #123
Epic: CRDB-357

Release note (category): Description of user-facing change
in past or present tense explaining what changed, how users
can see the change, and why it's important.
```

**Key Requirements:**
- **Must** include release note annotation (even if "Release note: None")
- **Must** include issue or epic reference
- **Must** separate subject from body with blank line
- **Recommended** prefix subject with affected package/area
- **Recommended** use imperative mood in subject (e.g. "fix bug" not "fixes bug")
- **Recommended** wrap body at 72-100 characters

## Release Note Categories

**When to include release notes:**
- Changes to user interaction or experience
- Changes to product behavior (performance, command responses, architecture)
- Bug fixes affecting external users

**When to exclude release notes:**
- Internal refactors, testing, infrastructure work
- Code that's too immature for docs (private preview features)
- Internal settings beginning with `crdb_internal.`
- Functionality not accessible to external users

**Valid Categories:**
- `backward-incompatible change` - Breaking changes to stable interfaces
- `enterprise change` - Features requiring enterprise license
- `ops change` - Commands/endpoints for operators (logging, metrics, CLI flags)
- `cli change` - Commands for developers/contributors (SQL shells, debug tools)
- `sql change` - SQL statements, functions, system catalogs
- `ui change` - DB Console changes
- `security update` - Security feature changes
- `performance improvement` - Performance enhancements
- `cluster virtualization` - Multi-tenancy infrastructure
- `bug fix` - Problem fixes
- `general change` - Changes that don't fit elsewhere
- `build change` - Source build requirements

## Release Note Best Practices

**Description guidelines:**
- Default to more information rather than less
- Explain **what** changed, **how** it changed, and **why** it's relevant
- Use past tense ("Added", "Fixed") or present tense ("CockroachDB now supports")
- For bug fixes: describe cause, symptoms, and affected versions
- Note if change is part of broader roadmap feature

**Examples:**

**Good bug fix:**
```
Release note (bug fix): Fixed a bug introduced in v19.2.3 that
caused duplicate rows in CREATE TABLE ... AS results when multiple
nodes attempt to populate the results.
```

**Good feature:**
```
Release note (enterprise change): Shortened the default interval
for the kv.closed_timestamp.target_duration cluster setting from
30s to 3s. This allows follower reads at 4.8 seconds in the past,
a much shorter window than the previous 48 seconds.
```

## Issue References
- `Resolves: #123` - Auto-closes issue on PR merge
- `See also: #456, #789` - Cross-references issues
- `Epic: CRDB-357` - Links to epic

## How to Avoid Common Pitfalls
- Always include a release note annotation (even "Release note: None")
- Use only valid category names from the list above
- Keep release notes focused on user-facing information
- Write specific descriptions that explain the impact to users
- Use `backward-incompatible change` category for any breaking changes
- End subject lines without punctuation
- Explain the "why" behind changes, not just the "what"

## Pull Request Guidelines
- **Create PRs from your personal fork**, not directly on cockroachdb/cockroach
- **Single-commit PRs**: PR title should match commit title, PR body should match commit body