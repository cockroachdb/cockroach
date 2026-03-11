# Commits and PRs

## Commit title

Prefix with the affected package/area, lowercase after the colon, imperative
mood, no trailing period. Keep under ~70 characters.

```
server: use net.Pipe instead of TCP for HTTP/gRPC connections
```

Use `*:` or `all:` for changes spanning many packages with no clear primary.

## Commit body

Explain *what* changed (before/after) and *why*, not *how*. Wrap at 100
characters. For general commit-message philosophy, see
[Chris Beams's guide](https://chris.beams.io/posts/git-commit/).

## Issue and epic references

Place references after the explanation, before the release note.

```
Fixes #123
See also: #456, #789
Epic: CRDB-357

Release note (bug fix): ...
```

`Fixes` / `Resolves` auto-closes the issue on merge. `See also` creates
cross-references. `Epic` links to the Jira epic.

## Release note

Release note annotations in commit messages are extracted by a script and
published in our [user-facing release notes](https://www.cockroachlabs.com/docs/releases/).
The Docs team aggregates and edits them, but what you write is the starting
point. Annotations must be in **commit messages** -- annotations that only
appear in the PR description are not picked up. Write for CockroachDB users
and operators, not for developers -- avoid referencing internal code, package
names, or implementation details.

Every PR must have at least one release note annotation. A missing annotation
forces the Docs team to investigate the PR manually.

### When to write `Release note: None`

Use `Release note: None` for internal changes or changes with no observable
effect on the behavior of documented, supported features:

- Code refactors, test-only changes, and internal plumbing.
- Changes to debugging interfaces, internal settings, or undocumented
  commands/APIs (e.g. `crdb_internal.*`, `cockroach gen settings-list`).
- Changes to features still in development that are not yet documented
  for external use.

### What to write

Explain *what* changed, *how* it changed, and *why it matters to users*.
Default to more detail rather than less -- a thin release note forces a Docs
writer to read the full PR. Use past tense ("Added...", "Fixed...") or
present tense ("CockroachDB now supports..."). If the change is part of
progress toward a broader feature, mention that.

For **bug fixes**, describe the cause and symptoms of the bug and the version
it was introduced in.

For **backward-incompatible changes**, describe what breaks and how users
should adapt.

### Format

```
Release note (<category>): <description>
```

If a commit covers multiple user-visible changes in different areas, write
multiple annotations each with its own `Release note (<category>):` prefix.
Release notes must appear last in the commit message so that subsequent
paragraphs are not mistaken for part of the note.

### Categories

Use one category per release note. When a change could fit multiple
categories, choose the one that makes the most sense from a user perspective.
Use the category names below verbatim -- the extraction script catches common
misspellings, but unrecognized categories end up in "Miscellaneous" for the
Docs team to sort out.

| Category | When to use |
|----------|-------------|
| `backward-incompatible change` | Changes that can break programmatic usage of a CockroachDB interface (SQL statements, CLI flags, logging formats, endpoints, etc.). Use this *instead of* the usual category when the change is backward-incompatible. Mandatory for changes to stable interfaces per the API Support Policy. |
| `enterprise change` | Changes to features requiring an enterprise license (BACKUP/RESTORE, CDC, etc.). |
| `ops change` | Changes affecting operators maintaining production clusters: logging, metrics, health/monitoring endpoints, HTTP endpoints, environment variables, CLI flags for server commands, exit codes. |
| `cli change` | Changes to `cockroach` commands primarily affecting app developers or contributors: SQL shell, `userfile`, `workload`, `debug` commands on non-running servers, etc. |
| `sql change` | Changes to SQL statements, functions/operators, system catalogs, or execution. |
| `ui change` | Changes to the DB Console. |
| `security update` | Changes affecting security features (IAM, TLS, etc.) or the security profile of the product. |
| `performance improvement` | Measurable performance gains. |
| `cluster virtualization` | User-facing changes to cluster virtualization / multi-tenancy. |
| `bug fix` | Fixes to known problems (as opposed to new functionality). |
| `general change` | User-visible changes that don't fit any other category. Only use this as a last resort. |
| `build change` | Changes to requirements for building CockroachDB from source. |

### Examples

**Bug fix** -- poor vs. good:

```
# Poor: invalid category, no cause or affected versions.
Release note (bug): No more duplicate rows for CREATE TABLE ... AS

# Good: valid category, describes cause, symptoms, and affected version.
Release note (bug fix): Fixed a bug introduced in v19.2.3 that caused
duplicate rows in the results of CREATE TABLE ... AS when multiple
nodes attempt to populate the results.
```

**Backward-incompatible change** -- a change that breaks existing behavior
must use `backward-incompatible change` even if it would otherwise be an
`sql change`, `ops change`, etc.:

```
# Wrong: this is an sql change but it breaks existing behavior.
Release note (sql change): Match extract('epoch' from interval)
behavior in PostgreSQL/CockroachDB to 365.25 days.

# Right: correct category, explains what/how/why.
Release note (backward-incompatible change): Casting intervals to
integers and floats now values a year at 365.25 days in seconds
instead of 365 days, for Postgres compatibility.
```

## PR organization

One commit per logical change. Each commit must build and pass tests
independently (enables `git bisect`). Consider splitting a PR when it exceeds
3-5 commits or ~200 changed lines of non-mechanical edits.

When moving code to a new file or package, use one commit for the move (no
logical changes) and a second commit for the modifications. This keeps logical
changes visible to reviewers.

Avoid gratuitous aesthetic changes (spelling fixes, reformatting) that pollute
`git blame` without meaningful improvement.

## Review iteration

Use `git commit --fixup <sha>` for changes requested during review. Before
merging, squash fixups with `git rebase -i --autosquash origin/master` and
polish each resulting commit message.

## Incomplete work

Merging incomplete implementations is acceptable when unimplemented paths are
loud -- use `unimplemented.New(...)` or similar. Track remaining work with a
TODO referencing a GitHub issue:

```go
// TODO(username): support multi-column indexes. See #12345.
```
