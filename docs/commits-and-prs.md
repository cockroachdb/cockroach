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

Every PR must have at least one release note annotation. Use `Release note: None`
for non-user-facing changes.

Categories: `bug fix`, `general change`, `sql change`,
`performance improvement`, `backward-incompatible change`, `security update`,
`build change`.

A release note can span multiple paragraphs; it must appear last in the commit
message so that subsequent paragraphs are not mistaken for part of it.

```
Release note (sql change): Added support for the `SHOW RANGES FOR
INDEX` statement, which displays the range boundaries for a given
index. Previously, users had to query `crdb_internal.ranges` directly
to obtain this information.
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
