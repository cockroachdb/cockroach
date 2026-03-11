---
name: file-crdb-issue
description: Use when filing, creating, or reporting GitHub issues for CockroachDB. Use when asked to open a bug report, feature request, investigation issue, or performance inquiry. Also use when the user mentions wanting to track a problem, report a regression, or document unexpected behavior in CockroachDB.
---

# Filing GitHub Issues for CockroachDB

File issues on `cockroachdb/cockroach` using the repo's issue templates and
CockroachDB labeling conventions.

## Workflow

### Step 1: Discover Issue Templates

The repo has issue templates in `.github/ISSUE_TEMPLATE/`. Blank issues are
disabled, so one of these templates must be used:

- **bug_report.md** — Reproducible bugs
- **feature_request.md** — New functionality proposals
- **performance-inquiry.md** — Performance problems

For **investigation issues** (tracking research/analysis work that doesn't
fit the above), use the bug report template adapted with investigation
framing (see the Investigation section below).

Read the chosen template file to get its structure and required sections.

### Step 2: Gather Information

If the user has already provided context (e.g., they described a bug before
invoking this skill), use that context to pre-fill sections and confirm
rather than re-asking.

Otherwise, ask the user for:
1. **Issue type** — Which template fits best
2. **Title** — Concise, prefixed with the affected package (e.g., `sql/schemachanger: ...`)
3. **Body content** — Walk through template sections conversationally

### Step 3: Compose and Confirm

Assemble the full issue using the template structure. Show the user a
preview of the complete issue (title, labels, and body) and get explicit
confirmation before filing.

### Step 4: File the Issue

```bash
gh issue create -R cockroachdb/cockroach \
  --title "<title>" \
  --label "<labels>" \
  --body "$(cat <<'EOF'
<assembled issue body>
EOF
)"
```

Display the issue URL after filing.

## Investigation Issues

For research/analysis work, use this structure:

```markdown
**Summary:**
Brief description of what needs investigation.

**Findings:**
- Finding 1
- Finding 2

**Code References:**
- [file.go:123](https://github.com/cockroachdb/cockroach/blob/master/pkg/path/file.go#L123)

**Next Steps:**
- [ ] Action item 1
- [ ] Action item 2
```

## Label Conventions

| Prefix | Purpose | Examples |
|--------|---------|----------|
| `T-` | Team owner | `T-sql-foundations`, `T-kv`, `T-storage`, `T-obs` |
| `A-` | Technical area | `A-sql-optimizer`, `A-sql-execution`, `A-kv-replication`, `A-spanconfig` |
| `O-` | Origin/discovery | `O-roachtest`, `O-support`, `O-community` |
| `C-` | Category/type | `C-bug`, `C-enhancement`, `C-investigation`, `C-test-failure` |

Apply appropriate labels from the template frontmatter plus any relevant
`T-`, `A-`, `O-`, and `C-` labels. Ask the user if they want additional
labels or assignees.

## Jira Epic Association

If the issue should be associated with a Jira epic, add a line at the very
bottom of the issue body:

```
Epic CRDB-12358
```

This causes the Jira issue (created automatically by automation) to be
associated with that epic. Ask the user if there's a relevant epic to link.

## Best Practices

- **Code references**: Use GitHub URLs with line numbers: `[file.go:123](https://github.com/cockroachdb/cockroach/blob/master/pkg/path/file.go#L123)`
- **Stable references**: Include commit SHA in URLs when line numbers may shift.
- **Correlation evidence**: Include timestamped logs for performance issues. Paste excerpts inline.
- **Content**: Use markdown. Be specific about versions, configs, environments. Include repro steps.
- **No release notes** in GitHub issues (release notes are only for PRs and commits).
- **No Jira footer**: Do not add a "Jira issue: CRDB-XXXXX" line. Jira linking is handled automatically by automation after the issue is created.
- **Confirm before filing**: Always show the complete issue and get explicit approval before running `gh issue create`.
