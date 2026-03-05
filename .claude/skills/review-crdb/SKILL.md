---
name: review-crdb
description: >
  Review code changes or PRs for quality, correctness, and reviewability.
  Use when asked to "review", "check", "provide feedback", or "post a review".
  Dispatches specialized agents in parallel for thorough analysis.
---

# Code Review

Review the change and provide structured, actionable feedback.

The primary goal is **correctness**. This is a distributed database — read the
code with a critical eye for concurrency issues, locking discipline, failure
modes, and architectural soundness. The specialized agents below are a checklist
of common patterns to verify, but they're secondary to understanding whether the
code is actually correct.

## Identifying the change

Determine what to review based on context:

- **Current branch vs merge base**: find the merge base against the most
  up-to-date master ref (local `master` is often stale — prefer remote-tracking
  refs):
  ```bash
  # Pick the most recently updated master-like ref (upstream/master, origin/master, etc.)
  master_ref=$(git for-each-ref --sort=-committerdate --format='%(refname:short)' \
    --count=1 'refs/remotes/*/master' 'refs/heads/master')
  merge_base=$(git merge-base HEAD "$master_ref")
  git log --oneline "$merge_base"..HEAD
  git diff "$merge_base"..HEAD
  ```
- **Staged changes**: `git diff --cached`.
- **A PR**: `gh pr diff <number>` and `gh pr view <number>`.
- **Specific files**: as named by the user.

Read enough surrounding code to understand context — don't review the diff in
isolation. When reviewing a PR, this may require checking out the branch
(`gh pr checkout <number>`) to read files at the PR's version. Before doing so,
check `git status` — if the user has uncommitted changes to tracked files, ask
before checking out.

## Review aspects

Each aspect is handled by a specialized agent in `.claude/agents/`. Not every
aspect applies to every change — scan the diff, determine which apply, and skip
the rest.

| Aspect | Agent | Applies when |
|--------|-------|--------------|
| Correctness & safety | `crdb-correctness-reviewer` | Always for non-trivial changes |
| Error handling | `crdb-error-reviewer` | Code touches error paths, retry logic, or resource cleanup |
| Go conventions & comments | `crdb-conventions-reviewer` | Always — focuses on new/changed code |
| Test coverage | `crdb-test-reviewer` | Test files changed, or new behavior without tests |
| Commit structure & PR description | `crdb-commit-reviewer` | Reviewing a branch/PR with commits |
| Type design | `crdb-type-reviewer` | New structs/interfaces added or significantly modified |
| Simplification | `crdb-simplifier` | After other aspects pass — polish step |

### Selecting aspects

The user can request specific aspects:

- `/review-crdb` — run all applicable aspects (default)
- `/review-crdb correctness tests` — run only those two
- `/review-crdb simplify` — run only the simplifier

Valid aspect names: `correctness`, `errors`, `conventions`, `tests`, `commits`,
`types`, `simplify`, `all`.

## Dispatching agents

### Parallel approach (default)

Launch all applicable agents simultaneously using the Agent tool. Each agent
receives:
- The diff (or file list) to review
- Instructions to save its output as structured findings

This is faster and gives comprehensive results. Use this unless the user
requests sequential review.

### Sequential approach

Run agents one at a time. Useful for interactive review where the user wants to
discuss findings as they come in. The user can request this with "review
sequentially" or similar phrasing.

### Agent input

When dispatching each agent, provide:
1. The diff output (or instructions on how to get it)
2. The list of changed files
3. Any relevant context about what the change does (from PR description, commit
   messages, or user explanation)

## Aggregating results

After all agents complete, produce a unified summary. Keep it concise — only
report findings that need action or highlight a genuinely noteworthy pattern.
Don't include empty sections for aspects where no issues were found.

```markdown
# Review Summary

## Blocking Issues (must fix)
- [agent]: issue description [file:line]

## Suggestions (should fix)
- [agent]: issue description [file:line]

## Nits (take it or leave it)
- [agent]: observation [file:line]

## Strengths
- What's well-done in this change

## Aspects skipped
- [aspect]: why it didn't apply

## Next steps
1. Fix blocking issues first
2. Address suggestions
3. Re-run `/review-crdb` on the affected aspects to verify fixes
```

Deduplicate findings across agents. If two agents flag the same issue, keep the
more specific one.

## Posting reviews on PRs

If the user asks to "post a review" or "submit feedback" on a PR, post a
single batched review using the GitHub API. This puts the summary in the
review body and each finding as an inline comment on the relevant line.

Use `gh api` to create the review in one call:

```bash
gh api repos/{owner}/{repo}/pulls/<number>/reviews \
  --method POST \
  --input /tmp/review-payload.json
```

Build the JSON payload with this structure:

```json
{
  "event": "REQUEST_CHANGES or COMMENT",
  "body": "Summary of the review (the top-level review comment).",
  "comments": [
    {
      "path": "pkg/some/file.go",
      "line": 42,
      "side": "RIGHT",
      "body": "**suggestion**: The comment should reflect that the key is only used for cleanup.\n\n```suggestion\n// DeprecatedStoreClusterVersionKey is only read during cleanup migration\n// and then deleted. Retained until MinSupported > V26_2.\n```"
    }
  ]
}
```

- **`body`**: the summary — what the change does, overall assessment, and any
  findings that aren't tied to a specific line (e.g., commit structure
  suggestions, missing version gating). Commit structure and brief PR
  description feedback belong here. Don't paste rewritten descriptions — just
  note what's missing or misleading. End the body with a footer line:
  `\n\n---\n*(made with [/review-crdb](https://github.com/cockroachdb/cockroach/blob/master/.claude/skills/review-crdb/SKILL.md))*`
- **`comments`**: one entry per finding that has a specific file and line. Use
  `line` (the actual line number in the file) and `side: "RIGHT"` (commenting
  on the new code). Prefix each comment body with the severity. For small,
  concrete fixes, use GitHub's suggested changes syntax — a fenced code block
  with the `suggestion` language tag. GitHub renders this as a diff with an
  "Apply suggestion" button. The content replaces the line(s) the comment is
  attached to. Use `start_line` + `start_side` to cover multi-line replacements.
  Only use suggestions for small, local changes — not for structural feedback.
- **`event`**:
  - `REQUEST_CHANGES`: if there are any **blocking** or **suggestion** findings.
  - `COMMENT`: if there are only **nit** findings, or the change looks good
    but you have minor observations.
  - Never `APPROVE` unless the user explicitly says to approve (e.g., "post a
    review, approve if appropriate"). If the review has no findings and you
    would otherwise approve, ask the user: "No issues found — do you want me
    to approve this for you?"

Always confirm with the user before posting. Show them a preview of the review
body, the inline comments, and the event, then ask for the go-ahead.
