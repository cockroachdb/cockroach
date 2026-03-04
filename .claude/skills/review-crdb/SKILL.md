---
name: review-crdb
description: >
  Review code changes or PRs for quality, correctness, and reviewability.
  Use when asked to "review", "check", "provide feedback", or "post a review".
---

# Code Review

Review the change and provide structured, actionable feedback.

The primary goal is **correctness**. This is a distributed database — read the
code with a critical eye for concurrency issues, locking discipline, failure
modes, and architectural soundness. The specific aspects below are a checklist
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

Each aspect has an authoritative source. Load that source and review against it.
Not every aspect applies to every change — scan the diff, determine which apply,
and skip the rest.

| Aspect | Source | Applies when |
|--------|--------|--------------|
| Redactability | `.claude/rules/redactability.md` (auto-loaded) | Diff adds/modifies types with `String()`, `Error()`, or log output |
| Error handling | `.claude/rules/error-handling.md` (auto-loaded) | Diff creates or wraps errors |
| Comments | `.claude/rules/commenting-standards.md` (auto-loaded) | Always — but focus on new/changed code |
| Go conventions | `.claude/rules/go-conventions.md` (auto-loaded) | Always — but focus on new/changed code |
| Commit structure | `commit-arc` skill (see below) | Change is non-trivial (whether or not it's already split into commits) |
| Red/green testing | `commit-arc` skill (see below) | Behavioral fix/improvement alongside test changes |
| PR/commit description | `commit-helper` skill (auto-loaded) | Reviewing a PR or branch with commit messages |

**Rules** (`.claude/rules/`) are auto-loaded into context. Review the diff
against them directly — don't re-state their contents, just apply them.

**The `commit-arc` skill** covers commit structure and red/green testing in
depth. It's a user-level skill, not installed project-wide. If the
`commit-arc` skill is loaded (check whether it appears in the available skills
list), review commit structure and red/green testing against its guidance.
Otherwise, still review commit structure using basic principles (self-contained
commits, mechanical/semantic separation, progressive ordering, whether a large
single commit should be split) and note in your review: "The `commit-arc` skill
was not available — consider installing it via `roachdev claude` for richer
commit-structure guidance at development time."

An important goal is to make the change pleasant to review by a human. A single commit
that mixes a refactor, a bug fix, and a test update is harder to review than
three focused commits — flag this even (especially) when the author hasn't
split their work yet.

### PR and commit descriptions

The PR description (and commit messages for multi-commit PRs) should orient the
reviewer. The bar scales with the complexity of the change:

- **Small, obvious changes** (typo fixes, one-line bug fixes, mechanical
  refactors): a brief description is fine. Don't demand an essay for a one-liner.
- **Non-trivial changes**: the description should give the reviewer a high-level
  understanding of *what* the change achieves and *why*, so they can review the
  code with the right mental model. Flag descriptions that are missing this
  context, are misleading about what the code actually does, or that would leave
  a reviewer guessing about the motivation.
- **Multi-commit PRs**: the PR body should explain how the commits fit together
  and give the reviewer a roadmap for reading them in order.

When posting to GitHub, brief directional feedback on the PR description is fine
in the review body (e.g., "the description doesn't mention the compatibility
implications" or "consider explaining why the refactor was necessary"). But
don't paste a rewritten description into the review — if a full rewrite is
warranted, suggest it to the user in conversation and let them update the
message themselves.

### Beyond the sources

The sources above cover specific patterns. Also watch for:

- **Compatibility**: does the change affect RPCs, persisted formats, or shared
  state? If so, is cluster-version gating considered?
- **Unnecessary complexity**: could the change be simpler? Is anything
  over-engineered for the problem at hand?

Don't nitpick formatting — `crlfmt` handles that.

## Output format

Keep the review concise. Only report findings that need action or that highlight
a genuinely noteworthy pattern. Don't list aspects where you found no issues —
the "aspects skipped" section already covers what wasn't checked and why.

1. **Summary**: one or two sentences on what the change does and overall
   assessment (looks good / needs work / has blocking issues).
2. **Findings**: grouped by aspect. For each finding:
   - File and line (or commit, for structure issues).
   - The problem and why it matters.
   - Suggested fix when possible.
   - Severity: **blocking** (must fix), **suggestion** (should fix), or
     **nit** (take it or leave it).
3. **Aspects skipped**: which aspects didn't apply, briefly. Only list if there
   are non-obvious skips worth explaining.
4. **Missing sources**: only list if a rule or skill was unavailable.

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
