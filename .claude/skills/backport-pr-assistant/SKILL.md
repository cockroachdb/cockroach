---
name: backport-pr-assistant
description: Help backport PRs to release branches using the backport CLI tool. Use when backporting changes that have merge conflicts requiring manual resolution.
---

# CockroachDB Backport Assistant

Help the user backport pull requests to older release branches, especially when conflicts need resolution.

## Backport CLI Tool Reference

**Basic Usage:**
```bash
backport <pull-request>...              # Backport entire PR(s)
backport <pr> -r <release>              # Target specific release (e.g., -r 23.2)
backport <pr> -b <branch>               # Target specific branch (e.g., -b release-23.1.10-rc)
backport <pr> -j "justification"        # Add release justification
backport <pr> -c <commit> -c <commit>   # Cherry-pick specific commits only
backport <pr> -f                        # Force operation
```

**Conflict Resolution:**
```bash
backport --continue                     # Resume after resolving conflicts
backport --abort                        # Cancel in-progress backport
```

**Common Examples:**
```bash
backport 23437                          # Simple backport
backport 23437 -r 23.2                  # To release-23.2 branch
backport 23437 -j "test-only changes"   # With justification
backport 23437 -b release-23.1.10-rc    # To specific release candidate branch
```

## Determining Target Branches

When the user specifies exact release branches, use those directly. When the user
says something like "backport to all branches" or "backport to all supported releases"
without listing specific versions, determine which branches are still supported by
fetching the CockroachDB release support policy page:

```
https://www.cockroachlabs.com/docs/releases/release-support-policy
```

The page contains two tables: **Supported versions** and **Unsupported versions**.
Only backport to versions listed in the **Supported versions** table. Skip any
version that appears in the **Unsupported versions** table (it is EOL). Pay
attention to Innovation releases — they have shorter support windows and no
Assistance Support phase, so they may have recently gone EOL.

## Workflow

1. **Start the backport**: Run `backport <pr> -r <release>` for the target branch
2. **When conflicts occur**: The tool stops and lists conflicting files
3. **Analyze conflicts**: Read the conflicting files, understand what's different between branches
4. **Resolve conflicts**: Edit files to resolve, then `git add` the resolved files
5. **Continue**: Run `backport --continue` to resume
6. **Repeat** if more conflicts arise
7. **Complete**: The backport tool cherry-picks the commits and pushes the branch to the user's fork
8. **PR creation**: Only create a PR if the user explicitly asks for it. By default, stop after the backport tool pushes the branch. If the user does ask for a PR, use `gh pr create` (see "Creating Backport PRs" below)

## Creating Backport PRs

Only create PRs when the user explicitly requests it. Use `gh pr create` with the
following conventions:

**PR Title Format:**
```
release-XX.X: <original PR title>
```

The title is the release branch prefix, a colon, a space, and then the original PR
title verbatim. For example, if the original PR title is "keys: handle case where
keys targeted by GC request straddle header" and the target branch is release-24.3,
the backport PR title should be:

```
release-24.3: keys: handle case where keys targeted by GC request straddle header
```

**PR Body Format:**

Match the standard body format used by the backport tool:

```
Backport N/N commits from #<original-pr> on behalf of @<user>.

----

<original PR commit messages or body>

----

Release justification: <justification>
```

Before starting the first backport, prompt the user for a release justification
(e.g., "bug fix for potential data loss", "test-only changes", "security patch").
Use the same justification across all backport PRs. If the user declines or
doesn't provide one, leave the release justification line empty.

**Example `gh pr create` invocation:**
```bash
gh pr create \
  --repo cockroachdb/cockroach \
  --base release-24.3 \
  --head <user>:backport24.3-<pr-number> \
  --title "release-24.3: <original title>" \
  --body "<body following the format above>"
```

## Conflict Resolution Guidelines

**Simple conflicts you can resolve directly:**
- Import statement conflicts
- Simple variable name changes
- Basic formatting differences
- Minor API signature changes that are obvious

**Complex conflicts - ask the user for guidance:**
- Conflicts involving significant logic changes
- Dependencies that don't exist in the target branch
- API changes requiring substantial modification
- Multiple conflicting files with interdependent changes
- Changes that may not be appropriate for the target branch

## When Resolving Conflicts

1. **Explain what's conflicting** - show the relevant code sections
2. **Explain why** - what's different between branches that caused this
3. **Propose a resolution** - or ask for guidance if complex
4. **After resolving**: `git add <files>` then `backport --continue`
