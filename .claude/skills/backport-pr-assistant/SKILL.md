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

## Workflow

1. **Start the backport**: Run `backport <pr> -r <release>` for the target branch
2. **When conflicts occur**: The tool stops and lists conflicting files
3. **Analyze conflicts**: Read the conflicting files, understand what's different between branches
4. **Resolve conflicts**: Edit files to resolve, then `git add` the resolved files
5. **Continue**: Run `backport --continue` to resume
6. **Repeat** if more conflicts arise
7. **Complete**: The backport tool pushes and creates the PR (do not use `gh` CLI to make the PR)

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
