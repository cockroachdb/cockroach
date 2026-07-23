---
name: bump-cluster-ui
description: Bump cluster-ui package version after a release branch cut. Creates two PRs — one to drop the prerelease suffix on the release branch and one to increment the minor version on master.
user_invocable: true
---

# Bump cluster-ui Version

Automate the two cluster-ui version bumps required after a release branch is
created. This creates two PRs:

1. **Release branch PR**: Drop the prerelease suffix (e.g., `26.2.0-prerelease.0` → `26.2.0`)
2. **Master PR**: Increment minor version for next cycle (e.g., `26.2.0-prerelease.0` → `26.3.0-prerelease.0`)

The file to edit is `pkg/ui/workspaces/cluster-ui/package.json` (the `"version"` field).

## Workflow

### Step 1: Gather Inputs

Ask the user for:
1. **Release version** (required) — e.g., `26.2`. This is the version that was just branched.
2. **Jira epic** (optional) — e.g., `REL-4280`. Included in commit messages if provided.

### Step 2: Validate

Fetch remote branches and confirm `origin/release-{version}` exists:

```bash
git fetch origin release-{version} --no-tags
```

If the branch does not exist, report the error and stop.

### Step 3: Read Current State

Read `pkg/ui/workspaces/cluster-ui/package.json` on both branches to verify
the current version matches the expected `{major}.{minor}.0-prerelease.0` pattern:

```bash
git show origin/release-{version}:pkg/ui/workspaces/cluster-ui/package.json
git show origin/master:pkg/ui/workspaces/cluster-ui/package.json
```

If the version does not match expectations, warn the user and ask whether to
proceed.

### Step 4: Confirm with User

Show the user a summary of the two planned changes:

```
Release branch PR (targeting release-{version}):
  package.json version: {version}.0-prerelease.0 → {version}.0

Master PR (targeting master):
  package.json version: {version}.0-prerelease.0 → {next_version}.0-prerelease.0
```

Where `{next_version}` increments the minor: e.g., `26.2` → `26.3`.

Get explicit confirmation before proceeding.

### Step 5: Identify Fork Remote

The `cockroachdb/cockroach` repo restricts branch creation, so branches must be
pushed to the user's personal fork. Identify the fork remote:

```bash
git remote -v
```

Look for a remote pointing to the user's fork (e.g., `fork` → `kyle-a-wong/cockroach`).
If no fork remote is found, ask the user which remote to use.

Use `{fork}` below to refer to the identified fork remote name, and `{fork_owner}`
for the GitHub username (e.g., `kyle-a-wong`).

### Step 6: Create Release Branch PR

```bash
git checkout -b bump-cluster-ui-{version} origin/release-{version}
```

Edit `pkg/ui/workspaces/cluster-ui/package.json`: change the `"version"` field
from `"{version}.0-prerelease.0"` to `"{version}.0"`.

Commit with message:

```
ui: bump cluster-ui to {version}.0

Epic: {epic}
Release note: None
```

If no epic was provided, omit the `Epic:` line.

Push to the fork and create the PR:

```bash
git push -u {fork} bump-cluster-ui-{version}
gh pr create -R cockroachdb/cockroach \
  --head {fork_owner}:bump-cluster-ui-{version} \
  --base release-{version} \
  --title "ui: bump cluster-ui to {version}.0" \
  --body "$(cat <<'EOF'
Bump cluster-ui package version to {version}.0 for the release branch.

Epic: {epic}
Release note: None

Release justification: version bump that's part of release flow. no production code changed.
EOF
)"
```

### Step 7: Create Master PR

Compute the next version by incrementing the minor component:
- `26.2` → `26.3`
- `25.4` → `25.5`

```bash
git checkout -b bump-cluster-ui-{next_version}-prerelease origin/master
```

Edit `pkg/ui/workspaces/cluster-ui/package.json`: change the `"version"` field
from `"{version}.0-prerelease.0"` to `"{next_version}.0-prerelease.0"`.

Commit with message:

```
ui: bump cluster-ui version to {next_version}.0-prerelease.0

Epic: {epic}
Release note: None
```

If no epic was provided, omit the `Epic:` line.

Push to the fork and create the PR:

```bash
git push -u {fork} bump-cluster-ui-{next_version}-prerelease
gh pr create -R cockroachdb/cockroach \
  --head {fork_owner}:bump-cluster-ui-{next_version}-prerelease \
  --base master \
  --title "ui: bump cluster-ui version to {next_version}.0-prerelease.0" \
  --body "$(cat <<'EOF'
Bump cluster-ui package version to {next_version}.0-prerelease.0 for the next development cycle.

Epic: {epic}
Release note: None
EOF
)"
```

### Step 8: Report

Display both PR URLs to the user. Switch back to the original branch:

```bash
git checkout -
```

## Commit Message Conventions

Follow the historical pattern from prior bumps:

- Release branch: `ui: bump cluster-ui to {version}.0`
- Master: `ui: bump cluster-ui version to {next_version}.0-prerelease.0`
- Always include `Release note: None`
- Include `Epic: {epic}` when an epic is provided

## Error Handling

- If `origin/release-{version}` doesn't exist, stop and tell the user.
- If the current version in `package.json` doesn't match expectations, show the
  actual version and ask the user how to proceed.
- If a branch name like `bump-cluster-ui-{version}` already exists locally,
  warn the user and ask whether to delete and recreate it.
- If `gh pr create` fails, show the error and suggest the user check their
  GitHub authentication (`gh auth status`).
