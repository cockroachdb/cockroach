---
name: drpc-update
description: Check if the cockroachdb/drpc dependency is outdated in go.mod and, if so, update it and create a PR. Use when the user wants to sync the drpc version.
allowed-tools: Read, Grep, Bash
---

# DRPC Dependency Update

Check whether the `storj.io/drpc` dependency in the CockroachDB repo is up to
date with the latest version from `github.com/cockroachdb/drpc`, and if not,
update it and open a PR.

## Workflow

### Step 1: Update to latest master

```bash
git fetch origin master && git checkout master && git pull origin master
```

If there are local uncommitted changes, warn the user and stop.

### Step 2: Check the current drpc version in go.mod

In `go.mod`, find the line starting with
`replace storj.io/drpc => github.com/cockroachdb/drpc`. Extract the last word on
that line — this is the version. Record it as `CURRENT_VERSION`.

### Step 3: Get the latest drpc version

First, get the latest commit SHA from the `main` branch of `cockroachdb/drpc`:

```bash
LATEST_SHA=$(gh api repos/cockroachdb/drpc/commits/main --jq .sha)
```

Then resolve that SHA to a Go pseudo-version:

```bash
LATEST_VERSION=$(go list -mod=readonly -m "github.com/cockroachdb/drpc@${LATEST_SHA}" 2>/dev/null | awk '{print $2}')
```

Record the output as `LATEST_VERSION`.

### Step 4: Compare versions

If `CURRENT_VERSION` equals `LATEST_VERSION`, inform the user that drpc is
already up to date and stop.

### Step 5: Update the dependency

If the versions differ, verify that `LATEST_VERSION` is **newer** than
`CURRENT_VERSION` (compare the timestamp portion of the pseudo-version, i.e. the
`YYYYMMDDHHMMSS` segment). If `LATEST_VERSION` is older than `CURRENT_VERSION`,
warn the user that the cockroach repo references a newer version than what is on
the drpc main branch and stop.

Otherwise, edit the `replace` directive in `go.mod` directly.
Replace the old version on the line starting with
`replace storj.io/drpc => github.com/cockroachdb/drpc` with `LATEST_VERSION`.
Do **not** use `go get` — it changes the `require` line instead of the `replace`
directive.

After editing `go.mod`, run `go mod tidy` to update `go.sum`:

```bash
go mod tidy
```

Then regenerate BUILD files:

```bash
./dev generate bazel
./dev generate bazel --mirror
```

### Step 6: Get the list of new commits

Extract the short SHAs from `CURRENT_VERSION` and `LATEST_VERSION` (the last 12
characters of each pseudo-version, e.g. `v0.0.0-20260210055719-ba071c6f9395` →
`ba071c6f9395`). Then fetch the commit list between them:

```bash
gh api repos/cockroachdb/drpc/compare/<CURRENT_SHA>...<LATEST_SHA> \
  --jq '.commits[] | "- \(.sha[0:12]) \(.commit.message | split("\n") | .[0])"'
```

Record this as `COMMIT_LIST` for use in the PR description.

### Step 7: Create a commit and PR

**Before creating the commit or PR, ask the user for confirmation.** Show them
the old and new versions and the list of new commits, and ask if they want to
proceed.

Create a new branch, commit the changes, and open a **draft** PR. If the branch
already exists (locally or on the remote), ask the user whether to delete the
existing branch and recreate it, or to use a different branch name.

Use the `/commit-helper`
skill to format the commit message. The commit should follow this structure:

- **Package prefix**: `go.mod`
- **Subject**: `bump drpc to <LATEST_VERSION>`
- **Body**: State the previous version and the new version.
- **Release note**: None

**Push to a personal fork, not origin.** Direct pushes to
`cockroachdb/cockroach` are rejected. Use `git remote -v` to find a fork remote
(e.g. a remote pointing to `github.com/<username>/cockroach`), then push to
that remote. When creating the PR with `gh pr create`, use `--head
<username>:<branch>` to reference the fork branch.

The PR description should follow this format:

```
Bump storj.io/drpc from <CURRENT_VERSION> to <LATEST_VERSION>.

New commits:
<COMMIT_LIST>

Epic: none
Release note: None
```
