---
name: ui-visual-regression
description: Automates visual regression testing for DB Console UI changes. Compares screenshots and network requests between the current branch and its merge base using roachprod, playwright-cli, and ImageMagick. Use when the user wants to verify UI changes haven't introduced visual or behavioral regressions.
allowed-tools: Bash(roachprod:*), Bash(playwright-cli:*), Bash(compare:*), Bash(diff:*), Bash(git:*), Bash(./dev:*), Bash(npm:*), Bash(which:*), Bash(curl:*), Bash(mkdir:*), Bash(cp:*), Bash(ls:*), Bash(tail:*), Bash(sleep:*), Bash(rm:*), Bash(gh:*), Bash(grep:*)
---

# UI Visual Regression Testing

Automates screenshot-based visual comparison and network request diffing between the current branch and its merge base commit.

## Prerequisites

Check these before starting and install any missing tools:

```bash
# Check all prerequisites
which roachprod
which playwright-cli
which compare
```

- **roachprod**: Must be on PATH. Part of CockroachDB dev tooling.
- **playwright-cli**: Install with `npm install -g @playwright/cli` if missing. The old package `playwright-cli` is deprecated.
- **ImageMagick**: Install with `brew install imagemagick` if `compare` is not found.
- **./dev**: CockroachDB dev build tool (in repo root).

## Workflow

Follow these steps in order. Present the test plan (Step 3) to the user for approval before proceeding to screenshots.

### Step 1: Set Up Local Cluster

```bash
# Check for existing local cluster
roachprod list | grep local

# If exists, ask user whether to destroy it
roachprod destroy local

# Create and start a 3-node insecure cluster
roachprod create local -n 3
roachprod stage local release latest
roachprod start local --insecure
```

### Step 1b: Create Test Data

After starting the cluster, create test data so pages render with populated content rather than empty states. Tailor the data to the pages being tested.

For database/table/index pages:
```bash
roachprod sql local:1 --insecure -- -e "
CREATE TABLE defaultdb.test_table (id INT PRIMARY KEY, name STRING, value INT, INDEX idx_name (name), INDEX idx_value (value));
INSERT INTO defaultdb.test_table SELECT i, 'name_' || i::STRING, i * 10 FROM generate_series(1, 1000) AS g(i);
"
```

For pages that show statement fingerprints or index usage, run a workload to generate stats:
```bash
roachprod sql local:1 --insecure -- -e "
SELECT * FROM defaultdb.test_table WHERE id = 1;
SELECT * FROM defaultdb.test_table WHERE name = 'name_50';
SELECT * FROM defaultdb.test_table WHERE value = 500;
SELECT count(*) FROM defaultdb.test_table;
"
```

**Important**: Statement statistics are flushed periodically (not immediately). Wait at least 60 seconds after running the workload before capturing screenshots that show statement fingerprints or index usage data. Alternatively, capture the "before" screenshots first (which adds natural delay), then capture "after".

### Step 2: Analyze Git Diff for Changed UI Pages

Identify which DB Console pages are affected by the current branch's changes.

Check both committed and uncommitted changes:
```bash
# Check for commits ahead of master
git log --oneline master..HEAD

# Check for uncommitted changes
git status --short
```

If there are commits ahead of master, use the commit diff:
```bash
git diff --name-only $(git merge-base HEAD master)..HEAD
```

If all changes are uncommitted (no commits ahead of master), use the working tree diff:
```bash
git diff --name-only HEAD
# Also check for untracked files
git ls-files --others --exclude-standard
```

Filter results for files under:
- `pkg/ui/workspaces/db-console/src/views/`
- `pkg/ui/workspaces/cluster-ui/src/`

Map changed file paths to DB Console URL routes using the reference table below.

For cluster-ui changes, trace the changed exports to their db-console consumers by searching for imports of the changed module in `pkg/ui/workspaces/db-console/`.

### Step 3: Create and Present Test Plan

Before taking screenshots, present the user with:
1. List of changed files detected
2. Routes to test (mapped from changed files)
3. Viewport size (default: 1920x1080)
4. Any routes needing parameters and the defaults you'll use

Then proceed immediately — do NOT wait for explicit approval. The user can interrupt if they want changes to the plan.

### Step 4: Start Dev Server

**Important**: `roachprod adminui local` reports `https://` URLs, but insecure clusters actually serve HTTP. Always use `http://` when `--insecure` was used.

```bash
# Get the admin UI URL — take the first line
roachprod adminui local
# Output will show https:// URLs like https://127.0.0.1:29001/
# For --insecure clusters, replace https:// with http://

# Verify the correct protocol by testing connectivity
curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:29001/
# Should return 200

# Start webpack dev server in background, proxying to the cluster
./dev ui watch --db http://127.0.0.1:29001
```

Wait for the dev server to finish compiling. Monitor output for "Compiled successfully":
```bash
# Poll until compilation finishes (check the background task output)
tail -3 <background-task-output-file> | grep "Compiled successfully"
```

The dev server runs on `http://localhost:3000`.

### Step 5: Capture Screenshots & Network Requests — Current Branch (After)

```bash
mkdir -p /tmp/ui-regression-test/after/network
```

For each route in the test plan:
```bash
# Open browser and set viewport
playwright-cli open http://localhost:3000
playwright-cli resize 1920 1080

# Navigate to the route (DB Console uses hash routing)
playwright-cli goto "http://localhost:3000/#/<route>"

# Wait for page data to load (5 seconds is usually sufficient)
sleep 5
playwright-cli snapshot

# Capture network log — playwright-cli writes to a .log file
playwright-cli network
# Copy the output file to the test directory
cp .playwright-cli/network-<timestamp>.log /tmp/ui-regression-test/after/network/<page-name>.txt

# Take screenshot
playwright-cli screenshot --filename=/tmp/ui-regression-test/after/<page-name>.png

# Close browser when done with all routes
playwright-cli close
```

### Step 6: Checkout Base Commit

```bash
# Record current branch name
git rev-parse --abbrev-ref HEAD

# Find merge base
git merge-base HEAD master

# Stash uncommitted changes — use -u to include untracked files
git stash -u

# Checkout the merge base
git checkout <merge-base>
```

The `./dev ui watch` process will detect file changes and recompile automatically. Wait for recompilation to finish before proceeding.

### Step 7: Capture Screenshots & Network Requests — Base (Before)

Wait for the dev server to finish recompiling after the checkout. Monitor for "Compiled successfully" in the dev server output.

```bash
mkdir -p /tmp/ui-regression-test/before/network
```

Repeat the same capture process as Step 5, saving to `/tmp/ui-regression-test/before/` instead of `after/`.

### Step 8: Return to Original Branch

```bash
git checkout <original-branch>
# Pop stash to restore changes (including untracked files)
git stash pop
```

### Step 9: Compare Screenshots with ImageMagick

```bash
mkdir -p /tmp/ui-regression-test/diff
```

For each page:
```bash
# Get pixel difference count (0 = identical)
# Note: compare returns exit code 1 when images differ — this is normal
compare -metric AE \
  /tmp/ui-regression-test/before/<page>.png \
  /tmp/ui-regression-test/after/<page>.png \
  /tmp/ui-regression-test/diff/<page>-diff.png 2>&1
```

For pages with differences, generate a highlighted diff:
```bash
compare -highlight-color red -lowlight-color none \
  /tmp/ui-regression-test/before/<page>.png \
  /tmp/ui-regression-test/after/<page>.png \
  /tmp/ui-regression-test/diff/<page>-highlighted.png
```

When interpreting results, note that pixel differences in **dynamic data** (latency values, timestamps, node counts) are expected and do not indicate regressions. Focus on structural/layout differences.

### Step 10: Compare Network Requests

For each page:
```bash
diff /tmp/ui-regression-test/before/network/<page>.txt \
     /tmp/ui-regression-test/after/network/<page>.txt
```

Categorize differences:
- **New requests**: API calls added (expected for new features, flag if unexpected)
- **Removed requests**: API calls that no longer happen (may indicate broken data fetching)
- **Changed endpoints**: URL or method changes
- **Status code changes**: Requests that now fail or succeed differently
- **Request count changes**: Same endpoint called more/fewer times (watch for N+1 regressions)

Pay special attention to:
- Requests to `_status/*` and `_admin/*` endpoints (CockroachDB internal APIs)
- Any requests returning 4xx/5xx that previously returned 2xx
- Duplicate requests suggesting SWR deduplication isn't working

When comparing, ignore:
- **Request order**: SWR and Redux dispatch requests differently; order alone is not a regression
- **Health check count**: `_admin/v1/health` is polled on an interval; count depends on timing
- **Polling requests**: Focus on the distinct set of endpoints called, not repeated polling calls

### Step 11: Cleanup & Report Results

**Cleanup**:
```bash
# Stop the ./dev ui watch background process
# Destroy the roachprod cluster
roachprod destroy local
# Clean up playwright artifacts from the repo directory
rm -rf .playwright-cli/
```

**Report** with three sections:

**Visual Comparison Summary**
- Pages with zero pixel differences (no visual regression)
- Pages with differences: pixel count, percentage, path to diff image
- Whether differences are in dynamic data (expected) or structural layout (potential regression)
- Paths to before/after screenshots for manual inspection

**Network Request Summary**
- Pages with identical network behavior
- Pages with network changes: categorized list of differences
- Flag potentially problematic changes (removed requests, new errors, duplicate calls)

**Overall Assessment**
- Whether the changes appear to be a safe refactor (same visual output, same or equivalent network behavior)
- Any regressions that need investigation

## File Path to Route Mapping

Use this table to map changed files to DB Console routes for testing.

| File Path Pattern | Route | Default Test URL |
|---|---|---|
| `views/reports/containers/network/` | `/reports/network/:nodeId` | `/#/reports/network/region` |
| `views/cluster/containers/clusterOverview/` | `/overview/list` | `/#/overview/list` |
| `views/cluster/containers/nodeGraphs/` | `/metrics/:dashboard/cluster` | `/#/metrics/overview/cluster` |
| `views/databases/` | `/databases` | `/#/databases` |
| `views/sqlActivity/` | `/sql-activity` | `/#/sql-activity` |
| `views/jobs/` | `/jobs` | `/#/jobs` |
| `views/insights/` | `/insights` | `/#/insights` |
| `views/reports/containers/settings/` | `/reports/settings` | `/#/reports/settings` |
| `views/hotRanges/` | `/hotranges` | `/#/hotranges` |

For cluster-ui changes, trace exports back to db-console consumers:
```bash
# Find which db-console files import the changed cluster-ui module
grep -r "from.*cluster-ui.*<changed-module>" \
  pkg/ui/workspaces/db-console/src/
```

## Troubleshooting

### Proxy errors (`EPROTO`, `Error occurred while trying to proxy`)
The dev server proxy target uses the wrong protocol. Insecure roachprod clusters serve HTTP, not HTTPS. Verify with:
```bash
curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:29001/  # should return 200
curl -s -o /dev/null -w "%{http_code}" https://127.0.0.1:29001/ # will fail
```
Restart `./dev ui watch` with `--db http://...` instead of `https://`.

### Dev server recompilation errors after checkout
When checking out the base commit, the dev server may briefly show compilation errors as files change in stages (e.g., an import references a file that was removed by the stash before the checkout restores the old version). Wait for the final compilation to settle — the last "Compiled successfully" message is what matters.

### `playwright-cli` not found after install
The `@playwright/cli` npm package installs the `playwright-cli` binary. If `which playwright-cli` still fails after `npm install -g @playwright/cli`, check your npm global bin directory is on PATH:
```bash
npm bin -g  # shows the global bin directory
```

## Notes

- DB Console uses hash routing (`/#/path`), so URLs include the `#` prefix
- Screenshots go to `/tmp/ui-regression-test/` to avoid cluttering the repo
- The `./dev ui watch` process runs in the background and hot-reloads on file changes
- Some pages require data in the cluster to render meaningfully (e.g., SQL Activity needs running queries)
- This skill uses `roachprod` (not `roachdev`) for all cluster operations
- `playwright-cli` creates a `.playwright-cli/` directory in the working directory — clean it up after the test
- When stashing, always use `git stash -u` to include untracked files (new files not yet committed)
- ImageMagick `compare` returns exit code 1 when images differ — this is expected behavior, not an error
