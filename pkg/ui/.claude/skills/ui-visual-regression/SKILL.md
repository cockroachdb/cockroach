---
name: ui-visual-regression
description: Automates visual regression testing for DB Console UI changes. Compares screenshots and network requests between the current branch and its merge base using roachprod, playwright-cli, and ImageMagick. Use when the user wants to verify UI changes haven't introduced visual or behavioral regressions.
allowed-tools: Bash(roachprod:*), Bash(playwright-cli:*), Bash(compare:*), Bash(diff:*), Bash(git:*), Bash(./dev:*), Bash(npm:*), Bash(which:*), Bash(curl:*), Bash(mkdir:*), Bash(cp:*), Bash(ls:*), Bash(tail:*), Bash(sleep:*), Bash(rm:*), Bash(gh:*), Bash(grep:*), Bash(date:*)
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

Follow these steps in order. Present the test plan (Step 3) to the user and **wait for explicit approval** before proceeding.

### Step 1: Determine Output Directory

Each invocation stores results in a unique timestamped subdirectory to avoid overwriting previous runs:

```bash
RUN_ID=$(date +%Y%m%d-%H%M%S)
RUN_DIR="/tmp/ui-regression-test/${RUN_ID}"
mkdir -p "${RUN_DIR}"
```

All before/after/diff screenshots and network logs for this run go under `${RUN_DIR}/`.

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
5. Whether an existing local cluster was detected and what will happen to it

**Wait for the user to explicitly approve the plan before proceeding.** The user may want to adjust routes, viewport size, or other parameters.

### Step 4: Set Up Local Cluster

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

### Step 4b: Create Test Data

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

### Step 5: Start Dev Server

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

### Step 6: Capture Screenshots & Network Requests — Current Branch (After)

```bash
mkdir -p "${RUN_DIR}/after/<page-name>"
```

For each route in the test plan:
```bash
# Open browser and set viewport
playwright-cli open http://localhost:3000
playwright-cli resize 1920 1080

# Navigate to the route (DB Console uses hash routing)
playwright-cli goto "http://localhost:3000/#/<route>"

# Wait for data to load — do NOT rely on a fixed sleep alone.
# After an initial wait, take a snapshot and verify that data has
# actually rendered (e.g., node counts > 0, no loading skeletons).
# If the page still shows loading state, wait longer and retry.
sleep 10
playwright-cli snapshot
# Check snapshot output for signs of loading state (skeleton, "Nodes (0)",
# "No data to display", spinner). If found, wait another 10s and retry.

# Capture network log — playwright-cli writes to a .log file
playwright-cli network
# Copy the output file to the test directory
cp .playwright-cli/network-<timestamp>.log "${RUN_DIR}/after/<page-name>/<scenario>-network.txt"

# Take screenshot
playwright-cli screenshot --filename="${RUN_DIR}/after/<page-name>/<scenario>.png"

# Close browser when done with all routes
playwright-cli close
```

### Step 7: Checkout Base Commit

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

### Step 8: Capture Screenshots & Network Requests — Base (Before)

Wait for the dev server to finish recompiling after the checkout. Monitor for "Compiled successfully" in the dev server output.

```bash
mkdir -p "${RUN_DIR}/before/<page-name>"
```

Repeat the same capture process as Step 6, saving to `${RUN_DIR}/before/<page-name>/` instead of `after/<page-name>/`. Apply the same data-loading verification — check that the page has fully rendered data before taking the screenshot. The Redux-based data fetching path (on the base commit) may take longer to load than SWR; retry if the snapshot shows loading skeletons or empty state.

### Step 9: Return to Original Branch

```bash
git checkout <original-branch>
# Pop stash to restore changes (including untracked files)
git stash pop
```

### Step 10: Compare Screenshots with ImageMagick

```bash
mkdir -p "${RUN_DIR}/diff"
```

For each page:
```bash
# Get pixel difference count (0 = identical)
# Note: compare returns exit code 1 when images differ — this is normal
compare -metric AE \
  "${RUN_DIR}/before/<page>.png" \
  "${RUN_DIR}/after/<page>.png" \
  "${RUN_DIR}/diff/<page>-diff.png" 2>&1
```

For pages with differences, generate a highlighted diff:
```bash
compare -highlight-color red -lowlight-color none \
  "${RUN_DIR}/before/<page>.png" \
  "${RUN_DIR}/after/<page>.png" \
  "${RUN_DIR}/diff/<page>-highlighted.png"
```

### Interpreting Pixel Differences

When the diff image shows pixel differences, **view the before and after screenshots side by side** and classify each differing area. Not all data that changes between captures is acceptable — distinguish between truly dynamic values and stable data that indicates a regression.

**Capture ordering and temporal differences:** The "after" screenshots (current branch) are captured first, then the base commit's "before" screenshots are captured later. This means the "before" screenshots will always show a longer cluster uptime. Keep this in mind when interpreting differences — uptime, memory usage, and other time-dependent values will naturally differ between captures because of this ordering, not because of code changes.

**Truly dynamic (acceptable differences):**
- Timestamps and "time ago" values (e.g., "3 minutes" vs "6 minutes" uptime)
- Exact byte counts for memory/disk that fluctuate with system activity

**Stable data (differences indicate a regression):**
- Replica counts — these stabilize within seconds of cluster startup and should be similar between captures
- Capacity usage percentages — should be consistent (e.g., "0 %" vs "NaN %" is a bug, not dynamic data)
- Node status badges (LIVE, SUSPECT, DEAD) — must match the actual cluster state
- Column values showing "NaN", "undefined", empty, or missing where the baseline shows real values
- Version strings, node names, node IDs
- Values that differ by a large factor (e.g., 2x) between before and after — this suggests a data aggregation bug, not dynamic data

**Rule of thumb:** If a value is `NaN`, empty, `undefined`, or structurally different from the baseline (not just a slightly different number), treat it as a regression until proven otherwise. A value that went from a real number to `NaN` is never "dynamic data" — it means the data pipeline is broken.

### Step 11: Compare Network Requests

For each page:
```bash
diff "${RUN_DIR}/before/<page-name>/network.txt" \
     "${RUN_DIR}/after/<page-name>/network.txt"
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

### Step 12: Cleanup & Report Results

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
- For each differing area, classify as:
  - **Truly dynamic** (timestamps, fluctuating byte counts) — acceptable
  - **Stable data regression** (NaN, empty, missing, or structurally different values for replicas, capacity, status badges, etc.) — flag as a bug
- Include paths to before/after/diff screenshots for manual inspection
- Note the output directory: `${RUN_DIR}`

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

### Page shows loading state or empty data
Some pages (especially those using Redux sagas) may take longer to load data on initial render. Do not rely on a fixed `sleep` duration. After waiting, check the snapshot output for indicators of incomplete loading:
- Loading skeletons or shimmer bars
- "Nodes (0)" or "No data to display"
- Spinners or progress indicators

If found, wait another 10 seconds and retry the snapshot. Repeat up to 3 times before flagging the page as potentially broken.

## Notes

- DB Console uses hash routing (`/#/path`), so URLs include the `#` prefix
- Each invocation creates a timestamped subdirectory under `/tmp/ui-regression-test/` to preserve history across runs
- The `./dev ui watch` process runs in the background and hot-reloads on file changes
- Some pages require data in the cluster to render meaningfully (e.g., SQL Activity needs running queries)
- This skill uses `roachprod` (not `roachdev`) for all cluster operations
- `playwright-cli` creates a `.playwright-cli/` directory in the working directory — clean it up after the test
- When stashing, always use `git stash -u` to include untracked files (new files not yet committed)
- ImageMagick `compare` returns exit code 1 when images differ — this is expected behavior, not an error
- The "after" (current branch) is captured before the "before" (base commit), so the base commit screenshots will always show longer cluster uptime — this is expected and should not be flagged as a regression
