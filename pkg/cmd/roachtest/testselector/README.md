# Test Selector

The test selector reduces CI runtime by intelligently selecting which tests to run in nightly builds, while ensuring all tests eventually get coverage.

## Overview

The test selector queries Snowflake for historical test execution data and selects tests based on:
1. Recent failures
2. Recently added tests
3. Tests that haven't run recently
4. A percentage of stable, successful tests

## Selection Criteria

### Tests Always Selected (by Snowflake SQL query)

The following tests are automatically marked as `selected=true` by the Snowflake query:

1. **Failed recently**: Tests that failed at least once in the last 30 days
2. **New tests**: Tests first run within the last 20 days (on master branch only)
3. **Not run recently**: Tests that haven't run in the last 7 days
4. **Never run**: Tests with `first_run IS NULL` (no Snowflake history)

### Tests Selected Based on Percentage (by roachtest)

After Snowflake returns results, roachtest applies additional selection:

1. **Successful tests**: Of the tests marked `selected=false` by Snowflake (stable, successful tests), select `--successful-test-select-pct` percentage (default 35%)
2. Tests are selected in the order returned by Snowflake (sorted by last run time)

### Tests Never Skipped

The following tests bypass test selection and always run:

1. **Randomized tests**: Tests with `Randomized=true`
2. **Opt-out tests**: Tests with `TestSelectionOptOutSuites` matching the current suite

## Command-Line Flags

### `--selective-tests` (default: true)
Enable or disable test selection entirely.
- `--selective-tests=true`: Apply test selection logic
- `--selective-tests=false`: Run all tests

### `--successful-test-select-pct` (default: 0.35)
Controls what percentage of successful tests to run. Applies to tests marked `selected=false` by Snowflake.

Valid range: 0.0 to 1.0

## First Run Scenario

When a new release branch is created, Snowflake has no execution history for that branch. The test selector handles this gracefully by falling back to master branch data.

### Day 1: Master Branch Fallback
1. Snowflake query for `release-24.3` returns zero results
2. Test selector automatically queries master branch data
3. Uses master's test selection results to determine which tests to run
4. Example: If master selects 480/1000 tests based on its criteria, the same 480 tests run on the new release branch

### Day 2+: Auto-Selection + Safety Valve
1. Tests that ran on Day 1 now have Snowflake history on the release branch
2. Tests that were skipped on Day 1 still have `first_run IS NULL` on the release branch
3. Snowflake auto-selects these never-run tests
4. **Safety valve**: If never-run selections exceed 50% of total tests, randomly deselect half of them
   - Prevents CI overload from too many auto-selected tests
   - Example: 520 never-run tests (52%) > 50% threshold → deselect 260 tests → 260 selected

### Day 3+: Gradual Coverage
1. Remaining never-run tests continue to cycle through selection
2. Within 2-3 days, all tests achieve coverage on the new branch
3. Normal selection criteria take over

### Safety Valve Details

The `maxNeverRunSelectedPct` constant (50%) prevents CI overload:
- Only applies to tests with `first_run IS NULL`
- Triggers when never-run selected tests exceed 50% of total tests
- Randomly deselects half of the never-run tests
- Hardcoded because:
  - Only relevant for first few days of new branches
  - Users can use `--selective-tests=false` to run all tests
  - Users can increase `--successful-test-select-pct` for more coverage

## Branch-Specific Behavior

### Master Branch
- `firstRunOn` = 20 days: Tests newer than 20 days are auto-selected
- Normal test selection criteria apply

### Release Branches
- `firstRunOn` = 0 days: Disables "new test" criterion
- Prevents marking all tests as "new" when branch is created
- Falls back to master branch data if no Snowflake history exists

The branch is detected via `TC_BUILD_BRANCH` environment variable, defaulting to "master".

## Example Scenarios

### Scenario 1: Normal Nightly Run (Master Branch)
- Total tests: 1000
- Snowflake auto-selects: 200 (failures + new + not run recently)
- Successful tests: 800 (marked `selected=false`)
- Additional selection: 800 × 35% = 280 successful tests
- **Total selected: 480 tests** (200 + 280)

### Scenario 2: New Release Branch Day 1
- Total tests: 1000
- Snowflake results for `release-24.3`: 0 (no history)
- Falls back to master branch data
- Uses master's selection: 480 tests (same as master)
- **Total selected: 480 tests**

### Scenario 3: New Release Branch Day 2
- Total tests: 1000
- Tests with history (ran Day 1 on release branch): 480
- Tests never run on release branch (Day 1 skipped): 520
- Snowflake auto-selects: 520 never-run tests (52%)
- Safety valve triggers: 52% > 50%
- Deselect: 520 / 2 = 260 tests
- **Total selected: 260 never-run + normal selection from tests with history**

### Scenario 4: Test Opted Out of Nightly
```go
registry.TestSpec{
    Name: "acceptance/version-upgrade",
    TestSelectionOptOutSuites: registry.Suites(registry.Nightly),
    // ... other fields
}
```
This test always runs in Nightly suite, bypassing selection logic.

## Implementation Details

### Code Structure

**pkg/cmd/roachtest/testselector/selector.go**:
- `CategoriseTests()`: Queries Snowflake; falls back to master if no results
- `querySnowflake()`: Helper to query Snowflake with given parameters
- `applySafetyValve()`: Caps never-run test selection at 50%

**pkg/cmd/roachtest/main.go**:
- `updateSpecForSelectiveTests()`: Applies percentage selection to successful tests
- `testShouldBeSkipped()`: Determines if a test should be skipped

### Snowflake Query

The query is defined in `snowflake_query.sql`. It returns:
- `name`: Test name
- `selected`: 'yes' or 'no' based on auto-selection criteria
- `avg_duration`: Average test duration in milliseconds
- `last_failure_is_preempt`: Whether last failure was infrastructure-related
- `first_run`: First execution timestamp (NULL if never run)

### Test Spec Fields

**Set by test selector**:
- `Skip`: Set to "test selector" for skipped tests
- `SkipDetails`: Explanation of why test was skipped
- `Stats`: Average duration and preemption status

**Read by test selector**:
- `Randomized`: If true, test always runs
- `TestSelectionOptOutSuites`: Suites where test always runs

## Debugging

### Verbose Logging

Run with verbose mode to see selection details:
```bash
roachtest run --selective-tests --successful-test-select-pct=0.5 -v
```

Logs will show:
- `X selected out of Y successful tests` (percentage selection)
- `X out of Y tests selected for the run` (final count)

### Common Issues

**All tests skipped**:
- Check `--selective-tests=false` to disable selection
- Verify Snowflake credentials are set (`SNOWFLAKE_USER`, `SNOWFLAKE_PVT_KEY`)

**Too many tests running**:
- Lower `--successful-test-select-pct` (e.g., 0.2 for 20%)
- On new branches, behavior matches master for first day

**Specific test always skipped**:
- Check if test is randomized or opted out
- Verify test appears in Snowflake results
- Test may be very stable and not selected in master branch data

## References

- Main implementation: `pkg/cmd/roachtest/testselector/selector.go`
- Integration: `pkg/cmd/roachtest/main.go:updateSpecForSelectiveTests`
- Tests: `pkg/cmd/roachtest/main_test.go:Test_updateSpecForSelectiveTests`
