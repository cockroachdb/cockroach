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
Controls what percentage of successful tests to run. Has dual purposes:

**When Snowflake has history** (normal operation):
- Selects this percentage of tests marked `selected=false` by Snowflake
- Example: If Snowflake returns 100 successful tests, 35 are selected

**When Snowflake has no history** (first run scenario):
- Randomly selects this percentage of all available tests
- Example: New release branch with 200 tests → 70 randomly selected

Valid range: 0.0 to 1.0

## First Run Scenario

When a new release branch is created, Snowflake has no execution history. The test selector handles this gracefully:

### Day 1: Initial Random Selection
1. Snowflake query returns zero results
2. `--successful-test-select-pct` percentage of all tests are randomly selected
3. Example: 200 tests × 35% = 70 tests selected, 130 skipped

### Day 2: Auto-Selection + Safety Valve
1. Tests that ran on Day 1 now have Snowflake history
2. Tests that were skipped on Day 1 still have `first_run IS NULL`
3. Snowflake auto-selects these never-run tests
4. **Safety valve**: If never-run selections exceed 50% of total tests, randomly deselect half of them
   - Prevents CI overload from too many auto-selected tests
   - Example: 130 never-run tests (65%) > 50% threshold → deselect 65 tests → 65 selected

### Day 3+: Gradual Coverage
1. Remaining never-run tests continue to cycle through selection
2. Within 2-3 days, all tests achieve coverage
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
- Tests without history are treated as "new" and may be selected

### Release Branches
- `firstRunOn` = 0 days: Disables "new test" criterion
- Prevents marking all tests as "new" when branch is created
- Falls back to first-run scenario logic if no Snowflake history

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
- Snowflake results: 0 (no history)
- Random selection: 1000 × 35% = 350 tests
- **Total selected: 350 tests**

### Scenario 3: New Release Branch Day 2
- Total tests: 1000
- Tests with history (ran Day 1): 350
- Tests never run (Day 1 skipped): 650
- Snowflake auto-selects: 650 never-run tests (65%)
- Safety valve triggers: 65% > 50%
- Deselect: 650 / 2 = 325 tests
- **Total selected: 325 never-run + normal selection from Day 1 tests**

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
- `CategoriseTests()`: Queries Snowflake and applies first-run logic
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
- On new branches, wait 2-3 days for coverage to normalize

**Specific test always skipped**:
- Check if test is randomized or opted out
- Verify test appears in Snowflake results
- Test may be very stable and unlucky in random selection

## References

- Main implementation: `pkg/cmd/roachtest/testselector/selector.go`
- Integration: `pkg/cmd/roachtest/main.go:updateSpecForSelectiveTests`
- Tests: `pkg/cmd/roachtest/main_test.go:Test_updateSpecForSelectiveTests`