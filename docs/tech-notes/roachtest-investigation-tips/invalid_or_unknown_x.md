# Investigating "Invalid", "Unknown", or "Unimplemented" Feature Errors

When roachtests fail with errors mentioning unknown settings, unrecognized syntax, or unimplemented features, these failures typically fall into three categories:

1. **Test framework bugs** - incorrect test configuration or setup
2. **Renamed settings, flags, or features** - test using outdated names
3. **Version skew** - mismatched feature availability between test and cluster versions

## Common Error Patterns

- `ERROR: unknown cluster setting 'setting.name'`
- `SQLSTATE: 42P02` (undefined object)
- `unrecognized configuration parameter`
- `unimplemented feature`
- `invalid syntax`
- `unknown flag`

## Investigation Approach

### 1. Search for the Referenced Feature

First, determine if the setting/feature exists in the current codebase:

```bash
# Search for the exact setting/feature name
git grep -n "setting_name_here"

# Search for partial matches if the exact name doesn't exist
git grep -n "partial_name"
```

### 2. Check Recent Changes

Use `git log -S` with `--since` to avoid searching the entire commit history:

```bash
# Search for recent additions/removals of the setting
git log -S "setting_name_here" --oneline --since="3 months ago"

# For broader searches, extend the timeframe
git log -S "setting_name_here" --oneline --since="6 months ago"
```

### 3. Analyze the Results

**If the setting/feature EXISTS in the codebase:**
- Was it recently added? Check if the test is running against an older version that doesn't have it yet.
- Look for version gates or feature flags that might control availability.

**If the setting/feature DOESN'T EXIST:**
- Was it recently renamed or removed? 
- Search for similar names or check recent commits that modify the relevant area.
- Look for deprecation warnings or migration notes in commit messages.

### 4. Consider Version Skew Scenarios

**Branch Differences:**
- Is the test running on a release branch while using master branch test code?
- Are there backport differences between branches?

**Mixed-Version Tests:**
- Check if this is a mixed-version or upgrade test where different nodes run different versions.
- Verify that features are available across all versions in the test.

**Version-Specific Features:**
- Some settings may only be available in enterprise builds or specific version ranges.
- Check for version compatibility requirements in the feature documentation.

## Example Investigation

For an error like `unknown cluster setting 'cloudstorage.s3.enable_client_retry_token_bucket'`:

1. **Search current codebase:**
   ```bash
   git grep "cloudstorage.s3.enable_client_retry_token_bucket"
   # (no results)
   ```

2. **Check for similar settings:**
   ```bash
   git grep "client_retry_token_bucket"
   # Found: cloudstorage.s3.client_retry_token_bucket.enabled
   ```

3. **Search recent changes:**
   ```bash
   git log -S "cloudstorage.s3.enable_client_retry_token_bucket" --oneline --since="3 months ago"
   # Shows when the old name was removed/renamed
   ```

4. **Find the fix:**
   ```bash
   git log -S "client_retry_token_bucket" --oneline --since="3 months ago"
   # Shows when the new name was introduced
   ```

## Resolution Patterns

- **Renamed setting**: Update test to use correct current name
- **Version skew**: Ensure test compatibility with target cluster version
- **Missing feature**: Check if feature needs to be enabled or if test should skip on older versions
- **Test bug**: Fix incorrect configuration in test setup

## Prevention

- Keep test configurations in sync with current setting names
- Use version guards for version-specific features
- Regularly audit test settings against current cluster setting documentation
- Consider using setting validation in test framework setup