# CockroachDB Upgrade Migration Construction

You are helping to construct an upgrade migration in CockroachDB. Follow this comprehensive guide to ensure all components are properly implemented and tested.

## Overview

A CockroachDB upgrade migration involves multiple coordinated changes across several files to ensure consistency between the migration logic, bootstrap schemas, and version tracking. Use this when you need to modify system tables, add new features requiring schema changes, or update table storage parameters.

## Version Numbering System

CockroachDB uses a structured version numbering system:
- **Major.Minor.Patch-Internal** format (e.g., 25.3.0-6)
- **Internal versions must be even numbers** (2, 4, 6, 8...)
- Each release starts with a `V{Major}_{Minor}_Start` version
- Development versions use `{Major}.{Minor-1}.Internal` format during development

**Example for v25.4 development:**
- `V25_4_Start: {Major: 25, Minor: 3, Internal: 2}`
- `V25_4_YourFeature: {Major: 25, Minor: 3, Internal: 4}` (next even number)
- `V25_4_AnotherFeature: {Major: 25, Minor: 3, Internal: 6}` (next even number)

## Step-by-Step Process

### 1. Update System Table Schemas (if applicable)

**File:** `pkg/sql/catalog/systemschema/system.go`

If your migration modifies system table schemas, update the corresponding `*TableSchema` constants first:
```go
YourTableSchema = `
CREATE TABLE system.your_table (
	-- existing columns
	your_new_column YOUR_TYPE,
	-- indexes, constraints, etc.
) WITH (your_new_parameters = values)
`
```

**Common Schema Changes:**
- Adding columns: Update column list
- Adding indexes: Update index definitions
- Adding table parameters: Update `WITH` clause

### 2. Update Hardcoded Table Descriptors (if applicable)

**File:** `pkg/sql/catalog/systemschema/system.go`

If you modified table schemas, update the hardcoded table descriptors to match:

#### For new columns:
```go
YourTable = makeSystemTable(
	YourTableSchema,
	systemTable(
		// existing columns...
		{Name: "your_new_column", ID: N, Type: types.YourType, Nullable: true},
		// indexes, etc.
	),
	// modification function if needed
),
```

#### For table parameters:
```go
func(tbl *descpb.TableDescriptor) {
	// Existing modifications...
	
	// Add your new settings
	tbl.YourSetting = &catpb.YourSettingType{
		YourParameter: &[]yourType{yourValue}[0],
	}
},
```

### 3. Add a New Cluster Version

**File:** `pkg/clusterversion/cockroach_versions.go`

1. Add a new version constant in the version enum **before** the `// Step (1)` comment:
```go
// V25_4_YourFeatureName describes what your migration does
// and why it's needed for clarity.
V25_4_YourFeatureName
```

2. Add the corresponding version mapping **before** the `// Step (2)` comment:
```go
V25_4_YourFeatureName: {Major: 25, Minor: 3, Internal: 8}, // Use next even number
```

**Version Gate Guidelines:**
- Always use the next available even Internal number
- Prefix new version gates with their release version (e.g., V25_4_TenantNames)

### 4. Update Bootstrap Version

**File:** `pkg/sql/catalog/systemschema/system.go`

Update the `SystemDatabaseSchemaBootstrapVersion` to point to your new cluster version:
```go
var SystemDatabaseSchemaBootstrapVersion = clusterversion.V25_4_YourFeatureName.Version()
```

**Important:** This ensures new clusters start with your changes and existing clusters migrate to them.

### 5. Create the Migration File

Always include the version number at the start of the file using the
`v{Major}_{Minor}_{feature_name}.go` pattern.

For example: `pkg/upgrade/upgrades/v25_4_your_feature_name.go`

Choose the appropriate pattern based on your migration type:

#### Pattern A: Simple SQL Execution (table parameters, simple DDL)
```go
// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

const (
	// Define your SQL statements as constants
	yourMigrationSQL = `
	ALTER TABLE system.your_table
	SET (your_parameter = your_value)
	`
)

func yourMigrationFunction(
	ctx context.Context, version clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	// Execute your migration SQL
	_, err := deps.InternalExecutor.Exec(
		ctx, "descriptive-operation-name",
		nil, yourMigrationSQL,
	)
	return err
}
```

#### Pattern B: Table Schema Changes (columns, indexes)
```go
// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

const (
	addYourColumn = `
ALTER TABLE system.your_table
	ADD COLUMN IF NOT EXISTS your_column_name YOUR_TYPE
`

	addYourIndex = `
CREATE INDEX IF NOT EXISTS your_index_name ON system.your_table (your_column)`
)

func yourMigrationFunction(
	ctx context.Context, version clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	for _, op := range []operation{
		{
			name:           "add-your-column-to-table",
			schemaList:     []string{"your_column_name"},
			query:          addYourColumn,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "add-your-index-to-table", 
			schemaList:     []string{"your_index_name"},
			query:          addYourIndex,
			schemaExistsFn: hasIndex,
		},
	} {
		if err := migrateTable(ctx, version, deps, op, keys.YourTableID,
			systemschema.YourTable); err != nil {
			return err
		}
	}
	return nil
}
```

#### Pattern C: Job Creation
```go
// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

func yourMigrationFunction(
	ctx context.Context, version clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	// Check for testing knobs
	if d.TestingKnobs != nil && d.TestingKnobs.SkipYourJobBootstrap {
		return nil
	}

	return d.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		jr := jobs.Record{
			JobID:         jobs.YourJobID, // Define in jobs package
			Description:   "description of your background job",
			Details:       jobspb.YourJobDetails{},
			Progress:      jobspb.YourJobProgress{},
			CreatedBy:     &jobs.CreatedByInfo{Name: username.NodeUser, ID: username.NodeUserID},
			Username:      username.NodeUserName(),
			NonCancelable: true,
		}
		return d.JobRegistry.CreateIfNotExistAdoptableJobWithTxn(ctx, jr, txn)
	})
}
```

### 6. Register the Migration

**File:** `pkg/upgrade/upgrades/upgrades.go`

Add your migration to the upgrades slice **in the correct order** (before the final comment):

```go
upgrade.NewTenantUpgrade(
	"descriptive name of what the migration does",
	clusterversion.V25_4_YourFeatureName.Version(),
	upgrade.NoPrecondition, // or add preconditions if needed
	yourMigrationFunction,
	upgrade.RestoreActionNotRequired("explain why restore doesn't need special handling"),
),
```

**Migration Types:**
- `NewTenantUpgrade` - Runs on each tenant (most common)
- `NewSystemUpgrade` - Runs only on system tenant
- `NewPermanentTenantUpgrade` - For permanent changes that must survive tenant restore

### 7. Add Migration File to Build

Run bazel generation to add the new files to BUILD files:
```bash
./dev gen bazel
```

### 8. Generate Documentation Updates

Run the documentation generation to update version settings:
```bash
./dev gen docs
```

This updates:
- `docs/generated/settings/settings-for-tenants.txt`  
- `docs/generated/settings/settings.html`

### 9. Test Your Migration

Run the relevant tests to ensure your changes work:

```bash
# Test bootstrap version consistency
./dev test pkg/upgrade/upgrades -f=TestSystemDatabaseSchemaBootstrapVersionBumped -v

# Test migration execution
./dev test pkg/server -f=TestUpgradeHappensAfterMigrations -v

# Test system table consistency (if you modified schemas)
./dev test pkg/sql/tests -f="TestSystemTableLiterals/your_table" -v

# Test your specific migration (if you have tests)
./dev test pkg/upgrade/upgrades -f=TestYourMigration -v
```

### 10. Regenerate Datadriven Tests (if applicable)

When migrations modify system schemas or bootstrap behavior, datadriven tests may need regeneration to reflect the new expected outputs:

#### Identify Affected Tests
System schema changes typically affect these datadriven tests:
- `pkg/sql/catalog/bootstrap/testdata/testdata` - Bootstrap system schema validation
- `pkg/sql/catalog/systemschema_test/testdata/bootstrap_system` - System table structure validation

#### Regenerate Test Data
1. **Automatic regeneration** (for most tests):
   ```bash
   ./dev test pkg/sql/catalog/systemschema_test -f TestValidateSystemSchemaAfterBootStrap --rewrite
   ```

2. **Manual hash updates** (for bootstrap tests):
   ```bash
   # First run the test to see the new hash values
   ./dev test pkg/sql/catalog/bootstrap -f TestInitialValuesToString -v
   
   # Update the hash values in the testdata file manually:
   # - Look for "system hash=" and "tenant hash=" lines
   # - Replace with new hash values from test output
   # - The test error message provides the expected hash values
   ```

#### Example Hash Update
If you see test failures like:
```
Unexpected hash value e16c5d7a114749c0afbacd670238f447cdfe53a60ad6049d77ca7f4b22af344d for system.
```

Update the corresponding line in `pkg/sql/catalog/bootstrap/testdata/testdata`:
```diff
- system hash=1ce691fd2c1a70250d1c34fc85aae64faaa183734a6b200c63599fe36b01a99b
+ system hash=e16c5d7a114749c0afbacd670238f447cdfe53a60ad6049d77ca7f4b22af344d
```

#### Verify Test Regeneration
```bash
# Ensure both tests pass after regeneration
./dev test pkg/sql/catalog/bootstrap -f TestInitialValuesToString
./dev test pkg/sql/catalog/systemschema_test -f TestValidateSystemSchemaAfterBootStrap
```

**When to regenerate:** Always regenerate datadriven tests when your migration changes:
- System table schemas or structure
- Table storage parameters
- Bootstrap system database content
- System table descriptors

## Migration Best Practices

**Prefix new version gates** by their release version (e.g., V25_4_FeatureName)

### SQL Patterns
1. **Always use `IF NOT EXISTS`** for additive changes:
   ```sql
   ADD COLUMN IF NOT EXISTS your_column TYPE
   CREATE INDEX IF NOT EXISTS your_index ON table (column)
   ```

2. **Use descriptive operation names** in `InternalExecutor.Exec()`:
   ```go
   deps.InternalExecutor.Exec(ctx, "add-column-to-users", nil, sql)
   ```

### Error Handling
- Return errors immediately, don't log and continue
- Use idempotent operations that can be safely retried
- Consider partial failure scenarios

### Testing
- Write tests in `*_test.go` files alongside your migration
- Test both successful migration and edge cases
- Use the existing test patterns from other migrations

## Common Migration Types

### Table Storage Parameters
```go
// Simple parameter updates
const updateTableParams = `
ALTER TABLE system.your_table
SET (parameter1 = value1, parameter2 = value2)
`
```

### New System Columns
```go
// Adding columns
const addColumn = `
ALTER TABLE system.your_table
ADD COLUMN IF NOT EXISTS your_column JSONB
```

### Background Jobs
```go
// Creating system jobs
jr := jobs.Record{
	JobID:         jobs.YourJobID,
	Description:   "your job description",
	Details:       jobspb.YourDetails{},
	// ... other fields
}
```

## Troubleshooting

- **Bootstrap version test failures:** Update `SystemDatabaseSchemaBootstrapVersion`
- **System table literal test failures:** Ensure hardcoded descriptors match CREATE TABLE statements exactly
- **Migration test failures:** Check that your cluster version is properly registered and uses the next even Internal number
- **Build failures:** Ensure migration file is added to `BUILD.bazel`
- **Version conflicts:** Check that your Internal number doesn't conflict with existing versions

## Example Files to Reference

- **Column addition:** `v25_3_add_users_last_login_time_column.go`
- **Job creation:** `v25_3_add_hot_range_logger_job.go`
- **Complex schema changes:** `v25_3_add_event_log_column_and_index.go`

Use these as templates for your specific migration needs.

## Execution Checklist

When working on an upgrade migration, follow this checklist:

- [ ] Update system table schemas (if applicable)
- [ ] Update hardcoded table descriptors (if applicable)
- [ ] Determine the next available even Internal version number
- [ ] Add cluster version constant and mapping with proper V{Major}_{Minor}_ prefix
- [ ] Update SystemDatabaseSchemaBootstrapVersion
- [ ] Create migration file using appropriate pattern
- [ ] Register migration in upgrades.go
- [ ] Run `./dev gen bazel` to update build files
- [ ] Run `./dev gen docs` to update documentation
- [ ] Test migration with all relevant test suites
- [ ] Regenerate datadriven tests (if system schema changed)
- [ ] Verify all tests pass and migration works correctly

Always refer to existing migrations as examples and maintain consistency with established patterns.
