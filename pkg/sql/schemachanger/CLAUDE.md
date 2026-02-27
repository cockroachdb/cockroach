# SQL Schema Changer Development Guide

## Schema Changers: Legacy vs Declarative

CockroachDB has two schema change systems: the legacy schema changer and the newer declarative schema changer. Understanding their differences is crucial for DDL development work.

### Architectural Overview

**Legacy Schema Changer** (`/pkg/sql/schema_changer.go`):
- **Hard-coded state transitions**: Uses fixed sequences of descriptor mutations stored in the `mutations` slice
- **Imperative approach**: Logic written procedurally with specific code paths for each DDL operation
- **Limited state model**: Uses states like `DELETE_ONLY`, `WRITE_ONLY`, `BACKFILL_ONLY` from `DescriptorMutation`
- **Job type**: Uses `SCHEMA_CHANGE` and `TYPEDESC_SCHEMA_CHANGE` job types

**Declarative Schema Changer** (`/pkg/sql/schemachanger/`):
- **Element-based modeling**: Schema changes modeled as elements (columns, indexes, constraints) with target statuses
- **Declarative planning**: Uses rules and dependency graphs to generate execution plans
- **State stored in descriptors**: Uses `declarative_schema_changer_state` field instead of `mutations` slice
- **Job type**: Uses `NEW_SCHEMA_CHANGE` job type

### State Management Differences

**Legacy Schema Changer:**
```go
// Uses DescriptorMutation in mutations slice
type DescriptorMutation struct {
    State     DescriptorMutation_State  // DELETE_ONLY, WRITE_ONLY, etc.
    Direction DescriptorMutation_Direction // ADD, DROP
    // ... specific column/index descriptors
}
```

**Declarative Schema Changer:**
```proto
// Uses element model with target statuses
message Target {
    ElementProto element_proto = 1;
    Status target_status = 3;  // PUBLIC, ABSENT, TRANSIENT_ABSENT
}
```

### Element Model (Declarative)

Elements are defined in `/pkg/sql/schemachanger/scpb/elements.proto`:
- **Table-level**: `Table`, `TableComment`, `TableData`
- **Column elements**: `Column`, `ColumnName`, `ColumnType`, `ColumnDefaultExpression`
- **Index elements**: `PrimaryIndex`, `SecondaryIndex`, `IndexColumn`
- **Constraint elements**: `CheckConstraint`, `ForeignKeyConstraint`

Status transitions: `ABSENT` -> `DELETE_ONLY` -> `WRITE_ONLY` -> `BACKFILL_ONLY` -> `VALIDATED` -> `PUBLIC`

### Planning and Execution

**Legacy Schema Changer:**
- Planning and execution tightly coupled
- Each DDL statement has custom imperative logic
- State transitions are hard-coded sequences
- Difficult to handle complex multi-statement transactions

**Declarative Schema Changer:**
- Clear separation between planning and execution phases
- Uses dependency graphs and rules (`/pkg/sql/schemachanger/scplan/internal/rules/`)
- Can handle complex multi-statement transactions declaratively
- Better composition of multiple DDL operations

### Error Handling and Rollback

**Legacy Schema Changer:**
- Known rollback issues, especially with data loss during failed multi-operation schema changes
- Example: dropping columns alongside adding indexes can permanently lose data on rollback

**Declarative Schema Changer:**
- Designed for correct rollbacks by reversing target statuses
- Uses `OnFailOrCancel` method to revert by flipping directions (ADD becomes DROP)
- Better handling of complex failure scenarios

### Configuration

Enable declarative schema changer via:
```sql
-- Session level
SET use_declarative_schema_changer = 'on';  -- Options: 'off', 'on', 'unsafe', 'unsafe_always'

-- Cluster-wide default
SET CLUSTER SETTING sql.defaults.use_declarative_schema_changer = 'on';
```

### Development Patterns

**Adding New DDL Operations:**

*Legacy Schema Changer:*
- Add case statements in main schema changer loop
- Implement specific state transition logic
- Hard-code mutation sequences

*Declarative Schema Changer:*
- Define new elements in `elements.proto`
- Add rules for state transitions in `scplan/internal/rules/`
- Implement operations in `scop/` and execution in `scexec/`
- More modular and composable approach

**Element Query API — Avoid Deprecated Functions:**

The top-level `scpb.Find*` and `scpb.ForEach*` functions (e.g. `scpb.FindNamespace`, `scpb.ForEachColumn`) in `elements_generated.go` are deprecated. Use the fluent `Filter*().MustGetOneElement()` or `Filter*().ForEach()` methods on `ElementCollection` instead:

```go
// DEPRECATED — do not use:
_, _, ns := scpb.FindNamespace(tableElts)
scpb.ForEachColumn(tableElts, func(current Status, target TargetStatus, e *scpb.Column) { ... })

// PREFERRED — use the fluent API on ElementCollection:
ns := tableElts.FilterNamespace().MustGetOneElement()
tableElts.FilterColumn().ForEach(func(current Status, target TargetStatus, e *scpb.Column) { ... })
```

Common patterns for looking up name parts:
```go
// Table name from table ID.
tableElts := b.QueryByID(tableID)
ns := tableElts.FilterNamespace().MustGetOneElement()
scNs := b.QueryByID(ns.SchemaID).FilterNamespace().MustGetOneElement()
dbNs := b.QueryByID(ns.DatabaseID).FilterNamespace().MustGetOneElement()

// Function name from function ID.
fnElts := b.QueryByID(fnID)
fnName := fnElts.FilterFunctionName().MustGetOneElement()
fnParent := fnElts.FilterSchemaChild().MustGetOneElement()
fnScNs := b.QueryByID(fnParent.SchemaID).FilterNamespace().MustGetOneElement()
fnDbNs := b.QueryByID(fnScNs.DatabaseID).FilterNamespace().MustGetOneElement()
```

### Code Organization

**Legacy Schema Changer:**
- `/pkg/sql/schema_changer.go` - Main implementation
- `/pkg/sql/schema_changer_state.go` - State management
- `/pkg/sql/type_change.go` - Type changes

**Declarative Schema Changer:**
- `scpb/` - Protocol buffer definitions for elements and state
- `scbuild/` - Building targets from DDL ASTs
- `scplan/` - Planning state transitions and dependencies
- `scexec/` - Executing operations
- `scop/` - Operation definitions
- `screl/` - Relational model for elements

### When to Use Each

**Legacy Schema Changer:**
- Still required for some DDL operations not yet implemented in declarative changer
- May be faster for simple, single-operation schema changes
- More predictable performance due to hard-coded paths

**Declarative Schema Changer:**
- Better for complex multi-statement transactions
- More overhead in planning phase but better execution for complex scenarios
- Required for future transactional schema changes
- Better correctness guarantees and rollback handling

### Migration Path

CockroachDB is gradually migrating from legacy to declarative:
1. **Coexistence period**: Both schema changers run simultaneously
2. **Feature-by-feature migration**: DDL operations migrated individually
3. **Version compatibility**: Old jobs must continue during upgrades
4. **Extensive testing**: Test coverage in `schemachanger_test.go` and related files

### Key Benefits of Declarative Approach

1. **Correctness**: Better handling of complex scenarios and rollbacks
2. **Extensibility**: Easier to add new DDL operations
3. **Composability**: Can handle multiple DDL operations in single transaction
4. **Testing**: Better separation of concerns allows more targeted testing
5. **Future-ready**: Foundation for transactional schema changes

The declarative schema changer represents a significant architectural improvement addressing fundamental limitations of the legacy approach, particularly around correctness, extensibility, and complex transaction handling.
