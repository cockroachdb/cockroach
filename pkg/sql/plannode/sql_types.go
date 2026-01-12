// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plannode

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

// OptColumnsSlot is used for optional column information.
type OptColumnsSlot struct {
	columns colinfo.ResultColumns
}

func (c *OptColumnsSlot) getColumns(mut bool, cols colinfo.ResultColumns) colinfo.ResultColumns {
	if c.columns != nil {
		return c.columns
	}
	if !mut {
		return cols
	}
	c.columns = make(colinfo.ResultColumns, len(cols))
	copy(c.columns, cols)
	return c.columns
}

// PlanDependencies tracks descriptor dependencies for plans (e.g., views, functions).
type PlanDependencies map[descpb.ID]PlanDependencyInfo

// PlanDependencyInfo holds information about a plan dependency.
type PlanDependencyInfo struct {
	desc  interface{}  // descriptor
	deps  descpb.TableDescriptor_Reference
	flags PlanDependencyFlags
}

// PlanDependencyFlags are flags for plan dependencies.
type PlanDependencyFlags int

// TypeDependencies tracks type dependencies.
type TypeDependencies map[descpb.ID]struct{}

// Lowercase aliases for backward compatibility within this package
type optColumnsSlot = OptColumnsSlot
type planDependencies = PlanDependencies
type planDependencyInfo = PlanDependencyInfo
type planDependencyFlags = PlanDependencyFlags
type typeDependencies = TypeDependencies

// Additional type definitions for plannode implementations

// Note: planner type is intentionally not defined here to avoid
// conflicts with method definitions. Files that need planner should
// use planbase.PlannerAccessor instead

// MutationOutputHelper tracks mutation output
type MutationOutputHelper struct {
	rowsAffectedHelper RowsAffectedOutputHelper
	rows               interface{} // *rowcontainer.RowContainer
	rowIdx             int
	currentRow         interface{} // tree.Datums
}

// Lowercase alias
type mutationOutputHelper = MutationOutputHelper

// TableDeleter handles table deletions
type TableDeleter struct{}

// Lowercase alias
type tableDeleter = TableDeleter

// ReqOrdering represents required ordering
type ReqOrdering = colinfo.ColumnOrdering

// PlanMaybePhysical wraps a plan that might be physical
type PlanMaybePhysical struct{}

// Lowercase alias
type planMaybePhysical = PlanMaybePhysical

// PlanningCtx holds distributed planning context
type PlanningCtx struct{}

// OptCatalog provides catalog information for optimizer
type OptCatalog struct{}

// Lowercase alias
type optCatalog = OptCatalog

// DropCascadeState tracks cascade state during drops
type DropCascadeState struct{}

// Lowercase alias
type dropCascadeState = DropCascadeState

// PlanComponents holds components of a plan for explain
type PlanComponents struct{}

// Lowercase alias
type planComponents = PlanComponents

// TableInserter handles table insertions
type TableInserter struct{}

// Lowercase alias
type tableInserter = TableInserter

// CheckSet represents a set of checks
type CheckSet struct{}

// Lowercase alias
type checkSet = CheckSet

// ExecutorConfig holds executor configuration
type ExecutorConfig struct{}

// DistSQLPlanner handles distributed SQL planning
type DistSQLPlanner struct{}

// AuthorizationAccessor provides authorization checking
type AuthorizationAccessor interface{}

// ShowCreateDisplayOptions holds display options for SHOW CREATE
type ShowCreateDisplayOptions struct{}

// Lowercase alias
type showCreateDisplayOptions = ShowCreateDisplayOptions

// PersistedCursorHelper helps with persisted cursors
type PersistedCursorHelper struct{}

// Lowercase alias
type persistedCursorHelper = PersistedCursorHelper

// ConnExecutor executes SQL connections
type ConnExecutor struct{}

// Lowercase alias
type connExecutor = ConnExecutor

// TableUpdater handles table updates
type TableUpdater struct{}

// Lowercase alias
type tableUpdater = TableUpdater

// TableUpserter handles table upserts
type TableUpserter struct{}

// Lowercase alias
type tableUpserter = TableUpserter

// VirtualDefEntry represents a virtual table definition entry
type VirtualDefEntry struct{}

// Lowercase alias
type virtualDefEntry = VirtualDefEntry

// RowResultWriter writes row results
type RowResultWriter struct{}

// Lowercase alias
type rowResultWriter = RowResultWriter

// TopLevelQueryStats tracks query statistics
type TopLevelQueryStats struct{}

// Lowercase alias
type topLevelQueryStats = TopLevelQueryStats

// JobExecContext provides context for job execution
type JobExecContext struct{}

// DistSQLReceiver receives distributed SQL results
type DistSQLReceiver struct{}

// PhysicalPlan represents a physical execution plan
type PhysicalPlan struct{}

// Subquery represents a subquery
type Subquery struct{}

// Lowercase alias
type subquery = Subquery

// ExecFactory creates execution components
type ExecFactory struct{}

// Lowercase alias
type execFactory = ExecFactory

// VersionUpgradeHook is a hook for version upgrades
type VersionUpgradeHook interface{}

// UpdateVersionSystemSettingHook updates version system settings
type UpdateVersionSystemSettingHook func() error

// TraceRow represents a row from trace output
type TraceRow struct{}

// Lowercase alias
type traceRow = TraceRow

// TenantTestingKnobs holds testing knobs for tenant operations
type TenantTestingKnobs struct{}

// TxnJobsCollection manages jobs within a transaction
type TxnJobsCollection struct{}

// Lowercase alias
type txnJobsCollection = TxnJobsCollection

// InternalDB provides internal database access
type InternalDB struct{}
