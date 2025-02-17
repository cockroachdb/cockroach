// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/querycache"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// PreparedStatementOrigin is an enum representing the source of where
// the prepare statement was made.
type PreparedStatementOrigin int

const (
	// PreparedStatementOriginWire signifies the prepared statement was made
	// over the wire.
	PreparedStatementOriginWire PreparedStatementOrigin = iota + 1
	// PreparedStatementOriginSQL signifies the prepared statement was made
	// over a parsed SQL query.
	PreparedStatementOriginSQL
	// PreparedStatementOriginSessionMigration signifies that the prepared
	// statement came from a call to crdb_internal.deserialize_session.
	PreparedStatementOriginSessionMigration
)

// PreparedStatement is a SQL statement that has been parsed and the types
// of arguments and results have been determined.
//
// Note that PreparedStatements maintain a reference counter internally.
// References need to be registered with incRef() and de-registered with
// decRef().
type PreparedStatement struct {
	querycache.PrepareMetadata

	// BaseMemo is the memoized data structure constructed by the cost-based
	// optimizer during prepare of a SQL statement.
	BaseMemo *memo.Memo

	// GenericMemo, if present, is a fully-optimized memo that can be executed
	// as-is.
	GenericMemo *memo.Memo

	// IdealGenericPlan is true if GenericMemo is guaranteed to be optimal
	// across all executions of the prepared statement. Ideal generic plans are
	// generated when the statement has no placeholders nor fold-able stable
	// expressions, or when the placeholder fast-path is utilized.
	IdealGenericPlan bool

	// Costs tracks the costs of previously optimized custom and generic plans.
	Costs planCosts

	// refCount keeps track of the number of references to this PreparedStatement.
	// New references are registered through incRef().
	// Once refCount hits 0 (through calls to decRef()), the following memAcc is
	// closed.
	// Most references are being held by portals created from this prepared
	// statement.
	refCount int
	memAcc   mon.BoundAccount

	// createdAt is the timestamp this prepare statement was made at.
	// Used for reporting on `pg_prepared_statements`.
	createdAt time.Time
	// origin is the protocol in which this prepare statement was created.
	// Used for reporting on `pg_prepared_statements`.
	origin PreparedStatementOrigin
}

// MemoryEstimate returns a rough estimate of the PreparedStatement's memory
// usage, in bytes.
func (p *PreparedStatement) MemoryEstimate() int64 {
	// Account for the memory used by this prepared statement:
	//   1. Size of the prepare metadata.
	//   2. Size of the prepared memo, if using the cost-based optimizer.
	size := p.PrepareMetadata.MemoryEstimate()
	if p.BaseMemo != nil {
		size += p.BaseMemo.MemoryEstimate()
	}
	if p.GenericMemo != nil {
		size += p.GenericMemo.MemoryEstimate()
	}
	return size
}

func (p *PreparedStatement) decRef(ctx context.Context) {
	if p.refCount <= 0 {
		log.Fatal(ctx, "corrupt PreparedStatement refcount")
	}
	p.refCount--
	if p.refCount == 0 {
		p.memAcc.Close(ctx)
	}
}

func (p *PreparedStatement) incRef(ctx context.Context) {
	if p.refCount <= 0 {
		log.Fatal(ctx, "corrupt PreparedStatement refcount")
	}
	p.refCount++
}

const (
	// CustomPlanThreshold is the maximum number of custom plan costs tracked by
	// planCosts. It is also the number of custom plans executed when
	// plan_cache_mode=auto before attempting to generate a generic plan.
	CustomPlanThreshold = 5
)

// planCosts tracks costs of generic and custom plans.
type planCosts struct {
	generic memo.Cost
	custom  struct {
		nextIdx int
		length  int
		costs   [CustomPlanThreshold]memo.Cost
	}
}

// Generic returns the cost of the generic plan.
func (p *planCosts) Generic() memo.Cost {
	return p.generic
}

// SetGeneric sets the cost of the generic plan.
func (p *planCosts) SetGeneric(cost memo.Cost) {
	p.generic = cost
}

// AddCustom adds a custom plan cost to the planCosts, evicting the oldest cost
// if necessary.
func (p *planCosts) AddCustom(cost memo.Cost) {
	p.custom.costs[p.custom.nextIdx] = cost
	p.custom.nextIdx++
	if p.custom.nextIdx >= CustomPlanThreshold {
		p.custom.nextIdx = 0
	}
	if p.custom.length < CustomPlanThreshold {
		p.custom.length++
	}
}

// NumCustom returns the number of custom plan costs in the planCosts.
func (p *planCosts) NumCustom() int {
	return p.custom.length
}

// AvgCustom returns the average cost of all the custom plan costs in planCosts.
// If there are no custom plan costs, it returns 0.
//
// TODO(mgartner): Figure out how this should incorporate cost flags. Some of
// them, like UnboundedCardinality, are only set if session settings are set.
// When those session settings change, do we need to clear and recompute the
// average cost of custom plans?
func (p *planCosts) AvgCustom() memo.Cost {
	if p.custom.length == 0 {
		return memo.Cost{C: 0}
	}
	var sum memo.Cost
	for i := 0; i < p.custom.length; i++ {
		sum.Add(p.custom.costs[i])
	}
	sum.C /= float64(p.custom.length)
	return sum
}

// ClearGeneric clears the generic cost.
func (p *planCosts) ClearGeneric() {
	p.generic = memo.Cost{C: 0}
}

// ClearCustom clears any previously added custom costs.
func (p *planCosts) ClearCustom() {
	p.custom.nextIdx = 0
	p.custom.length = 0
}

// preparedStatementsAccessor gives a planner access to a session's collection
// of prepared statements.
type preparedStatementsAccessor interface {
	// List returns all prepared statements as a map keyed by name.
	// The map itself is a copy of the prepared statements.
	List() map[string]*PreparedStatement
	// Get returns the prepared statement with the given name. If touchLRU is
	// true, this counts as an access for LRU bookkeeping. The returned bool is
	// false if a statement with the given name doesn't exist.
	Get(name string, touchLRU bool) (*PreparedStatement, bool)
	// Delete removes the PreparedStatement with the provided name from the
	// collection. If a portal exists for that statement, it is also removed.
	// The method returns true if statement with that name was found and removed,
	// false otherwise.
	Delete(ctx context.Context, name string) bool
	// DeleteAll removes all prepared statements and portals from the collection.
	DeleteAll(ctx context.Context)
}

// emptyPreparedStatements is the default impl used by the planner when the
// connExecutor is not available.
type emptyPreparedStatements struct{}

var _ preparedStatementsAccessor = emptyPreparedStatements{}

func (e emptyPreparedStatements) List() map[string]*PreparedStatement {
	return nil
}

func (e emptyPreparedStatements) Get(string, bool) (*PreparedStatement, bool) {
	return nil, false
}

func (e emptyPreparedStatements) Delete(context.Context, string) bool {
	return false
}

func (e emptyPreparedStatements) DeleteAll(context.Context) {
}

// PortalPausablity mark if the portal is pausable and the reason. This is
// needed to give the correct error for usage of multiple active portals.
type PortalPausablity int64

const (
	// PortalPausabilityDisabled is the default status of a portal when
	// the session variable multiple_active_portals_enabled is false.
	PortalPausabilityDisabled PortalPausablity = iota
	// PausablePortal is set when the session variable multiple_active_portals_enabled
	// is set to true and the underlying statement is a read-only SELECT query
	// with no sub-queries or post-queries.
	PausablePortal
	// NotPausablePortalForUnsupportedStmt is used when the cluster setting
	// the session variable multiple_active_portals_enabled is set to true, while
	// we don't support underlying statement.
	NotPausablePortalForUnsupportedStmt
)

// PreparedPortal is a PreparedStatement that has been bound with query
// arguments.
type PreparedPortal struct {
	Name  string
	Stmt  *PreparedStatement
	Qargs tree.QueryArguments

	// OutFormats contains the requested formats for the output columns.
	OutFormats []pgwirebase.FormatCode

	// exhausted tracks whether this portal has already been fully exhausted,
	// meaning that any additional attempts to execute it should return no
	// rows.
	exhausted bool

	// portalPausablity is used to log the correct error message when user pause
	// a portal.
	// See comments for PortalPausablity for more details.
	portalPausablity PortalPausablity

	// pauseInfo is the saved info needed for a pausable portal.
	pauseInfo *portalPauseInfo
}

// makePreparedPortal creates a new PreparedPortal.
//
// accountForCopy() doesn't need to be called on the prepared statement.
func (ex *connExecutor) makePreparedPortal(
	ctx context.Context,
	name string,
	stmt *PreparedStatement,
	qargs tree.QueryArguments,
	outFormats []pgwirebase.FormatCode,
) (PreparedPortal, error) {
	portal := PreparedPortal{
		Name:       name,
		Stmt:       stmt,
		Qargs:      qargs,
		OutFormats: outFormats,
	}

	if ex.sessionData().MultipleActivePortalsEnabled && ex.executorType != executorTypeInternal {
		telemetry.Inc(sqltelemetry.StmtsTriedWithPausablePortals)
		// We will check whether the statement itself is pausable (i.e., that it
		// doesn't contain DDL or mutations) when we build the plan.
		portal.pauseInfo = &portalPauseInfo{}
		portal.pauseInfo.dispatchToExecutionEngine.queryStats = &topLevelQueryStats{}
		portal.portalPausablity = PausablePortal
	}
	return portal, portal.accountForCopy(ctx, &ex.extraTxnState.prepStmtsNamespaceMemAcc, name)
}

// accountForCopy updates the state to account for the copy of the
// PreparedPortal (p is the copy).
func (p *PreparedPortal) accountForCopy(
	ctx context.Context, prepStmtsNamespaceMemAcc *mon.BoundAccount, portalName string,
) error {
	if err := prepStmtsNamespaceMemAcc.Grow(ctx, p.size(portalName)); err != nil {
		return err
	}
	// Only increment the reference if we're going to keep it.
	p.Stmt.incRef(ctx)
	return nil
}

// close closes this portal.
func (p *PreparedPortal) close(
	ctx context.Context, prepStmtsNamespaceMemAcc *mon.BoundAccount, portalName string,
) {
	prepStmtsNamespaceMemAcc.Shrink(ctx, p.size(portalName))
	p.Stmt.decRef(ctx)
	if p.pauseInfo != nil {
		p.pauseInfo.cleanupAll(ctx)
		p.pauseInfo = nil
	}
}

func (p *PreparedPortal) size(portalName string) int64 {
	return int64(uintptr(len(portalName)) + unsafe.Sizeof(p))
}

// isPausable checks if a portal is pausable.
func (p *PreparedPortal) isPausable() bool {
	return p != nil && p.pauseInfo != nil
}

// cleanupFuncStack stores cleanup functions for a portal. The clean-up
// functions are added during the first-time execution of a portal. When the
// first-time execution is finished, we mark isComplete to true.
type cleanupFuncStack struct {
	stack      []func(context.Context)
	isComplete bool
}

func (n *cleanupFuncStack) appendFunc(f func(context.Context)) {
	n.stack = append(n.stack, f)
}

func (n *cleanupFuncStack) run(ctx context.Context) {
	for i := 0; i < len(n.stack); i++ {
		n.stack[i](ctx)
	}
	*n = cleanupFuncStack{}
}

// instrumentationHelperWrapper wraps the instrumentation helper.
// We need to maintain it for a paused portal.
type instrumentationHelperWrapper struct {
	ctx context.Context
	ih  instrumentationHelper
}

// portalPauseInfo stores info that enables the pause of a portal. After pausing
// the portal, execute any other statement, and come back to re-execute it or
// close it.
type portalPauseInfo struct {
	// curRes is the result channel of the current (or last, if the portal is
	// closing) portal execution. It is assumed to be a pgwire.limitedCommandResult.
	curRes RestrictedCommandResult
	// The following 4 structs store information to persist for a portal's
	// execution, as well as cleanup functions to be called when the portal is
	// closed. These structs correspond to a function call chain for a query's
	// execution:
	//   - connExecutor.execPortal()
	//   - connExecutor.execStmtInOpenStateCleanup()
	//   - connExecutor.dispatchToExecutionEngine()
	//   - DistSQLPlanner.Run()
	//
	// Each struct has two main parts:
	// 1. Fields that need to be retained for a resumption of a portal's execution.
	// 2. A cleanup function stack that will be called when the portal's execution
	//    has to be terminated or when the portal is to be closed. Each stack is
	//    defined as a closure in its corresponding function.
	//
	// When closing a portal, we need to follow the reverse order of its execution,
	// which means running the cleanup functions of the four structs in the
	// following order:
	//   - exhaustPortal.cleanup
	//   - execStmtInOpenState.cleanup
	//   - dispatchToExecutionEngine.cleanup
	//   - resumableFlow.cleanup
	//
	// If an error occurs in any of these functions, we run the cleanup functions of
	// this layer and its children layers, and propagate the error to the parent
	// layer. For example, if an error occurs in execStmtInOpenStateCleanup(), we
	// run the cleanup functions in the following order:
	//   - execStmtInOpenState.cleanup
	//   - dispatchToExecutionEngine.cleanup
	//   - resumableFlow.cleanup
	//
	// When exiting connExecutor.execStmtInOpenState(), we finally run the
	// exhaustPortal.cleanup function in connExecutor.execPortal().
	exhaustPortal struct {
		cleanup cleanupFuncStack
	}

	// TODO(sql-session): replace certain fields here with planner.
	// https://github.com/cockroachdb/cockroach/issues/99625
	execStmtInOpenState struct {
		spCtx context.Context
		// queryID stores the id of the query that this portal bound to. When we re-execute
		// an existing portal, we should use the same query id.
		queryID clusterunique.ID
		// ihWrapper stores the instrumentation helper that should be reused for
		// each execution of the portal.
		ihWrapper *instrumentationHelperWrapper
		// cancelQueryFunc will be called to cancel the context of the query when
		// the portal is closed.
		cancelQueryFunc context.CancelFunc
		// cancelQueryCtx is the context to be canceled when closing the portal.
		cancelQueryCtx context.Context
		// retPayload is needed for the cleanup steps as we will have to check the
		// latest encountered error, so this field should be updated for each execution.
		retPayload fsm.EventPayload
		// retErr is needed for the cleanup steps as we will have to check the latest
		// encountered error, so this field should be updated for each execution.
		retErr  error
		cleanup cleanupFuncStack
	}

	// TODO(sql-session): replace certain fields here with planner.
	// https://github.com/cockroachdb/cockroach/issues/99625
	dispatchToExecutionEngine struct {
		// rowsAffected is the number of rows queried with this portal. It is
		// accumulated from each portal execution, and will be logged when this portal
		// is closed.
		rowsAffected int
		// stmtFingerprintID is the fingerprint id of the underlying statement.
		stmtFingerprintID appstatspb.StmtFingerprintID
		// planTop collects the properties of the current plan being prepared.
		// We reuse it when re-executing the portal.
		// TODO(yuzefovich): consider refactoring distSQLFlowInfos from planTop to
		// avoid storing the planTop here.
		planTop planTop
		// queryStats stores statistics on query execution. It is incremented for
		// each execution of the portal.
		queryStats *topLevelQueryStats
		cleanup    cleanupFuncStack
	}

	resumableFlow struct {
		// We need to store the flow for a portal so that when re-executing it, we
		// continue from the previous execution. It lives along with the portal, and
		// will be cleaned-up when the portal is closed.
		flow flowinfra.Flow
		// outputTypes are the types of the result columns produced by the physical plan.
		// We need this as when re-executing the portal, we are reusing the flow
		// with the new receiver, but not re-generating the physical plan.
		outputTypes []*types.T
		cleanup     cleanupFuncStack
	}
}

// cleanupAll is to run all the cleanup layers.
func (pm *portalPauseInfo) cleanupAll(ctx context.Context) {
	pm.resumableFlow.cleanup.run(ctx)
	pm.dispatchToExecutionEngine.cleanup.run(ctx)
	pm.execStmtInOpenState.cleanup.run(ctx)
	pm.exhaustPortal.cleanup.run(ctx)
}

// isQueryIDSet returns true if the query id for the portal is set.
func (pm *portalPauseInfo) isQueryIDSet() bool {
	return !pm.execStmtInOpenState.queryID.Equal(clusterunique.ID{}.Uint128)
}
