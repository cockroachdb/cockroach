// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/querycache"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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

	// Memo is the memoized data structure constructed by the cost-based optimizer
	// during prepare of a SQL statement. It can significantly speed up execution
	// if it is used by the optimizer as a starting point.
	Memo *memo.Memo

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
	if p.Memo != nil {
		size += p.Memo.MemoryEstimate()
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

// PortalPausablity mark if the portal is pausable and the reason. This is
// needed to give the correct error for usage of multiple active portals.
type PortalPausablity int64

const (
	// PortalPausabilityDisabled is the default status of a portal when
	// sql.pgwire.multiple_active_portals.enabled is false.
	PortalPausabilityDisabled PortalPausablity = iota
	// PausablePortal is set when sql.pgwire.multiple_active_portals.enabled is
	// set to true and the underlying statement is a read-only SELECT query with
	// no sub-queries or post-queries.
	PausablePortal
	// NotPausablePortalForUnsupportedStmt is used when the cluster setting
	// sql.pgwire.multiple_active_portals.enabled is set to true, while we don't
	// support underlying statement.
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

	if EnableMultipleActivePortals.Get(&ex.server.cfg.Settings.SV) {
		portal.pauseInfo = &portalPauseInfo{}
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
		p.pauseInfo.cleanupAll()
		p.pauseInfo = nil
	}
}

func (p *PreparedPortal) size(portalName string) int64 {
	return int64(uintptr(len(portalName)) + unsafe.Sizeof(p))
}

func (p *PreparedPortal) isPausable() bool {
	return p.pauseInfo != nil
}

// cleanupFuncStack stores cleanup functions for a portal. The clean-up
// functions are added during the first-time execution of a portal. When the
// first-time execution is finished, we mark isComplete to true.
type cleanupFuncStack struct {
	stack      []namedFunc
	isComplete bool
}

func (n *cleanupFuncStack) appendFunc(f namedFunc) {
	n.stack = append(n.stack, f)
}

func (n *cleanupFuncStack) run() {
	for i := 0; i < len(n.stack); i++ {
		n.stack[i].f()
	}
	*n = cleanupFuncStack{}
}

// namedFunc is function with name, which makes the debugging easier. It is
// used just for clean up functions of a pausable portal.
type namedFunc struct {
	fName string
	f     func()
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
	// rowsAffected is the number of rows queried with this portal. It is
	// accumulated from each portal execution, and will be logged when this portal
	// is closed.
	rowsAffected int
	// sp stores the tracing span of the underlying statement. It is closed when
	// the portal finishes.
	sp    *tracing.Span
	spCtx context.Context
	// outputTypes are the types of the result columns produced by the physical plan.
	// We need this as when re-executing the portal, we are reusing the flow
	// with the new receiver, but not re-generating the physical plan.
	outputTypes []*types.T
	// We need to store the flow for a portal so that when re-executing it, we
	// continue from the previous execution. It lives along with the portal, and
	// will be cleaned-up when the portal is closed.
	flow flowinfra.Flow
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
	// planTop collects the properties of the current plan being prepared.
	// We reuse it when re-executing the portal.
	// TODO(yuzefovich): consider refactoring distSQLFlowInfos from planTop to
	// avoid storing the planTop here.
	planTop planTop
	// queryStats stores statistics on query execution. It is incremented for
	// each execution of the portal.
	queryStats *topLevelQueryStats
	// The following 4 stacks store functions to call when close the portal.
	// They should be called in this order:
	// flowCleanup -> dispatchToExecEngCleanup -> execStmtInOpenStateCleanup ->
	// exhaustPortal.
	// Each stack is defined in the closure of its corresponding function.
	// When encounter an error in any of these function, we run cleanup of this
	// layer and its children layers and propagate the error to the parent layer.
	// For example, when encounter an error in execStmtInOpenStateCleanup(),
	// run flowCleanup -> dispatchToExecEngCleanup -> execStmtInOpenStateCleanup
	// when exiting connExecutor.execStmtInOpenState(), and finally run
	// exhaustPortal in connExecutor.execPortal().
	exhaustPortal              cleanupFuncStack
	execStmtInOpenStateCleanup cleanupFuncStack
	dispatchToExecEngCleanup   cleanupFuncStack
	flowCleanup                cleanupFuncStack
}

// cleanupAll is to run all the cleanup layers.
func (pm *portalPauseInfo) cleanupAll() {
	pm.flowCleanup.run()
	pm.dispatchToExecEngCleanup.run()
	pm.execStmtInOpenStateCleanup.run()
	pm.exhaustPortal.run()
}

// isQueryIDSet returns true if the query id for the portal is set.
func (pm *portalPauseInfo) isQueryIDSet() bool {
	return !pm.queryID.Equal(clusterunique.ID{}.Uint128)
}
