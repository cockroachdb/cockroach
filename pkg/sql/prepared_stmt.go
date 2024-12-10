// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/querycache"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
func (p *planCosts) AvgCustom() memo.Cost {
	if p.custom.length == 0 {
		return 0
	}
	var sum memo.Cost
	for i := 0; i < p.custom.length; i++ {
		sum += p.custom.costs[i]
	}
	return sum / memo.Cost(p.custom.length)
}

// ClearGeneric clears the generic cost.
func (p *planCosts) ClearGeneric() {
	p.generic = 0
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

	pausableRes CommandResult

	// exhausted tracks whether this portal has already been fully exhausted,
	// meaning that any additional attempts to execute it should return no
	// rows.
	exhausted bool

	// portalPausablity is used to log the correct error message when user pause
	// a portal.
	// See comments for PortalPausablity for more details.
	portalPausablity PortalPausablity
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
	if p.pausableRes != nil {
		p.pausableRes.CloseAfterPause()
		_, _, _ = p.pausableRes.WaitForRows()
	}
}

func (p *PreparedPortal) size(portalName string) int64 {
	return int64(uintptr(len(portalName)) + unsafe.Sizeof(p))
}
