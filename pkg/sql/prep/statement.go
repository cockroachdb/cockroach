// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package prep

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/explain"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// StatementOrigin is an enum representing the source of where
// the prepare statement was made.
type StatementOrigin int

const (
	// StatementOriginWire signifies the prepared statement was made
	// over the wire.
	StatementOriginWire StatementOrigin = iota + 1
	// StatementOriginSQL signifies the prepared statement was made
	// over a parsed SQL query.
	StatementOriginSQL
	// StatementOriginSessionMigration signifies that the prepared
	// statement came from a call to crdb_internal.deserialize_session.
	StatementOriginSessionMigration
)

// Statement is a SQL statement that has been parsed and the types of arguments
// and results have been determined.
//
// Note that Statements maintain a reference counter internally. References need
// to be registered with incRef() and de-registered with decRef().
type Statement struct {
	Metadata

	// BaseMemo is the memoized data structure constructed by the cost-based
	// optimizer during prepare of a SQL statement.
	BaseMemo *memo.Memo

	// GenericMemo, if present, is a fully-optimized memo that can be executed
	// as-is.
	GenericMemo                 *memo.Memo
	GenericExecPlan             exec.Plan
	GenericExecPlanQueryMetrics exec.QueryMetrics
	GenericPlanGist             explain.PlanGist

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

	// origin is the protocol in which this prepare statement was created. Used
	// for reporting on `pg_prepared_statements`.
	origin StatementOrigin
	// createdAt is the timestamp this prepare statement was made at.
	// Used for reporting on `pg_prepared_statements`.
	createdAt time.Time
}

// NewStatement initializes and returns a new Statement.
func NewStatement(origin StatementOrigin, memAcc mon.BoundAccount) *Statement {
	return &Statement{
		origin:    origin,
		memAcc:    memAcc,
		refCount:  1,
		createdAt: timeutil.Now(),
	}
}

// Origin returns the origin the prepared statement.
func (p *Statement) Origin() StatementOrigin {
	return p.origin
}

// MemAcc returns the bound account of the prepared statement.
func (p *Statement) MemAcc() *mon.BoundAccount {
	return &p.memAcc
}

// CreatedAt returns the time that the prepared statement was created.
func (p *Statement) CreatedAt() time.Time {
	return p.createdAt
}

// MemoryEstimate returns a rough estimate of the PreparedStatement's memory
// usage, in bytes.
func (p *Statement) MemoryEstimate() int64 {
	// Account for the memory used by this prepared statement:
	//   1. Size of the prepare metadata.
	//   2. Size of the prepared memo, if using the cost-based optimizer.
	size := p.Metadata.MemoryEstimate()
	if p.BaseMemo != nil {
		size += p.BaseMemo.MemoryEstimate()
	}
	if p.GenericMemo != nil {
		g := p.GenericMemo.MemoryEstimate()
		if p.GenericExecPlan != nil {
			// Assume that a cached exec plan is roughly the same size as the
			// memo.
			g *= 2
		}
		size += p.GenericMemo.MemoryEstimate()
	}
	return size
}

func (p *Statement) DecRef(ctx context.Context) {
	if p.refCount <= 0 {
		log.Dev.Fatal(ctx, "corrupt PreparedStatement refcount")
	}
	p.refCount--
	if p.refCount == 0 {
		p.memAcc.Close(ctx)
	}
}

func (p *Statement) IncRef(ctx context.Context) {
	if p.refCount <= 0 {
		log.Dev.Fatal(ctx, "corrupt PreparedStatement refcount")
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

// HasGeneric returns true if the planCosts has a generic plan cost.
func (p *planCosts) HasGeneric() bool {
	return p.generic.C != 0
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

// IsGenericOptimal returns true if the generic plan is optimal w.r.t. the
// custom plans. The cost flags, auxiliary cost information, and the cost value
// are all considered. If any of the custom plan cost flags are less than the
// generic cost flags, then the generic plan is not optimal. If the generic plan
// has more full scans than any of the custom plans, then it is not optimal.
// Otherwise, the generic plan is optimal if its cost value is less than the
// average cost of the custom plans.
func (p *planCosts) IsGenericOptimal() bool {
	// Check cost flags and full scan counts.
	genFullScans := p.generic.FullScanCount()
	genUnboundedReads := p.generic.UnboundedReadCount()
	if genFullScans > 0 || genUnboundedReads > 0 || p.generic.Penalties != memo.NoPenalties {
		for i := 0; i < p.custom.length; i++ {
			custom := &p.custom.costs[i]
			if custom.Penalties < p.generic.Penalties ||
				genFullScans > custom.FullScanCount() ||
				genUnboundedReads > custom.UnboundedReadCount() {
				return false
			}
		}
	}

	// Compare the generic cost to the average custom cost. Clear the cost flags
	// because they have already been handled above.
	gen := memo.Cost{C: p.generic.C}
	return gen.Less(p.avgCustom())
}

// avgCustom returns the average cost of all the custom plan costs in planCosts.
// If there are no custom plan costs, it returns 0.
func (p *planCosts) avgCustom() memo.Cost {
	if p.custom.length == 0 {
		return memo.Cost{C: 0}
	}
	var sum float64
	for i := 0; i < p.custom.length; i++ {
		sum += p.custom.costs[i].C
	}
	return memo.Cost{C: sum / float64(p.custom.length)}
}

// Summary returns a single-line string summarizing the custom and generic plan
// costs and full scan counts. The format for custom costs is:
//
//	average_custom_cost_float [<count>]{custom_summary_0 custom_summary_1 ...}
//
// The custom costs are follow by the generic cost summary. See
// (memo.Cost).Summary for details.
//
// A full example:
//
//	custom costs: 1.5 [3]{1.25:U:0f0u 1.75:U:0f0u 1.50:U:0f0u}, generic cost: 4.56:U:0f0u
func (p *planCosts) Summary() string {
	var sb strings.Builder
	sb.WriteString("custom costs: ")
	if p.custom.length > 0 {
		sb.WriteString(fmt.Sprintf("%.9g [%d]{", p.avgCustom().C, p.custom.length))
		for i := 0; i < p.custom.length; i++ {
			if i > 0 {
				sb.WriteByte(' ')
			}
			sb.WriteString(p.custom.costs[i].Summary())
		}
		sb.WriteByte('}')
	} else {
		sb.WriteString("none")
	}
	sb.WriteString(", generic cost: ")
	if p.HasGeneric() {
		sb.WriteString(p.generic.Summary())
	} else {
		sb.WriteString("none")
	}
	return sb.String()
}

// Reset clears any previously set costs.
func (p *planCosts) Reset() {
	p.generic = memo.Cost{C: 0}
	p.custom.nextIdx = 0
	p.custom.length = 0
}
