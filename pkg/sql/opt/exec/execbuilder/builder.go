// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// ParallelScanResultThreshold is the number of results up to which, if the
// maximum number of results returned by a scan is known, the scan disables
// batch limits in the dist sender. This results in the parallelization of these
// scans.
const ParallelScanResultThreshold = 10000

// Builder constructs a tree of execution nodes (exec.Node) from an optimized
// expression tree (opt.Expr).
type Builder struct {
	factory          exec.Factory
	optimizer        *xform.Optimizer
	mem              *memo.Memo
	catalog          cat.Catalog
	e                opt.Expr
	disableTelemetry bool
	evalCtx          *tree.EvalContext

	// subqueries accumulates information about subqueries that are part of scalar
	// expressions we built. Each entry is associated with a tree.Subquery
	// expression node.
	subqueries []exec.Subquery

	// cascades accumulates cascades that run after the main query but before
	// checks.
	cascades []exec.Cascade

	// checks accumulates check queries that are run after the main query and
	// any cascades.
	checks []exec.Node

	// nameGen is used to generate names for the tables that will be created for
	// each relational subexpression when evalCtx.SessionData.SaveTablesPrefix is
	// non-empty.
	nameGen *memo.ExprNameGenerator

	// withExprs is the set of With expressions which may be referenced elsewhere
	// in the query.
	// TODO(justin): set this up so that we can look them up by index lookups
	// rather than scans.
	withExprs []builtWithExpr

	// allowAutoCommit is passed through to factory methods for mutation
	// operators. It allows execution to commit the transaction as part of the
	// mutation itself. See canAutoCommit().
	allowAutoCommit bool

	// initialAllowAutoCommit saves the allowAutoCommit value passed to New; used
	// for EXPLAIN.
	initialAllowAutoCommit bool

	allowInsertFastPath bool

	// forceForUpdateLocking is conditionally passed through to factory methods
	// for scan operators that serve as the input for mutation operators. When
	// set to true, it ensures that a FOR UPDATE row-level locking mode is used
	// by scans. See forUpdateLocking.
	forceForUpdateLocking bool

	// -- output --

	// IsDDL is set to true if the statement contains DDL.
	IsDDL bool

	// ContainsFullTableScan is set to true if the statement contains an
	// unconstrained primary index scan. This could be a full scan of any
	// cardinality.
	ContainsFullTableScan bool

	// ContainsFullIndexScan is set to true if the statement contains an
	// unconstrained non-partial secondary index scan. This could be a full scan
	// of any cardinality.
	ContainsFullIndexScan bool

	// ContainsLargeFullTableScan is set to true if the statement contains an
	// unconstrained primary index scan estimated to read more than
	// large_full_scan_rows (or without available stats).
	ContainsLargeFullTableScan bool

	// ContainsLargeFullIndexScan is set to true if the statement contains an
	// unconstrained non-partial secondary index scan estimated to read more than
	// large_full_scan_rows (or without without available stats).
	ContainsLargeFullIndexScan bool

	// containsBoundedStalenessScan is true if the query uses bounded
	// staleness and contains a scan.
	containsBoundedStalenessScan bool

	// ContainsMutation is set to true if the whole plan contains any mutations.
	ContainsMutation bool
}

// New constructs an instance of the execution node builder using the
// given factory to construct nodes. The Build method will build the execution
// node tree from the given optimized expression tree.
//
// catalog is only needed if the statement contains an EXPLAIN (OPT, CATALOG).
//
// If allowAutoCommit is true, mutation operators can pass the auto commit flag
// to the factory (when the optimizer determines it is correct to do so and
// `transaction_rows_read_err` guardrail is disabled.). It should be false if
// the statement is executed as part of an explicit transaction.
func New(
	factory exec.Factory,
	optimizer *xform.Optimizer,
	mem *memo.Memo,
	catalog cat.Catalog,
	e opt.Expr,
	evalCtx *tree.EvalContext,
	allowAutoCommit bool,
) *Builder {
	b := &Builder{
		factory:                factory,
		optimizer:              optimizer,
		mem:                    mem,
		catalog:                catalog,
		e:                      e,
		evalCtx:                evalCtx,
		allowAutoCommit:        allowAutoCommit,
		initialAllowAutoCommit: allowAutoCommit,
	}
	if evalCtx != nil {
		sd := evalCtx.SessionData()
		if sd.SaveTablesPrefix != "" {
			b.nameGen = memo.NewExprNameGenerator(sd.SaveTablesPrefix)
		}
		// If we have the limits on the number of rows read by a single txn, we
		// cannot auto commit if the query is not internal.
		//
		// Note that we don't impose such a requirement on the number of rows
		// written by a single txn because Builder.canAutoCommit ensures that we
		// try to auto commit iff there is a single mutation in the query, and
		// in such a scenario tableWriterBase.finalize is responsible for making
		// sure that the rows written limit is not reached before the auto
		// commit.
		prohibitAutoCommit := sd.TxnRowsReadErr != 0 && !sd.Internal
		b.allowAutoCommit = b.allowAutoCommit && !prohibitAutoCommit
		b.initialAllowAutoCommit = b.allowAutoCommit
		b.allowInsertFastPath = sd.InsertFastPath
	}
	return b
}

// Build constructs the execution node tree and returns its root node if no
// error occurred.
func (b *Builder) Build() (_ exec.Plan, err error) {
	plan, err := b.build(b.e)
	if err != nil {
		return nil, err
	}

	rootRowCount := int64(b.e.(memo.RelExpr).Relational().Stats.RowCountIfAvailable())
	return b.factory.ConstructPlan(plan.root, b.subqueries, b.cascades, b.checks, rootRowCount)
}

func (b *Builder) build(e opt.Expr) (_ execPlan, err error) {
	defer func() {
		if r := recover(); r != nil {
			// This code allows us to propagate errors without adding lots of checks
			// for `if err != nil` throughout the construction code. This is only
			// possible because the code does not update shared state and does not
			// manipulate locks.
			if ok, e := errorutil.ShouldCatch(r); ok {
				err = e
			} else {
				panic(r)
			}
		}
	}()

	rel, ok := e.(memo.RelExpr)
	if !ok {
		return execPlan{}, errors.AssertionFailedf(
			"building execution for non-relational operator %s", redact.Safe(e.Op()),
		)
	}

	canAutoCommit := b.canAutoCommit(rel)
	b.allowAutoCommit = b.allowAutoCommit && canAutoCommit

	// First condition from ConstructFastPathInsert:
	//  - there are no other mutations in the statement, and the output of the
	//    insert is not processed through side-effecting expressions (i.e. we can
	//    auto-commit).
	b.allowInsertFastPath = b.allowInsertFastPath && canAutoCommit

	return b.buildRelational(rel)
}

// BuildScalar converts a scalar expression to a TypedExpr.
func (b *Builder) BuildScalar() (tree.TypedExpr, error) {
	scalar, ok := b.e.(opt.ScalarExpr)
	if !ok {
		return nil, errors.AssertionFailedf("BuildScalar cannot be called for non-scalar operator %s", redact.Safe(b.e.Op()))
	}
	var ctx buildScalarCtx
	md := b.mem.Metadata()
	ctx.ivh = tree.MakeIndexedVarHelper(&mdVarContainer{md: md}, md.NumColumns())
	for i := 0; i < md.NumColumns(); i++ {
		ctx.ivarMap.Set(i+1, i)
	}
	return b.buildScalar(&ctx, scalar)
}

func (b *Builder) decorrelationError() error {
	return errors.Errorf("could not decorrelate subquery")
}

// builtWithExpr is metadata regarding a With expression which has already been
// added to the set of subqueries for the query.
type builtWithExpr struct {
	id opt.WithID
	// outputCols maps the output ColumnIDs of the With expression to the ordinal
	// positions they are output to. See execPlan.outputCols for more details.
	outputCols opt.ColMap
	bufferNode exec.Node
}

func (b *Builder) addBuiltWithExpr(id opt.WithID, outputCols opt.ColMap, bufferNode exec.Node) {
	b.withExprs = append(b.withExprs, builtWithExpr{
		id:         id,
		outputCols: outputCols,
		bufferNode: bufferNode,
	})
}

func (b *Builder) findBuiltWithExpr(id opt.WithID) *builtWithExpr {
	for i := range b.withExprs {
		if b.withExprs[i].id == id {
			return &b.withExprs[i]
		}
	}
	return nil
}

// boundedStaleness returns true if this query uses bounded staleness.
func (b *Builder) boundedStaleness() bool {
	return b.evalCtx != nil && b.evalCtx.AsOfSystemTime != nil &&
		b.evalCtx.AsOfSystemTime.BoundedStaleness
}

// mdVarContainer is an IndexedVarContainer implementation used by BuildScalar -
// it maps indexed vars to columns in the metadata.
type mdVarContainer struct {
	md *opt.Metadata
}

var _ tree.IndexedVarContainer = &mdVarContainer{}

// IndexedVarEval is part of the IndexedVarContainer interface.
func (c *mdVarContainer) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	return nil, errors.AssertionFailedf("no eval allowed in mdVarContainer")
}

// IndexedVarResolvedType is part of the IndexedVarContainer interface.
func (c *mdVarContainer) IndexedVarResolvedType(idx int) *types.T {
	return c.md.ColumnMeta(opt.ColumnID(idx + 1)).Type
}

// IndexedVarNodeFormatter is part of the IndexedVarContainer interface.
func (c *mdVarContainer) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return nil
}
