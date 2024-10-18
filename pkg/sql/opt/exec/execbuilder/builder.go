// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execbuilder

import (
	"context"
	"slices"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// parallelScanResultThreshold is the number of results up to which, if the
// maximum number of results returned by a scan is known, the scan disables
// batch limits in the dist sender. This results in the parallelization of these
// scans.
var parallelScanResultThreshold = uint64(metamorphic.ConstantWithTestRange(
	"parallel-scan-result-threshold",
	parallelScanResultThresholdProductionValue, /* defaultValue */
	1, /* min */
	parallelScanResultThresholdProductionValue, /* max */
))

const parallelScanResultThresholdProductionValue = 10000

func getParallelScanResultThreshold(forceProductionValue bool) uint64 {
	if forceProductionValue {
		return parallelScanResultThresholdProductionValue
	}
	return parallelScanResultThreshold
}

// Builder constructs a tree of execution nodes (exec.Node) from an optimized
// expression tree (opt.Expr).
type Builder struct {
	ctx              context.Context
	factory          exec.Factory
	optimizer        *xform.Optimizer
	mem              *memo.Memo
	catalog          cat.Catalog
	e                opt.Expr
	disableTelemetry bool
	semaCtx          *tree.SemaContext
	evalCtx          *eval.Context
	colOrdsAlloc     colOrdMapAllocator

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

	// forceForUpdateLocking is a set of opt catalog table IDs that serve as input
	// for mutation operators, and should be locked using forUpdateLocking to
	// reduce query retries.
	forceForUpdateLocking intsets.Fast

	// planLazySubqueries is true if the builder should plan subqueries that are
	// lazily evaluated as routines instead of a subquery which is evaluated
	// eagerly before the main query. This is required in cases that cannot be
	// handled by the subquery execution machinery, e.g., when building
	// subqueries for statements inside a UDF.
	planLazySubqueries bool

	// tailCalls is used when building the last body statement of a routine. It
	// identifies nested routines that are in tail-call position. This information
	// is used to determine whether tail-call optimization is applicable.
	tailCalls map[*memo.UDFCallExpr]struct{}

	// -- output --

	// flags tracks various properties of the plan accumulated while building.
	flags exec.PlanFlags

	// containsBoundedStalenessScan is true if the query uses bounded
	// staleness and contains a scan.
	containsBoundedStalenessScan bool

	// MaxFullScanRows is the maximum number of rows scanned by a full scan, as
	// estimated by the optimizer.
	MaxFullScanRows float64

	// TotalScanRows is the total number of rows read by all scans in the query,
	// as estimated by the optimizer.
	TotalScanRows float64

	// TotalScanRowsWithoutForecasts is the total number of rows read by all scans
	// in the query, as estimated by the optimizer without using forecasts. (If
	// forecasts were not used, this should be the same as TotalScanRows.)
	TotalScanRowsWithoutForecasts float64

	// NanosSinceStatsCollected is the maximum number of nanoseconds that have
	// passed since stats were collected on any table scanned by this query.
	NanosSinceStatsCollected time.Duration

	// NanosSinceStatsForecasted is the greatest quantity of nanoseconds that have
	// passed since the forecast time (or until the forecast time, if the it is in
	// the future, in which case it will be negative) for any table with
	// forecasted stats scanned by this query.
	NanosSinceStatsForecasted time.Duration

	// JoinTypeCounts records the number of times each type of logical join was
	// used in the query.
	JoinTypeCounts map[descpb.JoinType]int

	// JoinAlgorithmCounts records the number of times each type of join algorithm
	// was used in the query.
	JoinAlgorithmCounts map[exec.JoinAlgorithm]int

	// ScanCounts records the number of times scans were used in the query.
	ScanCounts [exec.NumScanCountTypes]int

	// builtScans collects all scans in the operation tree so post-build checking
	// for non-local execution can be done.
	builtScans []*memo.ScanExpr

	// doScanExprCollection, when true, causes buildScan to add any ScanExprs it
	// processes to the builtScans slice.
	doScanExprCollection bool

	// IsANSIDML is true if the AST the execbuilder is working on is one of the
	// 4 DML statements, SELECT, UPDATE, INSERT, DELETE, or an EXPLAIN of one of
	// these statements.
	IsANSIDML bool

	// IndexesUsed list the indexes used in query with the format tableID@indexID.
	IndexesUsed
}

// IndexesUsed is a list of indexes used in a query.
type IndexesUsed struct {
	indexes []struct {
		tableID cat.StableID
		indexID cat.StableID
	}
}

// add adds the given index to the list, if it is not already present.
func (iu *IndexesUsed) add(tableID, indexID cat.StableID) {
	s := struct {
		tableID cat.StableID
		indexID cat.StableID
	}{tableID, indexID}
	if !slices.Contains(iu.indexes, s) {
		iu.indexes = append(iu.indexes, s)
	}
}

// Strings returns a slice of strings with the format tableID@indexID for each
// index in the list.
//
// TODO(mgartner): Use a slice of struct{uint64, uint64} instead of converting
// to strings.
func (iu *IndexesUsed) Strings() []string {
	res := make([]string, len(iu.indexes))
	const base = 10
	for i, u := range iu.indexes {
		res[i] = strconv.FormatUint(uint64(u.tableID), base) + "@" +
			strconv.FormatUint(uint64(u.indexID), base)
	}
	return res
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
	ctx context.Context,
	factory exec.Factory,
	optimizer *xform.Optimizer,
	mem *memo.Memo,
	catalog cat.Catalog,
	e opt.Expr,
	semaCtx *tree.SemaContext,
	evalCtx *eval.Context,
	allowAutoCommit bool,
	isANSIDML bool,
) *Builder {
	b := &Builder{
		factory:                factory,
		optimizer:              optimizer,
		mem:                    mem,
		catalog:                catalog,
		e:                      e,
		ctx:                    ctx,
		semaCtx:                semaCtx,
		evalCtx:                evalCtx,
		allowAutoCommit:        allowAutoCommit,
		initialAllowAutoCommit: allowAutoCommit,
		IsANSIDML:              isANSIDML,
	}
	b.colOrdsAlloc.Init(mem.Metadata().MaxColumn())
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
	plan, _, err := b.build(b.e)
	if err != nil {
		return nil, err
	}

	rootRowCount := int64(b.e.(memo.RelExpr).Relational().Statistics().RowCountIfAvailable())
	return b.factory.ConstructPlan(
		plan.root, b.subqueries, b.cascades, b.checks, rootRowCount, b.flags,
	)
}

type functionLookupHelper func(context.Context, tree.UnresolvedRoutineName, tree.SearchPath) (*tree.ResolvedFunctionDefinition, error)

func (b *Builder) wrapFunctionImpl(
	fnName string, lookup functionLookupHelper,
) (tree.ResolvableFunctionReference, error) {
	if lookup != nil {
		unresolved := tree.MakeUnresolvedName(fnName)
		fnDef, err := lookup(
			context.Background(),
			tree.MakeUnresolvedFunctionName(&unresolved),
			&b.evalCtx.SessionData().SearchPath,
		)
		if err != nil {
			return tree.ResolvableFunctionReference{}, err
		}
		return tree.ResolvableFunctionReference{FunctionReference: fnDef}, nil
	}
	return tree.WrapFunction(fnName), nil
}

func (b *Builder) wrapBuiltinFunction(fnName string) (tree.ResolvableFunctionReference, error) {
	lookup := func(_ context.Context, fn tree.UnresolvedRoutineName, path tree.SearchPath) (*tree.ResolvedFunctionDefinition, error) {
		uname, err := fn.UnresolvedName().ToRoutineName()
		if err != nil {
			return nil, err
		}
		return tree.GetBuiltinFuncDefinitionOrFail(uname, path)
	}
	return b.wrapFunctionImpl(fnName, lookup)
}

func (b *Builder) wrapFunction(fnName string) (tree.ResolvableFunctionReference, error) {
	var lookup functionLookupHelper = nil
	// Some tests leave those unset.
	if b.evalCtx != nil && b.catalog != nil {
		lookup = b.catalog.ResolveFunction
	}
	return b.wrapFunctionImpl(fnName, lookup)
}

func (b *Builder) build(e opt.Expr) (_ execPlan, outputCols colOrdMap, err error) {
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
		return execPlan{}, colOrdMap{}, errors.AssertionFailedf(
			"building execution for non-relational operator %s", redact.Safe(e.Op()),
		)
	}

	canAutoCommit := memo.CanAutoCommit(rel)
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
	md := b.mem.Metadata()
	cols := b.colOrdsAlloc.Alloc()
	for i := 0; i < md.NumColumns(); i++ {
		cols.Set(opt.ColumnID(i+1), i)
	}
	ctx := makeBuildScalarCtx(cols)
	return b.buildScalar(&ctx, scalar)
}

func (b *Builder) decorrelationError() error {
	return errors.Errorf("could not decorrelate subquery")
}

func (b *Builder) decorrelationMutationError() error {
	return errors.Errorf("could not decorrelate subquery with mutation")
}

// builtWithExpr is metadata regarding a With expression which has already been
// added to the set of subqueries for the query.
type builtWithExpr struct {
	id opt.WithID
	// outputCols maps the output ColumnIDs of the With expression to the ordinal
	// positions they are output to. See execPlan.outputCols for more details.
	outputCols colOrdMap
	bufferNode exec.Node
}

func (b *Builder) addBuiltWithExpr(id opt.WithID, outputCols colOrdMap, bufferNode exec.Node) {
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
	return b.evalCtx != nil && b.evalCtx.BoundedStaleness()
}

// mdVarContainer is an eval.IndexedVarContainer implementation used by
// BuildScalar - it maps indexed vars to columns in the metadata.
type mdVarContainer struct {
	md *opt.Metadata
}

var _ tree.IndexedVarContainer = &mdVarContainer{}

// IndexedVarResolvedType is part of the IndexedVarContainer interface.
func (c *mdVarContainer) IndexedVarResolvedType(idx int) *types.T {
	return c.md.ColumnMeta(opt.ColumnID(idx + 1)).Type
}
