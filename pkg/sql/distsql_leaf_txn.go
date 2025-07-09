// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/errors"
)

// localPlanNodeMightUseTxn indicates whether the given LocalPlanNode might use
// the txn somehow.
func localPlanNodeMightUseTxn(spec *execinfrapb.LocalPlanNodeSpec) bool {
	switch spec.Name {
	case "scan buffer", "buffer":
		// scanBufferNode and bufferNode don't interact with txns directly.
		return false
	default:
		// All other planNodes haven't been audited, so we assume that they
		// might use the txn.
		return true
	}
}

// constructReadsTreeForLeaf iterates over the given processors and constructs
// an interval tree that contains all key spans that will be read by those
// processors.
//
// If we cannot guarantee that only some specified sets of key spans will be
// read, then nil is returned.
//
// This function assumes that it runs on the gateway node.
func constructReadsTreeForLeaf(
	evalCtx *eval.Context, processors []physicalplan.Processor,
) interval.Tree {
	if !planSafeForReducedLeaf(processors) {
		return nil
	}
	readsTree := interval.NewTree(interval.ExclusiveOverlapper)
	// For several processors we'll dynamically construct spans to read based on
	// the input rows that we'll receive. However, we know at least the index
	// they will be reading, so we'll add the corresponding index span into the
	// tree.
	addIndex := func(fetchSpec fetchpb.IndexFetchSpec) error {
		codec := evalCtx.Codec
		tableID := fetchSpec.TableID
		if ext := fetchSpec.External; ext != nil {
			codec = keys.MakeSQLCodec(ext.TenantID)
			tableID = ext.TableID
		}
		indexPrefix := rowenc.MakeIndexKeyPrefix(codec, tableID, fetchSpec.IndexID)
		var sp roachpb.Span
		sp.Key = indexPrefix
		sp.EndKey = sp.Key.PrefixEnd()
		return readsTree.Insert(intervalSpan(sp), true /* fast */)
	}
	// Examine all disk-reading processors that interact with LeafTxnInputState.
	for _, proc := range processors {
		var err error
		switch core := proc.Spec.Core; {
		case core.TableReader != nil:
			// For TableReaders we know the precise set of spans that we'll read
			// upfront.
			spans := core.TableReader.Spans
			for i := 0; i < len(spans) && err == nil; i++ {
				var sp roachpb.Span
				sp.Key = spans[i].Key
				sp.EndKey = spans[i].EndKey
				if sp.EndKey == nil { // represents a Get request
					sp.EndKey = sp.Key.PrefixEnd()
				}
				err = readsTree.Insert(intervalSpan(sp), true /* fast */)
			}
		case core.JoinReader != nil:
			err = addIndex(core.JoinReader.FetchSpec)
		case core.ZigzagJoiner != nil:
			zzSpec := core.ZigzagJoiner
			for i := 0; i < len(zzSpec.Sides) && err == nil; i++ {
				err = addIndex(zzSpec.Sides[i].FetchSpec)
			}
		case core.InvertedJoiner != nil:
			err = addIndex(core.InvertedJoiner.FetchSpec)
		}
		if err != nil {
			if buildutil.CrdbTestBuild {
				panic(errors.NewAssertionErrorWithWrappedErrf(
					err, "don't expect to see errors in constructReadsTreeForLeaf",
				))
			}
			return nil
		}
	}
	readsTree.AdjustRanges()
	return readsTree
}

// planSafeForReducedLeaf examines the given set of processors and returns
// whether it is eligible for reduced write sets in the LeafTxn.
//
// This function assumes that it runs on the gateway node.
func planSafeForReducedLeaf(processors []physicalplan.Processor) bool {
	v := &reducedLeafExprVisitor{}
	// unsafeExpr will return whether we consider the given expression "unsafe".
	unsafeExpr := func(expr execinfrapb.Expression) bool {
		if expr.LocalExpr == nil {
			// The expression is empty. (We're running on the gateway, so it's
			// ok to only look at the LocalExpr.)
			return false
		}
		v.unsafe = false
		tree.WalkExprConst(v, expr.LocalExpr)
		return v.unsafe
	}
	// Look at each possible processor core. We divide all of them in three
	// categories:
	// - if a core might need to have access to the full write sets of the leaf
	// txn, it goes into the "unsafe" category;
	// - if a core is not on the "hot" query path (i.e. part of bulk operations,
	// etc), it goes into the "unoptimized" category;
	// - all other processors are examined more thoroughly in terms of what
	// operations the concrete spec might perform, and it's then decided between
	// "unsafe" and "safe".
	const unsafeCore = false           // first category
	const unoptimizedProcessor = false // second category
	const unsafeProcessor = false      // third category where the core is safe but the concrete spec is not
	for _, proc := range processors {
		switch core := proc.Spec.Core; {
		case core.Noop != nil: // always safe
		case core.TableReader != nil: // always safe
		case core.JoinReader != nil:
			if unsafeExpr(core.JoinReader.LookupExpr) {
				return unsafeProcessor
			}
			if unsafeExpr(core.JoinReader.RemoteLookupExpr) {
				return unsafeProcessor
			}
			if unsafeExpr(core.JoinReader.OnExpr) {
				return unsafeProcessor
			}
		case core.Sorter != nil: // always safe
		case core.Aggregator != nil:
			// Aggregations field only contains constant tree.Datums, but we'll
			// include the check for completeness.
			for _, agg := range core.Aggregator.Aggregations {
				for i := range agg.Arguments {
					if unsafeExpr(agg.Arguments[i]) {
						return unsafeProcessor
					}
				}
			}
		case core.Distinct != nil: // always safe
		case core.MergeJoiner != nil:
			if unsafeExpr(core.MergeJoiner.OnExpr) {
				return unsafeProcessor
			}
		case core.HashJoiner != nil:
			if unsafeExpr(core.HashJoiner.OnExpr) {
				return unsafeProcessor
			}
		case core.Values != nil: // always safe
		case core.Backfiller != nil:
			return unoptimizedProcessor
		case core.ReadImport != nil:
			return unoptimizedProcessor
		case core.Sampler != nil:
			return unoptimizedProcessor
		case core.SampleAggregator != nil:
			return unoptimizedProcessor
		case core.ZigzagJoiner != nil:
			if unsafeExpr(core.ZigzagJoiner.OnExpr) {
				return unsafeProcessor
			}
		case core.ProjectSet != nil:
			for _, expr := range core.ProjectSet.Exprs {
				if unsafeExpr(expr) {
					return unsafeProcessor
				}
			}
		case core.Windower != nil: // always safe
		case core.LocalPlanNode != nil:
			if localPlanNodeMightUseTxn(core.LocalPlanNode) {
				// This LocalPlanNode might use the txn, so we'll consider it
				// "unsafe".
				return unsafeCore
			}
		case core.ChangeAggregator != nil:
			return unoptimizedProcessor
		case core.ChangeFrontier != nil:
			return unoptimizedProcessor
		case core.Ordinality != nil: // always safe
		case core.BulkRowWriter != nil:
			return unoptimizedProcessor
		case core.InvertedFilterer != nil:
			if preFilterer := core.InvertedFilterer.PreFiltererSpec; preFilterer != nil {
				if unsafeExpr(preFilterer.Expression) {
					return unsafeProcessor
				}
			}
		case core.InvertedJoiner != nil:
			if unsafeExpr(core.InvertedJoiner.InvertedExpr) {
				return unsafeProcessor
			}
			if unsafeExpr(core.InvertedJoiner.OnExpr) {
				return unsafeProcessor
			}
		case core.BackupData != nil:
			return unoptimizedProcessor
		case core.RestoreData != nil:
			return unoptimizedProcessor
		case core.Filterer != nil:
			if unsafeExpr(core.Filterer.Filter) {
				return unsafeProcessor
			}
		case core.StreamIngestionData != nil:
			return unoptimizedProcessor
		case core.StreamIngestionFrontier != nil:
			return unoptimizedProcessor
		case core.Exporter != nil:
			return unoptimizedProcessor
		case core.IndexBackfillMerger != nil:
			return unoptimizedProcessor
		case core.Ttl != nil:
			return unoptimizedProcessor
		case core.Inspect != nil:
			return unoptimizedProcessor
		case core.HashGroupJoiner != nil:
			if unsafeExpr(core.HashGroupJoiner.HashJoinerSpec.OnExpr) {
				return unsafeProcessor
			}
			// Aggregations field only contains constant tree.Datums, but we'll
			// include the check for completeness.
			for _, agg := range core.HashGroupJoiner.AggregatorSpec.Aggregations {
				for i := range agg.Arguments {
					if unsafeExpr(agg.Arguments[i]) {
						return unsafeProcessor
					}
				}
			}
		case core.GenerativeSplitAndScatter != nil:
			return unoptimizedProcessor
		case core.CloudStorageTest != nil:
			return unoptimizedProcessor
		case core.Insert != nil:
			if buildutil.CrdbTestBuild {
				panic(errors.AssertionFailedf("trying to create a LeafTxn for a plan with Insert processor"))
			}
			return unsafeCore
		case core.IngestStopped != nil:
			return unoptimizedProcessor
		case core.LogicalReplicationWriter != nil:
			return unoptimizedProcessor
		case core.LogicalReplicationOfflineScan != nil:
			return unoptimizedProcessor
		case core.VectorSearch != nil:
			// VectorSearch currently is not distributed, so we should never try
			// to create the LeafTxn with it.
			if buildutil.CrdbTestBuild {
				panic(errors.AssertionFailedf("implement this"))
			}
			return unsafeCore
		case core.VectorMutationSearch != nil:
			// VectorMutationSearch currently is not distributed, so we should
			// never try to create the LeafTxn with it.
			if buildutil.CrdbTestBuild {
				panic(errors.AssertionFailedf("implement this"))
			}
			return unsafeCore
		case core.CompactBackups != nil:
			return unoptimizedProcessor
		default:
			if buildutil.CrdbTestBuild {
				panic(errors.AssertionFailedf("unknown processor core"))
			}
			return unsafeCore
		}
		for _, expr := range proc.Spec.Post.RenderExprs {
			if unsafeExpr(expr) {
				return unsafeProcessor
			}
		}
	}
	return true
}

// reducedLeafExprVisitor determines whether an expression is safe for using the
// reduced write sets for LeafTxns. It is inspired by distSQLExprCheckVisitor.
//
// We'll call an expression "unsafe" if it contains something that might access
// the underlying txn (either explicitly or implicitly (via the planner)).
type reducedLeafExprVisitor struct {
	unsafe bool
}

var _ tree.Visitor = &reducedLeafExprVisitor{}

func (v *reducedLeafExprVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	if v.unsafe {
		return false, expr
	}
	switch t := expr.(type) {
	case *tree.FuncExpr:
		if t.IsDistSQLBlocklist() {
			// The fact that this builtin is DistSQL-blocklisted tells us that
			// it might do something non-trivial.
			v.unsafe = true
			return false, expr
		}
	case *tree.RoutineExpr:
		// Routines could do arbitrary things.
		v.unsafe = true
		return false, expr
	case *tree.DOid:
		// Oid expressions might incur resolution via the planner.
		v.unsafe = true
		return false, expr
	case *tree.Subquery:
		if hasOidType(t.ResolvedType()) {
			// Copied from distSQLExprCheckVisitor.
			v.unsafe = true
			return false, expr
		}
	case *tree.CastExpr:
		if typ, ok := tree.GetStaticallyKnownType(t.Type); ok {
			switch typ.Family() {
			case types.OidFamily:
				v.unsafe = true
				return false, expr
			}
		}
	}
	return true, expr
}

func (v *reducedLeafExprVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }

type intervalSpan roachpb.Span

var _ interval.Interface = intervalSpan{}

// ID is part of the interval.Interface. We don't need to distinguish the same
// spans, so we always return 0.
func (ie intervalSpan) ID() uintptr { return 0 }

// Range is part of the interval.Interface.
func (ie intervalSpan) Range() interval.Range {
	return interval.Range{Start: []byte(ie.Key), End: []byte(ie.EndKey)}
}
