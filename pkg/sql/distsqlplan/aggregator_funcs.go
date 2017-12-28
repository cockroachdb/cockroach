// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package distsqlplan

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// FinalStageInfo is a wrapper around an aggregation function performed
// in the final stage of distributed aggregations that allows us to specify the
// corresponding inputs from the local aggregations by their indices in the LocalStage.
type FinalStageInfo struct {
	Fn distsqlrun.AggregatorSpec_Func
	// Specifies the ordered slice of outputs from local aggregations to propagate
	// as inputs to Fn. This must be ordered according to the underlying aggregate builtin
	// arguments signature found in aggregate_builtins.go.
	LocalIdxs []uint32
}

// DistAggregationInfo is a blueprint for planning distributed aggregations. It
// describes two stages - a local stage performs local aggregations wherever
// data is available and generates partial results, and a final stage aggregates
// the partial results from all data "partitions".
//
// The simplest example is SUM: the local stage computes the SUM of the items
// on each node, and a final stage SUMs those partial sums into a final sum.
// Similar functions are MIN, MAX, BOOL_AND, BOOL_OR.
//
// A less trivial example is COUNT: the local stage counts (COUNT), the final stage
// adds the counts (SUM_INT).
//
// A more complex example is AVG, for which we have to do *multiple*
// aggregations in each stage: we need to get a sum and a count, so the local
// stage does SUM and COUNT, and the final stage does SUM and SUM_INT. We also
// need an expression that takes these two values and generates the final AVG
// result.
type DistAggregationInfo struct {
	// The local stage consists of one or more aggregations. All aggregations have
	// the same input.
	LocalStage []distsqlrun.AggregatorSpec_Func

	// The final stage consists of one or more aggregations that take in an
	// arbitrary number of inputs from the local stages. The inputs are ordered and
	// mapped by the indices of the local aggregations in LocalStage (specified by
	// inlocalIdxs).
	FinalStage []FinalStageInfo

	// An optional rendering expression used to obtain the final result; required
	// if there is more than one aggregation in each of the stages.
	//
	// Conceptually this is an expression that has access to the final stage
	// results (via IndexedVars), to be run as the PostProcessing step of the
	// final stage processor.  However, there are some complications:
	//   - this structure is a blueprint for aggregating inputs of different
	//     types, and in some cases the expression may be different depending on
	//     the types (see AVG below).
	//   - we support combining multiple "top level" aggregations into the same
	//     processors, so the correct indexing of the input variables is not
	//     predetermined.
	//
	// Instead of defining a canonical non-typed expression and then tweaking it
	// with visitors, we use a function that directly creates a typed expression
	// on demand. The expression will refer to the final stage results using
	// IndexedVars, with indices specified by varIdxs (1-1 mapping).
	FinalRendering func(h *tree.IndexedVarHelper, varIdxs []int) (tree.TypedExpr, error)
}

// Convenient value for FinalStageInfo.LocalIdxs when there is only one aggregation
// function in each of the LocalStage and FinalStage. Otherwise, specify the explicit
// index corresponding to the local stage.
var passThroughLocalIdxs = []uint32{0}

// DistAggregationTable is DistAggregationInfo look-up table. Functions that
// don't have an entry in the table are not optimized with a local stage.
var DistAggregationTable = map[distsqlrun.AggregatorSpec_Func]DistAggregationInfo{
	distsqlrun.AggregatorSpec_IDENT: {
		LocalStage: []distsqlrun.AggregatorSpec_Func{distsqlrun.AggregatorSpec_IDENT},
		FinalStage: []FinalStageInfo{
			{
				Fn:        distsqlrun.AggregatorSpec_IDENT,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
	},

	distsqlrun.AggregatorSpec_BOOL_AND: {
		LocalStage: []distsqlrun.AggregatorSpec_Func{distsqlrun.AggregatorSpec_BOOL_AND},
		FinalStage: []FinalStageInfo{
			{
				Fn:        distsqlrun.AggregatorSpec_BOOL_AND,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
	},

	distsqlrun.AggregatorSpec_BOOL_OR: {
		LocalStage: []distsqlrun.AggregatorSpec_Func{distsqlrun.AggregatorSpec_BOOL_OR},
		FinalStage: []FinalStageInfo{
			{
				Fn:        distsqlrun.AggregatorSpec_BOOL_OR,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
	},

	distsqlrun.AggregatorSpec_COUNT: {
		LocalStage: []distsqlrun.AggregatorSpec_Func{distsqlrun.AggregatorSpec_COUNT},
		FinalStage: []FinalStageInfo{
			{
				Fn:        distsqlrun.AggregatorSpec_SUM_INT,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
	},

	distsqlrun.AggregatorSpec_COUNT_ROWS: {
		LocalStage: []distsqlrun.AggregatorSpec_Func{distsqlrun.AggregatorSpec_COUNT_ROWS},
		FinalStage: []FinalStageInfo{
			{
				Fn:        distsqlrun.AggregatorSpec_SUM_INT,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
	},

	distsqlrun.AggregatorSpec_MAX: {
		LocalStage: []distsqlrun.AggregatorSpec_Func{distsqlrun.AggregatorSpec_MAX},
		FinalStage: []FinalStageInfo{
			{
				Fn:        distsqlrun.AggregatorSpec_MAX,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
	},

	distsqlrun.AggregatorSpec_MIN: {
		LocalStage: []distsqlrun.AggregatorSpec_Func{distsqlrun.AggregatorSpec_MIN},
		FinalStage: []FinalStageInfo{
			{
				Fn:        distsqlrun.AggregatorSpec_MIN,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
	},

	distsqlrun.AggregatorSpec_SUM: {
		LocalStage: []distsqlrun.AggregatorSpec_Func{distsqlrun.AggregatorSpec_SUM},
		FinalStage: []FinalStageInfo{
			{
				Fn:        distsqlrun.AggregatorSpec_SUM,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
	},

	distsqlrun.AggregatorSpec_XOR_AGG: {
		LocalStage: []distsqlrun.AggregatorSpec_Func{distsqlrun.AggregatorSpec_XOR_AGG},
		FinalStage: []FinalStageInfo{
			{
				Fn:        distsqlrun.AggregatorSpec_XOR_AGG,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
	},

	// AVG is more tricky than the ones above; we need two intermediate values in
	// the local and final stages:
	//  - the local stage accumulates the SUM and the COUNT;
	//  - the final stage sums these partial results (SUM and SUM_INT);
	//  - a final rendering then divides the two results.
	//
	// At a high level, this is analogous to rewriting AVG(x) as SUM(x)/COUNT(x).
	distsqlrun.AggregatorSpec_AVG: {
		LocalStage: []distsqlrun.AggregatorSpec_Func{
			distsqlrun.AggregatorSpec_SUM,
			distsqlrun.AggregatorSpec_COUNT,
		},
		FinalStage: []FinalStageInfo{
			{
				Fn:        distsqlrun.AggregatorSpec_SUM,
				LocalIdxs: []uint32{0},
			},
			{
				Fn:        distsqlrun.AggregatorSpec_SUM_INT,
				LocalIdxs: []uint32{1},
			},
		},
		FinalRendering: func(h *tree.IndexedVarHelper, varIdxs []int) (tree.TypedExpr, error) {
			if len(varIdxs) < 2 {
				panic("fewer than two final aggregation values passed into final render")
			}
			sum := h.IndexedVar(varIdxs[0])
			count := h.IndexedVar(varIdxs[1])

			expr := &tree.BinaryExpr{
				Operator: tree.Div,
				Left:     sum,
				Right:    count,
			}

			// There is no "FLOAT / INT" operator; cast the denominator to float in
			// this case. Note that there is a "DECIMAL / INT" operator, so we don't
			// need the same handling for that case.
			if sum.ResolvedType().Equivalent(types.Float) {
				expr.Right = &tree.CastExpr{
					Expr: count,
					Type: coltypes.NewFloat(0 /* prec */, false /* precSpecified */),
				}
			}
			ctx := &tree.SemaContext{IVarHelper: h}
			return expr.TypeCheck(ctx, types.Any)
		},
	},

	// For VARIANCE/STDDEV the local stage consists of three aggregations,
	// and the final stage aggregation uses all three values.
	// respectively:
	//  - the local stage accumulates the SQRDIFF, SUM and the COUNT
	//  - the final stage calculates the FINAL_(VARIANCE|STDDEV)
	//
	// At a high level, this is analogous to rewriting VARIANCE(x) as
	// SQRDIFF(x)/(COUNT(x) - 1) (and STDDEV(x) as sqrt(VARIANCE(x))).
	distsqlrun.AggregatorSpec_VARIANCE: {
		LocalStage: []distsqlrun.AggregatorSpec_Func{
			distsqlrun.AggregatorSpec_SQRDIFF,
			distsqlrun.AggregatorSpec_SUM,
			distsqlrun.AggregatorSpec_COUNT,
		},
		// Instead of have a SUM_SQRDIFFS and SUM_INT (for COUNT) stage
		// for VARIANCE (and STDDEV) then tailoring a FinalRendering
		// stage specific to each, it is better to use a specific
		// FINAL_(VARIANCE|STDDEV) aggregation stage: - For underlying
		// Decimal results, it is not possible to reduce trailing zeros
		// since the expression is wrapped in IndexVar. Taking the
		// BinaryExpr Pow(0.5) for STDDEV would result in trailing
		// zeros which is not ideal.
		// TODO(richardwu): Consolidate FinalStage and FinalRendering:
		// have one or the other
		FinalStage: []FinalStageInfo{
			{
				Fn:        distsqlrun.AggregatorSpec_FINAL_VARIANCE,
				LocalIdxs: []uint32{0, 1, 2},
			},
		},
	},

	distsqlrun.AggregatorSpec_STDDEV: {
		LocalStage: []distsqlrun.AggregatorSpec_Func{
			distsqlrun.AggregatorSpec_SQRDIFF,
			distsqlrun.AggregatorSpec_SUM,
			distsqlrun.AggregatorSpec_COUNT,
		},
		FinalStage: []FinalStageInfo{
			{
				Fn:        distsqlrun.AggregatorSpec_FINAL_STDDEV,
				LocalIdxs: []uint32{0, 1, 2},
			},
		},
	},

	// TODO(richardwu): we can't do local-final aggregation naively on
	// FIRST since between the local-final stages, any merge ordering from
	// before the local stage is not preserved, which is fundamental to the
	// semantics of FIRST.
	// For example, the query
	//    SELECT FIRST(a) FROM (SELECT * FROM bar ORDER BY b)
	// would return column a's value for the row with the smallest b.
	// However, this ordering information (and the rendering of b) is not
	// propagated to the final aggregation stage after local aggregation.
	// This will cause FIRST(a) to not only deviate from a serial (i.e.
	// local) execution but also return incorrect values.
	// To fix this, we can introduce pass-through renders in the aggregator
	// spec for any MergeOrdering columns.
	// For now, any MergeOrdering set (e.g. by a sort node) before the
	// aggregator should be preserved when constructing the plan.
}

// typeContainer is a helper type that implements tree.IndexedVarContainer; it
// is intended to be used during planning (to back FinalRendering expressions).
// It does not support evaluation.
type typeContainer struct {
	types []sqlbase.ColumnType
}

var _ tree.IndexedVarContainer = &typeContainer{}

func (tc *typeContainer) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	panic("no eval allowed in typeContainer")
}

func (tc *typeContainer) IndexedVarResolvedType(idx int) types.T {
	return tc.types[idx].ToDatumType()
}

func (tc *typeContainer) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return tree.Name(fmt.Sprintf("$%d", idx+1))
}

// MakeTypeIndexedVarHelper returns an IndexedVarHelper which creates IndexedVars
// with the given types.
func MakeTypeIndexedVarHelper(types []sqlbase.ColumnType) tree.IndexedVarHelper {
	return tree.MakeIndexedVarHelper(&typeContainer{types: types}, len(types))
}
