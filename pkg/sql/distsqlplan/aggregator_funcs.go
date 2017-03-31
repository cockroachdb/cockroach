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
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package distsqlplan

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

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

	// The final stage consists of the same number of aggregations as the local
	// stage (the input of each one is the corresponding result from each instance
	// of the local stage).
	FinalStage []distsqlrun.AggregatorSpec_Func

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
	// IndexedVars, with indices shifted by varIdxOffset.
	FinalRendering func(h *parser.IndexedVarHelper, varIdxOffset int) (parser.TypedExpr, error)
}

// DistAggregationTable is DistAggregationInfo look-up table. Functions that
// don't have an entry in the table are not optimized with a local stage.
var DistAggregationTable = map[distsqlrun.AggregatorSpec_Func]DistAggregationInfo{
	distsqlrun.AggregatorSpec_IDENT: {
		LocalStage: []distsqlrun.AggregatorSpec_Func{distsqlrun.AggregatorSpec_IDENT},
		FinalStage: []distsqlrun.AggregatorSpec_Func{distsqlrun.AggregatorSpec_IDENT},
	},

	distsqlrun.AggregatorSpec_BOOL_AND: {
		LocalStage: []distsqlrun.AggregatorSpec_Func{distsqlrun.AggregatorSpec_BOOL_AND},
		FinalStage: []distsqlrun.AggregatorSpec_Func{distsqlrun.AggregatorSpec_BOOL_AND},
	},

	distsqlrun.AggregatorSpec_BOOL_OR: {
		LocalStage: []distsqlrun.AggregatorSpec_Func{distsqlrun.AggregatorSpec_BOOL_OR},
		FinalStage: []distsqlrun.AggregatorSpec_Func{distsqlrun.AggregatorSpec_BOOL_OR},
	},

	distsqlrun.AggregatorSpec_COUNT: {
		LocalStage: []distsqlrun.AggregatorSpec_Func{distsqlrun.AggregatorSpec_COUNT},
		FinalStage: []distsqlrun.AggregatorSpec_Func{distsqlrun.AggregatorSpec_SUM_INT},
	},

	distsqlrun.AggregatorSpec_MAX: {
		LocalStage: []distsqlrun.AggregatorSpec_Func{distsqlrun.AggregatorSpec_MAX},
		FinalStage: []distsqlrun.AggregatorSpec_Func{distsqlrun.AggregatorSpec_MAX},
	},

	distsqlrun.AggregatorSpec_MIN: {
		LocalStage: []distsqlrun.AggregatorSpec_Func{distsqlrun.AggregatorSpec_MIN},
		FinalStage: []distsqlrun.AggregatorSpec_Func{distsqlrun.AggregatorSpec_MIN},
	},

	distsqlrun.AggregatorSpec_SUM: {
		LocalStage: []distsqlrun.AggregatorSpec_Func{distsqlrun.AggregatorSpec_SUM},
		FinalStage: []distsqlrun.AggregatorSpec_Func{distsqlrun.AggregatorSpec_SUM},
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
		FinalStage: []distsqlrun.AggregatorSpec_Func{
			distsqlrun.AggregatorSpec_SUM,
			distsqlrun.AggregatorSpec_SUM_INT,
		},
		FinalRendering: func(h *parser.IndexedVarHelper, varIdxOffset int) (parser.TypedExpr, error) {
			sum := h.IndexedVar(varIdxOffset)
			count := h.IndexedVar(varIdxOffset + 1)

			expr := &parser.BinaryExpr{
				Operator: parser.Div,
				Left:     sum,
				Right:    count,
			}

			// There is no "FLOAT / INT" operator; cast the denominator to float in
			// this case. Note that there is a "DECIMAL / INT" operator, so we don't
			// need the same handling for that case.
			if sum.ResolvedType().Equivalent(parser.TypeFloat) {
				expr.Right = &parser.CastExpr{
					Expr: count,
					Type: parser.NewFloatColType(0 /* prec */, false /* precSpecified */),
				}
			}
			return expr.TypeCheck(nil, parser.TypeAny)
		},
	},
}

// typeContainer is a helper type that implements parser.IndexedVarContainer; it
// is intended to be used during planning (to back FinalRendering expressions).
// It does not support evaluation.
type typeContainer struct {
	types []sqlbase.ColumnType
}

var _ parser.IndexedVarContainer = &typeContainer{}

func (tc *typeContainer) IndexedVarEval(idx int, ctx *parser.EvalContext) (parser.Datum, error) {
	panic("no eval allowed in typeContainer")
}

func (tc *typeContainer) IndexedVarResolvedType(idx int) parser.Type {
	return tc.types[idx].ToDatumType()
}

func (tc *typeContainer) IndexedVarFormat(buf *bytes.Buffer, f parser.FmtFlags, idx int) {
	fmt.Fprintf(buf, "@%d", idx+1)
}

// MakeTypeIndexedVarHelper returns an IndexedVarHelper which creates IndexedVars
// with the given types.
func MakeTypeIndexedVarHelper(types []sqlbase.ColumnType) parser.IndexedVarHelper {
	return parser.MakeIndexedVarHelper(&typeContainer{types: types}, len(types))
}
