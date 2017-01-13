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

import "github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"

// DistAggregationInfo describes how we can best compute a distributed
// aggregation. If we have multiple sources of data, we first compute a function
// on each group ("local stage") and then we aggregate those results (final
// "stage").
type DistAggregationInfo struct {
	LocalStage distsqlrun.AggregatorSpec_Func
	FinalStage distsqlrun.AggregatorSpec_Func
	// TODO(radu): in some cases (average, standard deviation, variance) we could
	// benefit from multiple functions per stage plus an additional rendering stage.
}

// DistAggregationTable is DistAggregationInfo look-up table. Functions that
// don't have an entry in the table are not optimized with a local stage.
var DistAggregationTable = map[distsqlrun.AggregatorSpec_Func]DistAggregationInfo{
	distsqlrun.AggregatorSpec_IDENT: {
		LocalStage: distsqlrun.AggregatorSpec_IDENT,
		FinalStage: distsqlrun.AggregatorSpec_IDENT,
	},

	distsqlrun.AggregatorSpec_BOOL_AND: {
		LocalStage: distsqlrun.AggregatorSpec_BOOL_AND,
		FinalStage: distsqlrun.AggregatorSpec_BOOL_AND,
	},

	distsqlrun.AggregatorSpec_BOOL_OR: {
		LocalStage: distsqlrun.AggregatorSpec_BOOL_OR,
		FinalStage: distsqlrun.AggregatorSpec_BOOL_OR,
	},

	distsqlrun.AggregatorSpec_COUNT: {
		LocalStage: distsqlrun.AggregatorSpec_COUNT,
		FinalStage: distsqlrun.AggregatorSpec_SUM_INT,
	},

	distsqlrun.AggregatorSpec_MAX: {
		LocalStage: distsqlrun.AggregatorSpec_MAX,
		FinalStage: distsqlrun.AggregatorSpec_MAX,
	},

	distsqlrun.AggregatorSpec_MIN: {
		LocalStage: distsqlrun.AggregatorSpec_MIN,
		FinalStage: distsqlrun.AggregatorSpec_MIN,
	},

	distsqlrun.AggregatorSpec_SUM: {
		LocalStage: distsqlrun.AggregatorSpec_SUM,
		FinalStage: distsqlrun.AggregatorSpec_SUM,
	},
}
