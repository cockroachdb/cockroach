// Copyright 2018 The Cockroach Authors.
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

package oporder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
)

// CanProvideOrdering returns true if the given operator returns rows that can
// satisfy the given required ordering.
func CanProvideOrdering(expr memo.RelExpr, required *props.OrderingChoice) bool {
	if required.Any() {
		return true
	}
	return funcMap[expr.Op()].canProvideOrdering(expr, required)
}

// BuildChildRequiredOrdering returns the ordering that must be required of its
// given child in order to satisfy a required ordering. Can only be called if
// CanProvideOrdering is true for the required ordering.
func BuildChildRequiredOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	return funcMap[parent.Op()].buildChildReqOrdering(parent, required, childIdx)
}

type funcs struct {
	canProvideOrdering func(expr memo.RelExpr, required *props.OrderingChoice) bool

	buildChildReqOrdering func(
		parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
	) props.OrderingChoice
}

var funcMap [opt.NumOperators]funcs

func init() {
	for _, op := range opt.RelationalOperators {
		funcMap[op] = funcs{
			canProvideOrdering:    canNeverProvideOrdering,
			buildChildReqOrdering: noChildReqOrdering,
		}
	}
	funcMap[opt.ScanOp] = funcs{
		canProvideOrdering:    scanCanProvideOrdering,
		buildChildReqOrdering: noChildReqOrdering,
	}
	funcMap[opt.SelectOp] = funcs{
		canProvideOrdering:    selectCanProvideOrdering,
		buildChildReqOrdering: selectBuildChildReqOrdering,
	}
	funcMap[opt.ProjectOp] = funcs{
		canProvideOrdering:    projectCanProvideOrdering,
		buildChildReqOrdering: projectBuildChildReqOrdering,
	}
	funcMap[opt.IndexJoinOp] = funcs{
		canProvideOrdering:    lookupOrIndexJoinCanProvideOrdering,
		buildChildReqOrdering: lookupOrIndexJoinBuildChildReqOrdering,
	}
	funcMap[opt.LookupJoinOp] = funcs{
		canProvideOrdering:    lookupOrIndexJoinCanProvideOrdering,
		buildChildReqOrdering: lookupOrIndexJoinBuildChildReqOrdering,
	}
}

func canNeverProvideOrdering(expr memo.RelExpr, required *props.OrderingChoice) bool {
	return false
}

func noChildReqOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	return props.OrderingChoice{}
}
