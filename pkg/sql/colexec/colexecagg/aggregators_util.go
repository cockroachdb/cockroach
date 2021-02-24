// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecagg

import (
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// NewAggregatorArgs encompasses all arguments necessary to instantiate either
// of the aggregators.
type NewAggregatorArgs struct {
	Allocator *colmem.Allocator
	// MemAccount should be the same as the one used by Allocator and will be
	// used by aggregatorHelper to handle DISTINCT clause.
	MemAccount     *mon.BoundAccount
	Input          colexecop.Operator
	InputTypes     []*types.T
	Spec           *execinfrapb.AggregatorSpec
	EvalCtx        *tree.EvalContext
	Constructors   []execinfrapb.AggregateConstructor
	ConstArguments []tree.Datums
	OutputTypes    []*types.T
}
