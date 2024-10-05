// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecagg

import (
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execagg"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
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
	EvalCtx        *eval.Context
	Constructors   []execagg.AggregateConstructor
	ConstArguments []tree.Datums
	OutputTypes    []*types.T

	TestingKnobs struct {
		// HashTableNumBuckets if positive will override the initial number of
		// buckets to be used by the hash table (if this struct is passed to the
		// hash aggregator).
		HashTableNumBuckets uint32
	}
}

// NewHashAggregatorArgs encompasses all mandatory arguments necessary to
// instantiate the hash aggregator.
type NewHashAggregatorArgs struct {
	*NewAggregatorArgs
	HashTableAllocator       *colmem.Allocator
	OutputUnlimitedAllocator *colmem.Allocator
	MaxOutputBatchMemSize    int64
}
