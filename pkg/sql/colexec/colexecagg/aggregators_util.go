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
)

// NewAggregatorArgs encompasses all arguments necessary to instantiate either
// of the aggregators.
type NewAggregatorArgs struct {
	// Allocator will be utilized for miscellaneous allocations in the
	// aggregators.
	//
	// In the in-memory hash aggregator it will be used for:
	// - aggregateFuncAlloc
	// - DISTINCT and FILTER helpers
	// - aggBucketAlloc
	// - AppendOnlyBufferedBatch for buffered tuples.
	Allocator      *colmem.Allocator
	Input          colexecop.Operator
	InputTypes     []*types.T
	Spec           *execinfrapb.AggregatorSpec
	EvalCtx        *eval.Context
	Constructors   []execagg.AggregateConstructor
	ConstArguments []tree.Datums
	OutputTypes    []*types.T
	// EstimatedRowCount, if set, is the number of rows that will be output by
	// the aggregator (i.e. total number of groups). At time of this writing it
	// is only used for initialAllocSize in the hash aggregator.
	// TODO(yuzefovich): consider using this information for other things too
	// (e.g. sizing the output batch).
	EstimatedRowCount uint64

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
