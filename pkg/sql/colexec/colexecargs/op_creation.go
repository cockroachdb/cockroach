// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecargs

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execreleasable"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/marusama/semaphore"
)

// TestNewColOperator is a test helper that's always aliased to
// colbuilder.NewColOperator. We inject this at test time, so tests can use
// NewColOperator from colexec* packages.
var TestNewColOperator func(ctx context.Context, flowCtx *execinfra.FlowCtx, args *NewColOperatorArgs,
) (r *NewColOperatorResult, err error)

// OpWithMetaInfo stores a colexecop.Operator together with miscellaneous meta
// information about the tree rooted in that operator.
//
// Note that at some point colexecop.Closers were also included into this
// struct, but for ease of tracking we pulled them out to the flow-level.
// Closers are different from the objects tracked here since we have a
// convenient place to close them from the main goroutine whereas the stats
// collection as well as metadata draining must happen in the goroutine that
// "owns" these objects.
// TODO(yuzefovich): figure out the story about pooling these objects.
type OpWithMetaInfo struct {
	Root colexecop.Operator
	// StatsCollectors are all stats collectors that are present in the tree
	// rooted in Root for which the responsibility of retrieving stats hasn't
	// been claimed yet.
	StatsCollectors []colexecop.VectorizedStatsCollector
	// MetadataSources are all sources of the metadata that are present in the
	// tree rooted in Root for which the responsibility of draining hasn't been
	// claimed yet.
	MetadataSources colexecop.MetadataSources
}

// NewColOperatorArgs is a helper struct that encompasses all of the input
// arguments to NewColOperator call.
type NewColOperatorArgs struct {
	Spec   *execinfrapb.ProcessorSpec
	Inputs []OpWithMetaInfo
	// StreamingMemAccount, if nil, is allocated lazily in NewColOperator.
	StreamingMemAccount *mon.BoundAccount
	// StreamingAllocator will be allocated lazily in NewColOperator.
	StreamingAllocator   *colmem.Allocator
	ProcessorConstructor execinfra.ProcessorConstructor
	LocalProcessors      []execinfra.LocalProcessor
	// any is actually a coldata.Batch, see physicalplan.PhysicalInfrastructure comments.
	LocalVectorSources map[int32]any
	DiskQueueCfg       colcontainer.DiskQueueCfg
	FDSemaphore        semaphore.Semaphore
	SemaCtx            *tree.SemaContext
	Factory            coldata.ColumnFactory
	MonitorRegistry    *MonitorRegistry
	CloserRegistry     *CloserRegistry
	TypeResolver       *descs.DistSQLTypeResolver
	TestingKnobs       struct {
		// SpillingCallbackFn will be called when the spilling from an in-memory
		// to disk-backed operator occurs. It should only be set in tests.
		// TODO(yuzefovich): this knob is a bit confusing because it is
		// currently inherited by "child" disk-backed operators. It is also
		// called every time when the disk-backed operator spills to disk,
		// including after it has been reset for reuse.
		SpillingCallbackFn func()
		// NumForcedRepartitions specifies a number of "repartitions" that a
		// disk-backed operator should be forced to perform. "Repartition" can
		// mean different things depending on the operator (for example, for
		// hash joiner it is dividing original partition into multiple new
		// partitions; for sorter it is merging already created partitions into
		// new one before proceeding to the next partition from the input).
		NumForcedRepartitions int
		// DiskSpillingDisabled specifies whether only in-memory operators
		// should be created.
		DiskSpillingDisabled bool
		// DelegateFDAcquisitions should be observed by users of a
		// PartitionedDiskQueue. During normal operations, these should acquire
		// the maximum number of file descriptors they will use from FDSemaphore
		// up front. Setting this testing knob to true disables that behavior
		// and lets the PartitionedDiskQueue interact with the semaphore as
		// partitions are opened/closed, which ensures that the number of open
		// files never exceeds what is expected.
		DelegateFDAcquisitions bool
	}
}

// NewColOperatorResult is a helper struct that encompasses all of the return
// values of NewColOperator call.
type NewColOperatorResult struct {
	OpWithMetaInfo
	KVReader colexecop.KVReader
	// Columnarizer is the root colexec.Columnarizer, if needed, that is hidden
	// behind the stats collector interface. We need to track it separately from
	// all other stats collectors since it requires special handling.
	Columnarizer colexecop.VectorizedStatsCollector
	// TODO(yuzefovich): consider keeping the reference to the types in Release.
	// This will also require changes to the planning code to actually reuse the
	// slice if possible. This is not exactly trivial since there is no clear
	// contract right now of whether or not a particular operator has to make a
	// copy of the type schema if it needs to use it later.
	ColumnTypes []*types.T
	Releasables []execreleasable.Releasable
}

var _ execreleasable.Releasable = &NewColOperatorResult{}

var newColOperatorResultPool = sync.Pool{
	New: func() interface{} {
		return &NewColOperatorResult{}
	},
}

// GetNewColOperatorResult returns a new NewColOperatorResult.
func GetNewColOperatorResult() *NewColOperatorResult {
	return newColOperatorResultPool.Get().(*NewColOperatorResult)
}

// Release implements the execinfra.Releasable interface.
func (r *NewColOperatorResult) Release() {
	for _, releasable := range r.Releasables {
		releasable.Release()
	}
	// Explicitly unset each slot in the slices of objects of non-trivial size
	// in order to lose references to the old objects. If we don't do it, we
	// might have a memory leak in case the slices aren't appended to for a
	// while (because we're slicing them up to 0 below, the references to the
	// old objects would be kept "alive" until the spot in the slice is
	// overwritten by a new object).
	for i := range r.StatsCollectors {
		r.StatsCollectors[i] = nil
	}
	for i := range r.MetadataSources {
		r.MetadataSources[i] = nil
	}
	for i := range r.Releasables {
		r.Releasables[i] = nil
	}
	*r = NewColOperatorResult{
		OpWithMetaInfo: OpWithMetaInfo{
			StatsCollectors: r.StatsCollectors[:0],
			MetadataSources: r.MetadataSources[:0],
		},
		Releasables: r.Releasables[:0],
	}
	newColOperatorResultPool.Put(r)
}
