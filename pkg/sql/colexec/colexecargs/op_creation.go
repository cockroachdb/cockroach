// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecargs

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/marusama/semaphore"
)

// TestNewColOperator is a test helper that's always aliased to
// colbuilder.NewColOperator. We inject this at test time, so tests can use
// NewColOperator from colexec* packages.
var TestNewColOperator func(ctx context.Context, flowCtx *execinfra.FlowCtx, args *NewColOperatorArgs,
) (r *NewColOperatorResult, err error)

// OpWithMetaInfo stores a colexecop.Operator together with miscellaneous meta
// information about the tree rooted in that operator.
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
	// ToClose are all colexecop.Closers that are present in the tree rooted in
	// Root for which the responsibility of closing hasn't been claimed yet.
	ToClose colexecop.Closers
}

// NewColOperatorArgs is a helper struct that encompasses all of the input
// arguments to NewColOperator call.
type NewColOperatorArgs struct {
	Spec                 *execinfrapb.ProcessorSpec
	Inputs               []OpWithMetaInfo
	StreamingMemAccount  *mon.BoundAccount
	ProcessorConstructor execinfra.ProcessorConstructor
	LocalProcessors      []execinfra.LocalProcessor
	DiskQueueCfg         colcontainer.DiskQueueCfg
	FDSemaphore          semaphore.Semaphore
	ExprHelper           *ExprHelper
	Factory              coldata.ColumnFactory
	TestingKnobs         struct {
		// SpillingCallbackFn will be called when the spilling from an in-memory
		// to disk-backed operator occurs. It should only be set in tests.
		SpillingCallbackFn func()
		// NumForcedRepartitions specifies a number of "repartitions" that a
		// disk-backed operator should be forced to perform. "Repartition" can
		// mean different things depending on the operator (for example, for
		// hash joiner it is dividing original partition into multiple new
		// partitions; for sorter it is merging already created partitions into
		// new one before proceeding to the next partition from the input).
		NumForcedRepartitions int
		// UseStreamingMemAccountForBuffering specifies whether to use
		// StreamingMemAccount when creating buffering operators and should only
		// be set to 'true' in tests. The idea behind this flag is reducing the
		// number of memory accounts and monitors we need to close, so we
		// plumbed it into the planning code so that it doesn't create extra
		// memory monitoring infrastructure (and so that we could use
		// testMemAccount defined in main_test.go).
		UseStreamingMemAccountForBuffering bool
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
		// PlanInvariantsCheckers indicates whether colexec.InvariantsCheckers
		// should be planned on top of the main operators. This knob is needed
		// so that correct operators are added to MetadataSources.
		PlanInvariantsCheckers bool
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
	ColumnTypes  []*types.T
	OpMonitors   []*mon.BytesMonitor
	OpAccounts   []*mon.BoundAccount
	Releasables  []execinfra.Releasable
}

var _ execinfra.Releasable = &NewColOperatorResult{}

// AssertInvariants confirms that all invariants are maintained by
// NewColOperatorResult.
func (r *NewColOperatorResult) AssertInvariants() {
	// Check that all memory monitor names are unique (colexec.diskSpillerBase
	// relies on this in order to catch "memory budget exceeded" errors only
	// from "its own" component).
	names := make(map[string]struct{}, len(r.OpMonitors))
	for _, m := range r.OpMonitors {
		if _, seen := names[m.Name()]; seen {
			colexecerror.InternalError(errors.AssertionFailedf("monitor named %q encountered twice", m.Name()))
		}
		names[m.Name()] = struct{}{}
	}
}

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
	for i := range r.ToClose {
		r.ToClose[i] = nil
	}
	for i := range r.Releasables {
		r.Releasables[i] = nil
	}
	*r = NewColOperatorResult{
		OpWithMetaInfo: OpWithMetaInfo{
			StatsCollectors: r.StatsCollectors[:0],
			MetadataSources: r.MetadataSources[:0],
			ToClose:         r.ToClose[:0],
		},
		// There is no need to deeply reset the column types and the memory
		// monitoring infra slices because these objects are very tiny in the
		// grand scheme of things.
		ColumnTypes: r.ColumnTypes[:0],
		OpMonitors:  r.OpMonitors[:0],
		OpAccounts:  r.OpAccounts[:0],
		Releasables: r.Releasables[:0],
	}
	newColOperatorResultPool.Put(r)
}
