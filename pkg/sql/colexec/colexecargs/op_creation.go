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
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/marusama/semaphore"
)

// TestNewColOperator is a test helper that's always aliased to
// colbuilder.NewColOperator. We inject this at test time, so tests can use
// NewColOperator from colexec* packages.
var TestNewColOperator func(ctx context.Context, flowCtx *execinfra.FlowCtx, args *NewColOperatorArgs,
) (r *NewColOperatorResult, err error)

// NewColOperatorArgs is a helper struct that encompasses all of the input
// arguments to NewColOperator call.
type NewColOperatorArgs struct {
	Spec                 *execinfrapb.ProcessorSpec
	Inputs               []colexecop.Operator
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
	Op              colexecop.Operator
	KVReader        colexecop.KVReader
	ColumnTypes     []*types.T
	MetadataSources []execinfrapb.MetadataSource
	// ToClose is a slice of components that need to be Closed.
	ToClose     []colexecop.Closer
	OpMonitors  []*mon.BytesMonitor
	OpAccounts  []*mon.BoundAccount
	Releasables []execinfra.Releasable
}

var _ execinfra.Releasable = &NewColOperatorResult{}

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
	*r = NewColOperatorResult{
		ColumnTypes:     r.ColumnTypes[:0],
		MetadataSources: r.MetadataSources[:0],
		ToClose:         r.ToClose[:0],
		OpMonitors:      r.OpMonitors[:0],
		OpAccounts:      r.OpAccounts[:0],
		Releasables:     r.Releasables[:0],
	}
	newColOperatorResultPool.Put(r)
}
