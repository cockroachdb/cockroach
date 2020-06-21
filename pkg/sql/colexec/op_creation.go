// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/marusama/semaphore"
)

// TestNewColOperator is a test helper that's always aliased to builder.NewColOperator.
// We inject this at test time, so tests can use NewColOperator from colexec
// package.
var TestNewColOperator func(ctx context.Context, flowCtx *execinfra.FlowCtx, args NewColOperatorArgs,
) (r NewColOperatorResult, err error)

// NewColOperatorArgs is a helper struct that encompasses all of the input
// arguments to NewColOperator call.
type NewColOperatorArgs struct {
	Spec                 *execinfrapb.ProcessorSpec
	Inputs               []colexecbase.Operator
	StreamingMemAccount  *mon.BoundAccount
	ProcessorConstructor execinfra.ProcessorConstructor
	DiskQueueCfg         colcontainer.DiskQueueCfg
	FDSemaphore          semaphore.Semaphore
	ExprHelper           ExprHelper
	TestingKnobs         struct {
		// UseStreamingMemAccountForBuffering specifies whether to use
		// StreamingMemAccount when creating buffering operators and should only be
		// set to 'true' in tests. The idea behind this flag is reducing the number
		// of memory accounts and monitors we need to close, so we plumbed it into
		// the planning code so that it doesn't create extra memory monitoring
		// infrastructure (and so that we could use testMemAccount defined in
		// main_test.go).
		UseStreamingMemAccountForBuffering bool
		// SpillingCallbackFn will be called when the spilling from an in-memory to
		// disk-backed operator occurs. It should only be set in tests.
		SpillingCallbackFn func()
		// DiskSpillingDisabled specifies whether only in-memory operators should
		// be created.
		DiskSpillingDisabled bool
		// NumForcedRepartitions specifies a number of "repartitions" that a
		// disk-backed operator should be forced to perform. "Repartition" can mean
		// different things depending on the operator (for example, for hash joiner
		// it is dividing original partition into multiple new partitions; for
		// sorter it is merging already created partitions into new one before
		// proceeding to the next partition from the input).
		NumForcedRepartitions int
		// DelegateFDAcquisitions should be observed by users of a
		// PartitionedDiskQueue. During normal operations, these should acquire the
		// maximum number of file descriptors they will use from FDSemaphore up
		// front. Setting this testing knob to true disables that behavior and
		// lets the PartitionedDiskQueue interact with the semaphore as partitions
		// are opened/closed, which ensures that the number of open files never
		// exceeds what is expected.
		DelegateFDAcquisitions bool
	}
}

// NewColOperatorResult is a helper struct that encompasses all of the return
// values of NewColOperator call.
type NewColOperatorResult struct {
	Op               colexecbase.Operator
	ColumnTypes      []*types.T
	InternalMemUsage int
	MetadataSources  []execinfrapb.MetadataSource
	// ToClose is a slice of components that need to be Closed. Close should be
	// idempotent.
	ToClose     []IdempotentCloser
	IsStreaming bool
	OpMonitors  []*mon.BytesMonitor
	OpAccounts  []*mon.BoundAccount
}
