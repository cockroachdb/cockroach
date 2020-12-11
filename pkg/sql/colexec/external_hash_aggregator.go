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
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecagg"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/marusama/semaphore"
)

// NewExternalHashAggregator returns a new disk-backed hash aggregator. It uses
// the in-memory hash aggregator as the "main" strategy for the hash-based
// partitioner and the external sort + ordered aggregator as the "fallback".
func NewExternalHashAggregator(
	flowCtx *execinfra.FlowCtx,
	args *NewColOperatorArgs,
	newAggArgs *colexecagg.NewAggregatorArgs,
	createDiskBackedSorter DiskBackedSorterConstructor,
	diskAcc *mon.BoundAccount,
) colexecbase.Operator {
	inMemMainOpConstructor := func(partitionedInputs []*partitionerToOperator) ResettableOperator {
		newAggArgs := *newAggArgs
		newAggArgs.Input = partitionedInputs[0]
		// We don't need to track the input tuples when we have already spilled.
		// TODO(yuzefovich): it might be worth increasing the number of buckets.
		op, err := NewHashAggregator(&newAggArgs, nil /* newSpillingQueueArgs */)
		if err != nil {
			colexecerror.InternalError(err)
		}
		return op
	}
	spec := newAggArgs.Spec
	diskBackedFallbackOpConstructor := func(
		partitionedInputs []*partitionerToOperator,
		maxNumberActivePartitions int,
		_ semaphore.Semaphore,
	) ResettableOperator {
		newAggArgs := *newAggArgs
		newAggArgs.Input = createDiskBackedSorter(
			partitionedInputs[0], newAggArgs.InputTypes,
			makeOrdering(spec.GroupCols), maxNumberActivePartitions,
		)
		diskBackedFallbackOp, err := NewOrderedAggregator(&newAggArgs)
		if err != nil {
			colexecerror.InternalError(err)
		}
		return diskBackedFallbackOp
	}
	numRequiredActivePartitions := ExternalSorterMinPartitions
	return newHashBasedPartitioner(
		newAggArgs.Allocator,
		flowCtx,
		args,
		"external hash aggregator", /* name */
		[]colexecbase.Operator{newAggArgs.Input},
		[][]*types.T{newAggArgs.InputTypes},
		[][]uint32{spec.GroupCols},
		inMemMainOpConstructor,
		diskBackedFallbackOpConstructor,
		diskAcc,
		numRequiredActivePartitions,
	)
}
