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
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecagg"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/marusama/semaphore"
)

const (
	// This limit comes from the fallback strategy where we are using an
	// external sort.
	ehaNumRequiredActivePartitions = colexecop.ExternalSorterMinPartitions
	// ehaNumRequiredFDs is the minimum number of file descriptors that are
	// needed for the machinery of the external aggregator (plus 1 is needed for
	// the in-memory hash aggregator in order to track tuples in a spilling
	// queue).
	ehaNumRequiredFDs = ehaNumRequiredActivePartitions + 1
)

// NewExternalHashAggregator returns a new disk-backed hash aggregator. It uses
// the in-memory hash aggregator as the "main" strategy for the hash-based
// partitioner and the external sort + ordered aggregator as the "fallback".
func NewExternalHashAggregator(
	flowCtx *execinfra.FlowCtx,
	args *colexecargs.NewColOperatorArgs,
	newAggArgs *colexecagg.NewAggregatorArgs,
	createDiskBackedSorter DiskBackedSorterConstructor,
	diskAcc *mon.BoundAccount,
) colexecop.Operator {
	inMemMainOpConstructor := func(partitionedInputs []*partitionerToOperator) colexecop.ResettableOperator {
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
	) colexecop.ResettableOperator {
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
	eha := newHashBasedPartitioner(
		newAggArgs.Allocator,
		flowCtx,
		args,
		"external hash aggregator", /* name */
		[]colexecop.Operator{newAggArgs.Input},
		[][]*types.T{newAggArgs.InputTypes},
		[][]uint32{spec.GroupCols},
		inMemMainOpConstructor,
		diskBackedFallbackOpConstructor,
		diskAcc,
		ehaNumRequiredActivePartitions,
	)
	// The last thing we need to do is making sure that the output has the
	// desired ordering if any is required. Note that since the input is assumed
	// to be already ordered according to the desired ordering, for the
	// in-memory hash aggregation we get it for "free" since it doesn't change
	// the ordering of tuples. However, that is not that the case with the
	// hash-based partitioner, so we might need to plan an external sort on top
	// of it.
	outputOrdering := args.Spec.Core.Aggregator.OutputOrdering
	if len(outputOrdering.Columns) == 0 {
		// No particular output ordering is required.
		return eha
	}
	// TODO(yuzefovich): the fact that we're planning an additional external
	// sort isn't accounted for when considering the number file descriptors to
	// acquire. Not urgent, but it should be fixed.
	maxNumberActivePartitions := calculateMaxNumberActivePartitions(flowCtx, args, ehaNumRequiredActivePartitions)
	return createDiskBackedSorter(eha, newAggArgs.OutputTypes, outputOrdering.Columns, maxNumberActivePartitions)
}

// HashAggregationDiskSpillingEnabled is a cluster setting that allows to
// disable hash aggregator disk spilling.
var HashAggregationDiskSpillingEnabled = settings.RegisterBoolSetting(
	"sql.distsql.temp_storage.hash_agg.enabled",
	"set to false to disable hash aggregator disk spilling "+
		"(this will improve performance, but the query might hit the memory limit)",
	true,
)
