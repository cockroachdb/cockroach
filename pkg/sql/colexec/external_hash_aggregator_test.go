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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecagg"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/marusama/semaphore"
	"github.com/stretchr/testify/require"
)

func TestExternalHashAggregator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
		DiskMonitor: testDiskMonitor,
	}

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()

	var (
		accounts []*mon.BoundAccount
		monitors []*mon.BytesMonitor
	)
	rng, _ := randutil.NewPseudoRand()
	numForcedRepartitions := rng.Intn(5)
	for _, diskSpillingEnabled := range []bool{true, false} {
		HashAggregationDiskSpillingEnabled.Override(ctx, &flowCtx.Cfg.Settings.SV, diskSpillingEnabled)
		// Test the case in which the default memory is used as well as the case
		// in which the hash aggregator spills to disk.
		for _, spillForced := range []bool{false, true} {
			if !diskSpillingEnabled && spillForced {
				continue
			}
			flowCtx.Cfg.TestingKnobs.ForceDiskSpill = spillForced
			for _, tc := range append(aggregatorsTestCases, hashAggregatorTestCases...) {
				if len(tc.groupCols) == 0 {
					// If there are no grouping columns, then the ordered
					// aggregator is planned.
					continue
				}
				if tc.aggFilter != nil {
					// Filtering aggregation is not supported with the ordered
					// aggregation which is required for the external hash
					// aggregator in the fallback strategy.
					continue
				}
				log.Infof(ctx, "spillForced=%t/numRepartitions=%d/%s", spillForced, numForcedRepartitions, tc.name)
				constructors, constArguments, outputTypes, err := colexecagg.ProcessAggregations(
					&evalCtx, nil /* semaCtx */, tc.spec.Aggregations, tc.typs,
				)
				require.NoError(t, err)
				verifier := colexectestutils.OrderedVerifier
				if tc.unorderedInput {
					verifier = colexectestutils.UnorderedVerifier
				}
				var numExpectedClosers int
				if diskSpillingEnabled {
					// The external sorter and the disk spiller should be added
					// as Closers (the latter is responsible for closing the
					// in-memory hash aggregator as well as the external one).
					numExpectedClosers = 2
					if len(tc.spec.OutputOrdering.Columns) > 0 {
						// When the output ordering is required, we also plan
						// another external sort.
						numExpectedClosers++
					}
				} else {
					// Only the in-memory hash aggregator should be added.
					numExpectedClosers = 1
				}
				var semsToCheck []semaphore.Semaphore
				colexectestutils.RunTestsWithTyps(
					t,
					testAllocator,
					[]colexectestutils.Tuples{tc.input},
					[][]*types.T{tc.typs},
					tc.expected,
					verifier,
					func(input []colexecop.Operator) (colexecop.Operator, error) {
						sem := colexecop.NewTestingSemaphore(ehaNumRequiredFDs)
						semsToCheck = append(semsToCheck, sem)
						op, accs, mons, closers, err := createExternalHashAggregator(
							ctx, flowCtx, &colexecagg.NewAggregatorArgs{
								Allocator:      testAllocator,
								MemAccount:     testMemAcc,
								Input:          input[0],
								InputTypes:     tc.typs,
								Spec:           tc.spec,
								EvalCtx:        &evalCtx,
								Constructors:   constructors,
								ConstArguments: constArguments,
								OutputTypes:    outputTypes,
							},
							queueCfg, sem, numForcedRepartitions,
						)
						accounts = append(accounts, accs...)
						monitors = append(monitors, mons...)
						require.Equal(t, numExpectedClosers, len(closers))
						if !diskSpillingEnabled {
							// Sanity check that indeed only the in-memory hash
							// aggregator was created.
							_, isHashAgg := op.(*hashAggregator)
							require.True(t, isHashAgg)
						}
						return op, err
					},
				)
				for i, sem := range semsToCheck {
					require.Equal(t, 0, sem.GetCount(), "sem still reports open FDs at index %d", i)
				}
			}
		}
	}
	for _, acc := range accounts {
		acc.Close(ctx)
	}
	for _, mon := range monitors {
		mon.Stop(ctx)
	}
}

func BenchmarkExternalHashAggregator(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
		DiskMonitor: testDiskMonitor,
	}
	var (
		memAccounts []*mon.BoundAccount
		memMonitors []*mon.BytesMonitor
	)

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(b, false /* inMem */)
	defer cleanup()

	aggFn := execinfrapb.Min
	numRows := []int{coldata.BatchSize(), 64 * coldata.BatchSize(), 4096 * coldata.BatchSize()}
	groupSizes := []int{1, 2, 32, 128, coldata.BatchSize()}
	if testing.Short() {
		numRows = []int{64 * coldata.BatchSize()}
		groupSizes = []int{1, coldata.BatchSize()}
	}
	for _, spillForced := range []bool{false, true} {
		flowCtx.Cfg.TestingKnobs.ForceDiskSpill = spillForced
		for _, numInputRows := range numRows {
			for _, groupSize := range groupSizes {
				benchmarkAggregateFunction(
					b, aggType{
						new: func(args *colexecagg.NewAggregatorArgs) (colexecop.ResettableOperator, error) {
							op, accs, mons, _, err := createExternalHashAggregator(
								ctx, flowCtx, args, queueCfg,
								&colexecop.TestingSemaphore{}, 0, /* numForcedRepartitions */
							)
							memAccounts = append(memAccounts, accs...)
							memMonitors = append(memMonitors, mons...)
							// The hash-based partitioner is not a
							// ResettableOperator, so in order to not change the
							// signatures of the aggregator constructors, we
							// wrap it with a noop operator. It is ok for the
							// purposes of this benchmark.
							return colexecop.NewNoop(op), err
						},
						name: fmt.Sprintf("spilled=%t", spillForced),
					},
					aggFn, []*types.T{types.Int}, groupSize,
					0 /* distinctProb */, numInputRows,
				)
			}
		}
	}

	for _, account := range memAccounts {
		account.Close(ctx)
	}
	for _, monitor := range memMonitors {
		monitor.Stop(ctx)
	}
}

// createExternalHashAggregator is a helper function that instantiates a
// disk-backed hash aggregator. It returns an operator and an error as well as
// memory monitors and memory accounts that will need to be closed once the
// caller is done with the operator.
func createExternalHashAggregator(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	newAggArgs *colexecagg.NewAggregatorArgs,
	diskQueueCfg colcontainer.DiskQueueCfg,
	testingSemaphore semaphore.Semaphore,
	numForcedRepartitions int,
) (colexecop.Operator, []*mon.BoundAccount, []*mon.BytesMonitor, []colexecop.Closer, error) {
	spec := &execinfrapb.ProcessorSpec{
		Input: []execinfrapb.InputSyncSpec{{ColumnTypes: newAggArgs.InputTypes}},
		Core: execinfrapb.ProcessorCoreUnion{
			Aggregator: newAggArgs.Spec,
		},
		Post:        execinfrapb.PostProcessSpec{},
		ResultTypes: newAggArgs.OutputTypes,
	}
	args := &colexecargs.NewColOperatorArgs{
		Spec:                spec,
		Inputs:              []colexecargs.OpWithMetaInfo{{Root: newAggArgs.Input}},
		StreamingMemAccount: testMemAcc,
		DiskQueueCfg:        diskQueueCfg,
		FDSemaphore:         testingSemaphore,
	}
	args.TestingKnobs.NumForcedRepartitions = numForcedRepartitions
	result, err := colexecargs.TestNewColOperator(ctx, flowCtx, args)
	return result.Root, result.OpAccounts, result.OpMonitors, result.ToClose, err
}
