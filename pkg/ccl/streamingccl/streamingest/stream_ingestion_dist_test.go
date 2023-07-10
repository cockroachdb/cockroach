// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestMeasurePlanChange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	makeProc := func(dstID base.SQLInstanceID, srcIDs []int) physicalplan.Processor {
		partitionMap := map[string]execinfrapb.StreamIngestionPartitionSpec{}
		for _, id := range srcIDs {
			partitionMap[fmt.Sprint(id)] = execinfrapb.StreamIngestionPartitionSpec{}
		}
		return physicalplan.Processor{SQLInstanceID: dstID,
			Spec: execinfrapb.ProcessorSpec{
				Core: execinfrapb.ProcessorCoreUnion{
					StreamIngestionData: &execinfrapb.StreamIngestionDataSpec{
						PartitionSpecs: partitionMap}}}}
	}

	makePlan := func(procs ...physicalplan.Processor) sql.PhysicalPlan {
		plan := sql.PhysicalPlan{}
		plan.PhysicalInfrastructure = &physicalplan.PhysicalInfrastructure{Processors: procs}
		return plan
	}

	for _, tc := range []struct {
		name   string
		before sql.PhysicalPlan
		after  sql.PhysicalPlan
		frac   float64
	}{
		{
			name:   "same node",
			before: makePlan(makeProc(1, []int{1})),
			after:  makePlan(makeProc(1, []int{1})),
			frac:   0,
		},
		{
			name:   "same nodes; swapped order",
			before: makePlan(makeProc(1, []int{1}), makeProc(1, []int{2})),
			after:  makePlan(makeProc(1, []int{2}), makeProc(1, []int{1})),
			frac:   0,
		},
		{
			name:   "dropped and added dest node",
			before: makePlan(makeProc(1, []int{1})),
			after:  makePlan(makeProc(2, []int{1})),
			frac:   1,
		},
		{
			name:   "added src and dest node",
			before: makePlan(makeProc(1, []int{1})),
			after:  makePlan(makeProc(1, []int{1}), makeProc(2, []int{2})),
			frac:   1,
		},
		{
			name:   "dropped src and dest node",
			before: makePlan(makeProc(1, []int{1}), makeProc(2, []int{2})),
			after:  makePlan(makeProc(1, []int{1})),
			frac:   0.5,
		},
		{
			name:   "added dest node",
			before: makePlan(makeProc(1, []int{1, 2, 3})),
			after:  makePlan(makeProc(1, []int{1, 3}), makeProc(2, []int{2})),
			frac:   .25,
		},
		{
			name:   "dropped dest node",
			before: makePlan(makeProc(1, []int{1, 3}), makeProc(2, []int{2})),
			after:  makePlan(makeProc(1, []int{1, 2, 3})),
			frac:   .2,
		},
		{
			name:   "dropped and added src node",
			before: makePlan(makeProc(1, []int{1})),
			after:  makePlan(makeProc(1, []int{2})),
			frac:   1,
		},
		{
			name:   "new src node",
			before: makePlan(makeProc(1, []int{1})),
			after:  makePlan(makeProc(1, []int{1, 2})),
			frac:   0.5,
		},
		{
			name:   "dropped src node",
			before: makePlan(makeProc(1, []int{1, 2, 3})),
			after:  makePlan(makeProc(1, []int{1, 2})),
			frac:   .25,
		},
		{
			name:   "swapped dest node",
			before: makePlan(makeProc(1, []int{1}), makeProc(2, []int{2})),
			after:  makePlan(makeProc(1, []int{2}), makeProc(2, []int{1})),
			frac:   0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frac := measurePlanChange(&tc.before, &tc.after)
			require.Equal(t, tc.frac, frac)
		})
	}
}

func fakeTopology(nls []sql.InstanceLocality) streamclient.Topology {
	topology := streamclient.Topology{
		SourceTenantID: roachpb.TenantID{InternalValue: uint64(2)},
		Partitions:     make([]streamclient.PartitionInfo, 0, len(nls)),
	}
	for _, nl := range nls {
		partition := streamclient.PartitionInfo{
			ID:          nl.GetInstanceID().String(),
			SrcLocality: nl.GetLocality(),
		}
		topology.Partitions = append(topology.Partitions, partition)
	}
	return topology
}

// TestSourceDestMatching checks for an expected matching on a given src-dest
// node topology. The matching algorithm prioritizes matching nodes with common
// localities and tie breaks by distributing load across destination nodes
// evenly.
func TestSourceDestMatching(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	rng, seed := randutil.NewTestRand()
	t.Logf("Random Seed %d", seed)

	nl := func(id int, locality string) sql.InstanceLocality {
		localityStruct := roachpb.Locality{}
		require.NoError(t, localityStruct.Set(locality))
		return sql.MakeInstanceLocality(base.SQLInstanceID(id), localityStruct)
	}

	nls := func(nodes ...sql.InstanceLocality) []sql.InstanceLocality {
		for i := len(nodes) - 1; i > 0; i-- {
			j := rng.Intn(i + 1)
			nodes[i], nodes[j] = nodes[j], nodes[i]
		}
		return nodes
	}
	type pair struct {
		srcID int
		dstID int
	}

	// validatePairs tests that src-dst assignments are expected.
	validatePairs := func(sipSpecs []*execinfrapb.StreamIngestionDataSpec, dstNodes []sql.InstanceLocality,
		expected map[pair]struct{}) {
		for i, spec := range sipSpecs {

			// SIPs are created in the order of the destination node ids
			dstID := dstNodes[i].GetInstanceID()
			for srcID := range spec.PartitionSpecs {
				srcIDNum, err := strconv.Atoi(srcID)
				require.NoError(t, err)
				_, ok := expected[pair{srcIDNum, int(dstID)}]
				require.True(t, ok, "Src %s,Dst %d do not match", srcID, dstID)
			}
		}
	}

	// validateEvenDistribution tests that source node assignments were evenly
	// distributed across destination nodes. This function is only called on test
	// cases without an expected exact src-dst node match.
	validateEvenDistribution := func(sipSpecs []*execinfrapb.StreamIngestionDataSpec, dstNodes []sql.InstanceLocality) {

		dstNodeAssignmentCount := make(map[base.SQLInstanceID]int, len(dstNodes))
		for i, spec := range sipSpecs {

			// SIPs are created in the order of the destination node ids
			dstID := dstNodes[i].GetInstanceID()
			for range spec.PartitionSpecs {
				dstNodeAssignmentCount[dstID]++
			}
		}

		var expectedCount int
		for _, count := range dstNodeAssignmentCount {
			if expectedCount == 0 {
				expectedCount = count
			} else {
				require.Equal(t, expectedCount, count)
			}
		}
	}

	mkPair := func(srcID int, dstID int) pair {
		return pair{srcID: srcID, dstID: dstID}
	}

	pairs := func(pairList ...pair) map[pair]struct{} {
		pairSet := make(map[pair]struct{}, len(pairList))
		for _, p := range pairList {
			pairSet[p] = struct{}{}
		}
		return pairSet
	}

	// For a variety of src dest topologies, ensure the right matching or distribution occurs.
	for _, tc := range []struct {
		name          string
		srcNodes      []sql.InstanceLocality
		dstNodes      []sql.InstanceLocality
		expectedPairs map[pair]struct{}
	}{
		{
			name:          "two matched regions",
			srcNodes:      nls(nl(1, "a=x"), nl(2, "a=y")),
			dstNodes:      nls(nl(99, "a=x"), nl(98, "a=y")),
			expectedPairs: pairs(mkPair(1, 99), mkPair(2, 98)),
		},
		{
			name:          "one mismatched region",
			srcNodes:      nls(nl(1, "a=x"), nl(2, "a=z")),
			dstNodes:      nls(nl(99, "a=x"), nl(98, "a=y")),
			expectedPairs: pairs(mkPair(1, 99), mkPair(2, 98)),
		},
		{
			name:          "prioritize region match over even distribution",
			srcNodes:      nls(nl(1, "a=x"), nl(2, "a=x")),
			dstNodes:      nls(nl(99, "a=x"), nl(98, "a=y")),
			expectedPairs: pairs(mkPair(1, 99), mkPair(2, 99)),
		},
		{
			name:          "multi tiered match",
			srcNodes:      nls(nl(1, "a=x,b=x"), nl(2, "a=x,b=y")),
			dstNodes:      nls(nl(99, "a=x,b=x"), nl(98, "a=x,b=y")),
			expectedPairs: pairs(mkPair(1, 99), mkPair(2, 98)),
		},
		{
			name:          "prioritize stronger match",
			srcNodes:      nls(nl(1, "a=x"), nl(2, "a=x,b=y")),
			dstNodes:      nls(nl(99, "a=x"), nl(98, "a=x,b=y")),
			expectedPairs: pairs(mkPair(1, 99), mkPair(2, 98)),
		},
		{
			name:          "prioritize stronger match with fewer locality tiers",
			srcNodes:      nls(nl(1, "a=x"), nl(2, "a=z,b=y")),
			dstNodes:      nls(nl(99, "a=x"), nl(98, "a=y,b=y")),
			expectedPairs: pairs(mkPair(1, 99), mkPair(2, 98)),
		},
		{
			name:     "ensure even distribution within region",
			srcNodes: nls(nl(1, "a=x"), nl(2, "a=x"), nl(3, "a=x")),
			dstNodes: nls(nl(99, "a=x"), nl(98, "a=x"), nl(97, "a=x")),
		},
		{
			name:     "ensure even distribution in overloaded destination nodes",
			srcNodes: nls(nl(1, "a=x"), nl(2, "a=x"), nl(3, "a=x"), nl(4, "a=x")),
			dstNodes: nls(nl(99, "a=x"), nl(98, "a=x")),
		},
		{
			name:     "ensure even distribution across mismatched regions",
			srcNodes: nls(nl(1, "a=x"), nl(2, "a=y"), nl(3, "a=z")),
			dstNodes: nls(nl(99, "a=a"), nl(98, "a=b"), nl(97, "a=c")),
		},
		{
			name:     "ensure even distribution in overloaded mismatched destination nodes",
			srcNodes: nls(nl(1, "a=x"), nl(2, "a=y"), nl(3, "a=z"), nl(4, "a=x")),
			dstNodes: nls(nl(99, "a=a"), nl(98, "a=b")),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fakeStreamAddress := streamingccl.StreamAddress("")
			sipSpecs, _, err := constructStreamIngestionPlanSpecs(
				ctx,
				fakeStreamAddress,
				fakeTopology(tc.srcNodes),
				tc.dstNodes,
				hlc.Timestamp{},
				hlc.Timestamp{},
				jobspb.StreamIngestionCheckpoint{},
				jobspb.InvalidJobID,
				streampb.StreamID(2),
				roachpb.TenantID{InternalValue: 2},
				roachpb.TenantID{InternalValue: 2},
			)
			require.NoError(t, err)
			if len(tc.expectedPairs) > 0 {
				validatePairs(sipSpecs, tc.dstNodes, tc.expectedPairs)
			} else {
				validateEvenDistribution(sipSpecs, tc.dstNodes)
			}
		})
	}
}
