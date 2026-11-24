// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"context"
	"fmt"
	sort "sort"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
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
		{
			name:   "lots of processors",
			before: makePlan(makeProc(1, []int{1}), makeProc(1, []int{1}), makeProc(1, []int{1})),
			after:  makePlan(makeProc(1, []int{1}), makeProc(1, []int{1}), makeProc(1, []int{1}), makeProc(2, []int{1})),
			frac:   0.5,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frac := sql.MeasurePlanChange(&tc.before, &tc.after, getNodes)
			require.Equal(t, tc.frac, frac)
		})
	}
}

func fakeTopology(nls []sql.InstanceLocality, tenantSpan roachpb.Span) streamclient.Topology {
	topology := streamclient.Topology{
		SourceTenantID: roachpb.TenantID{InternalValue: uint64(2)},
		Partitions:     make([]streamclient.PartitionInfo, 0, len(nls)),
	}

	// EndKey will get moved Key on the first iteration.
	sp := roachpb.Span{EndKey: tenantSpan.Key}

	for i, nl := range nls {
		sp = roachpb.Span{Key: sp.EndKey, EndKey: sp.EndKey.Next()}
		if i == len(nls)-1 {
			sp.EndKey = tenantSpan.EndKey
		}
		partition := streamclient.PartitionInfo{
			ID:          nl.GetInstanceID().String(),
			SrcLocality: nl.GetLocality(),
			Spans:       roachpb.Spans{sp},
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
	defer log.Scope(t).Close(t)

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
	validatePairs := func(t *testing.T,
		sipSpecs map[base.SQLInstanceID][]execinfrapb.StreamIngestionDataSpec,
		expected map[pair]struct{}) {
		for dstID, sips := range sipSpecs {
			for _, spec := range sips {
				require.True(t, len(spec.PartitionSpecs) > 0, "empty node %s included in partition specs", dstID)
				for srcID := range spec.PartitionSpecs {
					srcIDNum, err := strconv.Atoi(srcID)
					require.NoError(t, err)
					expectKey := pair{srcIDNum, int(dstID)}
					_, ok := expected[expectKey]
					require.True(t, ok, "Src %s,Dst %d do not match", srcID, dstID)
					delete(expected, expectKey)
				}
			}
		}
		require.Equal(t, 0, len(expected), "expected matches not included")
	}

	// validateEvenDistribution tests that source node assignments were evenly
	// distributed across destination nodes. This function is only called on test
	// cases without an expected exact src-dst node match.
	validateEvenDistribution := func(t *testing.T, sipSpecs map[base.SQLInstanceID][]execinfrapb.StreamIngestionDataSpec, dstNodes []sql.InstanceLocality) {
		require.Equal(t, len(sipSpecs), len(dstNodes))
		dstNodeAssignmentCount := make(map[base.SQLInstanceID]int, len(dstNodes))
		for dstID, specs := range sipSpecs {
			dstNodeAssignmentCount[dstID] = 0
			for _, spec := range specs {
				for range spec.PartitionSpecs {
					dstNodeAssignmentCount[dstID]++
				}
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
		{
			name:          "ensure nodes with no work are not included in specs",
			srcNodes:      nls(nl(1, "a=a")),
			dstNodes:      nls(nl(99, "a=a"), nl(98, "a=b")),
			expectedPairs: pairs(mkPair(1, 99)),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sipSpecs, _, err := constructStreamIngestionPlanSpecs(
				ctx,
				fakeTopology(tc.srcNodes, keys.MakeTenantSpan(roachpb.TenantID{InternalValue: 2})),
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
				validatePairs(t, sipSpecs, tc.expectedPairs)
			} else {
				validateEvenDistribution(t, sipSpecs, tc.dstNodes)
			}
		})
	}
}

type testSplitter struct {
	mu struct {
		// fields that may get updated while read are put in the lock.
		syncutil.Mutex

		splits   []roachpb.Key
		scatters []roachpb.Key
	}

	splitErr   func(key roachpb.Key) error
	scatterErr func(key roachpb.Key) error
}

func (ts *testSplitter) split(_ context.Context, splitKey roachpb.Key, _ hlc.Timestamp) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.mu.splits = append(ts.mu.splits, splitKey)
	if ts.splitErr != nil {
		return ts.splitErr(splitKey)
	}
	return nil
}

func (ts *testSplitter) scatter(_ context.Context, scatterKey roachpb.Key) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.mu.scatters = append(ts.mu.scatters, scatterKey)
	if ts.scatterErr != nil {
		return ts.scatterErr(scatterKey)
	}
	return nil
}

func (ts *testSplitter) now() hlc.Timestamp {
	return hlc.Timestamp{}
}

func (ts *testSplitter) sortOutput() {
	sort.Slice(ts.mu.splits, func(i, j int) bool {
		return ts.mu.splits[i].Compare(ts.mu.splits[j]) < 0
	})

	sort.Slice(ts.mu.scatters, func(i, j int) bool {
		return ts.mu.scatters[i].Compare(ts.mu.scatters[j]) < 0
	})
}

func TestCreateInitialSplits(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	sourceTenantID := roachpb.MustMakeTenantID(11)
	inputCodec := keys.MakeSQLCodec(sourceTenantID)
	destTenantID := roachpb.MustMakeTenantID(12)
	outputCodec := keys.MakeSQLCodec(destTenantID)
	rng, _ := randutil.NewTestRand()
	numDestnodes := rng.Intn(4) + 1

	testSpan := func(codec keys.SQLCodec, tableID uint32) roachpb.Span {
		return roachpb.Span{
			Key:    codec.IndexPrefix(tableID, 1),
			EndKey: codec.IndexPrefix(tableID, 2),
		}
	}
	inputSpans := []roachpb.Span{
		testSpan(inputCodec, 100),
		testSpan(inputCodec, 200),
		testSpan(inputCodec, 300),
		testSpan(inputCodec, 400),
	}
	outputSpans := []roachpb.Span{
		testSpan(outputCodec, 100),
		testSpan(outputCodec, 200),
		testSpan(outputCodec, 300),
		testSpan(outputCodec, 400),
	}
	topo := streamclient.Topology{
		SourceTenantID: sourceTenantID,
		Partitions: []streamclient.PartitionInfo{
			{Spans: inputSpans},
		},
	}

	t.Run("rekeys before splitting", func(t *testing.T) {
		ts := &testSplitter{}
		err := createInitialSplits(ctx, keys.SystemSQLCodec, ts, topo, numDestnodes, destTenantID)
		require.NoError(t, err)
		expectedSplitsAndScatters := make([]roachpb.Key, 0, len(outputSpans))
		for _, sp := range outputSpans {
			expectedSplitsAndScatters = append(expectedSplitsAndScatters, sp.Key)
		}

		ts.sortOutput()

		require.Equal(t, expectedSplitsAndScatters, ts.mu.splits)
		require.Equal(t, expectedSplitsAndScatters, ts.mu.scatters)

	})
	t.Run("split errors are fatal", func(t *testing.T) {
		require.Error(t, createInitialSplits(ctx, keys.SystemSQLCodec, &testSplitter{
			splitErr: func(_ roachpb.Key) error {
				return errors.New("test error")
			},
		}, topo, numDestnodes, destTenantID))
	})
	t.Run("ignores scatter errors", func(t *testing.T) {
		require.NoError(t, createInitialSplits(ctx, keys.SystemSQLCodec, &testSplitter{
			scatterErr: func(_ roachpb.Key) error {
				return errors.New("test error")
			},
		}, topo, numDestnodes, destTenantID))
	})
}

func TestParallelInitialSplits(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	rng, _ := randutil.NewTestRand()
	sourceTenantID := roachpb.MustMakeTenantID(11)
	inputCodec := keys.MakeSQLCodec(sourceTenantID)
	destTenantID := roachpb.MustMakeTenantID(12)
	outputCodec := keys.MakeSQLCodec(destTenantID)

	minSpans := 5
	numSpans := rng.Intn(20) + minSpans + 1
	numPartitions := rng.Intn(minSpans) + 1
	numDestNodes := rng.Intn(minSpans) + 1

	testSpan := func(codec keys.SQLCodec, tableID uint32) roachpb.Span {
		return roachpb.Span{
			Key:    codec.IndexPrefix(tableID, 1),
			EndKey: codec.IndexPrefix(tableID, 2),
		}
	}

	genPartitions := func() ([]streamclient.PartitionInfo, roachpb.Spans, roachpb.Spans) {
		srcPartitions := make([]streamclient.PartitionInfo, numPartitions)
		sortedInputSpans := make(roachpb.Spans, 0, numSpans)
		outputSpans := make(roachpb.Spans, 0, numSpans)
		for i := range srcPartitions {
			srcPartitions[i].Spans = make(roachpb.Spans, 0, numSpans/numPartitions)
		}
		for i := 0; i < numSpans; i++ {
			partitionIdx := rng.Intn(numPartitions)
			tableID := uint32((i + 1) * 100)
			srcSP := testSpan(inputCodec, tableID)
			dstSP := testSpan(outputCodec, tableID)
			srcPartitions[partitionIdx].Spans = append(srcPartitions[partitionIdx].Spans, srcSP)
			sortedInputSpans = append(sortedInputSpans, srcSP)
			outputSpans = append(outputSpans, dstSP)
		}
		return srcPartitions, sortedInputSpans, outputSpans
	}

	srcPartitions, sortedInputSpans, outputSpans := genPartitions()

	// Test the Span Sorter
	require.Equal(t, sortSpans(srcPartitions), sortedInputSpans)

	topo := streamclient.Topology{
		SourceTenantID: sourceTenantID,
		Partitions:     srcPartitions,
	}

	ts := &testSplitter{}
	err := createInitialSplits(ctx, keys.SystemSQLCodec, ts, topo, numDestNodes, destTenantID)
	require.NoError(t, err)
	require.Equal(t, len(outputSpans), len(ts.mu.splits))
	require.Equal(t, len(outputSpans), len(ts.mu.scatters))

	ts.sortOutput()

	for i, sp := range outputSpans {
		require.Equal(t, sp.Key.String(), ts.mu.splits[i].String())
		require.Equal(t, sp.Key.String(), ts.mu.scatters[i].String())
	}
}

func TestRepartition(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p := func(node, parts, start int) streamclient.PartitionInfo {
		spans := make([]roachpb.Span, parts)
		for i := range spans {
			spans[i].Key = roachpb.Key(fmt.Sprintf("n%d-%d-a", node, i+start))
			spans[i].EndKey = roachpb.Key(fmt.Sprintf("n%d-%d-b", node, i+start))
		}
		return streamclient.PartitionInfo{SrcInstanceID: node, Spans: spans}
	}
	for _, parts := range []int{1, 4, 100} {
		for _, input := range [][]streamclient.PartitionInfo{
			{p(1, 43, 0), p(2, 44, 0), p(3, 41, 0)},
			{p(1, 1, 0), p(2, 1, 0), p(3, 1, 0)},
			{p(1, 43, 0), p(2, 44, 0), p(3, 38, 0)},
		} {
			got, err := repartitionTopology(streamclient.Topology{Partitions: input}, parts)

			require.NoError(t, err)

			var expectedSpans, gotSpans roachpb.Spans
			for _, part := range input {
				expectedSpans = append(expectedSpans, part.Spans...)
			}
			for _, part := range got.Partitions {
				gotSpans = append(gotSpans, part.Spans...)
			}
			require.LessOrEqual(t, min(parts, len(input)), len(got.Partitions))
			require.GreaterOrEqual(t, max(parts, len(input)), len(got.Partitions))

			// Regardless of how we partitioned, make sure we have all the spans.
			sort.Sort(expectedSpans)
			sort.Sort(gotSpans)
			require.Equal(t, len(expectedSpans), len(gotSpans))
			require.Equal(t, expectedSpans, gotSpans)
		}
	}
}
