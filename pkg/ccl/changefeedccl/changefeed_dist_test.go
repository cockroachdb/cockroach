// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// mockRangeIterator iterates over ranges in a span.
type mockRangeIterator struct {
	rangeDesc *roachpb.RangeDescriptor
}

var _ rangeIterator = (*mockRangeIterator)(nil)

func nextKey(startKey []byte) []byte {
	return []byte{startKey[0] + 1}
}

// Desc implements the rangeIterator interface.
func (ri *mockRangeIterator) Desc() *roachpb.RangeDescriptor {
	return ri.rangeDesc
}

// NeedAnother implements the rangeIterator interface.
func (ri *mockRangeIterator) NeedAnother(rs roachpb.RSpan) bool {
	return ri.rangeDesc.EndKey.Less(rs.EndKey)
}

// Valid implements the rangeIterator interface.
func (ri *mockRangeIterator) Valid() bool {
	return true
}

// Error implements the rangeIterator interface.
func (ri *mockRangeIterator) Error() error {
	panic("unexpected call to Error()")
}

// Next implements the rangeIterator interface.
func (ri *mockRangeIterator) Next(ctx context.Context) {
	ri.rangeDesc.StartKey = nextKey(ri.rangeDesc.StartKey)
	ri.rangeDesc.EndKey = nextKey(ri.rangeDesc.EndKey)
}

// Seek implements the rangeIterator interface.
func (ri *mockRangeIterator) Seek(_ context.Context, key roachpb.RKey, _ kvcoord.ScanDirection) {
	ri.rangeDesc = &roachpb.RangeDescriptor{
		StartKey: key,
		EndKey:   nextKey(key),
	}
}

// TestPartitionSpans unit tests the rebalanceSpanPartitions function.
func TestPartitionSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()

	partitions := func(p ...sql.SpanPartition) []sql.SpanPartition {
		return p
	}
	mkPart := func(n base.SQLInstanceID, spans ...roachpb.Span) sql.SpanPartition {
		return sql.SpanPartition{SQLInstanceID: n, Spans: spans}
	}
	mkSpan := func(start, end string) roachpb.Span {
		return roachpb.Span{Key: []byte(start), EndKey: []byte(end)}
	}
	dedupe := func(in []int) []int {
		ret := intsets.Fast{}
		for _, id := range in {
			ret.Add(id)
		}
		return ret.Ordered()
	}
	copySpans := func(partitions []sql.SpanPartition) (g roachpb.SpanGroup) {
		for _, p := range partitions {
			for _, sp := range p.Spans {
				g.Add(sp)
			}
		}
		return
	}

	const sensitivity = 0.00
	for i, tc := range []struct {
		input  []sql.SpanPartition
		expect []sql.SpanPartition
	}{
		{
			input: partitions(
				mkPart(1, mkSpan("a", "g")), // 6 ranges
				mkPart(2, mkSpan("h", "s")), // 11
				mkPart(3, mkSpan("s", "z")), // 7
			),
			expect: partitions(
				mkPart(1, mkSpan("a", "g"), mkSpan("h", "j")), // 8
				mkPart(2, mkSpan("k", "s")),                   // 8
				mkPart(3, mkSpan("j", "k"), mkSpan("s", "z")), // 8
			),
		},
		{
			input: partitions(
				mkPart(1, mkSpan("a", "c"), mkSpan("e", "p"), mkSpan("r", "z")), // 21
				mkPart(2), // 0
				mkPart(3, mkSpan("c", "e"), mkSpan("p", "r")), // 4
			),
			expect: partitions(
				mkPart(1, mkSpan("o", "p"), mkSpan("r", "z")),                   // 9
				mkPart(2, mkSpan("a", "c"), mkSpan("e", "l")),                   // 9
				mkPart(3, mkSpan("c", "e"), mkSpan("l", "o"), mkSpan("p", "r")), // 7
			),
		},
		{
			input: partitions(
				mkPart(5, mkSpan("c", "f"), mkSpan("a", "c"), mkSpan("f", "j")), // 9
				mkPart(4, mkSpan("k", "t"), mkSpan("j", "k")),                   // 10
				mkPart(3, mkSpan("t", "v")),                                     // 2
				mkPart(2, mkSpan("v", "x")),                                     // 2
				mkPart(1, mkSpan("x", "z")),                                     // 2
			),
			expect: partitions(
				mkPart(5, mkSpan("b", "c"), mkSpan("f", "j")),                   // 5
				mkPart(4, mkSpan("j", "k"), mkSpan("p", "t")),                   // 5
				mkPart(3, mkSpan("a", "b"), mkSpan("d", "f"), mkSpan("t", "v")), // 5                   // 6
				mkPart(2, mkSpan("c", "d"), mkSpan("n", "p"), mkSpan("v", "x")), // 5
				mkPart(1, mkSpan("k", "n"), mkSpan("x", "z")),                   // 5
			),
		},
	} {
		t.Run(fmt.Sprintf("simple-%d", i), func(t *testing.T) {
			sp, err := rebalanceSpanPartitions(context.Background(),
				&mockRangeIterator{}, sensitivity, tc.input)

			require.NoError(t, err)
			require.Equal(t, tc.expect, sp)
		})
	}

	// Create a random input and assert that the output has the same
	// spans as the input.
	t.Run("random", func(t *testing.T) {
		rng, _ := randutil.NewTestRand()
		numPartitions := rng.Intn(8) + 1
		numSpans := rng.Intn(25) + 1

		// Randomly create spans and assign them to nodes. For example,
		// {1 {h-i}, {m-n}, {t-u}}
		// {2 {a-c}, {d-f}, {l-m}, {s-t}, {x-z}}
		// {3 {c-d}, {i-j}, {u-w}}
		// {4 {w-x}}
		// {5 {f-h}, {p-s}}
		// {6 {j-k}, {k-l}, {n-o}, {o-p}}
		input := make([]sql.SpanPartition, numPartitions)
		for i := range input {
			input[i] = mkPart(base.SQLInstanceID(i + 1))
		}
		spanIdxs := make([]int, numSpans)
		for i := range spanIdxs {
			spanIdxs[i] = rng.Intn((int('z')-int('a'))-1) + int('a') + 1
		}
		sort.Slice(spanIdxs, func(i int, j int) bool {
			return spanIdxs[i] < spanIdxs[j]
		})
		spanIdxs = dedupe(spanIdxs)
		for i, key := range spanIdxs {
			assignTo := rng.Intn(numPartitions)
			if i == 0 {
				input[assignTo].Spans = append(input[assignTo].Spans, mkSpan("a", string(rune(key))))
			} else {
				input[assignTo].Spans = append(input[assignTo].Spans, mkSpan(string(rune(spanIdxs[i-1])), string(rune(key))))
			}
		}
		last := rng.Intn(numPartitions)
		input[last].Spans = append(input[last].Spans, mkSpan(string(rune(spanIdxs[len(spanIdxs)-1])), "z"))

		t.Log(input)

		// Ensure the set of input spans matches the set of output spans.
		g1 := copySpans(input)
		output, err := rebalanceSpanPartitions(context.Background(),
			&mockRangeIterator{}, sensitivity, input)
		require.NoError(t, err)

		t.Log(output)

		g2 := copySpans(output)
		require.True(t, g1.Encloses(g2.Slice()...))
		require.True(t, g2.Encloses(g1.Slice()...))
	})
}

type rangeDistributionTester struct {
	ctx context.Context
	t   *testing.T
	tc  *testcluster.TestCluster

	partitionsCh chan []sql.SpanPartition

	sqlDB      *sqlutils.SQLRunner
	lastNode   serverutils.ApplicationLayerInterface
	distSender *kvcoord.DistSender
}

func (rdt *rangeDistributionTester) getPartitions() (partitions []sql.SpanPartition) {
	testutils.SucceedsSoon(rdt.t, func() error {
		select {
		case partitions = <-rdt.partitionsCh:
			return nil
		default:
			return errors.New("no partitions found")
		}
	})
	return
}

func (rdt *rangeDistributionTester) cleanup() {
	rdt.tc.Stopper().Stop(rdt.ctx)
}

// newRangeDistributionTester creates the tester and starts a cluster with 8
// nodes using the supplied localities. It creates a table 'x' with 64 ranges
// and distributes the leaseholders across the first 6 nodes using an
// exponential distribution.
func newRangeDistributionTester(
	t *testing.T, localityFunc func(nodeIdx int) []roachpb.Tier,
) *rangeDistributionTester {
	partitionsCh := make(chan []sql.SpanPartition, 1)

	const nodes = 8
	args := base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{},
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestTenantProbabilistic,
		},
	}
	for i := 0; i < nodes; i++ {
		args.ServerArgsPerNode[i] = base.TestServerArgs{
			Locality: roachpb.Locality{
				Tiers: localityFunc(i),
			},
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				DistSQL: &execinfra.TestingKnobs{
					Changefeed: &TestingKnobs{
						SpanPartitionsCallback: func(partitions []sql.SpanPartition) {
							select {
							case partitionsCh <- partitions:
							default:
								t.Logf("skipping partition %v", partitions)
							}
						},
					},
				},
			},
		}
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, nodes, args)

	lastNode := tc.Server(len(tc.Servers) - 1).ApplicationLayer()
	sqlDB := sqlutils.MakeSQLRunner(lastNode.SQLConn(t))
	distSender := lastNode.DistSenderI().(*kvcoord.DistSender)

	systemDB := sqlutils.MakeSQLRunner(tc.SystemLayer(len(tc.Servers) - 1).SQLConn(t))
	systemDB.Exec(t, "SET CLUSTER SETTING kv.rangefeed.enabled = true")
	if tc.StartedDefaultTestTenant() {
		systemDB.Exec(t, `ALTER TENANT [$1] GRANT CAPABILITY can_admin_relocate_range=true`, serverutils.TestTenantID().ToUint64())
	}

	// Use manual replication only.
	tc.ToggleReplicateQueues(false)

	sqlDB.ExecMultiple(t,
		"CREATE TABLE x (id INT PRIMARY KEY)",
		"INSERT INTO x SELECT generate_series(0, 63)",
		"ALTER TABLE x SPLIT AT SELECT id FROM x WHERE id > 0",
	)

	// Distribute the leases exponentially across the first 5 nodes.
	for i := 0; i < 64; i += 1 {
		nodeID := int(math.Floor(math.Log2(float64(i)))) + 1
		cmd := fmt.Sprintf(`ALTER TABLE x EXPERIMENTAL_RELOCATE VALUES (ARRAY[%d], %d)`,
			nodeID, i,
		)
		// Relocate can fail with errors like `change replicas... descriptor changed` thus the SucceedsSoon.
		sqlDB.ExecSucceedsSoon(t, cmd)
	}

	return &rangeDistributionTester{
		ctx:          ctx,
		t:            t,
		tc:           tc,
		partitionsCh: partitionsCh,

		sqlDB:      sqlDB,
		lastNode:   lastNode,
		distSender: distSender,
	}
}

// countRanges returns an array where each index i stores the ranges assigned to node i.
func (rdt *rangeDistributionTester) countRanges(partitions []sql.SpanPartition) []int {
	ri := kvcoord.MakeRangeIterator(rdt.distSender)
	var counts = make([]int, 8)
	for _, p := range partitions {
		for _, sp := range p.Spans {
			rSpan, err := keys.SpanAddr(sp)
			require.NoError(rdt.t, err)
			for ri.Seek(rdt.ctx, rSpan.Key, kvcoord.Ascending); ; ri.Next(rdt.ctx) {
				if !ri.Valid() {
					rdt.t.Fatal(ri.Error())
				}
				counts[p.SQLInstanceID-1] += 1
				if !ri.NeedAnother(rSpan) {
					break
				}
			}
		}

	}
	return counts
}

// When balancing 64 ranges across numNodes, we aim for at most (1 + rebalanceThreshold) * (64 / numNodes)
// ranges per node. We add an extra 10% to the threshold for extra tolerance during testing.
func (rdt *rangeDistributionTester) balancedDistributionUpperBound(numNodes int) int {
	return int(math.Ceil((1 + rebalanceThreshold.Get(&rdt.lastNode.ClusterSettings().SV) + 0.1) * 64 / float64(numNodes)))
}

func TestChangefeedDistributionStrategy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// The test is slow.
	skip.UnderShort(t)
	skip.UnderStress(t)
	skip.UnderRace(t)

	noLocality := func(i int) []roachpb.Tier {
		return make([]roachpb.Tier, 0)
	}

	// The replica oracle selects the leaseholder replica for each range. Then, distsql assigns the replica
	// to the same node which stores it. No load balancing is performed afterwards. Thus, the distribution of
	// ranges assigned to nodes is the same as the distribution of leaseholders on nodes.
	t.Run("locality=none,distribution=none", func(t *testing.T) {
		tester := newRangeDistributionTester(t, noLocality)
		defer tester.cleanup()

		tester.sqlDB.Exec(t, "SET CLUSTER SETTING changefeed.default_range_distribution_strategy = 'none'")
		tester.sqlDB.Exec(t, "CREATE CHANGEFEED FOR x INTO 'null://' WITH initial_scan='no'")
		partitions := tester.getPartitions()
		counts := tester.countRanges(partitions)
		require.True(t, reflect.DeepEqual(counts, []int{2, 2, 4, 8, 16, 32, 0, 0}),
			"unexpected counts %v, partitions: %v", counts, partitions)
	})

	// The replica oracle selects the leaseholder replica for each range. Then, distsql assigns the replica
	// to the same node which stores it. Afterwards, load balancing is performed to attempt an even distribution.
	// Check that we roughly assign (64 ranges / 6 nodes) ranges to each node.
	t.Run("locality=none,distribution=balanced_simple", func(t *testing.T) {
		tester := newRangeDistributionTester(t, noLocality)
		defer tester.cleanup()
		tester.sqlDB.Exec(t, "SET CLUSTER SETTING changefeed.default_range_distribution_strategy = 'balanced_simple'")
		tester.sqlDB.Exec(t, "CREATE CHANGEFEED FOR x INTO 'null://' WITH initial_scan='no'")
		partitions := tester.getPartitions()
		counts := tester.countRanges(partitions)
		upper := int(math.Ceil((1 + rebalanceThreshold.Get(&tester.lastNode.ClusterSettings().SV)) * 64 / 6))
		for _, count := range counts {
			require.LessOrEqual(t, count, upper, "counts %v contains value greater than upper bound %d",
				counts, upper)
		}
	})

	// The replica oracle selects the leaseholder replica for each range. Then, distsql assigns the replica
	// to the same node which stores it. However, node of these nodes don't pass the filter. The replicas assigned
	// to these nodes are distributed arbitrarily to any nodes which pass the filter.
	t.Run("locality=constrained,distribution=none", func(t *testing.T) {
		tester := newRangeDistributionTester(t, func(i int) []roachpb.Tier {
			if i%2 == 1 {
				return []roachpb.Tier{{Key: "y", Value: "1"}}
			}
			return []roachpb.Tier{}
		})
		defer tester.cleanup()
		tester.sqlDB.Exec(t, "SET CLUSTER SETTING changefeed.default_range_distribution_strategy = 'none'")
		tester.sqlDB.Exec(t, "CREATE CHANGEFEED FOR x INTO 'null://' WITH initial_scan='no', execution_locality='y=1'")
		partitions := tester.getPartitions()
		counts := tester.countRanges(partitions)

		totalRanges := 0
		for i, count := range counts {
			if i%2 == 1 {
				totalRanges += count
			} else {
				require.Equal(t, count, 0)
			}
		}
		require.Equal(t, totalRanges, 64)
	})

	// The replica oracle selects the leaseholder replica for each range. Then, distsql assigns the replica
	// to the same node which stores it. However, node of these nodes don't pass the filter. The replicas assigned
	// to these nodes are distributed arbitrarily to any nodes which pass the filter.
	// Afterwards, we perform load balancing on this set of nodes.
	t.Run("locality=constrained,distribution=balanced_simple", func(t *testing.T) {
		tester := newRangeDistributionTester(t, func(i int) []roachpb.Tier {
			if i%2 == 1 {
				return []roachpb.Tier{{Key: "y", Value: "1"}}
			}
			return []roachpb.Tier{}
		})
		defer tester.cleanup()
		tester.sqlDB.Exec(t, "SET CLUSTER SETTING changefeed.default_range_distribution_strategy = 'balanced_simple'")
		tester.sqlDB.Exec(t, "CREATE CHANGEFEED FOR x INTO 'null://' WITH initial_scan='no', execution_locality='y=1'")
		partitions := tester.getPartitions()
		counts := tester.countRanges(partitions)

		upper := tester.balancedDistributionUpperBound(len(partitions))
		totalRanges := 0
		for i, count := range counts {
			if i%2 == 1 {
				require.LessOrEqual(t, count, upper)
				totalRanges += count
			} else {
				require.Equal(t, count, 0)
			}
		}
		require.Equal(t, totalRanges, 64)
	})
}
