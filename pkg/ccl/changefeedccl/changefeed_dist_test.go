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
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var partitions = func(p ...sql.SpanPartition) []sql.SpanPartition {
	return p
}

var mkPart = func(n base.SQLInstanceID, spans ...roachpb.Span) sql.SpanPartition {
	return sql.SpanPartition{SQLInstanceID: n, Spans: spans}
}

// mkRange makes a range containing a single rune.
var mkRange = func(val rune) (s roachpb.Span) {
	s.Key = append(s.Key, byte(val))
	s.EndKey = append(s.EndKey, byte(val+1))
	return s
}

// mkSpan makes a span for [start, end).
var mkSpan = func(start, end rune) (s roachpb.Span) {
	s.Key = append(s.Key, byte(start))
	s.EndKey = append(s.EndKey, byte(end))
	return s
}

// mkSingleLetterRanges makes returns one span for each rune in [start, end]
// which represents a range.
var mkSingleLetterRanges = func(start, end rune) (result []roachpb.Span) {
	for r := start; r < end; r++ {
		result = append(result, mkRange(r))
	}
	return result
}

// letterRangeResolver resolves spans such that each letter is a range.
type letterRangeResolver struct{}

func (r *letterRangeResolver) getRangesForSpans(
	_ context.Context, inSpans []roachpb.Span,
) (spans []roachpb.Span, _ error) {
	for _, sp := range inSpans {
		spans = append(spans, mkSingleLetterRanges(rune(sp.Key[0]), rune(sp.EndKey[0]))...)
	}
	return spans, nil
}

// TestPartitionSpans unit tests the rebalanceSpanPartitions function.
func TestPartitionSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const sensitivity = 0.01

	// 26 nodes, 1 range per node.
	make26NodesBalanced := func() (p []sql.SpanPartition) {
		for i := rune(0); i < 26; i += 1 {
			p = append(p, sql.SpanPartition{
				SQLInstanceID: base.SQLInstanceID(i + 1),
				Spans:         []roachpb.Span{mkRange('a' + i)},
			})
		}
		return p
	}

	// 26 nodes. All empty except for the first, which has 26 ranges.
	make26NodesImBalanced := func() (p []sql.SpanPartition) {
		for i := rune(0); i < 26; i += 1 {
			sp := sql.SpanPartition{
				SQLInstanceID: base.SQLInstanceID(i + 1),
			}
			if i == 0 {
				sp.Spans = append(sp.Spans, mkSpan('a', 'z'+1))
			}
			p = append(p, sp)
		}
		return p
	}

	for _, tc := range []struct {
		name   string
		input  []sql.SpanPartition
		expect []sql.SpanPartition
	}{
		{
			name: "simple",
			input: partitions(
				mkPart(1, mkSpan('a', 'j')),   // 9
				mkPart(2, mkSpan('j', 'q')),   // 7
				mkPart(3, mkSpan('q', 'z'+1)), // 10
			),
			expect: partitions(
				mkPart(1, mkSpan('a', 'j')),               // 9
				mkPart(2, mkSpan('j', 'q'), mkRange('z')), // 8
				mkPart(3, mkSpan('q', 'z')),               // 9
			),
		},
		{
			name: "empty partition",
			input: partitions(
				mkPart(1, mkSpan('a', 'c'), mkSpan('e', 'p'), mkSpan('r', 'z')), // 21
				mkPart(2), // 0
				mkPart(3, mkSpan('c', 'e'), mkSpan('p', 'r')), // 4
			),
			expect: partitions(
				mkPart(1, mkSpan('a', 'c'), mkSpan('e', 'l')), // 9
				mkPart(2, mkSpan('r', 'z')),                   // 8
				mkPart(3, mkSpan('c', 'e'), mkSpan('l', 'r')), // 8
			),
		},
		{
			name: "one node with many ranges",
			input: partitions(
				mkPart(1, mkSingleLetterRanges('a', 'y')...), // 24
				mkPart(2, mkRange('y')),                      // 1
				mkPart(3, mkRange('z')),                      // 1
			),
			expect: partitions(
				mkPart(1, mkSpan('a', 'k')),                   // 10
				mkPart(2, mkSpan('k', 'r'), mkSpan('y', 'z')), // 8
				mkPart(3, mkSpan('r', 'y'), mkRange('z')),     // 7
			),
		},
		{
			name: "no rebalance with one node",
			input: partitions(
				mkPart(1, mkSpan('a', 'z'+1)), // 26
			),
			expect: partitions(
				mkPart(1, mkSpan('a', 'z'+1)), // 26
			),
		},
		{
			name:   "25 nodes balanced",
			input:  make26NodesBalanced(),
			expect: make26NodesBalanced(),
		},
		{
			name:   "25 nodes imbalanced",
			input:  make26NodesImBalanced(),
			expect: make26NodesBalanced(),
		},
		{
			name: "non alphabetical",
			input: partitions(
				mkPart(1, mkRange('z'), mkRange('y')),
				mkPart(2, mkRange('x'), mkRange('w')),
				mkPart(3, mkRange('v'), mkRange('u')),
				mkPart(4, mkRange('t'), mkRange('s')),
			),
			expect: partitions(
				mkPart(1, mkSpan('y', 'z'+1)),
				mkPart(2, mkSpan('w', 'y')),
				mkPart(3, mkSpan('u', 'w')),
				mkPart(4, mkSpan('s', 'u')),
			),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sp, err := rebalanceSpanPartitions(context.Background(),
				&letterRangeResolver{}, sensitivity, tc.input)
			t.Log("expected partitions")
			for _, p := range tc.expect {
				t.Log(p)
			}
			t.Log("actual partitions")
			for _, p := range sp {
				t.Log(p)
			}
			require.NoError(t, err)
			require.Equal(t, tc.expect, sp)
		})
	}
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
	rdt.t.Logf("found partitions: %v", partitions)
	return
}

func (rdt *rangeDistributionTester) cleanup() {
	rdt.tc.Stopper().Stop(rdt.ctx)
}

// newRangeDistributionTester creates the tester and starts a cluster with 8
// nodes using the supplied localities. It creates a table 'x' with 64 ranges
// and distributes the leaseholders across the first 6 nodes using an
// exponential distribution ([]int{2, 2, 4, 8, 16, 32, 0, 0}).
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

// countRangesPerNode returns an array where each index i stores the ranges assigned to node i.
func (rdt *rangeDistributionTester) countRangesPerNode(partitions []sql.SpanPartition) []int {
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
	rdt.t.Logf("range counts: %v", counts)
	return counts
}

// When balancing 64 ranges across numNodes, we aim for at most (1 + rebalanceThreshold) * (64 / numNodes)
// ranges per node. We add an extra 10% to the threshold for extra tolerance during testing.
func (rdt *rangeDistributionTester) balancedDistributionUpperBound(numNodes int) int {
	return int(math.Ceil((1 + rebalanceThreshold.Get(&rdt.lastNode.ClusterSettings().SV) + 0.1) * 64 / float64(numNodes)))
}

func TestChangefeedWithNoDistributionStrategy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// The test is slow and will time out under deadlock/race/stress.
	skip.UnderShort(t)
	skip.UnderDuress(t)

	noLocality := func(i int) []roachpb.Tier {
		return make([]roachpb.Tier, 0)
	}

	// The replica oracle selects the leaseholder replica for each range. Then, distsql assigns the replica
	// to the same node which stores it. No load balancing is performed afterwards. Thus, the distribution of
	// ranges assigned to nodes is the same as the distribution of leaseholders on nodes.
	tester := newRangeDistributionTester(t, noLocality)
	defer tester.cleanup()

	tester.sqlDB.Exec(t, "SET CLUSTER SETTING changefeed.default_range_distribution_strategy = 'default'")
	tester.sqlDB.Exec(t, "CREATE CHANGEFEED FOR x INTO 'null://' WITH initial_scan='no'")
	partitions := tester.getPartitions()
	counts := tester.countRangesPerNode(partitions)
	require.True(t, reflect.DeepEqual(counts, []int{2, 2, 4, 8, 16, 32, 0, 0}),
		"unexpected counts %v, partitions: %v", counts, partitions)
}

func TestChangefeedWithSimpleDistributionStrategy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// The test is slow and will time out under deadlock/race/stress.
	skip.UnderShort(t)
	skip.UnderDuress(t)

	noLocality := func(i int) []roachpb.Tier {
		return make([]roachpb.Tier, 0)
	}

	// The replica oracle selects the leaseholder replica for each range. Then, distsql assigns the replica
	// to the same node which stores it. Afterwards, load balancing is performed to attempt an even distribution.
	// Check that we roughly assign (64 ranges / 6 nodes) ranges to each node.
	tester := newRangeDistributionTester(t, noLocality)
	defer tester.cleanup()
	tester.sqlDB.Exec(t, "SET CLUSTER SETTING changefeed.default_range_distribution_strategy = 'balanced_simple'")
	tester.sqlDB.Exec(t, "CREATE CHANGEFEED FOR x INTO 'null://' WITH initial_scan='no'")
	partitions := tester.getPartitions()
	counts := tester.countRangesPerNode(partitions)
	upper := int(math.Ceil((1 + rebalanceThreshold.Get(&tester.lastNode.ClusterSettings().SV)) * 64 / 6))
	for _, count := range counts {
		require.LessOrEqual(t, count, upper, "counts %v contains value greater than upper bound %d",
			counts, upper)
	}
}

func TestChangefeedWithNoDistributionStrategyAndConstrainedLocality(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// The test is slow and will time out under deadlock/race/stress.
	skip.UnderShort(t)
	skip.UnderDuress(t)

	// The replica oracle selects the leaseholder replica for each range. Then, distsql assigns the replica
	// to the same node which stores it. However, node of these nodes don't pass the filter. The replicas assigned
	// to these nodes are distributed arbitrarily to any nodes which pass the filter.
	tester := newRangeDistributionTester(t, func(i int) []roachpb.Tier {
		if i%2 == 1 {
			return []roachpb.Tier{{Key: "y", Value: "1"}}
		}
		return []roachpb.Tier{}
	})
	defer tester.cleanup()
	tester.sqlDB.Exec(t, "SET CLUSTER SETTING changefeed.default_range_distribution_strategy = 'default'")
	tester.sqlDB.Exec(t, "CREATE CHANGEFEED FOR x INTO 'null://' WITH initial_scan='no', execution_locality='y=1'")
	partitions := tester.getPartitions()
	counts := tester.countRangesPerNode(partitions)

	totalRanges := 0
	for i, count := range counts {
		if i%2 == 1 {
			totalRanges += count
		} else {
			require.Equal(t, count, 0)
		}
	}
	require.Equal(t, totalRanges, 64)
}

func TestChangefeedWithSimpleDistributionStrategyAndConstrainedLocality(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// The test is slow and will time out under deadlock/race/stress.
	skip.UnderShort(t)
	skip.UnderDuress(t)

	// The replica oracle selects the leaseholder replica for each range. Then, distsql assigns the replica
	// to the same node which stores it. However, node of these nodes don't pass the filter. The replicas assigned
	// to these nodes are distributed arbitrarily to any nodes which pass the filter.
	// Afterwards, we perform load balancing on this set of nodes.
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
	counts := tester.countRangesPerNode(partitions)

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
}
