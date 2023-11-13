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
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
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

type echoResolver struct {
	result []roachpb.Spans
	pos    int
}

func (r *echoResolver) getRangesForSpans(
	_ context.Context, _ []roachpb.Span,
) (spans []roachpb.Span, _ error) {
	spans = r.result[r.pos]
	r.pos++
	return spans, nil
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
	spans := func(s ...roachpb.Span) roachpb.Spans {
		return s
	}
	const sensitivity = 0.01

	for i, tc := range []struct {
		input   []sql.SpanPartition
		resolve []roachpb.Spans
		expect  []sql.SpanPartition
	}{
		{
			input: partitions(
				mkPart(1, mkSpan("a", "j")),
				mkPart(2, mkSpan("j", "q")),
				mkPart(3, mkSpan("q", "z")),
			),
			// 6 total ranges, 2 per node.
			resolve: []roachpb.Spans{
				spans(mkSpan("a", "c"), mkSpan("c", "e"), mkSpan("e", "j")),
				spans(mkSpan("j", "q")),
				spans(mkSpan("q", "y"), mkSpan("y", "z")),
			},
			expect: partitions(
				mkPart(1, mkSpan("a", "e")),
				mkPart(2, mkSpan("e", "q")),
				mkPart(3, mkSpan("q", "z")),
			),
		},
		{
			input: partitions(
				mkPart(1, mkSpan("a", "c"), mkSpan("e", "p"), mkSpan("r", "z")),
				mkPart(2),
				mkPart(3, mkSpan("c", "e"), mkSpan("p", "r")),
			),
			// 5 total ranges -- on 2 nodes; target should be 1 per node.
			resolve: []roachpb.Spans{
				spans(mkSpan("a", "c"), mkSpan("e", "p"), mkSpan("r", "z")),
				spans(),
				spans(mkSpan("c", "e"), mkSpan("p", "r")),
			},
			expect: partitions(
				mkPart(1, mkSpan("a", "c"), mkSpan("e", "p")),
				mkPart(2, mkSpan("r", "z")),
				mkPart(3, mkSpan("c", "e"), mkSpan("p", "r")),
			),
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			sp, err := rebalanceSpanPartitions(context.Background(),
				&echoResolver{result: tc.resolve}, sensitivity, tc.input)
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

	tableDesc catalog.TableDescriptor
	tableSpan roachpb.Span
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

	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[len(tc.Conns)-1])
	lastNode := tc.Server(len(tc.Servers) - 1).ApplicationLayer()
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
		sqlDB.Exec(t, cmd)
	}

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(
		lastNode.DB(), lastNode.Codec(), "defaultdb", "x")
	tableSpan := tableDesc.PrimaryIndexSpan(keys.SystemSQLCodec)

	return &rangeDistributionTester{
		ctx:          ctx,
		t:            t,
		tc:           tc,
		partitionsCh: partitionsCh,

		sqlDB:      sqlDB,
		lastNode:   lastNode,
		distSender: distSender,

		tableDesc: tableDesc,
		tableSpan: tableSpan,
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
// ranges per node.
func (rdt *rangeDistributionTester) balancedDistributionUpperBound(numNodes int) int {
	return int(math.Ceil((1 + rebalanceThreshold.Get(&rdt.lastNode.ClusterSettings().SV)) * 64 / float64(numNodes)))
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
