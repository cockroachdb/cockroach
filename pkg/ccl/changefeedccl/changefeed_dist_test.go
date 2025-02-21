// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
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
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// mockRangeIterator iterates over ranges in a span assuming that each range
// contains one character.
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

var partitions = func(p ...sql.SpanPartition) []sql.SpanPartition {
	return p
}

var mkPart = func(n base.SQLInstanceID, spans ...roachpb.Span) sql.SpanPartition {
	var count int
	for _, sp := range spans {
		count += int(rune(sp.EndKey[0]) - rune(sp.Key[0]))
	}
	return sql.MakeSpanPartitionWithRangeCount(n, spans, count)
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

// TestPartitionSpans unit tests the rebalanceSpanPartitions function.
func TestPartitionSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// 26 nodes, 1 range per node.
	make26NodesBalanced := func() (p []sql.SpanPartition) {
		for i := rune(0); i < 26; i += 1 {
			p = append(p, sql.MakeSpanPartitionWithRangeCount(
				base.SQLInstanceID(i+1),
				[]roachpb.Span{mkRange('z' - i)},
				1,
			))
		}
		return p
	}

	// 26 nodes. All empty except for the first, which has 26 ranges.
	make26NodesImBalanced := func() (p []sql.SpanPartition) {
		for i := rune(0); i < 26; i += 1 {
			if i == 0 {
				p = append(p, sql.MakeSpanPartitionWithRangeCount(
					base.SQLInstanceID(i+1), []roachpb.Span{mkSpan('a', 'z'+1)}, 26))
			} else {
				p = append(p, sql.MakeSpanPartitionWithRangeCount(base.SQLInstanceID(i+1), []roachpb.Span{}, 0))
			}

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
				mkPart(1, mkSpan('a', 'j')),   // 9
				mkPart(2, mkSpan('j', 'r')),   // 8
				mkPart(3, mkSpan('r', 'z'+1)), // 9
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
				mkPart(1, mkSpan('o', 'p'), mkSpan('r', 'z')),                   // 9
				mkPart(2, mkSpan('a', 'c'), mkSpan('e', 'l')),                   // 9
				mkPart(3, mkSpan('c', 'e'), mkSpan('l', 'o'), mkSpan('p', 'r')), // 7
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
				mkPart(1, mkSpan('p', 'y')),                   // 9
				mkPart(2, mkSpan('i', 'p'), mkSpan('y', 'z')), // 8
				mkPart(3, mkSpan('a', 'i'), mkRange('z')),     // 9
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
				&mockRangeIterator{}, 0.00, tc.input)
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

		// First, select some indexes in ['a' ... 'z'] to partition at.
		spanIdxs := make([]int, numSpans)
		for i := range spanIdxs {
			spanIdxs[i] = rng.Intn((int('z')-int('a'))-1) + int('a') + 1
		}
		sort.Slice(spanIdxs, func(i int, j int) bool {
			return spanIdxs[i] < spanIdxs[j]
		})
		// Make sure indexes are unique.
		spanIdxs = dedupe(spanIdxs)

		// Generate spans and assign them randomly to partitions.
		input := make([]sql.SpanPartition, numPartitions)
		for i, key := range spanIdxs {
			assignTo := rng.Intn(numPartitions)
			if i == 0 {
				input[assignTo].Spans = append(input[assignTo].Spans, mkSpan('a', (rune(key))))
			} else {
				input[assignTo].Spans = append(input[assignTo].Spans, mkSpan((rune(spanIdxs[i-1])), rune(key)))
			}
		}
		last := rng.Intn(numPartitions)
		input[last].Spans = append(input[last].Spans, mkSpan(rune(spanIdxs[len(spanIdxs)-1]), 'z'))

		// Populate the remaining fields in the partitions.
		for i := range input {
			input[i] = mkPart(base.SQLInstanceID(i+1), input[i].Spans...)
		}

		t.Log(input)

		// Ensure the set of input spans matches the set of output spans.
		g1 := copySpans(input)
		output, err := rebalanceSpanPartitions(context.Background(),
			&mockRangeIterator{}, 0.00, input)
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
		ReplicationMode:   base.ReplicationManual,
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

	start := timeutil.Now()
	tc := testcluster.StartTestCluster(t, nodes, args)
	t.Logf("%s: starting the test cluster took %s", timeutil.Now().Format(time.DateTime), timeutil.Since(start))

	lastNode := tc.Server(len(tc.Servers) - 1).ApplicationLayer()
	sqlDB := sqlutils.MakeSQLRunner(lastNode.SQLConn(t))
	distSender := lastNode.DistSenderI().(*kvcoord.DistSender)

	systemDB := sqlutils.MakeSQLRunner(tc.SystemLayer(len(tc.Servers) - 1).SQLConn(t))
	systemDB.Exec(t, "SET CLUSTER SETTING kv.rangefeed.enabled = true")
	if tc.DefaultTenantDeploymentMode().IsExternal() {
		tc.GrantTenantCapabilities(
			ctx, t, serverutils.TestTenantID(),
			map[tenantcapabilities.ID]string{tenantcapabilities.CanAdminRelocateRange: "true"})
	}

	if tc.StartedDefaultTestTenant() {
		// Give 1,000,000 upfront tokens to the tenant, and keep the tokens per
		// second rate to the default value of 10,000. This helps avoid throttling
		// in the tests.
		systemDB.Exec(t,
			"SELECT crdb_internal.update_tenant_resource_limits($1::INT, 1000000, 10000, 0, now(), 0)",
			serverutils.TestTenantID().ToUint64())
	}

	t.Logf("%s: creating and inserting rows into table", timeutil.Now().Format(time.DateTime))
	start = timeutil.Now()
	sqlDB.ExecMultiple(t,
		"CREATE TABLE x (id INT PRIMARY KEY)",
		"INSERT INTO x SELECT generate_series(0, 63)",
	)
	t.Logf("%s: creating and inserting rows into table took %s", timeutil.Now().Format(time.DateTime), timeutil.Since(start))

	t.Logf("%s: splitting table into single-key ranges", timeutil.Now().Format(time.DateTime))
	start = timeutil.Now()
	sqlDB.Exec(t,
		"ALTER TABLE x SPLIT AT SELECT id FROM x WHERE id > 0",
	)
	t.Logf("%s: spitting the table took %s", timeutil.Now().Format(time.DateTime), timeutil.Since(start))

	// Distribute the leases exponentially across the first 5 nodes.
	t.Logf("%s: relocating ranges in exponential distribution", timeutil.Now().Format(time.DateTime))
	start = timeutil.Now()
	// Relocate can fail with errors like `change replicas... descriptor changed` thus the SucceedsSoon.
	sqlDB.ExecSucceedsSoon(t,
		`ALTER TABLE x RELOCATE SELECT ARRAY[floor(log(greatest(1,id)::DECIMAL)/log(2::DECIMAL))::INT+1], id FROM x`)
	t.Logf("%s: relocating ranges took %s", timeutil.Now().Format(time.DateTime), timeutil.Since(start))

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

	skip.WithIssue(t, 120470)

	// The test is slow and will time out under deadlock/race/stress.
	skip.UnderShort(t)
	skip.UnderDuress(t)

	noLocality := func(i int) []roachpb.Tier {
		return nil
	}

	// The replica oracle selects the leaseholder replica for each range. Then, distsql assigns the replica
	// to the same node which stores it. No load balancing is performed afterwards. Thus, the distribution of
	// ranges assigned to nodes is the same as the distribution of leaseholders on nodes.
	tester := newRangeDistributionTester(t, noLocality)
	defer tester.cleanup()

	serverutils.SetClusterSetting(t, tester.tc, "changefeed.default_range_distribution_strategy", "default")
	serverutils.SetClusterSetting(t, tester.tc, "changefeed.random_replica_selection.enabled", false)
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
	skip.WithIssue(t, 121408)

	noLocality := func(i int) []roachpb.Tier {
		return make([]roachpb.Tier, 0)
	}

	// The replica oracle selects the leaseholder replica for each range. Then, distsql assigns the replica
	// to the same node which stores it. Afterwards, load balancing is performed to attempt an even distribution.
	// Check that we roughly assign (64 ranges / 6 nodes) ranges to each node.
	tester := newRangeDistributionTester(t, noLocality)
	defer tester.cleanup()
	tester.sqlDB.Exec(t, "SET CLUSTER SETTING changefeed.default_range_distribution_strategy = 'balanced_simple'")
	// We need to disable the bulk oracle in order to ensure the leaseholder is selected.
	tester.sqlDB.Exec(t, "SET CLUSTER SETTING changefeed.random_replica_selection.enabled = false")
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

// TestDistSenderAllRangeSpans tests (*distserver).AllRangeSpans.
func TestDistSenderAllRangeSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const nodes = 1
	args := base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{},
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestTenantProbabilisticOnly,
		},
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, nodes, args)
	defer tc.Stopper().Stop(ctx)

	node := tc.Server(0).ApplicationLayer()
	systemLayer := tc.Server(0).SystemLayer()
	sqlDB := sqlutils.MakeSQLRunner(node.SQLConn(t))
	distSender := node.DistSenderI().(*kvcoord.DistSender)

	tenantPrefix := ""
	if tc.StartedDefaultTestTenant() {
		tenantPrefix = "/Tenant/10"
	}

	// Use manual merging/splitting only.
	tc.ToggleReplicateQueues(false)

	mergeRange := func(key interface{}) {
		err := systemLayer.DB().AdminMerge(ctx, key)
		if err != nil {
			if !strings.Contains(err.Error(), "cannot merge final range") {
				t.Fatal(err)
			}
		}
	}
	getTableSpan := func(tableName string) roachpb.Span {
		desc := desctestutils.TestingGetPublicTableDescriptor(
			node.DB(), node.Codec(), "defaultdb", "a")
		return desc.PrimaryIndexSpan(node.Codec())
	}
	getTableDesc := func(tableName string) catalog.TableDescriptor {
		return desctestutils.TestingGetPublicTableDescriptor(
			node.DB(), node.Codec(), "defaultdb", "a")
	}

	// Regression test for the issue in #117286.
	t.Run("returned range does not exceed input span boundaries", func(t *testing.T) {
		// Merge 3 tables into one range.
		sqlDB.Exec(t, "create table a (i int primary key)")
		sqlDB.Exec(t, "create table b (j int primary key)")
		sqlDB.Exec(t, "create table c (k int primary key)")
		mergeRange(getTableSpan("a").Key)
		mergeRange(getTableSpan("b").Key)

		bTableID := getTableDesc("b").GetID()
		rangeSpans, _, err := distSender.AllRangeSpans(ctx, []roachpb.Span{getTableSpan("b")})
		require.NoError(t, err)

		// Assert that the returned range is trimmed, so it only contains spans from "b" and not the entire
		// range containing "a" and "c".
		require.Equal(t, 1, len(rangeSpans), fmt.Sprintf("%v", rangeSpans))
		require.Equal(t, fmt.Sprintf("%s/Table/%d/{1-2}", tenantPrefix, bTableID), rangeSpans[0].String())
	})

}
