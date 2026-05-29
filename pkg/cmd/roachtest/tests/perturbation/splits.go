// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package perturbation

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
)

// splits drives the cluster into a high replicas-per-vcpu state so the
// foreground KV workload's latency can be measured under that load. The
// purpose is NOT to benchmark splits or scatter; splits are just the means
// by which we grow the replica population.
//
// Splits are issued in "batches": each batch issues a fixed number of new
// splits (batchSize, summed across all groups) and is also the unit at
// which we report progress and density metrics. Total range count grows
// linearly with batch count until we hit targetSplits.
//
// To grow the population without paying for sustained rebalance churn, the
// cluster's stores are partitioned into disjoint groups of three. Each
// group's three stores live on three distinct nodes (so RF=3 placements
// satisfy diversity constraints), and every store appears in exactly one
// group. Per group we create one database whose voters are pinned to that
// group's three stores via voter_constraints, and one table inside that
// database. After waiting for each table's initial range to converge, every
// batch issues more splits against those same tables; children of each
// split inherit their parent's correct placement, so no SCATTER and no
// rebalance load are needed and no fresh-table initial-placement race is
// possible.
//
// Pinning to specific stores (rather than to nodes) keeps the per-store
// replica counts even by construction: with N stores partitioned into
// N/3 groups of 3, and equal split traffic per group, each store ends up
// holding the same number of voters. An earlier node-pinning version of
// this test ran ~1 TB of background rebalance traffic during the split
// storm because the allocator was free to put both replicas of a node-pinned
// range on either of that node's two stores, then continually shuffled them
// to balance store usage.
//
// On the default 12-node × 2-store layout this yields 8 groups of 3 stores;
// each store holds targetSplits/8 voters at steady state.
//
// Per "batch" (the unit at which we report density), each group's pinned
// table is split at batchSize/numGroups new keys. Keys are interleaved
// across batches: the totalPerGroup keys are evenly spaced across the int64
// PK space, and batch B picks the indices {B, B+totalBatches, ...}. This
// keeps each batch's splits spread across the whole keyspace, so the
// per-range fan-in stays bounded as the table grows.
//
// The test runs in two flavors selected by the asleep field:
//
// asleep=true measures total replica capacity: followers fall asleep via
// store liveness quiescence, so the bulk of the replica population costs no
// per-tick CPU. The default 600k splits on the standard perturbation cluster
// (12 nodes × 16 vcpu, RF=3) land at ~9,400 replicas/vcpu — 40% higher than
// the production telemetry cluster (crl-prod-38z, 8 nodes × 16 vcpu, ~6,700
// replicas/vcpu) as of 2026-05-22. This headroom stress-tests the primary
// quiescence path beyond current production density.
//
// asleep=false disables follower sleep (via kv.raft.store_liveness.
// quiescence.enabled=false) so every replica costs its full per-tick CPU.
// This measures the cluster's capacity for active replicas, which is the
// relevant ceiling during outages where store liveness can't withdraw and
// followers can't fall asleep. The 400k split target lands at ~6,250
// replicas/vcpu, matching the telemetry cluster's observed non-quiescent
// density. Because awake replicas are far more expensive than quiescent
// ones, this is 33% lower than asleep=true's target despite being close to
// production density.
//
// Under leader leases (the default), leaseholders never quiesce regardless
// of the asleep setting; only followers are affected. Traditional Raft
// quiescence (the COCKROACH_DISABLE_QUIESCENCE knob) is already inert
// under leader leases, so the cluster setting toggled here
// (kv.raft.store_liveness.quiescence.enabled) is the only knob that
// matters.
type splits struct {
	// targetSplits is the cumulative number of split points to issue across
	// all batches and all groups.
	targetSplits int
	// batchSize is the number of split points per batch, summed across all
	// groups (one fresh table per group per batch).
	batchSize int
	// asleep selects the test flavor. See the struct-level comment.
	asleep bool
}

var _ perturbation = splits{}
var _ perturbationNamer = splits{}
var _ devSizer = splits{}
var _ metamorphicSizer = splits{}

func (s splits) nameSuffix() string {
	return fmt.Sprintf("/asleep=%t", s.asleep)
}

func (s splits) setup() variations {
	// Construct the perturbation as a fresh literal rather than mutating
	// the (value-receiver) `s`: the per-call configuration is documented
	// at the construction site, and there is no surrounding-code ordering
	// dependency for the reader to keep track of.
	if s.asleep {
		s = splits{targetSplits: 600_000, batchSize: 10_000, asleep: true}
	} else {
		s = splits{targetSplits: 400_000, batchSize: 5_000, asleep: false}
	}
	v := setup(s, defaultThresholds())
	// The kv workload table is incidental here — only enough splits to
	// avoid being a single-range hotspot for the foreground workload.
	// Scatter it so its leases distribute evenly across all 12 nodes
	// (no voter_constraints on this table, so the allocator has full
	// freedom and SCATTER converges).
	v.splits = 500
	v.scatter = true
	// The split storm (400k–600k splits depending on flavor) plus
	// end-of-perturbation lease rebalance can run for over an hour on its
	// own; combined with the standard fill / validation windows the default
	// 3h timeout is too tight.
	v.timeout = 5 * time.Hour
	// Bump the post-test token-return wait. The 10m default is enough for
	// real outstanding tokens to drain (observed within seconds in the
	// nightly run), but ValidateTokensReturned's SQL query races against
	// background flow-control activity (replicate queue, lease queue,
	// jobs) at high replica counts, occasionally observing transient
	// non-zero diffs that never represented real outstanding state. A
	// longer window gives SucceedsWithin's retry loop a better chance to
	// land on a clean snapshot.
	v.tokenReturnTime = 1 * time.Hour
	// The split storm itself is expected to crush foreground throughput
	// (every node is admitting and replicating split commands across
	// 1–2M replicas while also serving the workload). The interesting
	// question is whether the cluster recovers afterwards, so let the
	// perturbation interval slide and gate on the recovery interval
	// instead.
	v.impact = acceptableImpact{
		maxP99Impact:        math.Inf(1),
		maxP50Impact:        math.Inf(1),
		maxThroughputImpact: 10,
	}
	v.recoveryImpact = acceptableImpact{
		maxP99Impact:        math.Inf(1),
		maxP50Impact:        math.Inf(1),
		maxThroughputImpact: 3,
	}
	if !s.asleep {
		// Prevent followers from sleeping so we measure active-replica
		// capacity. Under leader leases, leaseholders never sleep regardless;
		// this setting only changes follower behavior.
		v.clusterSettings["kv.raft.store_liveness.quiescence.enabled"] = "false"
	}
	// Skip the post-test replica divergence check. At 400k–600k splits the
	// cluster carries 1.2M–1.8M ranges, and the framework's 20m budget for
	// the consistency check is not enough to scan a meaningful fraction of
	// them (the previous run got through 0 ranges in 20m before timing
	// out). The framework already tolerates the timeout but logs it as a
	// post-assertion failure; skipping explicitly keeps the artifact clean.
	v.skipPostValidations = registry.PostValidationReplicaDivergence
	return v
}

func (s splits) setupMetamorphic(rng *rand.Rand) variations {
	v := s.setup()
	v = v.randomize(rng)
	return v
}

// sizeForDev shrinks the split storm to something appropriate for the
// 5-node × 4 vcpu dev cluster: a few thousand splits land in seconds,
// which is enough to exercise the test code paths without spending half
// an hour on a developer's machine.
func (s splits) sizeForDev() perturbation {
	s.targetSplits = 4_000
	s.batchSize = 200
	return s
}

// sizeForMetamorphic shrinks the split storm for the randomized
// metamorphic clusters, which are smaller than the nightly target.
func (s splits) sizeForMetamorphic() perturbation {
	if s.asleep {
		s.targetSplits = 60_000
		s.batchSize = 5_000
	} else {
		s.targetSplits = 40_000
		s.batchSize = 2_000
	}
	return s
}

func (s splits) startTargetNode(ctx context.Context, t test.Test, v variations) {
	v.startNoBackup(ctx, t, v.targetNodes())
}

// startPerturbation creates one pinned database per 3-node group, creates a
// single split-target table per group and waits for it to converge, then
// issues splits batch-by-batch against those same tables in parallel.
// Returns the wall time spent in the perturbation.
func (s splits) startPerturbation(ctx context.Context, t test.Test, v variations) time.Duration {
	startTime := timeutil.Now()
	db := v.Conn(ctx, t.L(), 1)
	defer db.Close()

	groups := makeGroups(v.numNodes, v.disks)
	if len(groups) == 0 {
		t.Fatalf("splits: need at least 3 nodes to form a pinned group, got %d", v.numNodes)
	}
	t.L().Printf("splits: %d pinned groups across %d nodes × %d stores", len(groups), v.numNodes, v.disks)

	// Setup all groups concurrently: each group is an independent database
	// with disjoint stores, so concurrent CREATE/ALTER ZONE/CREATE TABLE
	// and waitForVoterPlacement don't contend. The wait is the slow part
	// (per-range up-replication via the replicate queue) — running it 8x
	// in parallel cuts the setup phase to the longest single group rather
	// than the sum.
	setupGrp := t.NewErrorGroup(task.WithContext(ctx))
	for _, g := range groups {
		setupGrp.Go(func(ctx context.Context, l *logger.Logger) error {
			return s.setupGroup(ctx, l, v, db, g)
		})
	}
	if err := setupGrp.WaitE(); err != nil {
		t.Fatal(err)
	}

	s.reportDensity(ctx, t, v, db, "before splits")

	// Fail loudly if setup() didn't run — the divide-by-zero further down
	// would otherwise silently land on a 1-split run.
	if s.targetSplits == 0 || s.batchSize == 0 {
		t.Fatalf("splits: setup() not called (targetSplits=%d, batchSize=%d)",
			s.targetSplits, s.batchSize)
	}

	// Precompute the full set of split keys for each group: targetSplits/groups
	// keys, evenly spaced across the int64 PK space. Each batch picks an
	// interleaved subset so each batch's splits stay spread across the whole
	// keyspace as the table grows.
	totalPerGroup := s.targetSplits / len(groups)
	if totalPerGroup == 0 {
		totalPerGroup = 1
	}
	perBatchPerGroup := s.batchSize / len(groups)
	if perBatchPerGroup == 0 {
		perBatchPerGroup = 1
	}
	totalBatches := (totalPerGroup + perBatchPerGroup - 1) / perBatchPerGroup
	allKeys := evenlySpacedKeys(totalPerGroup)

	for batch := 0; batch < totalBatches; batch++ {
		// Interleaved subset of allKeys for this batch, ordered descending.
		//
		// Order matters when many splits land on the same range. Ascending
		// keys would mean each split lands in the freshly-created RIGHT range
		// — each subsequent split's range descriptor was just written by the
		// previous split, on a brand-new raft group that just elected a leader,
		// and a lagging follower of any one of those new ranges stalls the
		// chain. Descending keys keep each split falling into the LEFTMOST
		// range (which is the original range until the first split from this
		// batch lands), so all splits in the batch stay in the same raft log
		// behind the same stable leader — a single pipeline rather than a
		// chain of new ones.
		batchKeys := make([]int64, 0, perBatchPerGroup)
		for i := batch; i < totalPerGroup; i += totalBatches {
			batchKeys = append(batchKeys, allKeys[i])
		}
		slices.Reverse(batchKeys)

		grp := t.NewErrorGroup(task.WithContext(ctx))
		for _, g := range groups {
			grp.Go(func(ctx context.Context, l *logger.Logger) error {
				table := fmt.Sprintf("%s.t", g.dbName())
				if _, err := db.ExecContext(ctx, splitAtKeys(table, batchKeys)); err != nil {
					return errors.Wrapf(err, "split %s batch %d", table, batch)
				}
				// One lease-balance pass after batch 0 only. Reasoning: batch
				// 0's descending splits all land on the original single range,
				// so all children inherit one leaseholder. Subsequent batches
				// issue keys at interleaved positions so each new split's
				// right child inherits its parent's leaseholder; if leases
				// are spread post-batch-0, subsequent batches stay spread.
				//
				// We use deterministic round-robin (rebalanceLeases) rather
				// than SCATTER: under per-store voter_constraints the
				// allocator scoring picks the same lease target on every
				// re-scatter (observed: SCATTER stalled at 1.5–3x spread for
				// most groups after 5 attempts in the SMOKE run).
				if batch == 0 {
					if err := rebalanceLeases(ctx, l, t, db, table, g); err != nil {
						return errors.Wrapf(err, "balance leases %s after batch 0", table)
					}
				}
				return nil
			})
		}
		if err := grp.WaitE(); err != nil {
			t.Fatal(err)
		}

		splitsDone := (batch + 1) * perBatchPerGroup * len(groups)
		if splitsDone > s.targetSplits {
			splitsDone = s.targetSplits
		}
		t.L().Printf("splits: batch %d/%d done (%d/%d total splits)",
			batch+1, totalBatches, splitsDone, s.targetSplits)
		s.reportDensity(ctx, t, v, db,
			fmt.Sprintf("after batch %d/%d", batch+1, totalBatches))
		s.reportBalance(ctx, t, db,
			fmt.Sprintf("after batch %d/%d", batch+1, totalBatches))
	}

	// Final lease-balance pass: with deterministic round-robin batch-0 sets
	// the initial spread, but subsequent batches can drift slightly because
	// each split's right child inherits its parent's leaseholder rather than
	// alternating. One final pass cleans up.
	{
		grp := t.NewErrorGroup(task.WithContext(ctx))
		for _, g := range groups {
			grp.Go(func(ctx context.Context, l *logger.Logger) error {
				table := fmt.Sprintf("%s.t", g.dbName())
				return rebalanceLeases(ctx, l, t, db, table, g)
			})
		}
		if err := grp.WaitE(); err != nil {
			t.Fatal(err)
		}
	}

	s.reportDensity(ctx, t, v, db, "end of perturbation")
	s.reportBalance(ctx, t, db, "end of perturbation")
	return timeutil.Since(startTime)
}

// endPerturbation lets the workload run alone for the validation window so we
// can measure how long the cluster takes to recover from the split storm.
func (s splits) endPerturbation(ctx context.Context, t test.Test, v variations) time.Duration {
	waitDuration(ctx, v.validationDuration)
	return v.validationDuration
}

// storeRef identifies a single store by its (node ID, store-index) pair,
// where store-index is the 1-based index of the store within its node (i.e.
// the order it was passed via --store at cockroach start). This pair is what
// roachprod exposes as the per-store attribute "node{N}store{I}" (see
// pkg/roachprod/install/cockroach.go where stores are started with
// attrs=store{i}:node{N}:node{N}store{i}), so we can pin a voter to a
// specific store with the constraint "+node{N}store{I}".
type storeRef struct {
	node, storeIdx int
}

// storeGroup is a set of stores whose database's voters are pinned to
// exactly those stores (one voter per store). All stores in a group must
// live on distinct nodes so that RF=3 placements satisfy CockroachDB's
// per-node diversity constraint.
type storeGroup []storeRef

// assertValid panics if g violates the storeGroup invariants. Cheap
// front-loaded check: catches construction bugs at registration time
// instead of after a 90-minute waitForVoterPlacement timeout.
func (g storeGroup) assertValid() {
	if len(g) != 3 {
		panic(fmt.Sprintf("splits: storeGroup must have exactly 3 stores, got %d: %+v", len(g), g))
	}
	seen := map[int]struct{}{}
	for _, s := range g {
		if _, ok := seen[s.node]; ok {
			panic(fmt.Sprintf("splits: storeGroup has duplicate node n%d: %+v", s.node, g))
		}
		seen[s.node] = struct{}{}
	}
}

func (g storeGroup) dbName() string {
	var b strings.Builder
	b.WriteString("splitperf_g")
	for i, s := range g {
		if i > 0 {
			b.WriteByte('_')
		}
		fmt.Fprintf(&b, "n%ds%d", s.node, s.storeIdx)
	}
	return b.String()
}

// voterConstraints returns a dict-form zone-config constraint that places
// one voter on each store in the group, using the per-store attribute
// "node{N}store{I}" that roachprod sets by default at startup.
//
// Keys are double-quoted: the unquoted form (e.g. {+node1store1: 1}) is
// rejected by YAML because the leading + token confuses the scanner. The
// locality-tier form is always written quoted in the codebase too (see e.g.
// '{"+region=test": 3}' in pkg/sql/logictest/testdata/logic_test/zone_config).
func (g storeGroup) voterConstraints() string {
	parts := make([]string, len(g))
	for i, s := range g {
		parts[i] = fmt.Sprintf(`"+node%dstore%d": 1`, s.node, s.storeIdx)
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// makeGroups partitions the cluster's stores into as many disjoint 3-store
// groups as possible, where the three stores of any group live on three
// distinct nodes. Nodes are taken in order [1..numNodes] and partitioned
// into 3-node triples; for each triple we then emit storesPerNode groups,
// one per store-index. With the default 12-node × 2-store layout this
// produces 8 groups (4 triples × 2 store-indices); with a 5-node × 1-store
// dev variant it produces 1 group ({n1s1, n2s1, n3s1}). Any leftover nodes
// (numNodes mod 3) are not pinned and continue to serve only system ranges
// and the foreground workload.
func makeGroups(numNodes, storesPerNode int) []storeGroup {
	const groupSize = 3
	triples := numNodes / groupSize
	var gs []storeGroup
	for t := 0; t < triples; t++ {
		for s := 1; s <= storesPerNode; s++ {
			grp := make(storeGroup, groupSize)
			for j := 0; j < groupSize; j++ {
				grp[j] = storeRef{node: t*groupSize + j + 1, storeIdx: s}
			}
			grp.assertValid()
			gs = append(gs, grp)
		}
	}
	return gs
}

// setupGroup creates the group's database, applies a zone config that pins
// its voters to the group's nodes, creates the single split-target table,
// and waits for the table's initial range to converge to that placement
// before any splits are issued. The table created here (named "t") is the
// only target the per-batch split loop ever writes to; doing this once up
// front avoids the per-batch fresh-table initial-placement race that
// otherwise lets ~80% of a group's ranges land on a sibling node and
// inherit forward through every split.
func (s splits) setupGroup(
	ctx context.Context, l *logger.Logger, v variations, db *gosql.DB, g storeGroup,
) error {
	g.assertValid()
	if _, err := db.ExecContext(ctx,
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", g.dbName())); err != nil {
		return errors.Wrapf(err, "create database %s", g.dbName())
	}

	// Pin one voter per group node. We pin both replicas and voters with the
	// same dict, and set num_voters == num_replicas == 3 explicitly so the
	// conformance check below — which compares against voting_replicas — is
	// equivalent to checking placement of all replicas.
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		`ALTER DATABASE %s CONFIGURE ZONE USING
			num_replicas = 3,
			num_voters = 3,
			constraints = '%s',
			voter_constraints = '%s'`,
		g.dbName(), g.voterConstraints(), g.voterConstraints())); err != nil {
		return errors.Wrapf(err, "configure zone %s", g.dbName())
	}

	// Create the actual split-target table. CREATE DATABASE alone does not
	// materialize a range, so this also gives waitForVoterPlacement something
	// to inspect.
	if _, err := db.ExecContext(ctx,
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.t (k INT PRIMARY KEY)", g.dbName())); err != nil {
		return errors.Wrapf(err, "create table %s.t", g.dbName())
	}

	// Dump the resolved zone config so debugging is easier when conformance
	// fails: SHOW ZONE CONFIGURATION returns a raw_config_yaml column that
	// captures what the constraint string was actually parsed into.
	var zoneYaml string
	if err := db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT raw_config_yaml FROM [SHOW ZONE CONFIGURATION FOR DATABASE %s]",
			g.dbName())).Scan(&zoneYaml); err != nil {
		// Diagnostic only — don't fail the test if SHOW ZONE returns nothing.
		l.Printf("splits[%s]: SHOW ZONE CONFIGURATION failed: %v", g.dbName(), err)
	} else {
		l.Printf("splits[%s]: zone config:\n%s", g.dbName(), zoneYaml)
	}

	// Scale the conformance wait to the test budget. Default is 10
	// minutes (suits the dev variant, where v.timeout is cleared and
	// conformance converges in seconds anyway); v.timeout/3 can lift it
	// up to a 90-minute ceiling for the nightly variant. The dev floor
	// matters because a hung conformance check otherwise inherits the
	// hard-coded 90 minutes and hides the hang behind a long wait.
	const (
		defaultWaitBudget = 10 * time.Minute
		maxWaitBudget     = 90 * time.Minute
	)
	waitBudget := defaultWaitBudget
	if v.timeout > 0 && v.timeout/3 > waitBudget {
		waitBudget = v.timeout / 3
	}
	if waitBudget > maxWaitBudget {
		waitBudget = maxWaitBudget
	}
	if err := waitForVoterPlacement(ctx, l, v, g, waitBudget); err != nil {
		return err
	}

	l.Printf("splits[%s]: group converged to %s", g.dbName(), g.voterConstraints())
	return nil
}

// waitForVoterPlacement polls until every range belonging to the group's
// database has its three voters on EXACTLY the group's three stores. Times
// out after the supplied deadline and returns the non-conforming ranges in
// the error.
//
// SHOW RANGES exposes voting_replicas as STORE IDs (per the comment on
// crdb_internal.ranges_no_leases), so we first translate the group's
// (node, storeIdx) pairs to global store IDs via crdb_internal.kv_store_status.
// Within each node, store-index is the 1-based ordinal of the store sorted
// by store_id (matching the order the stores were registered at startup,
// which is also the order roachprod sets the +node{N}store{I} attribute).
//
// To nudge non-conforming ranges into the replicate queue we use
// crdb_internal.kv_enqueue_replica, which is gateway-only — the call only
// inspects local stores. To make it actually fire, we route each enqueue to
// the leaseholder (the only replica that proposes), translating its store_id
// back to a node_id. Without this routing the enqueue silently returns
// "replica not found on this node" and we sit waiting for the scanner
// (default 10m) to pick the range up.
func waitForVoterPlacement(
	ctx context.Context, l *logger.Logger, v variations, g storeGroup, timeout time.Duration,
) error {
	deadline := timeutil.Now().Add(timeout)

	db := v.Conn(ctx, l, 1)
	defer db.Close()

	storeToNode, refToStore, err := queryStoreMaps(ctx, db)
	if err != nil {
		return err
	}

	// Translate the group's (node, storeIdx) refs to global store IDs.
	groupStoreIDs := make([]string, len(g))
	for i, ref := range g {
		sid, ok := refToStore[ref]
		if !ok {
			return errors.Errorf("no store found for n%ds%d", ref.node, ref.storeIdx)
		}
		groupStoreIDs[i] = strconv.Itoa(sid)
	}
	sort.Strings(groupStoreIDs)
	wantArr := "ARRAY[" + strings.Join(groupStoreIDs, ",") + "]::INT[]"
	l.Printf("splits[%s]: pinning voters to stores %v",
		g.dbName(), groupStoreIDs)

	// Conformance: voting_replicas must be set-equal to the group's three
	// store IDs. With num_voters = num_replicas = 3 (set in setupGroup),
	// "set equal" + length-3 is equivalent to exact placement.
	q := fmt.Sprintf(
		`SELECT range_id, voting_replicas, lease_holder
		 FROM [SHOW RANGES FROM DATABASE %s WITH DETAILS]
		 WHERE NOT (voting_replicas @> %s
		            AND voting_replicas <@ %s
		            AND array_length(voting_replicas, 1) = 3)
		 ORDER BY range_id LIMIT 10`,
		g.dbName(), wantArr, wantArr)

	// Cache one connection per cluster node so we don't reopen for every
	// nudge. Lazily populated on first use per node.
	nodeConns := map[int]*gosql.DB{}
	defer func() {
		for _, c := range nodeConns {
			_ = c.Close()
		}
	}()
	connFor := func(nid int) *gosql.DB {
		if c, ok := nodeConns[nid]; ok {
			return c
		}
		c := v.Conn(ctx, l, nid)
		nodeConns[nid] = c
		return c
	}

	// Polling cadence: 30s. The replicate queue's planner output for a given
	// range is largely deterministic against the current cluster snapshot; a
	// tighter loop just spams the log with the same error. 30s gives ambient
	// store usage / leaseholder churn enough time to nudge the rebalancer's
	// scorer into a different tie-break.
	const interval = 30 * time.Second
	for {
		rows, err := db.QueryContext(ctx, q)
		if err != nil {
			return errors.Wrapf(err, "querying ranges for %s", g.dbName())
		}
		type bad struct {
			id     int
			voters string
			node   int // leaseholder node to enqueue against
		}
		var bads []bad
		for rows.Next() {
			var b bad
			var votersArr pq.Int64Array
			var leaseStore gosql.NullInt64
			if err := rows.Scan(&b.id, &votersArr, &leaseStore); err != nil {
				_ = rows.Close()
				return errors.Wrap(err, "scanning ranges")
			}
			parts := make([]string, len(votersArr))
			for i, v := range votersArr {
				parts[i] = strconv.FormatInt(v, 10)
			}
			b.voters = "{" + strings.Join(parts, ",") + "}"
			// Enqueue MUST go to the leaseholder; replicate-queue actions are
			// proposed through raft and the leaseholder is the only replica
			// that proposes. lease_holder is a store_id, so translate to a
			// node_id via storeToNode. If the lease is invalid (NULL), fall
			// back to the first voter.
			switch {
			case leaseStore.Valid:
				b.node = storeToNode[int(leaseStore.Int64)]
			case len(votersArr) > 0:
				b.node = storeToNode[int(votersArr[0])]
			}
			bads = append(bads, b)
		}
		if err := rows.Close(); err != nil {
			return errors.Wrap(err, "closing rows")
		}
		if len(bads) == 0 {
			return nil
		}
		if timeutil.Now().After(deadline) {
			descs := make([]string, len(bads))
			for i, b := range bads {
				descs[i] = fmt.Sprintf("r%d=%s", b.id, b.voters)
			}
			return errors.Errorf(
				"timed out waiting for %s to converge to %s; non-conforming ranges: %s",
				g.dbName(), g.voterConstraints(), strings.Join(descs, ", "))
		}
		// Nudge each non-conforming range into the replicate queue rather than
		// waiting for the next scanner pass.
		for _, b := range bads {
			if b.node == 0 {
				continue
			}
			if _, err := connFor(b.node).ExecContext(ctx,
				"SELECT crdb_internal.kv_enqueue_replica($1, 'replicate', true)", b.id); err != nil {
				l.Printf("splits[%s]: kv_enqueue_replica r%d on n%d failed (continuing): %v",
					g.dbName(), b.id, b.node, err)
			}
		}
		descs := make([]string, len(bads))
		for i, b := range bads {
			descs[i] = fmt.Sprintf("r%d=%s", b.id, b.voters)
		}
		l.Printf("splits[%s]: waiting for convergence; non-conforming: %s",
			g.dbName(), strings.Join(descs, ", "))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

// evenlySpacedKeys returns n int64 keys placed at evenly-spaced positions
// across the full int64 PK space (avoiding the endpoints).
func evenlySpacedKeys(n int) []int64 {
	stride := math.MaxUint64 / uint64(n+1)
	keys := make([]int64, n)
	for i := 1; i <= n; i++ {
		// Map i in [1..n] to a key in [MinInt64+stride .. ~MaxInt64]. The
		// uint64 arithmetic avoids signed overflow; the final cast wraps back
		// into the full int64 range. minU64 is the unsigned representation of
		// math.MinInt64 (= 1<<63), written this way because uint64(math.MinInt64)
		// does not compile.
		const minU64 = uint64(1) << 63
		keys[i-1] = int64(minU64 + uint64(i)*stride)
	}
	return keys
}

// rebalanceLeases drives the table's leaseholder distribution toward
// perfect balance across the group's three stores. It reads the current
// (range_id, lease_holder) for the table, computes a target count per
// store (total/3), and for every range whose store is over-target issues
// `ALTER RANGE r RELOCATE LEASE TO s` toward an under-target store.
//
// Deterministic rather than SCATTER-based: under per-store voter_constraints
// the allocator's lease-scoring picks the same target on every re-scatter,
// so SCATTER stalls at whatever spread the scoring biases toward (observed:
// 1.5–3x on most groups). Round-robin assignment converges in one pass.
//
// Individual RELOCATE calls can fail benignly (range merged, lease moved by
// the queue mid-loop); these are logged and skipped.
//
// Moves are dispatched via a worker pool — different ranges' lease transfers
// are independent (different raft groups), so serializing them within a
// group leaves the cluster underused. A small pool keeps the per-coordinator
// RTT cost from dominating wall time on tables with tens of thousands of
// ranges.
func rebalanceLeases(
	ctx context.Context,
	l *logger.Logger,
	tasks task.GroupProvider,
	db *gosql.DB,
	table string,
	g storeGroup,
) error {
	_, refToStore, err := queryStoreMaps(ctx, db)
	if err != nil {
		return errors.Wrap(err, "store map lookup")
	}
	groupStores := make([]int, len(g))
	for i, ref := range g {
		sid, ok := refToStore[ref]
		if !ok {
			return errors.Errorf("no store found for n%ds%d", ref.node, ref.storeIdx)
		}
		groupStores[i] = sid
	}
	sort.Ints(groupStores)

	type rangeRow struct {
		id    int
		lease int
	}
	rows, err := db.QueryContext(ctx, fmt.Sprintf(
		"SELECT range_id, lease_holder FROM [SHOW RANGES FROM TABLE %s WITH DETAILS]", table))
	if err != nil {
		return errors.Wrap(err, "list ranges")
	}
	var ranges []rangeRow
	for rows.Next() {
		var rr rangeRow
		var lh gosql.NullInt64
		if err := rows.Scan(&rr.id, &lh); err != nil {
			_ = rows.Close()
			return errors.Wrap(err, "scan range")
		}
		if lh.Valid {
			rr.lease = int(lh.Int64)
		}
		ranges = append(ranges, rr)
	}
	if err := rows.Close(); err != nil {
		return errors.Wrap(err, "close ranges")
	}

	counts := make(map[int]int, len(groupStores))
	for _, sid := range groupStores {
		counts[sid] = 0
	}
	for _, r := range ranges {
		counts[r.lease]++
	}
	target := len(ranges) / len(groupStores)

	// Build a queue of (store, deficit) for stores below target, then walk
	// the ranges once and shift every over-target range toward a needy
	// store. With 3 stores and N total ranges this completes in one linear
	// scan and at most 2*(N/3) transfers (the worst case is one empty store
	// and two full ones).
	type need struct {
		store   int
		deficit int
	}
	var needy []need
	for _, sid := range groupStores {
		if counts[sid] < target {
			needy = append(needy, need{store: sid, deficit: target - counts[sid]})
		}
	}
	// Phase 1: precompute all (range, target) moves. Update counts as we
	// plan so we don't over-shift any one store.
	type move struct {
		rangeID int
		target  int
	}
	var moves []move
	needIdx := 0
	for _, r := range ranges {
		if needIdx >= len(needy) {
			break
		}
		if counts[r.lease] <= target {
			continue
		}
		tgt := needy[needIdx].store
		if r.lease == tgt {
			continue
		}
		moves = append(moves, move{rangeID: r.id, target: tgt})
		counts[r.lease]--
		counts[tgt]++
		needy[needIdx].deficit--
		if needy[needIdx].deficit == 0 {
			needIdx++
		}
	}

	// Phase 2: dispatch moves in parallel — different ranges' lease transfers
	// are independent. counts already reflects the post-move plan (updated
	// optimistically in phase 1); failures are rare (only happen if a range
	// merges out from under us between SHOW RANGES and ALTER RANGE), so the
	// planned counts are a good approximation for the log line.
	//
	// Bound concurrency to 16 per group: well above what the gateway needs
	// to keep its pipeline full but well below what produces useful raft /
	// lease-transfer queue saturation. Empirically (the parallelize-dispatch
	// commit), saturating beyond this point did not reduce per-group wall
	// time: per-call latency climbed under heavy concurrency to roughly
	// match the gain.
	const concurrency = 16
	sem := make(chan struct{}, concurrency)
	var failed int64
	var errOnce sync.Once
	var firstErr error
	grp := tasks.NewErrorGroup(task.WithContext(ctx))
	for _, m := range moves {
		// Try to acquire a concurrency token, but if the sema is full
		// and ctx is cancelled (parent timeout, sibling failure), spawn
		// the goroutine anyway without a token. Its inherited ctx will
		// be Done() immediately and db.ExecContext returns without
		// doing real work, but the spawn keeps grp.WaitE() below
		// accounting for every move we dispatched — no goroutine leak,
		// no early return that skips the summary log.
		acquired := true
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			acquired = false
		}
		grp.Go(func(ctx context.Context, _ *logger.Logger) error {
			if acquired {
				defer func() { <-sem }()
			}
			if _, err := db.ExecContext(ctx, fmt.Sprintf(
				"ALTER RANGE %d RELOCATE LEASE TO %d", m.rangeID, m.target)); err != nil {
				atomic.AddInt64(&failed, 1)
				// Surface the first error so the threshold-exceeded log
				// line below has a concrete cause to point at, instead of
				// just a count.
				errOnce.Do(func() { firstErr = err })
			}
			return nil
		})
	}
	// waitErr captures dispatch infrastructure failures (panic propagated by
	// errgroup, ctx cancellation, etc). Don't early-return on it: the
	// per-call counters and summary log below give triage the only
	// per-range context they're going to get, so always emit them and
	// return the infra error at the end.
	//
	// firstErr is set inside the goroutines and read here. errgroup.Wait
	// establishes the happens-before, so the read is race-free.
	waitErr := grp.WaitE()
	if waitErr != nil {
		l.Printf("splits[%s]: WaitE returned: %v (continuing to summary)", table, waitErr)
	}
	moved := int64(len(moves)) - failed

	// Recompute spread for the log line. minC == -1 is a sentinel for
	// "no values yet"; an actual minimum of 0 leases means the ratio is
	// undefined and we report "inf (store empty)" rather than fudging
	// the denominator.
	minC, maxC := -1, -1
	for _, sid := range groupStores {
		c := counts[sid]
		if minC < 0 || c < minC {
			minC = c
		}
		if maxC < 0 || c > maxC {
			maxC = c
		}
	}
	var spreadStr string
	if minC <= 0 {
		spreadStr = "inf (store empty)"
	} else {
		spreadStr = fmt.Sprintf("%.2fx", float64(maxC)/float64(minC))
	}
	l.Printf("splits[%s]: lease rebalance: ranges=%d target=%d moved=%d failed=%d counts=%v spread=%s",
		table, len(ranges), target, moved, failed, counts, spreadStr)

	// Per-call failures are tolerated up to a threshold: ALTER RANGE
	// RELOCATE LEASE can lose to a concurrent merge, a queue-driven lease
	// move, or a transient leader change. Above ~10% something systemic is
	// wrong (a node went down, the gateway is overloaded), and continuing
	// would mask the actual problem. The plain-count floor keeps groups
	// with very few moves from failing the test on a single transient
	// failure, but we still bail out below the floor if EVERY move failed
	// (the floor doesn't apply when 100% failed: that's never noise).
	if failed > 0 {
		// Warn even under the threshold so an upward drift is visible in
		// CI triage rather than silent until the threshold trips.
		l.Printf("splits[%s]: WARN: %d/%d lease moves failed (first error: %v)",
			table, failed, len(moves), firstErr)
	}
	const (
		// Below 50 moves a single failure is >2% and pure noise; the
		// percentage gate alone would generate spurious test failures.
		minMovesToCheck = 50
		maxFailRate     = 0.1
	)
	if len(moves) > 0 && failed == int64(len(moves)) {
		return errors.Wrapf(firstErr,
			"splits[%s]: every one of %d lease moves failed; first error",
			table, len(moves))
	}
	if len(moves) >= minMovesToCheck && float64(failed)/float64(len(moves)) > maxFailRate {
		return errors.Wrapf(firstErr,
			"splits[%s]: lease rebalance failure rate %d/%d = %.0f%% exceeds %.0f%%; first error",
			table, failed, len(moves),
			100*float64(failed)/float64(len(moves)),
			100*maxFailRate)
	}
	return waitErr
}

// queryStoreMaps returns two cluster-wide store maps:
//   - storeToNode: store_id → node_id
//   - refToStore: (node, storeIdx) → store_id, where storeIdx is the 1-based
//     ordinal of the store within its node ordered by store_id (matching the
//     order roachprod sets the +node{N}store{I} attribute at startup).
func queryStoreMaps(
	ctx context.Context, db *gosql.DB,
) (storeToNode map[int]int, refToStore map[storeRef]int, err error) {
	rows, err := db.QueryContext(ctx, `
		SELECT store_id, node_id,
		       row_number() OVER (PARTITION BY node_id ORDER BY store_id) AS store_idx
		FROM crdb_internal.kv_store_status`)
	if err != nil {
		return nil, nil, errors.Wrap(err, "querying store map")
	}
	defer rows.Close()
	storeToNode = make(map[int]int)
	refToStore = make(map[storeRef]int)
	for rows.Next() {
		var sid, nid, idx int
		if err := rows.Scan(&sid, &nid, &idx); err != nil {
			return nil, nil, errors.Wrap(err, "scanning store map row")
		}
		storeToNode[sid] = nid
		refToStore[storeRef{node: nid, storeIdx: idx}] = sid
	}
	return storeToNode, refToStore, nil
}

// splitAtKeys returns an ALTER TABLE ... SPLIT AT VALUES statement that
// splits the given table at the supplied keys.
func splitAtKeys(table string, keys []int64) string {
	var b strings.Builder
	fmt.Fprintf(&b, "ALTER TABLE %s SPLIT AT VALUES ", table)
	for i, k := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "(%d)", k)
	}
	return b.String()
}

// reportDensity logs an estimate of replicas per vcpu per node based on the
// current cluster range count. We approximate replicas as ranges*3 (default
// replication factor); system ranges with RF=5 contribute a small
// underestimate that doesn't matter at the scales we care about.
func (s splits) reportDensity(
	ctx context.Context, t test.Test, v variations, db *gosql.DB, label string,
) {
	var ranges int64
	if err := db.QueryRowContext(ctx,
		"SELECT count(*) FROM crdb_internal.ranges_no_leases").Scan(&ranges); err != nil {
		// Diagnostic only — do not fail the test on a missing density line.
		t.L().Printf("splits: density query failed (%s): %v", label, err)
		return
	}
	const rf = 3
	replicas := ranges * rf
	perVcpuPerNode := float64(replicas) / float64(v.numNodes*v.vcpu)
	t.L().Printf("splits: density (%s): ranges=%d replicas≈%d replicas/vcpu/node≈%.1f",
		label, ranges, replicas, perVcpuPerNode)
}

// reportBalance logs a one-line summary of the per-node and per-store
// range and lease counts (min/max with the offending IDs and the spread)
// so a glance at the test log makes it obvious whether placement is
// staying even. The per-store view matters because the whole point of
// pinning to specific stores (rather than nodes) is to keep store-level
// balance even by construction; per-node spread can stay near 1.00x while
// per-store spread blows up if pinning fails.
func (s splits) reportBalance(ctx context.Context, t test.Test, db *gosql.DB, label string) {
	rows, err := db.QueryContext(ctx, `
		SELECT node_id, store_id, range_count, lease_count
		FROM crdb_internal.kv_store_status`)
	if err != nil {
		// Diagnostic only — do not fail the test on a missing balance line.
		t.L().Printf("splits: balance query failed (%s): %v", label, err)
		return
	}
	defer rows.Close()
	rangesPerNode := map[int64]int64{}
	leasesPerNode := map[int64]int64{}
	type sk struct{ node, store int64 }
	rangesPerStore := map[sk]int64{}
	leasesPerStore := map[sk]int64{}
	for rows.Next() {
		var n, st, r, l int64
		if err := rows.Scan(&n, &st, &r, &l); err != nil {
			// Diagnostic only — do not fail the test on a partial balance read.
			t.L().Printf("splits: balance scan failed (%s): %v", label, err)
			return
		}
		rangesPerNode[n] += r
		leasesPerNode[n] += l
		rangesPerStore[sk{n, st}] += r
		leasesPerStore[sk{n, st}] += l
	}
	if len(rangesPerNode) == 0 {
		return
	}
	nodeFmt := func(n int64) string { return fmt.Sprintf("n%d", n) }
	storeFmt := func(k sk) string { return fmt.Sprintf("n%ds%d", k.node, k.store) }
	rN := summarize(rangesPerNode, nodeFmt)
	lN := summarize(leasesPerNode, nodeFmt)
	rS := summarize(rangesPerStore, storeFmt)
	lS := summarize(leasesPerStore, storeFmt)
	t.L().Printf("splits: balance (%s): ranges/node %s | leases/node %s | ranges/store %s | leases/store %s",
		label, rN, lN, rS, lS)
}

// summarize formats a "min=K:V max=K:V spread=X.XXx" string for the given
// counts, where K is rendered via labelFn. Iterates in label order so that
// ties on the min/max value pick a deterministic key (otherwise the log
// line is flaky run-to-run on equally-loaded stores).
func summarize[K comparable](counts map[K]int64, labelFn func(K) string) string {
	keys := make([]K, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return labelFn(keys[i]) < labelFn(keys[j]) })
	var minK, maxK K
	var minV, maxV int64
	first := true
	for _, k := range keys {
		v := counts[k]
		if first || v < minV {
			minK, minV = k, v
		}
		if first || v > maxV {
			maxK, maxV = k, v
		}
		first = false
	}
	ratio := float64(maxV) / max(float64(minV), 1)
	return fmt.Sprintf("min=%s:%d max=%s:%d spread=%.2fx",
		labelFn(minK), minV, labelFn(maxK), maxV, ratio)
}
