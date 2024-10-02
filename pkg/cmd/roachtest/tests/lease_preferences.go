// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// lease-preferences/* roachtests assert that after a related event, lease
// preferences are conformed to. Currently, the events being tested are
// stopping all or some of the preferred localities nodes, and manually
// transferring all leases to a violating locality.
//
// The timeout is controlled by the test timeout, and is currently 30 minutes.
// In most cases there should be no violating preferences immediately. However,
// this test chooses not to assert on this in order to reduce noise.
//
// The results are printed to the log file:
//
//	30.56s: violating(false) duration=0.00s: [n3: 0, n4: 0, n5: 0]
//	        less-preferred(true) duration=30.56s: [n3: 10, n4: 12, n5: 0]
//
//	Where violating(true|false) indicates whether there is a lease violating a
//	preference currently. less-preferred(true|false) indicates whether there is
//	a lease on locality which is preferred, but not the first preference. The
//	elapsed duration of violating/less-preferred is also shown.

type leasePreferencesEventFn func(context.Context, test.Test, cluster.Cluster)

type leasePreferencesSpec struct {
	preferences           string
	ranges, replFactor    int
	eventFn               leasePreferencesEventFn
	checkNodes            []int
	waitForLessPreferred  bool
	postEventWaitDuration time.Duration
}

// makeStopNodesEventFn returns a leasePreferencesEventFn which stops the
// node targets supplied as arguments, when invoked.
func makeStopNodesEventFn(targets ...int) leasePreferencesEventFn {
	return func(ctx context.Context, t test.Test, c cluster.Cluster) {
		t.L().Printf(
			"stopping nodes %v and waiting for lease preference conformance",
			targets)
		stopOpts := option.DefaultStopOpts()
		stopOpts.RoachprodOpts.Sig = 9
		c.Stop(ctx, t.L(), stopOpts, c.Nodes(targets...))
	}
}

// makeTransferLeasesEventFn returns a leasePreferencesEventFn which transfers
// every lease in the workload table to the target node, when invoked.
func makeTransferLeasesEventFn(gateway, target int) leasePreferencesEventFn {
	return func(ctx context.Context, t test.Test, c cluster.Cluster) {
		t.L().Printf(
			"transferring leases to node %v and waiting for lease preference conformance",
			target)
		conn := c.Conn(ctx, t.L(), gateway)
		defer conn.Close()
		_, err := conn.ExecContext(ctx, fmt.Sprintf(`
      ALTER RANGE RELOCATE LEASE TO %d
      FOR SELECT range_id
      FROM [SHOW RANGES FROM DATABASE kv WITH DETAILS]
      WHERE lease_holder <> %d
      `,
			target, target,
		))
		require.NoError(t, err)
	}
}

func registerLeasePreferences(r registry.Registry) {
	r.Add(registry.TestSpec{
		// NB: This test takes down 1(/2) nodes in the most preferred locality.
		// Some of the leases on the stopped node will be acquired by node's which
		// are not in the preferred locality; or in a secondary preferred locality.
		Name:    "lease-preferences/partial-first-preference-down",
		Owner:   registry.OwnerKV,
		Timeout: 30 * time.Minute,
		// This test purposefully kills nodes. Skip the dead node post-test
		// validation.
		SkipPostValidations: registry.PostValidationNoDeadNodes,
		Cluster:             r.MakeClusterSpec(5, spec.CPU(4)),
		CompatibleClouds:    registry.OnlyGCE,
		Suites:              registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runLeasePreferences(ctx, t, c, leasePreferencesSpec{
				preferences:           `[+dc=1],[+dc=2]`,
				ranges:                1000,
				replFactor:            5,
				checkNodes:            []int{1, 3, 4, 5},
				eventFn:               makeStopNodesEventFn(2 /* targets */),
				waitForLessPreferred:  true,
				postEventWaitDuration: 5 * time.Minute,
			})
		},
	})
	r.Add(registry.TestSpec{
		// NB: This test takes down 2(/2) nodes in the most preferred locality. The
		// leases on the stopped node will be acquired by node's which are not in
		// the most preferred locality. This test waits until all the leases are on
		// the secondary preference.
		Name:    "lease-preferences/full-first-preference-down",
		Owner:   registry.OwnerKV,
		Timeout: 30 * time.Minute,
		// This test purposefully kills nodes. Skip the dead node post-test
		// validation.
		SkipPostValidations: registry.PostValidationNoDeadNodes,
		Cluster:             r.MakeClusterSpec(5, spec.CPU(4)),
		CompatibleClouds:    registry.OnlyGCE,
		Suites:              registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runLeasePreferences(ctx, t, c, leasePreferencesSpec{
				preferences:           `[+dc=1],[+dc=2]`,
				ranges:                1000,
				replFactor:            5,
				eventFn:               makeStopNodesEventFn(1, 2 /* targets */),
				checkNodes:            []int{3, 4, 5},
				waitForLessPreferred:  false,
				postEventWaitDuration: 5 * time.Minute,
			})
		},
	})
	r.Add(registry.TestSpec{
		// NB: This test manually transfers leases onto [+dc=3], which violates the
		// lease preferences. This test then waits until all the leases are back on
		// the most preferred locality.
		Name:             "lease-preferences/manual-violating-transfer",
		Owner:            registry.OwnerKV,
		Timeout:          30 * time.Minute,
		Cluster:          r.MakeClusterSpec(5, spec.CPU(4)),
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runLeasePreferences(ctx, t, c, leasePreferencesSpec{
				preferences: `[+dc=1],[+dc=2]`,
				ranges:      1000,
				replFactor:  5,
				eventFn: makeTransferLeasesEventFn(
					5 /* gateway */, 5 /* target */),
				checkNodes:            []int{1, 2, 3, 4, 5},
				waitForLessPreferred:  true,
				postEventWaitDuration: 5 * time.Minute,
			})
		},
	})
}

func runLeasePreferences(
	ctx context.Context, t test.Test, c cluster.Cluster, spec leasePreferencesSpec,
) {
	// stableDuration is how long the test will wait, after satisfying the most
	// preferred preference, or just some preference (when
	// waitForLessPreferred=false). This duration is used to ensure the lease
	// preference satisfaction is reasonably permanent.
	const stableDuration = 30 * time.Second

	numNodes := c.Spec().NodeCount
	allNodes := make([]int, 0, numNodes)
	for i := 1; i <= numNodes; i++ {
		allNodes = append(allNodes, i)
	}

	// TODO(kvoli): temporary workaround for
	// https://github.com/cockroachdb/cockroach/issues/105274
	settings := install.MakeClusterSettings()
	settings.ClusterSettings["server.span_stats.span_batch_limit"] = "4096"
	settings.ClusterSettings["kv.enqueue_in_replicate_queue_on_span_config_update.enabled"] = "true"

	startNodes := func(nodes ...int) {
		for _, node := range nodes {
			// Don't start a backup schedule because this test is timing sensitive.
			opts := option.DefaultStartOpts()
			opts.RoachprodOpts.ScheduleBackups = false
			opts.RoachprodOpts.ExtraArgs = append(opts.RoachprodOpts.ExtraArgs,
				// Set 2 nodes per DC locality:
				// dc=1: n1 n2
				// dc=2: n3 n4
				// ...
				// dc=N: n2N-1 n2N
				fmt.Sprintf("--locality=region=fake-region,zone=fake-zone,dc=%d", (node-1)/2+1),
				"--vmodule=replica_proposal=2,lease_queue=3,lease=3")
			c.Start(ctx, t.L(), opts, settings, c.Node(node))

		}
	}

	if c.IsLocal() {
		// Reduce total number of ranges to a lower number when running locally.
		// Local tests have a default time out of 5 minutes.
		spec.ranges = 100
	}

	t.Status("starting cluster")
	startNodes(allNodes...)

	conn := c.Conn(ctx, t.L(), numNodes)
	defer conn.Close()

	checkLeasePreferenceConformance := func(ctx context.Context) {
		result, err := waitForLeasePreferences(
			ctx, t, c, spec.checkNodes, spec.waitForLessPreferred, stableDuration, spec.postEventWaitDuration)
		require.NoError(t, err, result)
		require.Truef(t, !result.violating(), "violating lease preferences %s", result)
		if spec.waitForLessPreferred {
			require.Truef(t, !result.lessPreferred(), "less preferred preferences %s", result)
		}
	}

	t.L().Printf("creating workload database")
	_, err := conn.ExecContext(ctx, `CREATE DATABASE kv;`)
	require.NoError(t, err)

	// Initially, set no lease preference and require every range to have the
	// replication factor specified in the test.
	t.L().Printf("setting zone configs")
	configureAllZones(t, ctx, conn, zoneConfig{
		replicas: spec.replFactor,
	})
	// Wait for the existing ranges (not kv) to be up-replicated. That way,
	// creating the splits and waiting for up-replication on kv will be much
	// quicker.
	require.NoError(t, roachtestutil.WaitForReplication(ctx, t.L(), conn, spec.replFactor, roachtestutil.AtLeastReplicationFactor))
	c.Run(ctx, option.WithNodes(c.Node(numNodes)), fmt.Sprintf(
		`./cockroach workload init kv --scatter --splits %d {pgurl:%d}`,
		spec.ranges, numNodes))
	// Wait for under-replicated ranges before checking lease preference
	// enforcement.
	require.NoError(t, roachtestutil.WaitForReplication(ctx, t.L(), conn, spec.replFactor, roachtestutil.AtLeastReplicationFactor))

	// Set a lease preference for the liveness range, to be on n5. This test
	// would occasionally fail due to the liveness heartbeat failures, when the
	// liveness lease is on a stopped node. This is not ideal behavior, #108512.
	configureZone(t, ctx, conn, "RANGE liveness", zoneConfig{
		replicas:        spec.replFactor,
		leasePreference: "[+node5]",
	})

	t.L().Printf("setting lease preferences: %s", spec.preferences)
	configureZone(t, ctx, conn, "DATABASE kv", zoneConfig{
		replicas:        spec.replFactor,
		leasePreference: spec.preferences,
	})
	t.L().Printf("waiting for initial lease preference conformance")
	checkLeasePreferenceConformance(ctx)

	// Run the spec event function. The event function will move leases to
	// non-conforming localities.
	spec.eventFn(ctx, t, c)
	// Run a full table scan to force lease acquisition, there may be unacquired
	// leases if the event stopped nodes.
	_, err = conn.ExecContext(ctx, `SELECT count(*) FROM kv.kv;`)
	require.NoError(t, err)

	// Wait for the preference conformance with some leases in non-conforming
	// localities.
	t.L().Printf("waiting for post-event lease preference conformance")
	checkLeasePreferenceConformance(ctx)
}

type leasePreferencesResult struct {
	nodes                                    []int
	violatingCounts, lessPreferredCounts     []int
	violatingDuration, lessPreferredDuration time.Duration
}

func (lpr leasePreferencesResult) violating() bool {
	for i := range lpr.nodes {
		if lpr.violatingCounts[i] > 0 {
			return true
		}
	}
	return false
}

func (lpr leasePreferencesResult) lessPreferred() bool {
	for i := range lpr.nodes {
		if lpr.lessPreferredCounts[i] > 0 {
			return true
		}
	}
	return false
}

func (lpr leasePreferencesResult) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf,
		"violating(%v) duration=%.2fs: [",
		lpr.violating(), lpr.violatingDuration.Seconds())
	for i := range lpr.nodes {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "n%d: %d", lpr.nodes[i], lpr.violatingCounts[i])
	}
	fmt.Fprintf(&buf,
		"] less-preferred(%v) duration=%.2fs: [",
		lpr.lessPreferred(), lpr.lessPreferredDuration.Seconds())
	for i := range lpr.nodes {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "n%d: %d", lpr.nodes[i], lpr.lessPreferredCounts[i])
	}
	buf.WriteString("]")
	return buf.String()
}

// waitForLeasePreferences waits until there are no leases violating
// preferences, or also that there are no leases on less preferred options.
// When checkViolatingOnly is true, this function only waits for leases
// violating preferences, and does not wait for leases on less preferred nodes.
func waitForLeasePreferences(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	nodes []int,
	waitForLessPreferred bool,
	stableDuration, maxWaitDuration time.Duration,
) (leasePreferencesResult, error) {
	// NB: We are querying metrics, expect these to be populated approximately
	// every 10s.
	const checkInterval = 10 * time.Second
	var checkTimer timeutil.Timer
	defer checkTimer.Stop()
	checkTimer.Reset(checkInterval)

	// preferenceMetrics queries the crdb_internal metrics table per-node and
	// returns two slices: (1) number of leases on each node which are violating
	// preferences, and (2) the number of leases on each node which are less
	// preferred, but satisfy a preference.
	preferenceMetrics := func(ctx context.Context) (violating, lessPreferred []int) {
		const violatingPreferenceMetricName = `leases.preferences.violating`
		const lessPreferredMetricMetricName = `leases.preferences.less-preferred`
		for _, node := range nodes {
			violating = append(violating,
				int(nodeMetric(ctx, t, c, node, violatingPreferenceMetricName)))
			lessPreferred = append(lessPreferred,
				int(nodeMetric(ctx, t, c, node, lessPreferredMetricMetricName)))
		}
		return violating, lessPreferred
	}

	var ret leasePreferencesResult
	ret.nodes = nodes
	start := timeutil.Now()
	maxWaitC := time.After(maxWaitDuration)
	for {
		select {
		case <-ctx.Done():
			return ret, ctx.Err()
		case <-maxWaitC:
			return ret, errors.Errorf("timed out before lease preferences satisfied")
		case <-checkTimer.C:
			checkTimer.Read = true
			violating, lessPreferred := preferenceMetrics(ctx)
			now := timeutil.Now()
			sinceStart := now.Sub(start)
			// Sanity check we are getting the correct number of metrics back, given
			// the nodes we are checking.
			require.Equal(t, len(nodes), len(violating))
			require.Equal(t, len(nodes), len(lessPreferred))
			ret.violatingCounts = violating
			ret.lessPreferredCounts = lessPreferred
			isViolating, isLessPreferred := ret.violating(), ret.lessPreferred()
			// Record either the stable duration if the preferences are satisfied, or
			// record the time since waiting began, if not.
			var violatingStable, lessPreferredStable bool
			if isViolating {
				ret.violatingDuration = sinceStart
			} else {
				violatingStable = sinceStart-ret.violatingDuration > stableDuration
			}
			if isLessPreferred {
				ret.lessPreferredDuration = sinceStart
			} else {
				lessPreferredStable = sinceStart-ret.lessPreferredDuration > stableDuration
			}

			// Report the status of the test every checkInterval (10s).
			t.L().Printf("%.2fs: %s", sinceStart.Seconds(), ret)
			stableNotViolating := !isViolating && violatingStable
			stableNotLessPreferred := !isLessPreferred && lessPreferredStable
			if stableNotViolating && stableNotLessPreferred {
				// Every lease is on the most preferred preference and has been for at
				// least stableDuration. Return early.
				return ret, nil
			} else if !waitForLessPreferred && stableNotViolating {
				// Every lease is on some preference for at least stableDuration. We
				// aren't going to wait for less preferred leases because
				// waitForLessPreferred is false.
				return ret, nil
			} else {
				// Some leases are on non-preferred localities, or there are less
				// preferred leases and we are waiting for both.
				t.L().Printf("not yet meeting requirements will check again in %s",
					checkInterval)
			}
			checkTimer.Reset(checkInterval)
		}
	}
}
