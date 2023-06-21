// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// lease-preferences/* roachtests assert that after a related event, that lease
// preferences are conformed to. Currently, the only event being tested is
// stopping all or some of the preferred localities nodes.
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

type leasePreferencesSpec struct {
	preferences          string
	ranges, replFactor   int
	stopNodes            []int
	waitForLessPreferred bool
}

func registerLeasePreferences(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "lease-preferences/partial-first-preference-down",
		Owner:   registry.OwnerKV,
		Timeout: 30 * time.Minute,
		// This test purposefully kills nodes. Skip the dead node post-test
		// validation.
		SkipPostValidations: registry.PostValidationNoDeadNodes,
		Cluster:             r.MakeClusterSpec(5, spec.CPU(4)),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runLeasePreferences(ctx, t, c, leasePreferencesSpec{
				preferences:          `[+dc=1],[+dc=2]`,
				ranges:               1000,
				replFactor:           5,
				stopNodes:            []int{2},
				waitForLessPreferred: true,
			})
		},
	})
	r.Add(registry.TestSpec{
		Name:    "lease-preferences/full-first-preference-down",
		Owner:   registry.OwnerKV,
		Timeout: 30 * time.Minute,
		// This test purposefully kills nodes. Skip the dead node post-test
		// validation.
		SkipPostValidations: registry.PostValidationNoDeadNodes,
		Cluster:             r.MakeClusterSpec(5, spec.CPU(4)),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runLeasePreferences(ctx, t, c, leasePreferencesSpec{
				preferences:          `[+dc=1],[+dc=2]`,
				ranges:               1000,
				replFactor:           5,
				stopNodes:            []int{1, 2},
				waitForLessPreferred: false,
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
	// Determine which nodes won't be stopped, these will be checked for lease
	// preference satisfaction.
	checkNodes := make([]int, 0, numNodes-len(spec.stopNodes))
	allNodes := make([]int, 0, numNodes)
	for i := 1; i <= numNodes; i++ {
		var stopped bool
		for _, node := range spec.stopNodes {
			if i == node {
				stopped = true
				break
			}
		}
		if !stopped {
			checkNodes = append(checkNodes, i)
		}
		allNodes = append(allNodes, i)
	}

	settings := install.MakeClusterSettings()
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
				fmt.Sprintf("--locality=region=fake-region,zone=fake-zone,dc=%d", (node-1)/2+1))
			c.Start(ctx, t.L(), opts, settings, c.Node(node))

		}
	}

	if c.IsLocal() {
		// Reduce total number of ranges to a lower number when running locally.
		// Local tests have a default time out of 5 minutes.
		spec.ranges = 100
	}

	c.Put(ctx, t.Cockroach(), "./cockroach")
	t.Status("starting cluster")
	startNodes(allNodes...)

	conn := c.Conn(ctx, t.L(), numNodes)
	defer conn.Close()

	setLeasePreferences := func(ctx context.Context, preferences string) {
		_, err := conn.ExecContext(ctx, fmt.Sprintf(
			`ALTER database kv CONFIGURE ZONE USING 
        num_replicas = %d, 
        num_voters = %d,
        voter_constraints='[]',
        lease_preferences='[%s]'
      `,
			spec.replFactor, spec.replFactor, spec.preferences,
		))
		require.NoError(t, err)
	}

	checkLeasePreferenceConformance := func(ctx context.Context) {
		result, err := waitForLeasePreferences(
			ctx, t, c, checkNodes, spec.waitForLessPreferred, stableDuration)
		require.NoError(t, err)
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
	require.NoError(t, WaitForReplication(ctx, t, conn, spec.replFactor))
	c.Run(ctx, c.Node(numNodes), fmt.Sprintf(
		`./cockroach workload init kv --scatter --splits %d {pgurl:%d}`, spec.ranges, numNodes))
	// Wait for under-replicated ranges before checking lease preference
	// enforcement.
	require.NoError(t, WaitForReplication(ctx, t, conn, spec.replFactor))

	t.L().Printf("waiting for initial preference conformance")
	setLeasePreferences(ctx, spec.preferences)
	checkLeasePreferenceConformance(ctx)

	// Stop the nodes specified by spec.stopNodes.
	t.L().Printf("stopping nodes %v and waiting for lease preference conformance",
		spec.stopNodes)
	stopOpts := option.DefaultStopOpts()
	stopOpts.RoachprodOpts.Sig = 9
	c.Stop(ctx, t.L(), stopOpts, c.Nodes(spec.stopNodes...))

	// Wait for the preference conformance with some non-live nodes.
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
	stableDuration time.Duration,
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
	for {
		select {
		case <-ctx.Done():
			return ret, ctx.Err()
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
