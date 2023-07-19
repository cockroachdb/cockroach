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
	gosql "database/sql"
	"fmt"
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

// ideas for faster lease preference enforcement
// - add a chained action for maybe shedding the lease
// - try to shed the lease directly upon acquisition
// - increase the priority of lease preference satisfaction
//   - for enqueue only
//   - with an action
//
// scenarios
// - universal
//   - 3 racks rack=0..2
//   - N nodes
//   - R ranges
//
// - outages
//   - 1 node in rack=0
//   - rack=0
//
// - preferences
//   - [[+rack=0]]
//   - [[+rack=0],[+rack=1]]
//   - [[+rack=0]] -> [[+rack=1]]
//   - [[-rack=2]]
//
// - other activity
//   - zone config change (upreplication/downreplication)
//   - load
//
// assume starting with lease_preferences: [[+dc=1],[+dc=2]] num_replicas=5
//
// 1. stopping rand node in dc=1 causes all leases to move to other nodes in dc=1
// 2. stopping all nodes in dc=1 causes all leases to move to dc=2
// 3. changing lease preference from [[dc=1],[dc=2]] -> [[dc=2],[dc=1]] causes
//
// events
// - kv workload
// - num_replicas++

// 1=n1,n2
// 2=n3,n4
// 3=n5
var localityMap = map[int]string{
	1: "1",
	2: "1",
	3: "2",
	4: "2",
	5: "3",
	6: "3",
}

type leasePreferencesSpec struct {
	nodeLocalities map[int]string
	preferences    string
	ranges         int
}

func registerLeasePreferences(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "lease-preferences/partial-first-preference-down",
		Owner:   registry.OwnerKV,
		Timeout: 30 * time.Minute,
		Cluster: r.MakeClusterSpec(7, spec.CPU(4)),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runLeasePreferences(ctx, t, c, leasePreferencesSpec{
				nodeLocalities: localityMap,
				preferences:    `[+dc=0], [+dc=1]`,
				ranges:         1000,
			})
		},
	})
}

func runLeasePreferences(
	ctx context.Context, t test.Test, c cluster.Cluster, spec leasePreferencesSpec,
) {
	// Setup Cluster
	//
	// Every node's locality should be specified. The last node is not part of
	// the cockroach cluster but can be used for the workload.
	require.Equal(t, len(spec.nodeLocalities), c.Spec().NodeCount-1)
	c.Put(ctx, t.Cockroach(), "./cockroach")
	t.Status("starting cluster")
	for i := 1; i <= len(localityMap); i++ {
		// Don't start a backup schedule because....
		opts := option.DefaultStartOpts()
		opts.RoachprodOpts.ScheduleBackups = false
		opts.RoachprodOpts.ExtraArgs = append(opts.RoachprodOpts.ExtraArgs,
			fmt.Sprintf("locality=dc=%s", localityMap[i]))
		c.Start(ctx, t.L(), opts, install.MakeClusterSettings(), c.Node(i))
	}

	workloadNode := c.Spec().NodeCount
	numNodes := c.Spec().NodeCount - 1

	conn := c.Conn(ctx, t.L(), numNodes)

	// Setup DB
	t.L().Printf("creating workload database")
	_, err := conn.ExecContext(ctx, `CREATE DATABASE kv`)
	require.NoError(t, err)

	// Setup Zone Configs
	t.L().Printf("setting zone configs")
	// Every node will have a replica for every range. Set the lease preference
	// to be on the last node, which won't be stopped/decommissioned etc.
	configureAllZones(t, ctx, conn, zoneConfig{
		replicas:  numNodes,
		leaseNode: numNodes,
	})
	_, err = conn.ExecContext(ctx, fmt.Sprintf(
		`ALTER kv CONFIGURE ZONE USING num_replicas = %d, lease_preferences = '[%s]'`,
		numNodes, spec.preferences,
	))
	require.NoError(t, err)
	require.NoError(t, WaitForReplication(ctx, t, conn, numNodes))

	// Setup ranges/replicas
	c.Run(ctx, c.Node(workloadNode), fmt.Sprintf(
		`./cockroach workload init kv --splits %d {pgurl:%d}`, spec.ranges, numNodes))
	require.NoError(t, WaitForReplication(ctx, t, conn, numNodes))

	stopOpts := option.DefaultStopOpts()
	stopOpts.RoachprodOpts.Sig = 9
	c.Stop(ctx, t.L(), stopOpts, c.Node(1))

	sleepFor(ctx, t, 5*time.Second)
	start := timeutil.Now()
	defer t.L().Printf("lease preference satisfaction took %v", timeutil.Now().Sub(start))

	if err := waitForExpectedLeaseCounts(ctx, t, conn, map[string]int{
		"1": 0,
		"2": 1000,
		"3": 0,
	}); err != nil {
		t.L().Printf("err=%v", err)
	}
}

func waitForExpectedLeaseCounts(
	ctx context.Context, t test.Test, conn *gosql.DB, expectedLeaseCounts map[string]int,
) error {
	const checkInterval = 5 * time.Second

	var checkTimer timeutil.Timer
	defer checkTimer.Stop()
	checkTimer.Reset(checkInterval)

	t.L().Printf(
		"checking for locality lease counts match %+v",
		expectedLeaseCounts)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-checkTimer.C:
			checkTimer.Read = true
			leaseCounts, err := dcLeaseCounts(ctx, t, conn)
			if err != nil {
				return err
			}
			t.L().Printf("locality lease counts %+v", leaseCounts)
			matches := true
			for dc := range expectedLeaseCounts {
				if count, ok := leaseCounts[dc]; expectedLeaseCounts[dc] == 0 && (!ok || count == 0) {
					continue
				}
				if expectedLeaseCounts[dc] != leaseCounts[dc] {
					matches = false
					break
				}
			}
			if matches {
				return nil
			}
			checkTimer.Reset(checkInterval)
		}
	}
}

func dcLeaseCounts(ctx context.Context, t test.Test, conn *gosql.DB) (map[string]int, error) {
	rows, err := conn.QueryContext(ctx, `
    SELECT 
      SPLIT_PART(lease_holder_locality, 'dc=', 2) AS locality_value,
      COUNT(*)
    FROM
      (SELECT lease_holder_locality FROM [SHOW RANGES FROM DATABASE kv WITH DETAILS])
    GROUP BY
     locality_value;
  `)
	if err != nil {
		return nil, err
	}
	dcLeaseCounts := map[string]int{}
	var dc string
	var leaseCount int
	for rows.Next() {
		require.NoError(t, rows.Scan(&dc, &leaseCount))
		dcLeaseCounts[dc] = leaseCount
	}
	return dcLeaseCounts, nil
}
