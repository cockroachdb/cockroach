// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metrics_test

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/scheduled"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/require"
)

const testingMetricsInterval = 10 * time.Second

func Example_noWriters() {
	ctx := context.Background()
	start := state.TestingStartTime()
	s := state.LoadConfig(state.ComplexConfig, state.SingleRangeConfig, config.DefaultSimulationSettings())
	m := metrics.NewTracker(testingMetricsInterval)

	m.Tick(ctx, start, s)
	// Output:
}

func Example_tickEmptyState() {
	ctx := context.Background()
	start := state.TestingStartTime()
	s := state.LoadConfig(state.ComplexConfig, state.SingleRangeConfig, config.DefaultSimulationSettings())
	m := metrics.NewTracker(testingMetricsInterval, metrics.NewClusterMetricsTracker(os.Stdout))

	m.Tick(ctx, start, s)
	// Output:
	//tick,c_ranges,c_write,c_write_b,c_read,c_read_b,s_ranges,s_write,s_write_b,s_read,s_read_b,c_lease_moves,c_replica_moves,c_replica_b_moves
	//2022-03-21 11:00:00 +0000 UTC,1,0,0,0,0,0,0,0,0,0,0,0
}

func TestTickEmptyState(t *testing.T) {
	ctx := context.Background()
	start := state.TestingStartTime()
	s := state.LoadConfig(state.ComplexConfig, state.SingleRangeConfig, config.DefaultSimulationSettings())

	var buf bytes.Buffer
	m := metrics.NewTracker(testingMetricsInterval, metrics.NewClusterMetricsTracker(&buf))

	m.Tick(ctx, start, s)

	expected :=
		"tick,c_ranges,c_write,c_write_b,c_read,c_read_b,s_ranges,s_write,s_write_b,s_read,s_read_b,c_lease_moves,c_replica_moves,c_replica_b_moves\n" +
			"2022-03-21 11:00:00 +0000 UTC,1,0,0,0,0,0,0,0,0,0,0,0\n"
	require.Equal(t, expected, buf.String())
}

func Example_multipleWriters() {
	ctx := context.Background()
	start := state.TestingStartTime()
	s := state.LoadConfig(state.ComplexConfig, state.SingleRangeConfig, config.DefaultSimulationSettings())
	m := metrics.NewTracker(testingMetricsInterval, metrics.NewClusterMetricsTracker(os.Stdout, os.Stdout))

	m.Tick(ctx, start, s)
	// Output:
	//tick,c_ranges,c_write,c_write_b,c_read,c_read_b,s_ranges,s_write,s_write_b,s_read,s_read_b,c_lease_moves,c_replica_moves,c_replica_b_moves
	//tick,c_ranges,c_write,c_write_b,c_read,c_read_b,s_ranges,s_write,s_write_b,s_read,s_read_b,c_lease_moves,c_replica_moves,c_replica_b_moves
	//2022-03-21 11:00:00 +0000 UTC,1,0,0,0,0,0,0,0,0,0,0,0
	//2022-03-21 11:00:00 +0000 UTC,1,0,0,0,0,0,0,0,0,0,0,0
}

func Example_leaseTransfer() {
	ctx := context.Background()
	start := state.TestingStartTime()
	s := state.LoadConfig(state.ComplexConfig, state.SingleRangeConfig, config.DefaultSimulationSettings())
	m := metrics.NewTracker(testingMetricsInterval, metrics.NewClusterMetricsTracker(os.Stdout))

	changer := state.NewReplicaChanger()
	changer.Push(state.TestingStartTime(), &state.LeaseTransferChange{
		RangeID:        1,
		TransferTarget: 2,
		Author:         1,
		Wait:           0,
	})
	changer.Tick(state.TestingStartTime(), s)
	m.Tick(ctx, start, s)
	// Output:
	//tick,c_ranges,c_write,c_write_b,c_read,c_read_b,s_ranges,s_write,s_write_b,s_read,s_read_b,c_lease_moves,c_replica_moves,c_replica_b_moves
	//2022-03-21 11:00:00 +0000 UTC,1,0,0,0,0,0,0,0,0,1,0,0
}

func Example_rebalance() {
	ctx := context.Background()
	start := state.TestingStartTime()
	s := state.LoadConfig(state.ComplexConfig, state.SingleRangeConfig, config.DefaultSimulationSettings())
	m := metrics.NewTracker(testingMetricsInterval, metrics.NewClusterMetricsTracker(os.Stdout))

	// Apply load, to get a replica size greater than 0.
	le := workload.LoadBatch{workload.LoadEvent{Writes: 1, WriteSize: 7, Reads: 2, ReadSize: 9, Key: 5}}
	s.ApplyLoad(le)

	// Do the rebalance.
	c := &state.ReplicaChange{
		RangeID: 1,
		Author:  1,
		Changes: append(kvpb.MakeReplicationChanges(roachpb.ADD_VOTER, roachpb.ReplicationTarget{
			NodeID:  4,
			StoreID: 4,
		}), kvpb.MakeReplicationChanges(roachpb.REMOVE_VOTER, roachpb.ReplicationTarget{
			NodeID:  1,
			StoreID: 1,
		})...),
		Wait: 0,
	}
	c.Apply(s)

	m.Tick(ctx, start, s)
	// Output:
	//tick,c_ranges,c_write,c_write_b,c_read,c_read_b,s_ranges,s_write,s_write_b,s_read,s_read_b,c_lease_moves,c_replica_moves,c_replica_b_moves
	//2022-03-21 11:00:00 +0000 UTC,1,3,21,2,9,1,7,2,9,0,1,7
}

func Example_workload() {
	ctx := context.Background()
	settings := config.DefaultSimulationSettings()
	duration := 200 * time.Second
	rwg := make([]workload.Generator, 1)
	rwg[0] = workload.TestCreateWorkloadGenerator(settings.Seed, settings.StartTime, 10, 10000)
	m := metrics.NewTracker(testingMetricsInterval, metrics.NewClusterMetricsTracker(os.Stdout))

	s := state.LoadConfig(state.ComplexConfig, state.SingleRangeConfig, settings)

	sim := asim.NewSimulator(duration, rwg, s, settings, m, scheduled.NewExecutorWithNoEvents())
	sim.RunSim(ctx)
	// WIP: non deterministic
	// Output:
	//tick,c_ranges,c_write,c_write_b,c_read,c_read_b,s_ranges,s_write,s_write_b,s_read,s_read_b,c_lease_moves,c_replica_moves,c_replica_b_moves
	//2022-03-21 11:00:10 +0000 UTC,1,7500,1430259,47500,9113574,2500,476753,47500,9113574,1,0,0
	//2022-03-21 11:00:20 +0000 UTC,1,15000,2860140,95000,18230385,5000,953380,95000,18230385,1,0,0
	//2022-03-21 11:00:30 +0000 UTC,2,22500,4301097,142500,27362846,7500,1433699,142500,27362846,2,0,0
	//2022-03-21 11:00:40 +0000 UTC,3,30000,5750298,190000,36500898,10000,1916766,190000,36500898,3,0,0
	//2022-03-21 11:00:50 +0000 UTC,4,37500,7189272,237500,45627899,12500,2396424,237500,45627899,5,0,0
	//2022-03-21 11:01:00 +0000 UTC,5,45000,8626290,285000,54751653,15000,2875430,285000,54751653,7,0,0
	//2022-03-21 11:01:10 +0000 UTC,6,52500,10059840,332500,63860672,17500,3353280,332500,63860672,9,1,716849
	//2022-03-21 11:01:20 +0000 UTC,7,60000,11493504,380000,72979157,20000,3831168,380000,72979157,11,2,1316807
	//2022-03-21 11:01:30 +0000 UTC,8,67500,12924417,427500,82089114,22500,4308139,427500,82089114,13,4,2573464
	//2022-03-21 11:01:40 +0000 UTC,10,75000,14363499,475000,91200047,25000,4787833,475000,91200047,16,6,3799720
	//2022-03-21 11:01:50 +0000 UTC,12,82500,15812037,522500,100318896,27500,5270679,522500,100318896,19,8,4399678
	//2022-03-21 11:02:00 +0000 UTC,15,90000,17252352,570000,109434086,30000,5750784,570000,109434086,24,11,5478968
	//2022-03-21 11:02:10 +0000 UTC,18,97500,18702216,617500,118565208,32500,6234072,617500,118565208,30,14,6408268
	//2022-03-21 11:02:20 +0000 UTC,21,105000,20147733,665000,127690714,35000,6715911,665000,127690714,34,16,7036848
	//2022-03-21 11:02:30 +0000 UTC,25,112500,21594528,712500,136804862,37500,7198176,712500,136804862,39,19,7815417
	//2022-03-21 11:02:40 +0000 UTC,29,120000,23035728,760000,145924346,40000,7678576,760000,145924346,44,20,8301175
	//2022-03-21 11:02:50 +0000 UTC,33,127500,24475320,807500,155053079,42500,8158440,807500,155053079,51,22,8862279
	//2022-03-21 11:03:00 +0000 UTC,36,135000,25916628,855000,164185683,45000,8638876,855000,164185683,59,25,10108216
	//2022-03-21 11:03:10 +0000 UTC,42,142500,27350499,902500,173314547,47500,9116833,902500,173314547,71,29,10969643
	//2022-03-21 11:03:20 +0000 UTC,49,150000,28791705,950000,182430770,50000,9597235,950000,182430770,85,36,12021821
}
