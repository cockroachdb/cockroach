// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package asim_test

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
)

func Example_tickEmpty() {
	start := state.TestingStartTime()
	s := state.LoadConfig(state.ComplexConfig)
	m := asim.NewMetricsTracker()

	_ = m.Tick(start, s)
	// Output:
	//tick,c_ranges,c_write,c_write_b,c_read,c_read_b,s_ranges,s_write,s_write_b,s_read,s_read_b,c_lease_moves,c_replica_moves,c_replica_b_moves
	//2022-03-21 11:00:00 +0000 UTC,1,0,0,0,0,0,0,0,0,1,0,0
}

func Example_tickWithWorkload() {
	ctx := context.Background()
	start := state.TestingStartTime()
	end := start.Add(200 * time.Second)
	interval := 10 * time.Second
	rwg := make([]workload.Generator, 1)
	rwg[0] = testCreateWorkloadGenerator(start, 10, 10000)
	m := asim.NewMetricsTracker()

	exchange := state.NewFixedDelayExhange(start, interval, interval)
	changer := state.NewReplicaChanger()
	s := state.LoadConfig(state.ComplexConfig)
	testPreGossipStores(s, exchange, start)

	sim := asim.NewSimulator(start, end, interval, rwg, s, exchange, changer, interval, m)
	sim.RunSim(ctx)
	// Output:
	//tick,c_ranges,c_write,c_write_b,c_read,c_read_b,s_ranges,s_write,s_write_b,s_read,s_read_b,c_lease_moves,c_replica_moves,c_replica_b_moves
	//2022-03-21 11:00:10 +0000 UTC,1,150000,28770981,50000,9590327,50000,9590327,50000,9590327,1,0,0
	//2022-03-21 11:00:20 +0000 UTC,1,300000,57551295,100000,19183765,100000,19183765,100000,19183765,1,0,0
	//2022-03-21 11:00:30 +0000 UTC,1,450000,86389635,150000,28796545,150000,28796545,150000,28796545,1,0,0
	//2022-03-21 11:00:40 +0000 UTC,1,600000,115252992,200000,38417664,200000,38417664,200000,38417664,1,0,0
	//2022-03-21 11:00:50 +0000 UTC,1,750000,144072969,250000,48024323,250000,48024323,250000,48024323,1,0,0
	//2022-03-21 11:01:00 +0000 UTC,1,900000,172881249,300000,57627083,300000,57627083,300000,57627083,1,0,0
	//2022-03-21 11:01:10 +0000 UTC,1,1050000,201641856,350000,67213952,350000,67213952,350000,67213952,1,0,0
	//2022-03-21 11:01:20 +0000 UTC,1,1200000,230430975,400000,76810325,400000,76810325,400000,76810325,1,0,0
	//2022-03-21 11:01:30 +0000 UTC,1,1350000,259191759,450000,86397253,450000,86397253,450000,86397253,1,0,0
	//2022-03-21 11:01:40 +0000 UTC,1,1500000,287963640,500000,95987880,500000,95987880,500000,95987880,1,0,0
	//2022-03-21 11:01:50 +0000 UTC,1,1650000,316768725,550000,105589575,550000,105589575,550000,105589575,1,0,0
	//2022-03-21 11:02:00 +0000 UTC,1,1800000,345554610,600000,115184870,600000,115184870,600000,115184870,1,0,0
	//2022-03-21 11:02:10 +0000 UTC,1,1950000,374397840,650000,124799280,650000,124799280,650000,124799280,1,0,0
	//2022-03-21 11:02:20 +0000 UTC,1,2100000,403219875,700000,134406625,700000,134406625,700000,134406625,1,0,0
	//2022-03-21 11:02:30 +0000 UTC,1,2250000,432009114,750000,144003038,750000,144003038,750000,144003038,1,0,0
	//2022-03-21 11:02:40 +0000 UTC,1,2400000,460808766,800000,153602922,800000,153602922,800000,153602922,1,0,0
	//2022-03-21 11:02:50 +0000 UTC,1,2550000,489634557,850000,163211519,850000,163211519,850000,163211519,1,0,0
	//2022-03-21 11:03:00 +0000 UTC,1,2700000,518473677,900000,172824559,900000,172824559,900000,172824559,1,0,0
	//2022-03-21 11:03:10 +0000 UTC,1,2850000,547294140,950000,182431380,950000,182431380,950000,182431380,1,0,0
	//2022-03-21 11:03:20 +0000 UTC,1,3000000,576084015,1000000,192028005,1000000,192028005,1000000,192028005,1,0,0
}
