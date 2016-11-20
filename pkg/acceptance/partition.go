// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package acceptance

import (
	"math/rand"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/acceptance/iptables"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

func mustGetHosts(
	ctx context.Context, t *testing.T, c cluster.Cluster,
) ([]iptables.IP, map[iptables.IP]int) {
	var addrs []iptables.IP
	addrsToNode := make(map[iptables.IP]int)
	for i := 0; i < c.NumNodes(); i++ {
		addr := iptables.IP(c.InternalIP(ctx, i).String())
		addrsToNode[addr] = i
		addrs = append(addrs, addr)
	}
	return addrs, addrsToNode
}

func randomBidirectionalPartition(numNodes int) [][]int {
	if numNodes <= 1 {
		return ([][]int{{0}})[:numNodes]
	}
	all := rand.Perm(numNodes)
	r := 1 + rand.Intn(numNodes-1)
	return [][]int{all[:r], all[r:]}
}

// A NemesisFn runs a nemesis on the given cluster, shutting down in a timely
// manner when the stop channel is closed.
type NemesisFn func(ctx context.Context, t *testing.T, c cluster.Cluster, stopper *stop.Stopper)

// BidirectionalPartitionNemesis is a nemesis which randomly severs the network
// symmetrically between two random groups of nodes. Partitioned and connected
// mode take alternating turns, with random durations of up to 15s.
func BidirectionalPartitionNemesis(
	ctx context.Context, t *testing.T, c cluster.Cluster, stopper *stop.Stopper,
) {
	randSec := func() time.Duration { return time.Duration(rand.Int63n(15 * int64(time.Second))) }
	log.Infof(ctx, "cleaning up any previous rules")
	_ = restoreNetwork(ctx, t, c) // clean up any potential leftovers
	log.Infof(ctx, "starting partition nemesis")
	for {
		ch := make(chan struct{})
		go func() {
			select {
			case <-time.After(randSec()):
			case <-stopper.ShouldStop():
			}
			close(ch)
		}()
		cutNetwork(ctx, t, c, ch, randomBidirectionalPartition(c.NumNodes())...)
		select {
		case <-stopper.ShouldStop():
			return
		case <-time.After(randSec()):
		}
	}
}

var _ NemesisFn = BidirectionalPartitionNemesis

func restoreNetwork(ctx context.Context, t *testing.T, c cluster.Cluster) []error {
	var errs []error
	for i := 0; i < c.NumNodes(); i++ {
		for _, cmd := range iptables.Reset() {
			if err := c.ExecRoot(ctx, i, cmd); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errs
}

func cutNetwork(
	ctx context.Context, t *testing.T, c cluster.Cluster, closer <-chan struct{}, partitions ...[]int,
) {
	defer func() {
		if errs := restoreNetwork(ctx, t, c); len(errs) > 0 {
			t.Fatalf("errors restoring the network: %+v", errs)
		}
	}()
	addrs, addrsToNode := mustGetHosts(ctx, t, c)
	ipPartitions := make([][]iptables.IP, 0, len(partitions))
	for _, partition := range partitions {
		ipPartition := make([]iptables.IP, 0, len(partition))
		for _, nodeIndex := range partition {
			ipPartition = append(ipPartition, addrs[nodeIndex])
		}
		ipPartitions = append(ipPartitions, ipPartition)
	}
	log.Warningf(ctx, "partitioning: %v (%v)", partitions, ipPartitions)
	for host, cmds := range iptables.Rules(iptables.Bidirectional(ipPartitions...)) {
		for _, cmd := range cmds {
			if err := c.ExecRoot(ctx, addrsToNode[host], cmd); err != nil {
				t.Fatal(err)
			}
		}
	}
	<-closer
	log.Warningf(ctx, "resolved all partitions")
}
