// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

// +build acceptance

package acceptance

import (
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/acceptance/localcluster"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/randutil"
)

type checkGossipFunc func(map[string]interface{}) error

// checkGossip fetches the gossip infoStore from each node and invokes the given
// function. The test passes if the function returns 0 for every node,
// retrying for up to the given duration.
func checkGossip(t *testing.T, l *localcluster.Cluster, d time.Duration,
	f checkGossipFunc) {
	util.SucceedsWithin(t, d, func() error {
		select {
		case <-stopper:
			t.Fatalf("interrupted")
			return nil
		case <-time.After(1 * time.Second):
		}

		for i, node := range l.Nodes {
			var m map[string]interface{}
			if err := node.GetJSON("", "/_status/gossip/local", &m); err != nil {
				return err
			}
			infos := m["infos"].(map[string]interface{})
			if err := f(infos); err != nil {
				return util.Errorf("node %d: %s", i, err)
			}
		}

		return nil
	})
}

// hasPeers returns a checkGossipFunc that passes when the given
// number of peers are connected via gossip.
func hasPeers(expected int) checkGossipFunc {
	return func(infos map[string]interface{}) error {
		count := 0
		for k := range infos {
			if strings.HasPrefix(k, "node:") {
				count++
			}
		}
		if count != expected {
			return util.Errorf("expected %d peers, found %d", expected, count)
		}
		return nil
	}
}

// hasSentinel is a checkGossipFunc that passes when the sentinel gossip is present.
func hasSentinel(infos map[string]interface{}) error {
	if _, ok := infos[gossip.KeySentinel]; !ok {
		return util.Errorf("sentinel not found")
	}
	return nil
}

// hasClusterID is a checkGossipFunc that passes when the cluster ID gossip is present.
func hasClusterID(infos map[string]interface{}) error {
	if _, ok := infos[gossip.KeyClusterID]; !ok {
		return util.Errorf("cluster ID not found")
	}
	return nil
}

func TestGossipPeerings(t *testing.T) {
	l := localcluster.Create(*numNodes, stopper)
	l.Start()
	rand.Seed(randutil.NewPseudoSeed())
	pickedNode := rand.Intn(len(l.Nodes)-1) + 1

	defer func() {
		if err := l.AssertAndStop([]localcluster.Event{
			{NodeIndex: 0, Status: "kill"},
			{NodeIndex: 0, Status: "die"},
			{NodeIndex: 0, Status: "stop"},
			{NodeIndex: 0, Status: "restart"},
			{NodeIndex: pickedNode, Status: "kill"},
			{NodeIndex: pickedNode, Status: "die"},
			{NodeIndex: pickedNode, Status: "stop"},
			{NodeIndex: pickedNode, Status: "restart"},
		}); err != nil {
			t.Fatal(err)
		}
	}()

	checkGossip(t, l, 20*time.Second, hasPeers(len(l.Nodes)))

	// Restart the first node.
	log.Infof("restarting node 0")
	if err := l.Nodes[0].Restart(5); err != nil {
		t.Fatal(err)
	}
	checkGossip(t, l, 20*time.Second, hasPeers(len(l.Nodes)))

	// Restart another node.
	log.Infof("restarting node %d", pickedNode)
	if err := l.Nodes[pickedNode].Restart(5); err != nil {
		t.Fatal(err)
	}
	checkGossip(t, l, 20*time.Second, hasPeers(len(l.Nodes)))
}

// TestGossipRestart verifies that the gossip network can be
// re-bootstrapped after a time when all nodes were down
// simultaneously.
func TestGossipRestart(t *testing.T) {
	l := localcluster.Create(*numNodes, stopper)
	l.Start()
	var expEvents []localcluster.Event
	defer func() {
		if err := l.AssertAndStop(expEvents); err != nil {
			t.Fatal(err)
		}
	}()

	log.Infof("waiting for initial gossip connections")
	checkGossip(t, l, 20*time.Second, hasPeers(len(l.Nodes)))
	checkGossip(t, l, time.Second, hasClusterID)
	checkGossip(t, l, time.Second, hasSentinel)

	// The replication of the first range is important: as long as the
	// first range only exists on one node, that node can trivially
	// acquire the leader lease. Once the range is replicated, however,
	// nodes must be able to discover each other over gossip before the
	// lease can be acquired.
	log.Infof("waiting for range replication")
	checkRangeReplication(t, l, 10*time.Second)

	log.Infof("stopping all nodes")
	for i, node := range l.Nodes {
		node.Stop(5)
		expEvents = append(expEvents, []localcluster.Event{
			{NodeIndex: i, Status: "kill"},
			{NodeIndex: i, Status: "die"},
			{NodeIndex: i, Status: "stop"},
		}...)
	}

	log.Infof("restarting all nodes")
	for i, node := range l.Nodes {
		node.Restart(5)
		expEvents = append(expEvents, []localcluster.Event{
			{NodeIndex: i, Status: "restart"},
		}...)
	}

	log.Infof("waiting for gossip to be connected")
	checkGossip(t, l, 20*time.Second, hasPeers(len(l.Nodes)))
	checkGossip(t, l, time.Second, hasClusterID)
	checkGossip(t, l, time.Second, hasSentinel)
}
