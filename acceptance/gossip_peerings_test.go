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
// Author: Peter Mattis (peter.mattis@gmail.com)

// +build acceptance

package acceptance

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/acceptance/localcluster"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

func checkGossipNodes(node *localcluster.Container) int {
	var m map[string]interface{}
	if err := node.GetJSON("", "/_status/gossip", &m); err != nil {
		return 0
	}
	count := 0
	infos := m["infos"].(map[string]interface{})
	for k := range infos {
		if strings.HasPrefix(k, "node:") {
			count++
		}
	}
	return count
}

func checkGossipPeerings(t *testing.T, l *localcluster.Cluster, d time.Duration) {
	expected := len(l.Nodes) * len(l.Nodes)
	log.Infof("waiting for complete gossip network of %d peerings", expected)

	util.SucceedsWithin(t, d, func() error {
		select {
		case <-stopper:
			t.Fatalf("interrupted")
			return nil
		case e := <-l.Events:
			if log.V(1) {
				log.Infof("%+v", e)
			}
			return fmt.Errorf("event: %+v", e)
		case <-time.After(1 * time.Second):
			break
		}
		found := 0
		for j := 0; j < len(l.Nodes); j++ {
			found += checkGossipNodes(l.Nodes[j])
		}
		fmt.Fprintf(os.Stderr, "%d ", found)
		if found == expected {
			fmt.Printf("... all nodes verified in the cluster\n")
			return nil
		}
		return fmt.Errorf("found %d of %d", found, expected)
	})
}

func TestGossipPeerings(t *testing.T) {
	l := localcluster.Create(*numNodes, stopper)
	l.Events = make(chan localcluster.Event, 10)
	l.Start()
	defer l.Stop()

	checkGossipPeerings(t, l, 20*time.Second)

	// Restart the first node.
	log.Infof("restarting node 0")
	if err := l.Nodes[0].Restart(5); err != nil {
		t.Fatal(err)
	}
	checkGossipPeerings(t, l, 20*time.Second)

	// Restart another node.
	rand.Seed(util.NewPseudoSeed())
	pickedNode := rand.Intn(len(l.Nodes)-1) + 1
	log.Infof("restarting node %d", pickedNode)
	if err := l.Nodes[pickedNode].Restart(5); err != nil {
		t.Fatal(err)
	}
	checkGossipPeerings(t, l, 20*time.Second)
}
