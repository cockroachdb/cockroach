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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/acceptance/localcluster"
	"github.com/cockroachdb/cockroach/util/log"
)

func checkGossipNodes(client *http.Client, node *localcluster.Container) int {
	resp, err := client.Get(fmt.Sprintf("https://%s/_status/gossip", node.Addr("")))
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0
	}
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
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

func checkGossipPeerings(t *testing.T, l *localcluster.Cluster, attempts int, done chan struct{}) {
	go func() {
		defer close(done)

		client := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			}}

		log.Infof("waiting for complete gossip network of %d peerings",
			len(l.Nodes)*len(l.Nodes))

		for i := 0; i < attempts; i++ {
			time.Sleep(1 * time.Second)
			found := 0
			for j := 0; j < len(l.Nodes); j++ {
				found += checkGossipNodes(client, l.Nodes[j])
			}
			fmt.Fprintf(os.Stderr, "%d ", found)
			if found == len(l.Nodes)*len(l.Nodes) {
				fmt.Printf("... all nodes verified in the cluster\n")
				return
			}
		}

		fmt.Fprintf(os.Stderr, "\n")
		t.Errorf("failed to verify all nodes in cluster\n")
	}()
}

func TestGossipPeerings(t *testing.T) {
	cluster := localcluster.Create(*numNodes, stopper)
	if !cluster.Start() {
		return
	}
	defer cluster.Stop()

	done := make(chan struct{})
	checkGossipPeerings(t, cluster, 20, done)

	select {
	case <-stopper:
	case <-done:
	}
}
