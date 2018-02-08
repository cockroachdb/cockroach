// Copyright 2018 The Cockroach Authors.
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

package nightly

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

var clusters = map[string]struct{}{}
var clustersMu syncutil.Mutex

func registerCluster(clusterName string) {
	clustersMu.Lock()
	clusters[clusterName] = struct{}{}
	clustersMu.Unlock()
}

func unregisterCluster(clusterName string) {
	clustersMu.Lock()
	delete(clusters, clusterName)
	clustersMu.Unlock()
}

func TestMain(m *testing.M) {
	go func() {
		// Shut down test clusters when interrupted (for example CTRL+C).
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		<-sig

		// Fire off a goroutine to destroy all of the clusters.
		done := make(chan struct{})
		go func() {
			defer close(done)

			var wg sync.WaitGroup
			clustersMu.Lock()
			wg.Add(len(clusters))
			for name := range clusters {
				go func(name string) {
					cmd := exec.Command("roachprod", "destroy", name)
					if err := cmd.Run(); err != nil {
						fmt.Fprintln(os.Stderr, err)
					}
				}(name)
			}
			clustersMu.Unlock()

			wg.Wait()
		}()

		// Wait up to 5 min for clusters to be destroyed. This can take a while and
		// we don't want to rush it.
		select {
		case <-done:
		case <-time.After(5 * time.Minute):
		}
	}()

	os.Exit(m.Run())
}
