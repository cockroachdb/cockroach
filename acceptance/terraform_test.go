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
// permissions and limitations under the License.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package acceptance

import (
	"os"
	"os/signal"
	"testing"
	"time"
)

// TestBuildBabyCluster resizes the cluster to one node. It does not tear down
// the cluster after it's done and is mostly useful for testing code changes in
// the `terrafarm` package.
func TestBuildBabyCluster(t *testing.T) {
	t.Skip("only enabled during testing")
	f := farmer(t, "baby")
	defer f.CollectLogs()
	if err := f.Resize(1); err != nil {
		t.Fatal(err)
	}
	f.Assert(t)
}

// TestFiveNodesAndWriters runs a cluster and one writer per node.
// The test runs until SIGINT is received or the specified duration
// has passed.
//
// Run this as follows:
// make test \
//	 TESTTIMEOUT=30m \
//	 PKG=./acceptance \
//	 TESTS=FiveNodesAndWriters \
//	 TESTFLAGS='-v -remote -key-name google_compute_engine -cwd terraform'
func TestFiveNodesAndWriters(t *testing.T) {
	deadline := time.After(*flagDuration)
	f := farmer(t, "write-5n5w")
	defer f.MustDestroy(t)
	assertClusterUp := func() {
		f.Assert(t)
		for _, host := range f.Nodes() {
			f.AssertState(t, host, "block_writer", "RUNNING")
		}
	}

	const size = 5
	if err := f.Resize(size); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < size; i++ {
		if err := f.Start(i, "block_writer"); err != nil {
			t.Fatal(err)
		}
	}

	if err := f.WaitReady(3 * time.Minute); err != nil {
		t.Fatal(err)
	}
	checkGossip(t, f, longWaitTime, hasPeers(size))

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	assertClusterUp()
	for {
		select {
		case <-deadline:
			return
		case <-c:
			return
		case <-time.After(time.Minute):
			assertClusterUp()
		}
	}
}
