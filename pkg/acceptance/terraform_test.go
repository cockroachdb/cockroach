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

package acceptance

import (
	"context"
	"os"
	"os/signal"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestBuildBabyCluster resizes the cluster to one node. It does not tear down
// the cluster after it's done and is mostly useful for testing code changes in
// the `terrafarm` package.
func TestBuildBabyCluster(t *testing.T) {
	t.Skip("only enabled during testing")

	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	f := MakeFarmer(t, "baby", stopper)
	defer func() {
		if err := f.CollectLogs(); err != nil {
			t.Logf("error collecting cluster logs: %s\n", err)
		}
	}()
	if err := f.Resize(1); err != nil {
		t.Fatal(err)
	}
	f.Assert(ctx, t)
}

// TestFiveNodesAndWriters runs a cluster and one writer per node.
// The test runs until SIGINT is received or the specified duration
// has passed.
//
// Run this as follows:
// make test \
//	 TESTTIMEOUT=30m \
//	 PKG=./pkg/acceptance \
//	 TESTS=FiveNodesAndWriters \
//	 TESTFLAGS='-v -remote -key-name google_compute_engine -cwd terraform'
func TestFiveNodesAndWriters(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	deadline := time.After(*flagDuration)
	f := MakeFarmer(t, "write-5n5w", stopper)
	defer f.MustDestroy(t)
	assertClusterUp := func() {
		f.Assert(ctx, t)
		for i := 0; i < f.NumNodes(); i++ {
			if ch := f.GetProcDone(i, "block_writer"); ch == nil {
				t.Fatalf("block writer not running on node %d", i)
			} else {
				select {
				case err := <-ch:
					t.Fatalf("block writer exited on node %d: %s", i, err)
				default:
				}
			}
		}
	}

	const size = 5
	if err := f.Resize(size); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < size; i++ {
		if err := f.Start(ctx, i, "block_writer"); err != nil {
			t.Fatal(err)
		}
	}

	if err := f.WaitReady(3 * time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := CheckGossip(ctx, f, longWaitTime, HasPeers(size)); err != nil {
		t.Fatal(err)
	}

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

func TestRandomNameMeetsTerraformRules(t *testing.T) {
	prefix := "testprefix-" + getRandomName()
	if !prefixRE.MatchString(prefix) {
		t.Fatalf("generated prefix '%s' doesn't match Terraform-enforced regex %s", prefix, prefixRE)
	}
}
