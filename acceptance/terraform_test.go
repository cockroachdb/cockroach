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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

// +build acceptance

package acceptance

import (
	"os"
	"os/signal"
	"testing"
	"time"
)

// TestBuildCluster resizes the cluster to one node, one writer.
// It does not tear down the cluster after it's done and is mostly
// useful for testing code changes in the `terrafarm` package.
func TestBuildCluster(t *testing.T) {
	t.Skip("only enabled during testing")
	f := farmer(t)
	defer f.CollectLogs()
	if err := f.Resize(1, 1); err != nil {
		t.Fatal(err)
	}
	f.Assert(t)
}

// TestFiveNodesAndWriters runs a cluster and one writer per node.
// The test runs until SIGINT is received or the specified duration
// has passed.
func TestFiveNodesAndWriters(t *testing.T) {
	deadline := time.After(*duration)
	f := farmer(t)
	defer f.MustDestroy()
	if err := f.Resize(5, 5); err != nil {
		t.Fatal(err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	f.Assert(t)
	for {
		select {
		case <-deadline:
			return
		case <-c:
			return
		case <-time.After(time.Minute):
			f.Assert(t)
		}
	}
}
