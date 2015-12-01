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
	"flag"
	"os"
	"os/signal"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/acceptance/cluster"
	"github.com/cockroachdb/cockroach/util/randutil"
)

var duration = flag.Duration("d", 5*time.Second, "duration to run the test")
var numNodes = flag.Int("num", 0, "start a local cluster of the given size")
var peers = flag.String("peers", "", "comma-separated list of remote cluster nodes")
var stall = flag.Duration("stall", time.Minute, "duration after which if no forward progress is made, consider the test stalled")
var stopper = make(chan struct{})

// StartCluster starts a cluster from the relevant flags.
func StartCluster(t *testing.T) cluster.Cluster {
	if *numNodes > 0 {
		if len(*peers) > 0 {
			t.Fatal("cannot both specify -num and -peers")
		}
		l := cluster.CreateLocal(*numNodes, stopper)
		l.Start()
		checkRangeReplication(t, l, 20*time.Second)
		return l
	}
	if len(*peers) == 0 {
		t.Fatal("need to either specify -num or -peers")
	}
	return cluster.CreateRemote(strings.Split(*peers, ","))
}

func TestMain(m *testing.M) {
	randutil.SeedForTests()
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		<-sig
		select {
		case <-stopper:
		default:
			// There is a very tiny race here: the cluster might be closing
			// the stopper simultaneously.
			close(stopper)
		}
	}()
	os.Exit(m.Run())
}
