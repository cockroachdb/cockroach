// Copyright 2014 The Cockroach Authors.
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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Ben Darnell

package multiraft

import (
	"fmt"
	"testing"
	"time"
)

type testCluster struct {
	states []*state
}

func newTestCluster(size int, t *testing.T) *testCluster {
	transport := NewLocalTransport()
	cluster := &testCluster{make([]*state, 0)}
	for i := 0; i < size; i++ {
		config := &Config{
			Transport:          transport,
			ElectionTimeoutMin: 10 * time.Millisecond,
			ElectionTimeoutMax: 20 * time.Millisecond,
			Strict:             true,
		}
		mr, err := NewMultiRaft(fmt.Sprintf("%d", i), config)
		if err != nil {
			t.Fatal(err)
		}
		state := newState(mr)
		cluster.states = append(cluster.states, state)
	}
	// Let all the states listen before starting any.
	for i := 0; i < size; i++ {
		go cluster.states[i].start()
	}
	return cluster
}

func TestInitialLeaderElection(t *testing.T) {
	cluster := newTestCluster(3, t)
	for i := 0; i < 3; i++ {
		err := cluster.states[i].CreateGroup("group", []string{"0", "1", "2"})
		if err != nil {
			t.Fatal(err)
		}
	}
	// Temporary hack: just wait for some instance to declare itself the winner of an
	// election.
	<-hackyTestChannel
}
