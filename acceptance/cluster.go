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
// Author: Tobias Schottdorf

// +build acceptance

package acceptance

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/acceptance/localcluster"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/util/stop"
)

type panicTester struct{}

func (pt panicTester) Failed() bool                              { return false }
func (pt panicTester) Fatal(args ...interface{})                 { panic(fmt.Sprintf("%v", args...)) }
func (pt panicTester) Fatalf(format string, args ...interface{}) { panic(fmt.Sprintf(format, args...)) }

// A Cluster is an abstraction away from a concrete cluster deployment (i.e.
// a local docker cluster, or an AWS-provisioned one). It exposes a shared
// set of methods for test-related manipulation.
type Cluster interface {
	// NumNodes returns the number of nodes in the cluster, running or not.
	NumNodes() int
	// MakeClient returns a client which is pointing at the node with the given
	// index. The given integer must be in the range [0,len(NumNodes())-1].
	MakeClient(int) (*client.DB, *stop.Stopper)
	// Assert verifies that the cluster state is as expected (i.e. no unexpected
	// restarts or node deaths occurred). Tests can call this periodically to
	// ascertain cluster health.
	Assert(t *testing.T)
	// AssertAndStop performs the same test as Assert but then proceeds to
	// dismantle the cluster.
	AssertAndStop(t *testing.T)
	// Kill terminates the cockroach process running on the given node number.
	// The given integer must be in the range [0,len(NumNodes())-1].
	Kill(int) error
	// Restart terminates the cockroach process running on the given node
	// number, unless it is already stopped, and restarts it.
	// The given integer must be in the range [0,len(NumNodes())-1].
	Restart(int) error
	// Get queries the given node's HTTP endpoint for the given path, unmarshaling
	// into the supplied interface (or returning an error).
	Get(int, string, interface{}) error
}

// A dockerCluster implements Cluster for a localcluster.Cluster.
type dockerCluster struct {
	l *localcluster.Cluster
	t panicTester
}

func (dc *dockerCluster) MakeClient(i int) (*client.DB, *stop.Stopper) {
	return makeDBClient(dc.t, dc.l, 0)
}

func (dc *dockerCluster) NumNodes() int {
	return len(dc.l.Nodes)
}

func (dc *dockerCluster) Assert(t *testing.T) {
	dc.l.Assert(t)
}

func (dc *dockerCluster) AssertAndStop(t *testing.T) {
	dc.l.AssertAndStop(t)
}

func (dc *dockerCluster) Kill(i int) error {
	return dc.l.Nodes[i].Kill()
}

func (dc *dockerCluster) Restart(i int) error {
	return dc.l.Nodes[i].Restart(5)
}

func (dc *dockerCluster) Get(i int, path string, dest interface{}) error {
	return dc.l.Nodes[i].GetJSON("", path, dest)
}
