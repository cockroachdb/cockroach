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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/acceptance/localcluster"
)

func TestCertGen(t *testing.T) {
	l := localcluster.Create(2, stopper)
	l.UseTestCerts = false
	l.Start()
	defer l.Stop()

	// Check the gossip peerings which will indicate whether the nodes
	// can talk to each other with the generated certs.
	checkGossip(t, l, 20*time.Second, hasPeers(len(l.Nodes)))
}
