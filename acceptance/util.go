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

// This file intentionally does not require the "acceptance" build tag in order
// to silence a warning from the emacs flycheck package.

package acceptance

import (
	"github.com/cockroachdb/cockroach/acceptance/localcluster"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/stop"
)

// makeDBClient creates a DB client for node 'i' using the cluster certs dir.
func makeDBClient(t util.Tester, cluster *localcluster.Cluster, node int) (*client.DB, *stop.Stopper) {
	return makeDBClientForUser(t, cluster, security.NodeUser, node)
}

// makeDBClientForUser creates a DB client for node 'i' and user 'user'.
func makeDBClientForUser(t util.Tester, cluster *localcluster.Cluster, user string, node int) (*client.DB, *stop.Stopper) {
	stopper := stop.NewStopper()

	// We need to run with "InsecureSkipVerify" (set when Certs="" inside the http sender).
	// This is due to the fact that we're running outside docker, so we cannot use a fixed hostname
	// to reach the cluster. This in turn means that we do not have a verified server name in the certs.
	db, err := client.Open(stopper, "rpcs://"+user+"@"+
		cluster.Nodes[node].Addr("").String()+
		"?certs="+cluster.CertsDir)

	if err != nil {
		t.Fatal(err)
	}

	return db, stopper
}
