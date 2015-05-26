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
)

// makeDBClient creates a DB client for node 'i' using the cluster certs dir.
func makeDBClient(cluster *localcluster.Cluster, node int) (*client.DB, error) {
	// We always run these tests with certs.
	return client.Open("https://root@" +
		cluster.Nodes[node].Addr("").String() +
		"?certs=" + cluster.CertsDir)
}
