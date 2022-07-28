// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package base

// TestClusterReplicationMode represents the replication settings for a TestCluster.
type TestClusterReplicationMode int

//go:generate stringer -type=TestClusterReplicationMode

const (
	// ReplicationAuto means that ranges are replicated according to the
	// production default zone config. Replication is performed as in
	// production, by the replication queue.
	// If ReplicationAuto is used, StartTestCluster() blocks until the initial
	// ranges are fully replicated.
	ReplicationAuto TestClusterReplicationMode = iota
	// ReplicationManual means that the split, merge and replication queues of all
	// servers are stopped, and the test must manually control splitting, merging
	// and replication through the TestServer.
	// Note that the server starts with a number of system ranges,
	// all with a single replica on node 1.
	ReplicationManual
)
