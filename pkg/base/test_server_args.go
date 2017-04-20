// Copyright 2016 The Cockroach Authors.
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
// Author: Andrei Matei (andreimatei1@gmail.com)

package base

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// TestServerArgs contains the parameters one can set when creating a test
// server. Notably, TestServerArgs are passed to serverutils.StartServer().
// They're defined in base because they need to be shared between
// testutils/serverutils (test code) and server.TestServer (non-test code).
//
// The zero value is suitable for most tests.
type TestServerArgs struct {
	// Knobs for the test server.
	Knobs TestingKnobs

	// PartOfCluster must be set if the TestServer is joining others in a cluster.
	// If not set (and hence the server is the only one in the cluster), the
	// default zone config will be overridden to disable all replication - so that
	// tests don't get log spam about ranges not being replicated enough. This
	// is always set to true when the server is started via a TestCluster.
	PartOfCluster bool

	// Addr (if nonempty) is the address to use for the test server.
	Addr string
	// HTTPAddr (if nonempty) is the HTTP address to use for the test server.
	HTTPAddr string

	// JoinAddr (if nonempty) is the address of a node we are joining.
	JoinAddr string

	// StoreSpecs define the stores for this server. If you want more than one
	// store per node, populate this array with StoreSpecs each representing a
	// store. If no StoreSpecs are provided than a single DefaultTestStoreSpec
	// will be used.
	StoreSpecs []StoreSpec

	// Fields copied to the server.Config.
	Insecure                 bool
	RetryOptions             retry.Options
	MetricsSampleInterval    time.Duration
	RaftTickInterval         time.Duration
	RaftElectionTimeoutTicks int
	SocketFile               string
	ScanInterval             time.Duration
	ScanMaxIdleTime          time.Duration
	SSLCertsDir              string
	TimeSeriesQueryWorkerMax int
	SQLMemoryPoolSize        int64
	SendNextTimeout          time.Duration

	// If set, this will be appended to the Postgres URL by functions that
	// automatically open a connection to the server. That's equivalent to running
	// SET DATABASE=foo, which works even if the database doesn't (yet) exist.
	UseDatabase string

	// Stopper can be used to stop the server. If not set, a stopper will be
	// constructed and it can be gotten through TestServerInterface.Stopper().
	Stopper *stop.Stopper

	// If set, the recording of events to the event log tables is disabled.
	DisableEventLog bool
}

// TestClusterArgs contains the parameters one can set when creating a test
// cluster. It contains a TestServerArgs instance which will be copied over to
// every server.
//
// The zero value means "ReplicationAuto".
type TestClusterArgs struct {
	// ServerArgs will be copied to each constituent TestServer. Used for all the
	// servers not overridden in ServerArgsPerNode.
	ServerArgs TestServerArgs
	// ReplicationMode controls how replication is to be done in the cluster.
	ReplicationMode TestClusterReplicationMode

	// ServerArgsPerNode override the default ServerArgs with the value in this
	// map. The map's key is an index within TestCluster.Servers. If there is
	// no entry in the map for a particular server, the default ServerArgs are
	// used.
	ServerArgsPerNode map[int]TestServerArgs
}

// DefaultTestStoreSpec is just a single in memory store of 100 MiB with no
// special attributes.
var DefaultTestStoreSpec = StoreSpec{
	SizeInBytes: 100 << 20,
	InMemory:    true,
}

// TestClusterReplicationMode represents the replication settings for a TestCluster.
type TestClusterReplicationMode int

const (
	// ReplicationAuto means that ranges are replicated according to the
	// production default zone config. Replication is performed as in
	// production, by the replication queue.
	ReplicationAuto TestClusterReplicationMode = iota
	// ReplicationManual means that the split and replication queues of all
	// servers are stopped, and the test must manually control splitting and
	// replication through the TestServer.
	ReplicationManual
)
