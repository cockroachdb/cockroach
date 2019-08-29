// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// This file provides generic interfaces that allow tests to set up test
// clusters without importing the testcluster (and indirectly server) package
// (avoiding circular dependencies). To be used, the binary needs to call
// InitTestClusterFactory(testcluster.TestClusterFactory), generally from a
// TestMain() in an "foo_test" package (which can import testcluster and is
// linked together with the other tests in package "foo").

package serverutils

import (
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// TestClusterInterface defines TestCluster functionality used by tests.
type TestClusterInterface interface {
	NumServers() int

	// Server returns the TestServerInterface corresponding to a specific node.
	Server(idx int) TestServerInterface

	// ServerConn returns a gosql.DB connection to a specific node.
	ServerConn(idx int) *gosql.DB

	// StopServer stops a single server.
	StopServer(idx int)

	// Stopper retrieves the stopper for this test cluster. Tests should call or
	// defer the Stop() method on this stopper after starting a test cluster.
	Stopper() *stop.Stopper

	// AddReplicas adds replicas for a range on a set of stores.
	// It's illegal to have multiple replicas of the same range on stores of a single
	// node.
	// The method blocks until a snapshot of the range has been copied to all the
	// new replicas and the new replicas become part of the Raft group.
	AddReplicas(
		startKey roachpb.Key, targets ...roachpb.ReplicationTarget,
	) (roachpb.RangeDescriptor, error)

	// AddReplicasMulti is the same as AddReplicas but will execute multiple jobs.
	AddReplicasMulti(
		kts ...KeyAndTargets,
	) ([]roachpb.RangeDescriptor, []error)

	// AddReplicasOrFatal is the same as AddReplicas but will Fatal the test on
	// error.
	AddReplicasOrFatal(
		t testing.TB, startKey roachpb.Key, targets ...roachpb.ReplicationTarget,
	) roachpb.RangeDescriptor

	// RemoveReplicas removes one or more replicas from a range.
	RemoveReplicas(
		startKey roachpb.Key, targets ...roachpb.ReplicationTarget,
	) (roachpb.RangeDescriptor, error)

	// RemoveReplicasOrFatal is the same as RemoveReplicas but will Fatal the test on
	// error.
	RemoveReplicasOrFatal(
		t testing.TB, startKey roachpb.Key, targets ...roachpb.ReplicationTarget,
	) roachpb.RangeDescriptor

	// FindRangeLeaseHolder returns the current lease holder for the given range.
	// In particular, it returns one particular node's (the hint, if specified) view
	// of the current lease.
	// An error is returned if there's no active lease.
	//
	// Note that not all nodes have necessarily applied the latest lease,
	// particularly immediately after a TransferRangeLease() call. So specifying
	// different hints can yield different results. The one server that's guaranteed
	// to have applied the transfer is the previous lease holder.
	FindRangeLeaseHolder(
		rangeDesc roachpb.RangeDescriptor,
		hint *roachpb.ReplicationTarget,
	) (roachpb.ReplicationTarget, error)

	// TransferRangeLease transfers the lease for a range from whoever has it to
	// a particular store. That store must already have a replica of the range. If
	// that replica already has the (active) lease, this method is a no-op.
	//
	// When this method returns, it's guaranteed that the old lease holder has
	// applied the new lease, but that's about it. It's not guaranteed that the new
	// lease holder has applied it (so it might not know immediately that it is the
	// new lease holder).
	TransferRangeLease(
		rangeDesc roachpb.RangeDescriptor, dest roachpb.ReplicationTarget,
	) error

	// LookupRange returns the descriptor of the range containing key.
	LookupRange(key roachpb.Key) (roachpb.RangeDescriptor, error)

	// LookupRangeOrFatal is the same as LookupRange but will Fatal the test on
	// error.
	LookupRangeOrFatal(t testing.TB, key roachpb.Key) roachpb.RangeDescriptor

	// Target returns a roachpb.ReplicationTarget for the specified server.
	Target(serverIdx int) roachpb.ReplicationTarget

	// ReplicationMode returns the ReplicationMode that the test cluster was
	// configured with.
	ReplicationMode() base.TestClusterReplicationMode
}

// TestClusterFactory encompasses the actual implementation of the shim
// service.
type TestClusterFactory interface {
	// New instantiates a test server.
	StartTestCluster(t testing.TB, numNodes int, args base.TestClusterArgs) TestClusterInterface
}

var clusterFactoryImpl TestClusterFactory

// InitTestClusterFactory should be called once to provide the implementation
// of the service. It will be called from a xx_test package that can import the
// server package.
func InitTestClusterFactory(impl TestClusterFactory) {
	clusterFactoryImpl = impl
}

// StartTestCluster starts up a TestCluster made up of numNodes in-memory
// testing servers. The cluster should be stopped using Stopper().Stop().
func StartTestCluster(t testing.TB, numNodes int, args base.TestClusterArgs) TestClusterInterface {
	if clusterFactoryImpl == nil {
		panic("TestClusterFactory not initialized. One needs to be injected " +
			"from the package's TestMain()")
	}
	return clusterFactoryImpl.StartTestCluster(t, numNodes, args)
}

// KeyAndTargets contains replica startKey and targets.
type KeyAndTargets struct {
	StartKey roachpb.Key
	Targets  []roachpb.ReplicationTarget
}
