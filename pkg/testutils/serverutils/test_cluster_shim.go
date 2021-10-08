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
	"context"
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// TestClusterInterface defines TestCluster functionality used by tests.
type TestClusterInterface interface {
	// Start is used to start up the servers that were instantiated when
	// creating this cluster.
	Start(t testing.TB)

	// NumServers returns the number of servers this test cluster is configured
	// with.
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

	// AddVoters adds voter replicas for a range on a set of stores.
	// It's illegal to have multiple replicas of the same range on stores of a single
	// node.
	// The method blocks until a snapshot of the range has been copied to all the
	// new replicas and the new replicas become part of the Raft group.
	AddVoters(
		startKey roachpb.Key, targets ...roachpb.ReplicationTarget,
	) (roachpb.RangeDescriptor, error)

	// AddVotersMulti is the same as AddVoters but will execute multiple jobs.
	AddVotersMulti(
		kts ...KeyAndTargets,
	) ([]roachpb.RangeDescriptor, []error)

	// AddVotersOrFatal is the same as AddVoters but will Fatal the test on
	// error.
	AddVotersOrFatal(
		t testing.TB, startKey roachpb.Key, targets ...roachpb.ReplicationTarget,
	) roachpb.RangeDescriptor

	// RemoveVoters removes one or more voter replicas from a range.
	RemoveVoters(
		startKey roachpb.Key, targets ...roachpb.ReplicationTarget,
	) (roachpb.RangeDescriptor, error)

	// RemoveVotersOrFatal is the same as RemoveVoters but will Fatal the test on
	// error.
	RemoveVotersOrFatal(
		t testing.TB, startKey roachpb.Key, targets ...roachpb.ReplicationTarget,
	) roachpb.RangeDescriptor

	// AddNonVoters adds non-voting replicas for a range on a set of stores.
	//
	//This method blocks until the new replicas become a part of the Raft group.
	AddNonVoters(
		startKey roachpb.Key,
		targets ...roachpb.ReplicationTarget,
	) (roachpb.RangeDescriptor, error)

	// AddNonVotersOrFatal is the same as AddNonVoters but will fatal if it fails.
	AddNonVotersOrFatal(
		t testing.TB, startKey roachpb.Key, targets ...roachpb.ReplicationTarget,
	) roachpb.RangeDescriptor

	// RemoveNonVoters removes one or more non-voters from a range.
	RemoveNonVoters(
		startKey roachpb.Key, targets ...roachpb.ReplicationTarget,
	) (roachpb.RangeDescriptor, error)

	// RemoveNonVotersOrFatal is the same as RemoveNonVoters but will fatal if it
	// fails.
	RemoveNonVotersOrFatal(
		t testing.TB, startKey roachpb.Key, targets ...roachpb.ReplicationTarget,
	) roachpb.RangeDescriptor

	// SwapVoterWithNonVoter atomically "swaps" the voting replica located on
	// `voterTarget` with the non-voting replica located on `nonVoterTarget`. A
	// sequence of ReplicationChanges is considered to have "swapped" a voter on
	// s1 with a non-voter on s2 iff the resulting state after the execution of
	// these changes is such that s1 has a non-voter and s2 has a voter.
	SwapVoterWithNonVoter(
		startKey roachpb.Key, voterTarget, nonVoterTarget roachpb.ReplicationTarget,
	) (*roachpb.RangeDescriptor, error)

	// SwapVoterWithNonVoterOrFatal is the same as SwapVoterWithNonVoter but will
	// fatal if it fails.
	SwapVoterWithNonVoterOrFatal(
		t *testing.T, startKey roachpb.Key, voterTarget, nonVoterTarget roachpb.ReplicationTarget,
	) *roachpb.RangeDescriptor

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

	// MoveRangeLeaseNonCooperatively performs a non-cooperative transfer of the
	// lease for a range from whoever has it to a particular store. That store
	// must already have a replica of the range. If that replica already has the
	// (active) lease, this method is a no-op.
	//
	// The transfer is non-cooperative in that the lease is made to expire by
	// advancing the manual clock. The target is then instructed to acquire the
	// ownerless lease. Most tests should use the cooperative version of this
	// method, TransferRangeLease.
	//
	// Returns the new lease.
	//
	// If the lease starts out on dest, this is a no-op and the current lease is
	// returned.
	MoveRangeLeaseNonCooperatively(
		ctx context.Context,
		rangeDesc roachpb.RangeDescriptor,
		dest roachpb.ReplicationTarget,
		manual *hlc.HybridManualClock,
	) (*roachpb.Lease, error)

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

	// ScratchRange returns the start key of a span of keyspace suitable for use
	// as kv scratch space (it doesn't overlap system spans or SQL tables). The
	// range is lazily split off on the first call to ScratchRange.
	ScratchRange(t testing.TB) roachpb.Key

	// WaitForFullReplication waits until all stores in the cluster
	// have no ranges with replication pending.
	WaitForFullReplication() error
}

// TestClusterFactory encompasses the actual implementation of the shim
// service.
type TestClusterFactory interface {
	// NewTestCluster creates a test cluster without starting it.
	NewTestCluster(t testing.TB, numNodes int, args base.TestClusterArgs) TestClusterInterface
}

var clusterFactoryImpl TestClusterFactory

// InitTestClusterFactory should be called once to provide the implementation
// of the service. It will be called from a xx_test package that can import the
// server package.
func InitTestClusterFactory(impl TestClusterFactory) {
	clusterFactoryImpl = impl
}

// StartNewTestCluster creates and starts up a TestCluster made up of numNodes
// in-memory testing servers. The cluster should be stopped using
// Stopper().Stop().
func StartNewTestCluster(
	t testing.TB, numNodes int, args base.TestClusterArgs,
) TestClusterInterface {
	cluster := NewTestCluster(t, numNodes, args)
	cluster.Start(t)
	return cluster
}

// NewTestCluster creates TestCluster made up of numNodes in-memory testing
// servers. It can be started using the return type.
func NewTestCluster(t testing.TB, numNodes int, args base.TestClusterArgs) TestClusterInterface {
	if clusterFactoryImpl == nil {
		panic("TestClusterFactory not initialized. One needs to be injected " +
			"from the package's TestMain()")
	}
	return clusterFactoryImpl.NewTestCluster(t, numNodes, args)
}

// KeyAndTargets contains replica startKey and targets.
type KeyAndTargets struct {
	StartKey roachpb.Key
	Targets  []roachpb.ReplicationTarget
}
