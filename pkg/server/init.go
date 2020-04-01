// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// ErrClusterInitialized is reported when the Boostrap RPC is ran on
// a node already part of an initialized cluster.
var ErrClusterInitialized = fmt.Errorf("cluster has already been initialized")

// initServer manages the temporary init server used during
// bootstrapping.
type initServer struct {
	mu struct {
		syncutil.Mutex
		// If set, a Bootstrap() call is rejected with this error.
		rejectErr error
	}
	bootstrapBlockCh chan struct{} // blocks Bootstrap() until ServeAndWait() is invoked
	bootstrapReqCh   chan *initState

	engs []storage.Engine // late-bound in ServeAndWait

	binaryVersion, binaryMinSupportedVersion       roachpb.Version
	bootstrapVersion                               clusterversion.ClusterVersion
	bootstrapZoneConfig, bootstrapSystemZoneConfig *zonepb.ZoneConfig
}

func newInitServer(
	binaryVersion, binaryMinSupportedVersion roachpb.Version,
	bootstrapVersion clusterversion.ClusterVersion,
	bootstrapZoneConfig, bootstrapSystemZoneConfig *zonepb.ZoneConfig,
) *initServer {
	return &initServer{
		bootstrapReqCh:   make(chan *initState, 1),
		bootstrapBlockCh: make(chan struct{}),

		binaryVersion:             binaryVersion,
		binaryMinSupportedVersion: binaryMinSupportedVersion,
		bootstrapVersion:          bootstrapVersion,
		bootstrapZoneConfig:       bootstrapZoneConfig,
		bootstrapSystemZoneConfig: bootstrapSystemZoneConfig,
	}
}

// initDiskState contains the part of initState that is read from stable
// storage.
//
// TODO(tbg): the above is a lie in the case in which we join an existing
// cluster. In that case, the state returned from ServeAndWait will have the
// clusterID set from Gossip (and there will be no NodeID). The plan is to
// allocate the IDs in ServeAndWait itself eventually, at which point the
// lie disappears.
type initDiskState struct {
	// nodeID is zero if joining an existing cluster.
	//
	// TODO(tbg): see TODO above.
	nodeID roachpb.NodeID
	// All fields below are always set.
	clusterID          uuid.UUID
	clusterVersion     clusterversion.ClusterVersion
	initializedEngines []storage.Engine
	newEngines         []storage.Engine
}

// initState contains the cluster and node IDs as well as the stores, from which
// a CockroachDB server can be started up after ServeAndWait returns.
type initState struct {
	initDiskState
	// joined is true if this is a new node. Note that the initDiskState may
	// reflect the result of bootstrapping a new cluster, i.e. it is not true
	// that joined==true implies that the initDiskState shows no initialized
	// engines.
	//
	// This flag should only be used for logging and reporting. A newly
	// bootstrapped single-node cluster is functionally equivalent to one that
	// restarted; any decisions should be made on persisted data instead of
	// this flag.
	//
	// TODO(tbg): remove this bool. The Node can find out another way whether
	// it just joined or restarted.
	joined bool
	// bootstrapped is true if a new cluster was initialized. If this is true,
	// 'joined' above is also true. Usage of this field should follow that of
	// 'joined' as well.
	bootstrapped bool
}

// ServeAndWait sets up the initServer to accept Bootstrap requests (which will
// block until then). It uses the provided engines and gossip to block until
// either a new cluster was bootstrapped or Gossip connected to an existing
// cluster.
//
// This method must be called only once.
func (s *initServer) ServeAndWait(
	ctx context.Context, stopper *stop.Stopper, engs []storage.Engine, g *gossip.Gossip,
) (*initState, error) {
	if s.engs != nil {
		return nil, errors.New("cannot call ServeAndWait twice")
	}

	s.engs = engs // Bootstrap() is still blocked, so no data race here

	inspectState, err := inspectEngines(ctx, engs, s.binaryVersion, s.binaryMinSupportedVersion)
	if err != nil {
		return nil, err
	}

	if len(inspectState.initializedEngines) != 0 {
		// We have a NodeID/ClusterID, so don't allow bootstrap.
		if err := s.testOrSetRejectErr(ErrClusterInitialized); err != nil {
			return nil, errors.Wrap(err, "error unexpectedly set previously")
		}
		// If anyone mistakenly tried to bootstrap, unblock them so they can get
		// the above error.
		close(s.bootstrapBlockCh)

		// In fact, it's crucial that we return early. This is because Gossip
		// won't necessarily connect until a leaseholder for range 1 gossips the
		// cluster ID, and all nodes in the cluster might be starting up right
		// now. Without this return, we could have all nodes in the cluster
		// deadlocked on g.Connected below. For similar reasons, we can't ever
		// hope to initialize the newEngines below, for which we would need to
		// increment a KV counter.
		return &initState{
			initDiskState: *inspectState,
			joined:        false,
			bootstrapped:  false,
		}, nil
	}

	log.Info(ctx, "no stores bootstrapped and --join flag specified, awaiting init command or join with an already initialized node.")
	close(s.bootstrapBlockCh)

	select {
	case <-stopper.ShouldQuiesce():
		return nil, stop.ErrUnavailable
	case state := <-s.bootstrapReqCh:
		// Bootstrap() did its job.
		log.Infof(ctx, "**** cluster %s has been created", state.clusterID)
		return state, nil
	case <-g.Connected:
		// Gossip connected, that is, we know a ClusterID. Due to the early
		// return above, we know that all of our engines are empty, i.e. we
		// don't have a NodeID yet (and the cluster version is the minimum we
		// support). Commence startup; the Node will realize it's short a NodeID
		// and will request one.
		//
		// TODO(tbg): use a kv.DB to get NodeID and StoreIDs when necessary and
		// set everything up here. This will take the Node out of that business
		// entirely and means we'll need much fewer NodeID/ClusterIDContainers.
		// (It's also so much simpler to think about). The RPC will also tell us
		// a cluster version to use instead of the lowest possible one (reducing
		// the short amount of time until the Gossip hook bumps the version);
		// this doesn't fix anything but again, is simpler to think about.
		clusterID, err := g.GetClusterID()
		if err != nil {
			return nil, err
		}
		inspectState.clusterID = clusterID
		return &initState{
			initDiskState: *inspectState,
			joined:        true,
			bootstrapped:  false,
		}, nil
	}
}

// testOrSetRejectErr set the reject error unless a reject error was already
// set, in which case it returns the one that was already set. If no error had
// previously been set, returns nil.
func (s *initServer) testOrSetRejectErr(err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.rejectErr != nil {
		return s.mu.rejectErr
	}
	s.mu.rejectErr = err
	return nil
}

// Bootstrap implements the serverpb.Init service. Users set up a new
// CockroachDB server by calling this endpoint on *exactly one node* in the
// cluster (retrying only on that node).
// Attempting to bootstrap a node that was already bootstrapped will result in
// an error.
//
// NB: there is no protection against users erroneously bootstrapping multiple
// nodes. In that case, they end up with more than one cluster, and nodes
// panicking or refusing to connect to each other.
func (s *initServer) Bootstrap(
	ctx context.Context, request *serverpb.BootstrapRequest,
) (response *serverpb.BootstrapResponse, err error) {
	<-s.bootstrapBlockCh // block until ServeAndWait() is active
	// NB: this isn't necessary since bootstrapCluster would fail, but this is
	// cleaner.
	if err := s.testOrSetRejectErr(ErrClusterInitialized); err != nil {
		return nil, err
	}
	state, err := bootstrapCluster(ctx, s.engs, s.bootstrapVersion, s.bootstrapZoneConfig, s.bootstrapSystemZoneConfig)
	if err != nil {
		return nil, err
	}
	s.bootstrapReqCh <- state
	return &serverpb.BootstrapResponse{}, nil
}
