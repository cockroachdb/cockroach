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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// ErrClusterInitialized is reported when the Boostrap RPC is run on
// a node that is already part of an initialized cluster.
var ErrClusterInitialized = fmt.Errorf("cluster has already been initialized")

// initServer handles the bootstrapping process. It is instantiated early in the
// server startup sequence to determine whether a NodeID and ClusterID are
// available (true if and only if an initialized store is present). If all
// engines are empty, either a new cluster needs to be started (via incoming
// Bootstrap RPC) or an existing one joined. Either way, the goal is to learn a
// ClusterID and NodeID (and initialize at least one store). All of this
// subtlety is encapsulated by the initServer, which offers a primitive
// ServeAndWait() after which point the startup code can assume that the
// Node/ClusterIDs are known.
//
// TODO(tbg): at the time of writing, when joining an existing cluster for the
// first time, the initServer provides only the clusterID. Fix this by giving
// the initServer a *kv.DB that it can use to assign a NodeID and StoreID, and
// later by switching to the connect RPC (#32574).
type initServer struct {
	mu struct {
		syncutil.Mutex
		// If set, a Bootstrap() call is rejected with this error.
		rejectErr error
	}
	// The version at which to bootstrap the cluster in Bootstrap().
	bootstrapVersion roachpb.Version
	// The zone configs to bootstrap with.
	bootstrapZoneConfig, bootstrapSystemZoneConfig *zonepb.ZoneConfig
	// The state of the engines. This tells us whether the node is already
	// bootstrapped. The goal of the initServer is to complete this by the
	// time ServeAndWait returns.
	inspectState *initDiskState

	// If Bootstrap() succeeds, resulting initState will go here (to be consumed
	// by ServeAndWait).
	bootstrapReqCh chan *initState
}

func setupInitServer(
	ctx context.Context,
	binaryVersion, binaryMinSupportedVersion roachpb.Version,
	bootstrapVersion roachpb.Version,
	bootstrapZoneConfig, bootstrapSystemZoneConfig *zonepb.ZoneConfig,
	engines []storage.Engine,
) (*initServer, error) {
	inspectState, err := inspectEngines(ctx, engines, binaryVersion, binaryMinSupportedVersion)
	if err != nil {
		return nil, err
	}

	s := &initServer{
		bootstrapReqCh: make(chan *initState, 1),

		inspectState:              inspectState,
		bootstrapVersion:          bootstrapVersion,
		bootstrapZoneConfig:       bootstrapZoneConfig,
		bootstrapSystemZoneConfig: bootstrapSystemZoneConfig,
	}

	if len(inspectState.initializedEngines) > 0 {
		// We have a NodeID/ClusterID, so don't allow bootstrap.
		s.mu.rejectErr = ErrClusterInitialized
	}

	return s, nil
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

// NeedsInit returns true if (and only if) none if the engines are initialized.
// In this case, server startup is blocked until either an initialized node
// is reached via Gossip, or this node itself is bootstrapped.
func (s *initServer) NeedsInit() bool {
	return len(s.inspectState.initializedEngines) == 0
}

// ServeAndWait waits until the server is ready to bootstrap. In the common case
// of restarting an existing node, this immediately returns. When starting with
// a blank slate (i.e. only empty engines), it waits for incoming Bootstrap
// request or for Gossip to connect (whichever happens earlier).
//
// The returned initState may not reflect a bootstrapped cluster yet, but it
// is guaranteed to have a ClusterID set.
//
// This method must be called only once.
//
// TODO(tbg): give this a KV client and thus initialize at least one store in
// all cases.
func (s *initServer) ServeAndWait(
	ctx context.Context, stopper *stop.Stopper, sv *settings.Values, g *gossip.Gossip,
) (*initState, error) {
	if !s.NeedsInit() {
		// If already bootstrapped, return early.
		return &initState{
			initDiskState: *s.inspectState,
			joined:        false,
			bootstrapped:  false,
		}, nil
	}

	log.Info(ctx, "no stores bootstrapped and --join flag specified, awaiting "+
		"init command or join with an already initialized node.")

	select {
	case <-stopper.ShouldQuiesce():
		return nil, stop.ErrUnavailable
	case state := <-s.bootstrapReqCh:
		// Bootstrap() did its job. At this point, we know that the cluster
		// version will be bootstrapVersion (=state.clusterVersion.Version), but
		// the version setting does not know yet (it was initialized as
		// BinaryMinSupportedVersion because the engines were all
		// uninitialized). We *could* just let the server start, and it would
		// populate system.settings, which is then gossiped, and then the
		// callback would update the version, but we take this shortcut to avoid
		// having every freshly bootstrapped cluster spend time at an old
		// cluster version.
		if err := clusterversion.Initialize(ctx, state.clusterVersion.Version, sv); err != nil {
			return nil, err
		}

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
		// this doesn't fix anything but again, is simpler to think about. A
		// gotcha that may not immediately be obvious is that we can never hope
		// to have all stores initialized by the time ServeAndWait returns. This
		// is because *if this server is already bootstrapped*, it might hold a
		// replica of the range backing the StoreID allocating counter, and
		// letting this server start may be necessary to restore quorum to that
		// range. So in general, after this TODO, we will always leave this
		// method with *at least one* store initialized, but not necessarily
		// all. This is fine, since initializing additional stores later is
		// easy.
		clusterID, err := g.GetClusterID()
		if err != nil {
			return nil, err
		}
		s.inspectState.clusterID = clusterID
		return &initState{
			initDiskState: *s.inspectState,
			joined:        true,
			bootstrapped:  false,
		}, nil
	}
}

var errInternalBootstrapError = errors.New("unable to bootstrap due to internal error")

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
	ctx context.Context, _ *serverpb.BootstrapRequest,
) (*serverpb.BootstrapResponse, error) {
	// Bootstrap() only responds once. Everyone else gets an error, either
	// ErrClusterInitialized (in the success case) or errInternalBootstrapError.

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mu.rejectErr != nil {
		return nil, s.mu.rejectErr
	}

	state, err := s.tryBootstrap(ctx)
	if err != nil {
		log.Errorf(ctx, "bootstrap: %v", err)
		s.mu.rejectErr = errInternalBootstrapError
		return nil, s.mu.rejectErr
	}
	s.mu.rejectErr = ErrClusterInitialized
	s.bootstrapReqCh <- state
	return &serverpb.BootstrapResponse{}, nil
}

func (s *initServer) tryBootstrap(ctx context.Context) (*initState, error) {
	cv := clusterversion.ClusterVersion{Version: s.bootstrapVersion}
	if err := kvserver.WriteClusterVersionToEngines(ctx, s.inspectState.newEngines, cv); err != nil {
		return nil, err
	}
	return bootstrapCluster(
		ctx, s.inspectState.newEngines, s.bootstrapZoneConfig, s.bootstrapSystemZoneConfig,
	)
}

// DiskClusterVersion returns the cluster version synthesized from disk. This
// is always non-zero since it falls back to the BinaryMinSupportedVersion.
func (s *initServer) DiskClusterVersion() clusterversion.ClusterVersion {
	return s.inspectState.clusterVersion
}
