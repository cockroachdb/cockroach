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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/gossip/resolver"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrClusterInitialized is reported when the Bootstrap RPC is run on
// a node that is already part of an initialized cluster.
var ErrClusterInitialized = fmt.Errorf("cluster has already been initialized")

// ErrNodeUninitialized is reported when the Join RPC is run against
// a node that itself is uninitialized.
var ErrNodeUninitialized = fmt.Errorf("node has not been initialized")

// ErrJoinRPCUnimplemented is reported when the Join RPC is run against
// a node that does not know about the join RPC (i.e. it is running 20.1 or
// below).
//
// TODO(irfansharif): Remove this in 21.1.
var ErrJoinRPCUnimplemented = fmt.Errorf("node does not implement join rpc")

// initServer handles the bootstrapping process. It is instantiated early in the
// server startup sequence to determine whether a NodeID and ClusterID are
// available (true if and only if an initialized store is present). If all
// engines are empty, either a new cluster needs to be started (via incoming
// Bootstrap RPC) or an existing one joined (via the outgoing Join RPC). Either
// way, the goal is to learn a ClusterID and NodeID (and initialize at least one
// store). All of this subtlety is encapsulated by the initServer, which offers
// a primitive ServeAndWait() after which point the startup code can assume that
// the Node/ClusterIDs are known.
type initServer struct {
	log.AmbientContext
	// rpcContext embeds the fields needed by the RPC subsystem (the init server
	// sends out Join RPCs during bootstrap).
	rpcContext *rpc.Context

	// config houses a few configuration options needed by the init server.
	config initServerCfg

	mu struct {
		// This mutex is grabbed during bootstrap and is used to serialized
		// bootstrap attempts (and attempts to read whether or not his node has
		// been bootstrapped).
		syncutil.Mutex
		// If we encounter an unrecognized error during bootstrap, we use this
		// field to block out future bootstrap attempts.
		rejectErr error
	}

	// The state of the engines. This tells us whether the node is already
	// bootstrapped. The goal of the initServer is to stub this out by the time
	// ServeAndWait returns.
	//
	// TODO(irfansharif): The ownership/access of this is a bit all over the
	// place, and can be sanitized.
	inspectState *initDiskState

	// If this CRDB node was `cockroach init`-ialized, the resulting init state
	// will be passed through to this channel.
	bootstrapReqCh chan *initState
	// If this CRDB node was able to join a bootstrapped cluster, the resulting
	// init state will be passed through to this channel.
	joinCh chan *initState

	// We hold onto a *kv.DB object to assign node IDs through the Join RPC.
	db *kv.DB

	// resolvers is a list of node addresses that is used to form a connected
	// graph/network of CRDB servers. Once a connected graph is constructed, it
	// suffices for any node in the network to be initialized (which then
	// propagates the cluster ID to the rest of the nodes).
	//
	// TODO(irfansharif): This last sentence is not yet true. Update this list
	// is continually to maintain bidirectional network links. We want it so
	// that it suffices to bootstrap n2 after having n2 point to n1 and n1 point
	// to itself. n1 can learn about n2's address and attempt to connect back to
	// it. We'll need to make sure that each server loops through its join list
	// at least once so that the "links" are setup. We don't want get
	// insta-bootstrapped and never set up those links.
	resolvers []resolver.Resolver
}

func newInitServer(
	actx log.AmbientContext,
	rpcContext *rpc.Context,
	inspectState *initDiskState,
	db *kv.DB,
	config initServerCfg,
) (*initServer, error) {
	s := &initServer{
		AmbientContext: actx,
		bootstrapReqCh: make(chan *initState, 1),
		inspectState:   inspectState,
		joinCh:         make(chan *initState, 1),
		rpcContext:     rpcContext,
		config:         config,
		db:             db,
		resolvers:      config.resolvers(),
	}
	return s, nil
}

// initDiskState contains the part of initState that is read from stable
// storage.
//
// NB: The above is a lie in the case in which we join an existing mixed-version
// cluster. In that case, the state returned from ServeAndWait will have the
// clusterID set from Gossip (and there will be no NodeID). This is holdover
// behavior that can be removed in 21.1, at which point the lie disappears.
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
// request or for a successful Join RPC, whichever happens earlier. See [1].
//
// The returned initState reflects a bootstrapped cluster (i.e. it has a cluster
// ID and a node ID for this server). See [2].
//
// This method must be called only once.
//
// NB: A gotcha that may not immediately be obvious is that we can never hope to
// have all stores initialized by the time ServeAndWait returns. This is because
// if this server is already bootstrapped, it might hold a replica of the range
// backing the StoreID allocating counter, and letting this server start may be
// necessary to restore quorum to that range. So in general, after this TODO, we
// will always leave this method with at least one store initialized, but not
// necessarily all. This is fine, since initializing additional stores later is
// easy.
//
// TODO(tbg): give this a KV client and thus initialize at least one store in
// all cases.
//
// [1]: In mixed version clusters it waits until Gossip connects (but this is
// slated to be removed in 21.1).
// [2]: This is not technically true for mixed version clusters where we leave
// the node ID unassigned until later, but this too is part of the deprecated
// init server behavior that is slated for removal in 21.1.
func (s *initServer) ServeAndWait(
	ctx context.Context, stopper *stop.Stopper, sv *settings.Values, gossip *gossip.Gossip,
) (*initState, error) {
	// If already bootstrapped, return early.
	if !s.NeedsInit() {
		return &initState{
			initDiskState: *s.inspectState,
			joined:        false,
			bootstrapped:  false,
		}, nil
	}

	log.Info(ctx, "no stores bootstrapped")
	log.Info(ctx, "awaiting `cockroach init` or join with an already initialized node")

	joinCtx, cancelJoin := context.WithCancel(ctx)
	defer cancelJoin()

	errCh := make(chan error, 1)
	if err := stopper.RunTask(joinCtx, "init server: join loop", func(joinCtx context.Context) {
		stopper.RunWorker(joinCtx, func(joinCtx context.Context) {
			errCh <- s.startJoinLoop(joinCtx, stopper)
		})
	}); err != nil {
		return nil, err
	}

	// gossipConnectedCh is used as a place holder for gossip.Connected. We
	// don't trigger on gossip connectivity unless we have to, favoring instead
	// the join RPC to discover the cluster ID (and node ID). If we're in a
	// mixed-version cluster however (with 20.1 nodes), we'll fall back to using
	// the legacy gossip connectivity mechanism to discover the cluster ID.
	var gossipConnectedCh chan struct{}
	for {
		select {
		case state := <-s.bootstrapReqCh:
			// Bootstrap() did its job. At this point, we know that the cluster
			// version will be the bootstrap version (aka the binary version[1]),
			// but the version setting does not know yet (it was initialized as
			// BinaryMinSupportedVersion because the engines were all
			// uninitialized). We *could* just let the server start, and it
			// would populate system.settings, which is then gossiped, and then
			// the callback would update the version, but we take this shortcut
			// to avoid having every freshly bootstrapped cluster spend time at
			// an old cluster version.
			//
			// [1]: See the top-level comment in pkg/clusterversion to make
			// sense of the many versions of...versions.
			if err := clusterversion.Initialize(ctx, state.clusterVersion.Version, sv); err != nil {
				return nil, err
			}

			log.Infof(ctx, "**** cluster %s has been created", state.clusterID)
			log.Infof(ctx, "**** allocated node id %d for self", state.nodeID)

			s.inspectState.clusterID = state.clusterID
			s.inspectState.initializedEngines = state.initializedEngines

			// Ensure we're draining out join attempt.
			cancelJoin()
			if err := <-errCh; err != nil {
				log.Errorf(ctx, "**** error draining join thread: %v", err)
			}

			return state, nil
		case state := <-s.joinCh:
			// TODO(irfansharif): Right now this doesn't actually do anything.
			// We should have the Join RPC funnel in the right version to use.
			if err := clusterversion.Initialize(ctx, state.clusterVersion.Version, sv); err != nil {
				return nil, err
			}

			log.Infof(ctx, "**** joined cluster %s through join rpc", state.clusterID)
			log.Infof(ctx, "**** received node id %d for self", state.nodeID)

			s.inspectState.clusterID = state.clusterID
			// Ensure we're draining out join attempt.
			cancelJoin()
			if err := <-errCh; err != nil {
				log.Errorf(ctx, "**** error draining join thread: %v", err)
			}

			return state, nil
		case <-gossipConnectedCh:
			// We're in a mixed-version cluster, so we retain the legacy
			// behavior of retrieving the cluster ID and deferring node ID
			// allocation (happens in (*Node).start).
			//
			// TODO(irfansharif): Remove this in 21.1.

			// Gossip connected, that is, we know a ClusterID. Due to the early
			// return above, we know that all of our engines are empty, i.e. we
			// don't have a NodeID yet (and the cluster version is the minimum we
			// support). Commence startup; the Node will realize it's short a
			// NodeID and will request one.
			clusterID, err := gossip.GetClusterID()
			if err != nil {
				return nil, err
			}

			s.inspectState.clusterID = clusterID
			state := &initState{
				initDiskState: *s.inspectState,
				joined:        true,
				bootstrapped:  false,
			}
			log.Infof(ctx, "**** joined cluster %s through gossip (legacy behavior)", state.clusterID)
			return state, nil
		case err := <-errCh:
			if errors.Is(err, ErrJoinRPCUnimplemented) {
				// We're in a mixed-version cluster, we're going to wire up the
				// gossip connectivity mechanism to discover the cluster ID.
				gossipConnectedCh = gossip.Connected
				continue
			}
			log.Errorf(ctx, "error in attempting to join: %v", err)
			return nil, err
		case <-stopper.ShouldQuiesce():
			return nil, stop.ErrUnavailable
		}
	}
}

var errInternalBootstrapError = errors.New("unable to bootstrap due to internal error")

// Bootstrap implements the serverpb.Init service. Users set up a new CRDB
// cluster by calling this endpoint on exactly one node in the cluster
// (typically retrying only on that node). This endpoint is what powers
// `cockroach init`. Attempting to bootstrap a node that was already
// bootstrapped will result in an `ErrClusterInitialized` error.
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

	if !s.NeedsInit() {
		return nil, ErrClusterInitialized
	}

	if s.mu.rejectErr != nil {
		return nil, s.mu.rejectErr
	}

	state, err := s.tryBootstrap(ctx)
	if err != nil {
		log.Errorf(ctx, "bootstrap: %v", err)
		s.mu.rejectErr = errInternalBootstrapError
		return nil, s.mu.rejectErr
	}

	s.bootstrapReqCh <- state
	return &serverpb.BootstrapResponse{}, nil
}

// Join implements the serverpb.Init service. This is the "connectivity" API;
// individual CRDB servers are passed in a --join list and the join targets are
// addressed through this API.
//
// TODO(irfansharif): Perhaps we could opportunistically create a liveness
// record here so as to no longer have to worry about the liveness record not
// existing for a given node.
func (s *initServer) Join(
	ctx context.Context, _ *serverpb.JoinNodeRequest,
) (*serverpb.JoinNodeResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.NeedsInit() {
		// Server has not been bootstrapped yet
		return nil, ErrNodeUninitialized
	}

	ctxWithSpan, span := s.AnnotateCtxWithSpan(ctx, "alloc-node-id")
	defer span.Finish()

	nodeID, err := allocateNodeID(ctxWithSpan, s.db)
	if err != nil {
		return nil, err
	}

	log.Infof(ctxWithSpan, "**** allocated new node id %d", nodeID)
	return &serverpb.JoinNodeResponse{
		ClusterID: s.inspectState.clusterID.GetBytes(),
		NodeID:    int32(nodeID),
	}, nil
}

func (s *initServer) startJoinLoop(ctx context.Context, stopper *stop.Stopper) error {
	dialOpts, err := s.rpcContext.GRPCDialOptions()
	if err != nil {
		return err
	}

	var conns []*grpc.ClientConn
	for _, r := range s.resolvers {
		conn, err := grpc.DialContext(ctx, r.Addr(), dialOpts...)
		if err != nil {
			return err
		}

		stopper.RunWorker(ctx, func(ctx context.Context) {
			<-stopper.ShouldQuiesce()
			if err := conn.Close(); err != nil {
				log.Fatalf(ctx, "%v", err)
			}
		})
		conns = append(conns, conn)
	}

	const joinRPCBackoff = time.Second
	var tickChan <-chan time.Time
	{
		ticker := time.NewTicker(joinRPCBackoff)
		tickChan = ticker.C
		defer ticker.Stop()
	}

	for idx := 0; ; idx = (idx + 1) % len(conns) {
		select {
		case <-tickChan:
			success, err := s.attemptJoin(ctx, s.resolvers[idx].Addr(), conns[idx])
			if err != nil {
				return err
			}
			if !success {
				continue
			}
			return nil
		case <-ctx.Done():
			return nil
		case <-stopper.ShouldQuiesce():
			return nil
		}
	}
}

func (s *initServer) attemptJoin(
	ctx context.Context, addr string, conn *grpc.ClientConn,
) (success bool, err error) {
	initClient := serverpb.NewInitClient(conn)
	req := &serverpb.JoinNodeRequest{
		MinSupportedVersion: &clusterversion.TestingBinaryMinSupportedVersion,
		Addr:                s.config.advertiseAddr(),
	}
	resp, err := initClient.Join(ctx, req)
	if err != nil {
		if strings.Contains(err.Error(), ErrNodeUninitialized.Error()) {
			log.Warningf(ctx, "node running on %s is itself uninitialized, retrying..", addr)
			return false, nil
		}

		if grpcutil.ConnectionRefusedRe.MatchString(err.Error()) {
			log.Warningf(ctx, "unable to connect to %s, retrying..", addr)
			return false, nil
		}

		// If the target node does not implement the join RPC, we're in a
		// mixed-version cluster and are talking to a v20.1 node. We error out
		// so the init server knows to fall back on the gossip-based discovery
		// mechanism for the clusterID.
		if code := status.Code(errors.Cause(err)); code == codes.Unimplemented &&
			strings.Contains(err.Error(), `unknown method Join`) {
			log.Warningf(ctx, "%s running an older version", addr)
			return false, ErrJoinRPCUnimplemented
		}

		return false, err
	}

	clusterID, err := uuid.FromBytes(resp.ClusterID)
	if err != nil {
		return false, err
	}

	s.inspectState.clusterID = clusterID
	s.inspectState.nodeID = roachpb.NodeID(resp.NodeID)
	state := &initState{
		initDiskState: *s.inspectState,
		joined:        true,
		bootstrapped:  false,
	}

	s.joinCh <- state
	return true, nil
}

func (s *initServer) tryBootstrap(ctx context.Context) (*initState, error) {
	cv := clusterversion.ClusterVersion{Version: s.config.bootstrapVersion()}
	if err := kvserver.WriteClusterVersionToEngines(ctx, s.inspectState.newEngines, cv); err != nil {
		return nil, err
	}
	return bootstrapCluster(
		ctx, s.inspectState.newEngines, s.config.defaultZoneConfig(), s.config.defaultSystemZoneConfig(),
	)
}

// DiskClusterVersion returns the cluster version synthesized from disk. This
// is always non-zero since it falls back to the BinaryMinSupportedVersion.
func (s *initServer) DiskClusterVersion() clusterversion.ClusterVersion {
	return s.inspectState.clusterVersion
}

// initServerCfg is a thin wrapper around the server Config object, exposing
// only the fields needed by the init server.
type initServerCfg struct {
	wrapped Config
}

// bootstrapVersion returns the version at which to bootstrap the cluster in
// Bootstrap().
func (c *initServerCfg) bootstrapVersion() roachpb.Version {
	bootstrapVersion := c.wrapped.Settings.Version.BinaryVersion()
	if knobs := c.wrapped.TestingKnobs.Server; knobs != nil {
		if ov := knobs.(*TestingKnobs).BootstrapVersionOverride; ov != (roachpb.Version{}) {
			bootstrapVersion = ov
		}
	}
	return bootstrapVersion
}

// defaultZoneConfig returns the zone config to bootstrap with.
func (c *initServerCfg) defaultZoneConfig() *zonepb.ZoneConfig {
	return &c.wrapped.DefaultZoneConfig
}

// defaultSystemZoneConfig returns the zone config to bootstrap system ranges
// with.
func (c *initServerCfg) defaultSystemZoneConfig() *zonepb.ZoneConfig {
	return &c.wrapped.DefaultSystemZoneConfig
}

func (c *initServerCfg) resolvers() []resolver.Resolver {
	return c.wrapped.FilterGossipBootstrapResolvers(context.Background())
}

func (c *initServerCfg) advertiseAddr() string {
	return c.wrapped.AdvertiseAddr
}
