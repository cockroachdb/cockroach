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
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/gossip/resolver"
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

// ErrClusterInitialized is reported when the Bootstrap RPC is run on
// a node that is already part of an initialized cluster.
var ErrClusterInitialized = fmt.Errorf("cluster has already been initialized")

// errJoinRPCUnsupported is reported when the Join RPC is run against
// a node that does not know about the join RPC (i.e. it is running 20.1 or
// below).
//
// TODO(irfansharif): Remove this in 21.1.
var errJoinRPCUnsupported = fmt.Errorf("node does not support the Join RPC")

// ErrIncompatibleBinaryVersion is returned when a CRDB node with a binary version X
// attempts to join a cluster with an active version that's higher. This is not
// allowed.
var ErrIncompatibleBinaryVersion = fmt.Errorf("binary is incompatible with the cluster attempted to join")

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
	// config houses a few configuration options needed by the init server.
	config initServerCfg

	mu struct {
		// This mutex is grabbed during bootstrap and is used to serialized
		// bootstrap attempts (and attempts to read whether or not his node has
		// been bootstrapped). It's also used to guard inspectState below.
		syncutil.Mutex

		// The state of the engines. This tells us whether the node is already
		// bootstrapped. The goal of the initServer is to stub this out by the time
		// ServeAndWait returns.
		inspectState *initDiskState

		// If we encounter an unrecognized error during bootstrap, we use this
		// field to block out future bootstrap attempts.
		rejectErr error
	}

	// If this CRDB node was `cockroach init`-ialized, the resulting init state
	// will be passed through to this channel.
	bootstrapReqCh chan *initState
}

func newInitServer(
	actx log.AmbientContext, inspectState *initDiskState, config initServerCfg,
) (*initServer, error) {
	s := &initServer{
		AmbientContext: actx,
		bootstrapReqCh: make(chan *initState, 1),
		config:         config,
	}
	s.mu.inspectState = inspectState
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
//
// TODO(irfansharif): The usage of initState and then initDiskState is a bit
// confusing. It isn't the case today that the version held in memory for a
// running server will be wholly reconstructed if reloading from disk. It
// could if we always persisted any changes made to it back to disk. Right now
// when initializing after a successful join attempt, we don't persist back the
// disk state back to disk (we'd need to bootstrap the first store here, in the
// same we do when `cockroach init`-ialized).
type initState struct {
	initDiskState

	firstStoreID roachpb.StoreID
}

// NeedsInit is like needsInitLocked, except it acquires the necessary locks.
func (s *initServer) NeedsInit() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.needsInitLocked()
}

// needsInitLocked returns true if (and only if) none if the engines are
// initialized. In this case, ServeAndWait is blocked until either an
// initialized node is reached via the Join RPC (or Gossip if operating in mixed
// version clusters running v20.1, see ErrJoinUnimplemented), or this node
// itself is bootstrapped.
func (s *initServer) needsInitLocked() bool {
	return len(s.mu.inspectState.initializedEngines) == 0
}

// joinResult is used to represent the result of a node attempting to join
// an already bootstrapped cluster.
type joinResult struct {
	state *initState
	err   error
}

// ServeAndWait waits until the server is initialized, i.e. has a cluster ID,
// node ID and has permission to join the cluster. In the common case of
// restarting an existing node, this immediately returns. When starting with a
// blank slate (i.e. only empty engines), it waits for incoming Bootstrap
// request or for a successful outgoing Join RPC, whichever happens earlier.
// See [1].
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
// `initialBoot` is true if this is a new node. This flag should only be used
// for logging and reporting. A newly bootstrapped single-node cluster is
// functionally equivalent to one that restarted; any decisions should be made
// on persisted data instead of this flag.
//
// [1]: In mixed version clusters it waits until Gossip connects (but this is
// slated to be removed in 21.1).
//
// [2]: This is not technically true for mixed version clusters where we leave
// the node ID unassigned until later, but this too is part of the deprecated
// init server behavior that is slated for removal in 21.1.
func (s *initServer) ServeAndWait(
	ctx context.Context,
	stopper *stop.Stopper,
	sv *settings.Values,
	startGossipFn func() *gossip.Gossip,
) (state *initState, initialBoot bool, err error) {
	// If already bootstrapped, return early.
	s.mu.Lock()
	if !s.needsInitLocked() {
		diskState := *s.mu.inspectState
		s.mu.Unlock()

		return &initState{initDiskState: diskState}, false, nil
	}
	s.mu.Unlock()

	log.Info(ctx, "no stores bootstrapped")
	log.Info(ctx, "awaiting `cockroach init` or join with an already initialized node")

	joinCtx, cancelJoin := context.WithCancel(ctx)
	defer cancelJoin()

	var wg sync.WaitGroup
	wg.Add(1)
	// If this CRDB node was able to join a bootstrapped cluster, the resulting
	// init state will be passed through to this channel.
	joinCh := make(chan joinResult, 1)
	if err := stopper.RunTask(joinCtx, "init server: join loop", func(joinCtx context.Context) {
		stopper.RunWorker(joinCtx, func(joinCtx context.Context) {
			defer wg.Done()

			state, err := s.startJoinLoop(joinCtx, stopper)
			joinCh <- joinResult{
				state: state,
				err:   err,
			}
		})
	}); err != nil {
		return nil, false, err
	}

	// gossipConnectedCh is used as a place holder for gossip.Connected. We
	// don't trigger on gossip connectivity unless we have to, favoring instead
	// the join RPC to discover the cluster ID (and node ID). If we're in a
	// mixed-version cluster however (with 20.1 nodes), we'll fall back to using
	// the legacy gossip connectivity mechanism to discover the cluster ID.
	var gossipConnectedCh chan struct{}
	var g *gossip.Gossip

	for {
		select {
		case state := <-s.bootstrapReqCh:
			// Ensure we're draining out the join attempt. We're not going to
			// need it anymore and it had no chance of joining anywhere (since
			// we are starting the new cluster and are not serving Join yet).
			cancelJoin()
			wg.Wait()

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
			// sense of the many versions of ...versions.
			if err := clusterversion.Initialize(ctx, state.clusterVersion.Version, sv); err != nil {
				return nil, false, err
			}

			log.Infof(ctx, "cluster %s has been created", state.clusterID)
			log.Infof(ctx, "allocated node ID: n%d (for self)", state.nodeID)

			s.mu.Lock()
			s.mu.inspectState.clusterID = state.clusterID
			s.mu.inspectState.initializedEngines = state.initializedEngines
			s.mu.Unlock()

			return state, true, nil
		case result := <-joinCh:
			// Ensure we're draining out the join attempt.
			wg.Wait()

			if err := result.err; err != nil {
				if errors.Is(err, errJoinRPCUnsupported) {
					// We're in a mixed-version cluster, we start gossip and wire up
					// the gossip connectivity mechanism to discover the cluster ID.
					g = startGossipFn()
					gossipConnectedCh = g.Connected

					// Let's nil out joinCh to prevent accidental re-use.
					close(joinCh)
					joinCh = nil

					continue
				}

				if errors.Is(err, ErrIncompatibleBinaryVersion) {
					return nil, false, err
				}

				if err != nil {
					// We expect the join RPC to blindly retry on all errors
					// save for the two above. This should be unreachable code.
					return nil, false, errors.NewAssertionErrorWithWrappedErrf(err, "unexpected error: %v", err)
				}
			}

			state := result.state

			// TODO(irfansharif): We can try and initialize the the version
			// setting to the active cluster version, in the same way we do when
			// bootstrapping. We'd need to persis the cluster version to the
			// engines here if we're looking to do so.

			log.Infof(ctx, "joined cluster %s through join rpc", state.clusterID)
			log.Infof(ctx, "received node ID %d", state.nodeID)

			s.mu.Lock()
			s.mu.inspectState.clusterID = state.clusterID
			s.mu.Unlock()

			return state, true, nil
		case <-gossipConnectedCh:
			// Ensure we're draining out the join attempt.
			wg.Wait()

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
			clusterID, err := g.GetClusterID()
			if err != nil {
				return nil, false, err
			}

			s.mu.Lock()
			s.mu.inspectState.clusterID = clusterID
			diskState := *s.mu.inspectState
			s.mu.Unlock()

			state := &initState{
				initDiskState: diskState,
			}
			log.Infof(ctx, "joined cluster %s through gossip (legacy behavior)", state.clusterID)
			return state, true, nil
		case <-stopper.ShouldQuiesce():
			return nil, false, stop.ErrUnavailable
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

	if !s.needsInitLocked() {
		return nil, ErrClusterInitialized
	}

	if s.mu.rejectErr != nil {
		return nil, s.mu.rejectErr
	}

	state, err := s.tryBootstrapLocked(ctx)
	if err != nil {
		log.Errorf(ctx, "bootstrap: %v", err)
		s.mu.rejectErr = errInternalBootstrapError
		return nil, s.mu.rejectErr
	}

	s.bootstrapReqCh <- state
	return &serverpb.BootstrapResponse{}, nil
}

// startJoinLoop continuously tries connecting to nodes specified in the join
// list in order to determine what the cluster ID is, and to be allocated a
// node+store ID. It can return errJoinRPCUnsupported, in which case the caller
// is expected to fall back to the gossip-based cluster ID discovery mechanism.
// It can also fail with ErrIncompatibleBinaryVersion, in which case we know we're
// running a binary that's too old to join the rest of the cluster.
func (s *initServer) startJoinLoop(ctx context.Context, stopper *stop.Stopper) (*initState, error) {
	if len(s.config.resolvers) == 0 {
		// We're pointing to only ourselves, which is probably indicative of a
		// node that's going to be bootstrapped by the operator. We could opt to
		// not fall back to the gossip based connectivity mechanism, but we do
		// it anyway.
		return nil, errJoinRPCUnsupported
	}

	const joinRPCBackoff = time.Second
	var tickChan <-chan time.Time
	{
		ticker := time.NewTicker(joinRPCBackoff)
		tickChan = ticker.C
		defer ticker.Stop()
	}

	for idx := 0; ; idx = (idx + 1) % len(s.config.resolvers) {
		addr := s.config.resolvers[idx].Addr()
		select {
		case <-tickChan:
			state, err := s.attemptJoinTo(ctx, addr)
			if err == nil {
				return state, nil
			}

			if errors.Is(err, errJoinRPCUnsupported) || errors.Is(err, ErrIncompatibleBinaryVersion) {
				// Propagate upwards; these are error conditions the caller
				// knows to expect.
				return nil, err
			}

			// Blindly retry for all other errors, logging them for visibility.

			// TODO(irfansharif): If startup logging gets too spammy, we
			// could match against connection errors to generate nicer
			// logging. See grpcutil.connectionRefusedRe.

			if IsWaitingForInit(err) {
				log.Warningf(ctx, "%s is itself waiting for init, will retry", addr)
			} else {
				log.Warningf(ctx, "outgoing join rpc to %s unsuccessful: %v", addr, err.Error())
			}
		case <-ctx.Done():
			return nil, context.Canceled
		case <-stopper.ShouldQuiesce():
			return nil, stop.ErrUnavailable
		}
	}
}

// attemptJoinTo attempts to join to the node running at the given address. If
// successful, an initState is returned.
func (s *initServer) attemptJoinTo(ctx context.Context, addr string) (*initState, error) {
	conn, err := grpc.DialContext(ctx, addr, s.config.dialOpts...)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = conn.Close()
	}()

	binaryVersion := s.config.binaryVersion
	req := &roachpb.JoinNodeRequest{
		BinaryVersion: &binaryVersion,
	}

	initClient := roachpb.NewInternalClient(conn)
	resp, err := initClient.Join(ctx, req)
	if err != nil {
		// If the target node does not implement the Join RPC, or explicitly
		// returns errJoinRPCUnsupported (because the cluster version
		// introducing its usage is not yet active), we error out so the init
		// server knows to fall back on the gossip-based discovery mechanism for
		// the clusterID.

		status, ok := grpcstatus.FromError(errors.UnwrapAll(err))
		if !ok {
			return nil, err
		}

		// TODO(irfansharif): Here we're logging the error and also returning
		// it. We should wrap the logged message with the right error instead.
		// The caller code, as written, switches on the error type; that'll need
		// to be changed as well.

		if status.Code() == codes.Unimplemented {
			log.Infof(ctx, "%s running an older version; falling back to gossip-based cluster join", addr)
			return nil, errJoinRPCUnsupported
		}

		if status.Code() == codes.PermissionDenied {
			log.Infof(ctx, "%s is running a version higher than our binary version %s", addr, req.BinaryVersion.String())
			return nil, ErrIncompatibleBinaryVersion
		}

		return nil, err
	}

	clusterID, err := uuid.FromBytes(resp.ClusterID)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.mu.inspectState.clusterID = clusterID
	s.mu.inspectState.nodeID = roachpb.NodeID(resp.NodeID)
	s.mu.inspectState.clusterVersion = clusterversion.ClusterVersion{Version: *resp.ActiveVersion}
	diskState := *s.mu.inspectState
	s.mu.Unlock()

	state := &initState{
		initDiskState: diskState,
		firstStoreID:  roachpb.StoreID(resp.StoreID),
	}

	return state, nil
}

func (s *initServer) tryBootstrapLocked(ctx context.Context) (*initState, error) {
	// We use our binary version to bootstrap the cluster.
	cv := clusterversion.ClusterVersion{Version: s.config.binaryVersion}
	if err := kvserver.WriteClusterVersionToEngines(ctx, s.mu.inspectState.newEngines, cv); err != nil {
		return nil, err
	}
	return bootstrapCluster(
		ctx, s.mu.inspectState.newEngines, &s.config.defaultZoneConfig, &s.config.defaultSystemZoneConfig,
	)
}

// DiskClusterVersion returns the cluster version synthesized from disk. This
// is always non-zero since it falls back to the BinaryMinSupportedVersion.
func (s *initServer) DiskClusterVersion() clusterversion.ClusterVersion {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.mu.inspectState.clusterVersion
}

// initServerCfg is a thin wrapper around the server Config object, exposing
// only the fields needed by the init server.
type initServerCfg struct {
	advertiseAddr             string
	binaryMinSupportedVersion roachpb.Version
	binaryVersion             roachpb.Version // This is what's used for bootstrap.
	defaultSystemZoneConfig   zonepb.ZoneConfig
	defaultZoneConfig         zonepb.ZoneConfig

	// dialOpts holds onto the dial options used when sending out Join RPCs.
	dialOpts []grpc.DialOption

	// resolvers is a list of node addresses (populated using --join addresses)
	// that is used to form a connected graph/network of CRDB servers. Once a
	// strongly connected graph is constructed, it suffices for any node in the
	// network to be initialized (which would then then propagates the cluster
	// ID to the rest of the nodes).
	//
	// NB: Not that this does not work for weakly connected graphs. Let's
	// consider a network where n3 points only to n2 (and not vice versa). If
	// n2 is `cockroach init`-ialized, n3 will learn about it. The reverse will
	// not be true.
	resolvers []resolver.Resolver
}

func newInitServerConfig(cfg Config, dialOpts []grpc.DialOption) initServerCfg {
	binaryVersion := cfg.Settings.Version.BinaryVersion()
	if knobs := cfg.TestingKnobs.Server; knobs != nil {
		if ov := knobs.(*TestingKnobs).BinaryVersionOverride; ov != (roachpb.Version{}) {
			binaryVersion = ov
		}
	}
	binaryMinSupportedVersion := cfg.Settings.Version.BinaryMinSupportedVersion()
	resolvers := cfg.FilterGossipBootstrapResolvers(context.Background())
	return initServerCfg{
		advertiseAddr:             cfg.AdvertiseAddr,
		binaryMinSupportedVersion: binaryMinSupportedVersion,
		binaryVersion:             binaryVersion,
		defaultSystemZoneConfig:   cfg.DefaultSystemZoneConfig,
		defaultZoneConfig:         cfg.DefaultZoneConfig,
		dialOpts:                  dialOpts,
		resolvers:                 resolvers,
	}
}
