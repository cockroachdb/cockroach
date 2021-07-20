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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
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
		// This mutex is used to serialize bootstrap attempts.
		syncutil.Mutex

		// We use this field to guard against doubly bootstrapping clusters.
		bootstrapped bool

		// If we encounter an unrecognized error during bootstrap, we use this
		// field to block out future bootstrap attempts.
		rejectErr error
	}

	// inspectedDiskState captures the relevant bits of the on-disk state needed
	// by the init server. It's through this that the init server knows whether
	// or not this node needs to be bootstrapped. It does so by checking to see
	// if any engines were already initialized. If so, there's nothing left for
	// the init server to, it simply returns the inspected disk state in
	// ServeAndWait.
	//
	// Another function the inspected disk state provides is that it relays the
	// synthesized cluster version (this binary's minimum supported version if
	// there are no initialized engines). This is used as the cluster version if
	// we end up connecting to an existing cluster via gossip.
	//
	// TODO(irfansharif): The above function goes away once we remove the use of
	// gossip to join running clusters in 21.1.
	inspectedDiskState *initState

	// If this CRDB node was `cockroach init`-ialized, the resulting init state
	// will be passed through to this channel.
	bootstrapReqCh chan *initState
}

// NeedsBootstrap returns true if we haven't already been bootstrapped or
// haven't yet been able to join a running cluster.
func (s *initServer) NeedsBootstrap() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return !s.mu.bootstrapped
}

func newInitServer(
	actx log.AmbientContext, inspectedDiskState *initState, config initServerCfg,
) *initServer {
	initServer := &initServer{
		AmbientContext:     actx,
		bootstrapReqCh:     make(chan *initState, 1),
		config:             config,
		inspectedDiskState: inspectedDiskState,
	}
	// If we were already bootstrapped, we mark ourselves as such to prevent
	// future bootstrap attempts.
	if inspectedDiskState.bootstrapped() {
		initServer.mu.bootstrapped = true
	}
	return initServer
}

// initState is the entirety of what the init server is tasked with
// constructing. It's a view of our on-disk state, instantiated through
// inspectEngines (and inspectEngines alone).
//
// The init server is tasked with durably persisting state on-disk when this
// node is bootstrapped, or is able to join an already bootstrapped cluster.
// By state here we mean the cluster ID, node ID, at least one initialized
// engine, etc. After having persisted the relevant state, the init server
// constructs an initState with the details needed to fully start up a CRDB
// server.
type initState struct {
	nodeID               roachpb.NodeID
	clusterID            uuid.UUID
	clusterVersion       clusterversion.ClusterVersion
	initializedEngines   []storage.Engine
	uninitializedEngines []storage.Engine
	initialSettingsKVs   []roachpb.KeyValue
}

// bootstrapped is a shorthand to check if there exists at least one initialized
// engine.
func (i *initState) bootstrapped() bool {
	return len(i.initializedEngines) > 0
}

// validate asserts that the init state is a fully fleshed out one (i.e. with a
// non-empty cluster ID and node ID).
func (i *initState) validate() error {
	if (i.clusterID == uuid.UUID{}) {
		return errors.New("missing cluster ID")
	}
	if i.nodeID == 0 {
		return errors.New("missing node ID")
	}
	return nil
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
//
// The returned initState reflects a bootstrapped cluster (i.e. it has a cluster
// ID and a node ID for this server).
//
// This method must be called only once.
//
// NB: A gotcha that may not immediately be obvious is that we can never hope to
// have all stores initialized by the time ServeAndWait returns. This is because
// if this server is already bootstrapped, it might hold a replica of the range
// backing the StoreID allocating counter, and letting this server start may be
// necessary to restore quorum to that range. So in general, after this method,
// we will always leave this method with at least one store initialized, but not
// necessarily all. This is fine, since initializing additional stores later is
// easy (see `initializeAdditionalStores`).
//
// `initialBoot` is true if this is a new node. This flag should only be used
// for logging and reporting. A newly bootstrapped single-node cluster is
// functionally equivalent to one that restarted; any decisions should be made
// on persisted data instead of this flag.
func (s *initServer) ServeAndWait(
	ctx context.Context, stopper *stop.Stopper, sv *settings.Values,
) (state *initState, initialBoot bool, err error) {
	// If we're restarting an already bootstrapped node, return early.
	if s.inspectedDiskState.bootstrapped() {
		return s.inspectedDiskState, false, nil
	}

	log.Info(ctx, "no stores initialized")
	log.Info(ctx, "awaiting `cockroach init` or join with an already initialized node")

	// If we end up joining a bootstrapped cluster, the resulting init state
	// will be passed through this channel.
	var joinCh chan joinResult
	var cancelJoin = func() {}
	var wg sync.WaitGroup

	if len(s.config.resolvers) == 0 {
		// We're pointing to only ourselves or nothing at all, which (likely)
		// suggests that we're going to be bootstrapped by the operator. Since
		// we're not going to be sending out join RPCs, we don't bother spinning
		// up the join loop.
	} else {
		joinCh = make(chan joinResult, 1)
		wg.Add(1)

		var joinCtx context.Context
		joinCtx, cancelJoin = context.WithCancel(ctx)
		defer cancelJoin()

		err := stopper.RunAsyncTask(joinCtx, "init server: join loop",
			func(ctx context.Context) {
				defer wg.Done()

				state, err := s.startJoinLoop(ctx, stopper)
				joinCh <- joinResult{state: state, err: err}
			})
		if err != nil {
			wg.Done()
			return nil, false, err
		}
	}

	for {
		select {
		case state := <-s.bootstrapReqCh:
			// Ensure we're draining out the join attempt, if any. We're not
			// going to need it anymore and it had no chance of joining
			// elsewhere (since we are the ones bootstrapping the new cluster
			// and have not started serving Join yet).
			cancelJoin()
			wg.Wait()

			// Bootstrap() did its job. At this point, we know that the cluster
			// version will be the bootstrap version (aka the binary version[1]),
			// but the version setting does not know yet (it was initialized as
			// BinaryMinSupportedVersion because the engines were all
			// uninitialized). Given that the bootstrap version was persisted to
			// all the engines, it's now safe for us to bump the version setting
			// itself and start operating at the latest cluster version.
			//
			// TODO(irfansharif): We're calling Initialize a second time here.
			// There's no real reason to anymore, we can use
			// SetActiveClusterVersion instead. This will let us make
			// `Initialize` a bit stricter, which is a nice simplification to
			// have.
			//
			// [1]: See the top-level comment in pkg/clusterversion to make
			// sense of the many versions of ...versions.
			if err := clusterversion.Initialize(ctx, state.clusterVersion.Version, sv); err != nil {
				return nil, false, err
			}

			log.Infof(ctx, "cluster %s has been created", state.clusterID)
			log.Infof(ctx, "allocated node ID: n%d (for self)", state.nodeID)
			log.Infof(ctx, "active cluster version: %s", state.clusterVersion)

			return state, true, nil
		case result := <-joinCh:
			// Ensure we're draining out the join attempt.
			wg.Wait()

			if err := result.err; err != nil {
				if errors.Is(err, ErrIncompatibleBinaryVersion) {
					return nil, false, err
				}

				// We expect the join RPC to blindly retry on all
				// "connection" errors save for one above. If we're
				// here, we failed to initialize our first store after a
				// successful join attempt.
				return nil, false, errors.NewAssertionErrorWithWrappedErrf(err, "unexpected error")
			}

			state := result.state

			log.Infof(ctx, "joined cluster %s through join rpc", state.clusterID)
			log.Infof(ctx, "received node ID: %d", state.nodeID)
			log.Infof(ctx, "received cluster version: %s", state.clusterVersion)

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

	if s.mu.bootstrapped {
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

	// We've successfully bootstrapped (we've initialized at least one engine).
	// We mark ourselves as bootstrapped to prevent future bootstrap attempts.
	s.mu.bootstrapped = true
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
		return nil, errors.AssertionFailedf("expected to find at least one resolver, found none")
	}

	// Iterate through all the resolvers at least once to reduce time taken to
	// cluster convergence. Keep this code block roughly in sync with the one
	// below.
	for _, res := range s.config.resolvers {
		select {
		case <-ctx.Done():
			return nil, context.Canceled
		case <-stopper.ShouldQuiesce():
			return nil, stop.ErrUnavailable
		default:
		}

		addr := res.Addr()
		resp, err := s.attemptJoinTo(ctx, res.Addr())
		if errors.Is(err, ErrIncompatibleBinaryVersion) {
			// Propagate upwards; this is an error condition the caller knows
			// to expect.
			return nil, err
		}
		if err != nil {
			// Try the next node if unsuccessful.

			if IsWaitingForInit(err) {
				log.Infof(ctx, "%s is itself waiting for init, will retry", addr)
			} else {
				log.Warningf(ctx, "outgoing join rpc to %s unsuccessful: %v", addr, err.Error())
			}
			continue
		}

		state, err := s.initializeFirstStoreAfterJoin(ctx, resp)
		if err != nil {
			return nil, err
		}

		// We mark ourselves as bootstrapped to prevent future bootstrap attempts.
		s.mu.Lock()
		s.mu.bootstrapped = true
		s.mu.Unlock()

		return state, nil
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
			resp, err := s.attemptJoinTo(ctx, addr)
			if errors.Is(err, ErrIncompatibleBinaryVersion) {
				// Propagate upwards; this is an error condition the caller
				// knows to expect.
				return nil, err
			}
			if err != nil {
				// Blindly retry for all other errors, logging them for visibility.

				// TODO(irfansharif): If startup logging gets too spammy, we
				// could match against connection errors to generate nicer
				// logging. See grpcutil.connectionRefusedRe.

				if IsWaitingForInit(err) {
					log.Infof(ctx, "%s is itself waiting for init, will retry", addr)
				} else {
					log.Warningf(ctx, "outgoing join rpc to %s unsuccessful: %v", addr, err.Error())
				}
				continue
			}

			// We were able to successfully join an existing cluster. We'll now
			// initialize our first store, using the store ID handed to us.
			state, err := s.initializeFirstStoreAfterJoin(ctx, resp)
			if err != nil {
				return nil, err
			}

			// We mark ourselves as bootstrapped to prevent future bootstrap attempts.
			s.mu.Lock()
			s.mu.bootstrapped = true
			s.mu.Unlock()

			return state, nil
		case <-ctx.Done():
			return nil, context.Canceled
		case <-stopper.ShouldQuiesce():
			return nil, stop.ErrUnavailable
		}
	}
}

// attemptJoinTo attempts to join to the node running at the given address.
func (s *initServer) attemptJoinTo(
	ctx context.Context, addr string,
) (*roachpb.JoinNodeResponse, error) {
	conn, err := grpc.DialContext(ctx, addr, s.config.dialOpts...)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = conn.Close() // nolint:grpcconnclose
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

		if status.Code() == codes.PermissionDenied {
			log.Infof(ctx, "%s is running a version higher than our binary version %s", addr, req.BinaryVersion.String())
			return nil, ErrIncompatibleBinaryVersion
		}

		return nil, err
	}

	return resp, nil
}

func (s *initServer) tryBootstrap(ctx context.Context) (*initState, error) {
	// We use our binary version to bootstrap the cluster.
	cv := clusterversion.ClusterVersion{Version: s.config.binaryVersion}
	if err := kvserver.WriteClusterVersionToEngines(ctx, s.inspectedDiskState.uninitializedEngines, cv); err != nil {
		return nil, err
	}

	return bootstrapCluster(ctx, s.inspectedDiskState.uninitializedEngines, s.config)
}

// DiskClusterVersion returns the cluster version synthesized from disk. This
// is always non-zero since it falls back to the BinaryMinSupportedVersion.
func (s *initServer) DiskClusterVersion() clusterversion.ClusterVersion {
	return s.inspectedDiskState.clusterVersion
}

// initializeFirstStoreAfterJoin initializes the first store after a successful
// join attempt. It re-constructs the store identifier from the join response
// and persists the appropriate cluster version to disk. After having done so,
// it returns an initState that captures the newly initialized store.
func (s *initServer) initializeFirstStoreAfterJoin(
	ctx context.Context, resp *roachpb.JoinNodeResponse,
) (*initState, error) {
	firstEngine := s.inspectedDiskState.uninitializedEngines[0]
	clusterVersion := clusterversion.ClusterVersion{Version: *resp.ActiveVersion}
	if err := kvserver.WriteClusterVersion(ctx, firstEngine, clusterVersion); err != nil {
		return nil, err
	}

	sIdent, err := resp.CreateStoreIdent()
	if err != nil {
		return nil, err
	}
	if err := kvserver.InitEngine(ctx, firstEngine, sIdent); err != nil {
		return nil, err
	}

	return inspectEngines(
		ctx, s.inspectedDiskState.uninitializedEngines,
		s.config.binaryVersion, s.config.binaryMinSupportedVersion,
	)
}

// initServerCfg is a thin wrapper around the server Config object, exposing
// only the fields needed by the init server.
type initServerCfg struct {
	advertiseAddr             string
	binaryMinSupportedVersion roachpb.Version
	binaryVersion             roachpb.Version // the version used during bootstrap
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

	// testingKnobs is used for internal test controls only.
	testingKnobs base.TestingKnobs
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
		testingKnobs:              cfg.TestingKnobs,
	}
}
