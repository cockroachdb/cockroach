// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/pgurl"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/spf13/cobra"
	"golang.org/x/time/rate"
)

type transientCluster struct {
	connURL     string
	demoDir     string
	useSockets  bool
	stopper     *stop.Stopper
	firstServer *server.TestServer
	servers     []*server.TestServer
	defaultDB   string

	httpFirstPort int
	sqlFirstPort  int

	adminPassword string
	adminUser     security.SQLUsername

	stickyEngineRegistry server.StickyInMemEnginesRegistry
}

func (c *transientCluster) checkConfigAndSetupLogging(
	ctx context.Context, cmd *cobra.Command,
) (err error) {
	// useSockets is true on unix, false on windows.
	c.useSockets = useUnixSocketsInDemo()

	// The user specified some localities for their nodes.
	if len(demoCtx.localities) != 0 {
		// Error out of localities don't line up with requested node
		// count before doing any sort of setup.
		if len(demoCtx.localities) != demoCtx.nodes {
			return errors.Errorf("number of localities specified must equal number of nodes")
		}
	} else {
		demoCtx.localities = make([]roachpb.Locality, demoCtx.nodes)
		for i := 0; i < demoCtx.nodes; i++ {
			demoCtx.localities[i] = defaultLocalities[i%len(defaultLocalities)]
		}
	}

	// Override the default server store spec.
	//
	// This is needed because the logging setup code peeks into this to
	// decide how to enable logging.
	serverCfg.Stores.Specs = nil

	c.stopper, err = setupAndInitializeLoggingAndProfiling(ctx, cmd, false /* isServerCmd */)
	if err != nil {
		return err
	}
	maybeWarnMemSize(ctx)

	// Create a temporary directory for certificates (if secure) and
	// the unix sockets.
	// The directory is removed in the cleanup() method.
	if c.demoDir, err = ioutil.TempDir("", "demo"); err != nil {
		return err
	}

	if !demoCtx.insecure {
		if err := generateCerts(c.demoDir); err != nil {
			return err
		}
	}

	c.httpFirstPort = demoCtx.httpPort
	c.sqlFirstPort = demoCtx.sqlPort

	c.stickyEngineRegistry = server.NewStickyInMemEnginesRegistry()
	return nil
}

func (c *transientCluster) start(
	ctx context.Context, cmd *cobra.Command, gen workload.Generator,
) (err error) {
	ctx = logtags.AddTag(ctx, "start-demo-cluster", nil)

	// Initialize the connection database.
	// We can't do this earlier as this depends on which generator is used.
	c.defaultDB = catalogkeys.DefaultDatabaseName
	if gen != nil {
		c.defaultDB = gen.Meta().Name
	}

	// We now proceed to start all the nodes concurrently. This is
	// somewhat a complicated dance.
	//
	// On the one hand, we use a concurrent start, because the latency
	// map needs to be initialized after all the nodes have started
	// listening on the network, but before they proceed to initialize
	// their RPC context.
	//
	// On the other hand, we cannot use full concurrency either, because
	// we need to wait until the *first* node has started
	// listening on the network before we can start the next nodes.
	//
	// So we proceed in phases, as follows:
	//
	// 1. create and start the first node asynchronously.
	// 2. wait for the first node to listen on RPC and determine its
	//    listen addr; OR wait for an error in the first node initialization.
	// 3. create and start all the other nodes asynchronously.
	// 4. wait for all the nodes to listen on RPC, OR wait for an error
	//    from any node.
	// 5. if no error, proceed to initialize the latency map.
	// 6. in sequence, let each node initialize then wait for RPC readiness OR error from them.
	//    This ensures the node IDs are assigned sequentially.
	// 7. wait for the SQL readiness from all nodes.
	// 8. after all nodes are initialized, initialize SQL and telemetry.
	//

	timeoutCh := time.After(maxNodeInitTime)

	// errCh is used to catch errors when initializing servers.
	errCh := make(chan error, demoCtx.nodes)

	// rpcAddrReadyChs will be used in steps 2 and 4 below
	// to wait until the nodes know their RPC address.
	rpcAddrReadyChs := make([]chan struct{}, demoCtx.nodes)

	// latencyMapWaitChs is used to block test servers after RPC address
	// computation until the artificial latency map has been constructed.
	latencyMapWaitChs := make([]chan struct{}, demoCtx.nodes)

	// Step 1: create the first node.
	{
		phaseCtx := logtags.AddTag(ctx, "phase", 1)
		log.Infof(phaseCtx, "creating the first node")

		latencyMapWaitChs[0] = make(chan struct{})
		firstRPCAddrReadyCh, err := c.createAndAddNode(phaseCtx, 0, latencyMapWaitChs[0], timeoutCh)
		if err != nil {
			return err
		}
		rpcAddrReadyChs[0] = firstRPCAddrReadyCh
	}

	// Step 2: start the first node asynchronously, then wait for RPC
	// listen readiness or error.
	{
		phaseCtx := logtags.AddTag(ctx, "phase", 2)

		log.Infof(phaseCtx, "starting first node")
		if err := c.startNodeAsync(phaseCtx, 0, errCh, timeoutCh); err != nil {
			return err
		}
		log.Infof(phaseCtx, "waiting for first node RPC address")
		if err := c.waitForRPCAddrReadinessOrError(phaseCtx, 0, errCh, rpcAddrReadyChs, timeoutCh); err != nil {
			return err
		}
	}

	// Step 3: create the other nodes and start them asynchronously.
	{
		phaseCtx := logtags.AddTag(ctx, "phase", 3)
		log.Infof(phaseCtx, "starting other nodes")

		for i := 1; i < demoCtx.nodes; i++ {
			latencyMapWaitChs[i] = make(chan struct{})
			rpcAddrReady, err := c.createAndAddNode(phaseCtx, i, latencyMapWaitChs[i], timeoutCh)
			if err != nil {
				return err
			}
			rpcAddrReadyChs[i] = rpcAddrReady
		}

		// Ensure we close all sticky stores we've created when the stopper
		// instructs the entire cluster to stop. We do this only here
		// because we want this closer to be registered after all the
		// individual servers' Stop() methods have been registered
		// via createAndAddNode() above.
		c.stopper.AddCloser(stop.CloserFn(func() {
			c.stickyEngineRegistry.CloseAllStickyInMemEngines()
		}))

		// Start the remaining nodes asynchronously.
		for i := 1; i < demoCtx.nodes; i++ {
			if err := c.startNodeAsync(phaseCtx, i, errCh, timeoutCh); err != nil {
				return err
			}
		}
	}

	// Step 4: wait for all the nodes to know their RPC address,
	// or for an error or premature shutdown.
	{
		phaseCtx := logtags.AddTag(ctx, "phase", 4)
		log.Infof(phaseCtx, "waiting for remaining nodes to get their RPC address")

		for i := 0; i < demoCtx.nodes; i++ {
			if err := c.waitForRPCAddrReadinessOrError(phaseCtx, i, errCh, rpcAddrReadyChs, timeoutCh); err != nil {
				return err
			}
		}
	}

	// Step 5: optionally initialize the latency map, then let the servers
	// proceed with their initialization.

	{
		phaseCtx := logtags.AddTag(ctx, "phase", 5)

		// If latency simulation is requested, initialize the latency map.
		if demoCtx.simulateLatency {
			// Now, all servers have been started enough to know their own RPC serving
			// addresses, but nothing else. Assemble the artificial latency map.
			log.Infof(phaseCtx, "initializing latency map")
			for i, serv := range c.servers {
				latencyMap := serv.Cfg.TestingKnobs.Server.(*server.TestingKnobs).ContextTestingKnobs.ArtificialLatencyMap
				srcLocality, ok := serv.Cfg.Locality.Find("region")
				if !ok {
					continue
				}
				srcLocalityMap, ok := regionToRegionToLatency[srcLocality]
				if !ok {
					continue
				}
				for j, dst := range c.servers {
					if i == j {
						continue
					}
					dstLocality, ok := dst.Cfg.Locality.Find("region")
					if !ok {
						continue
					}
					latency := srcLocalityMap[dstLocality]
					latencyMap[dst.ServingRPCAddr()] = latency
				}
			}
		}
	}

	{
		phaseCtx := logtags.AddTag(ctx, "phase", 6)

		for i := 0; i < demoCtx.nodes; i++ {
			log.Infof(phaseCtx, "letting server %d initialize", i)
			close(latencyMapWaitChs[i])
			if err := c.waitForNodeIDReadiness(phaseCtx, i, errCh, timeoutCh); err != nil {
				return err
			}
			log.Infof(phaseCtx, "node n%d initialized", c.servers[i].NodeID())
		}
	}

	{
		phaseCtx := logtags.AddTag(ctx, "phase", 7)

		for i := 0; i < demoCtx.nodes; i++ {
			log.Infof(phaseCtx, "waiting for server %d SQL readiness", i)
			if err := c.waitForSQLReadiness(phaseCtx, i, errCh, timeoutCh); err != nil {
				return err
			}
			log.Infof(phaseCtx, "node n%d ready", c.servers[i].NodeID())
		}
	}

	{
		phaseCtx := logtags.AddTag(ctx, "phase", 8)

		// Run the SQL initialization. This takes care of setting up the
		// initial replication factor for small clusters and creating the
		// admin user.
		log.Infof(phaseCtx, "running initial SQL for demo cluster")

		const demoUsername = "demo"
		demoPassword := genDemoPassword(demoUsername)
		if err := runInitialSQL(phaseCtx, c.firstServer.Server, demoCtx.nodes < 3, demoUsername, demoPassword); err != nil {
			return err
		}
		if demoCtx.insecure {
			c.adminUser = security.RootUserName()
			c.adminPassword = "unused"
		} else {
			c.adminUser = security.MakeSQLUsernameFromPreNormalizedString(demoUsername)
			c.adminPassword = demoPassword
		}

		// Prepare the URL for use by the SQL shell.
		purl, err := c.getNetworkURLForServer(ctx, 0, true /* includeAppName */)
		if err != nil {
			return err
		}
		c.connURL = purl.ToPQ().String()

		// Start up the update check loop.
		// We don't do this in (*server.Server).Start() because we don't want this
		// overhead and possible interference in tests.
		if !demoCtx.disableTelemetry {
			log.Infof(phaseCtx, "starting telemetry")
			c.firstServer.StartDiagnostics(phaseCtx)
		}
	}
	return nil
}

// createAndAddNode is responsible for determining node parameters,
// instantiating the server component and connecting it to the
// cluster's stopper.
//
// The caller is responsible for calling createAndAddNode() with idx 0
// synchronously, then startNodeAsync(), then
// waitForNodeRPCListener(), before using createAndAddNode() with
// other indexes.
func (c *transientCluster) createAndAddNode(
	ctx context.Context, idx int, latencyMapWaitCh chan struct{}, timeoutCh <-chan time.Time,
) (rpcAddrReadyCh chan struct{}, err error) {
	var joinAddr string
	if idx > 0 {
		// The caller is responsible for ensuring that the method
		// is not called before the first server has finished
		// computing its RPC listen address.
		joinAddr = c.firstServer.ServingRPCAddr()
	}
	nodeID := roachpb.NodeID(idx + 1)
	args := testServerArgsForTransientCluster(
		c.sockForServer(nodeID), nodeID, joinAddr, c.demoDir,
		c.sqlFirstPort,
		c.httpFirstPort,
		c.stickyEngineRegistry,
	)
	if idx == 0 {
		// The first node also auto-inits the cluster.
		args.NoAutoInitializeCluster = false
	}

	serverKnobs := args.Knobs.Server.(*server.TestingKnobs)

	// SignalAfterGettingRPCAddress will be closed by the server startup routine
	// once it has determined its RPC address.
	rpcAddrReadyCh = make(chan struct{})
	serverKnobs.SignalAfterGettingRPCAddress = rpcAddrReadyCh

	// The server will wait until PauseAfterGettingRPCAddress is closed
	// after it has signaled SignalAfterGettingRPCAddress, and before
	// it continues the startup routine.
	serverKnobs.PauseAfterGettingRPCAddress = latencyMapWaitCh

	if demoCtx.simulateLatency {
		// The latency map will be populated after all servers have
		// started listening on RPC, and before they proceed with their
		// startup routine.
		serverKnobs.ContextTestingKnobs = rpc.ContextTestingKnobs{
			ArtificialLatencyMap: make(map[string]int),
		}
	}

	// Create the server instance. This also registers the in-memory store
	// into the sticky engine registry.
	s, err := server.TestServerFactory.New(args)
	if err != nil {
		return nil, err
	}
	serv := s.(*server.TestServer)

	// Ensure that this server gets stopped when the top level demo
	// stopper instructs the cluster to stop.
	c.stopper.AddCloser(stop.CloserFn(serv.Stop))

	if idx == 0 {
		// Remember the first server for later use by other APIs on
		// transientCluster.
		c.firstServer = serv
		// The first node connects its Settings instance to the `log`
		// package for crash reporting.
		//
		// There's a known shortcoming with this approach: restarting
		// node 1 using the \demo commands will break this connection:
		// if the user changes the cluster setting after restarting node
		// 1, the `log` package will not see this change.
		//
		// TODO(knz): re-connect the `log` package every time the first
		// node is restarted and gets a new `Settings` instance.
		settings.SetCanonicalValuesContainer(&serv.ClusterSettings().SV)
	}

	// Remember this server for the stop/restart primitives in the SQL
	// shell.
	c.servers = append(c.servers, serv)

	return rpcAddrReadyCh, nil
}

// startNodeAsync starts the node initialization asynchronously.
func (c *transientCluster) startNodeAsync(
	ctx context.Context, idx int, errCh chan error, timeoutCh <-chan time.Time,
) error {
	if idx > len(c.servers) {
		return errors.AssertionFailedf("programming error: server %d not created yet", idx)
	}

	serv := c.servers[idx]
	tag := fmt.Sprintf("start-n%d", idx+1)
	return c.stopper.RunAsyncTask(ctx, tag, func(ctx context.Context) {
		ctx = logtags.AddTag(ctx, tag, nil)
		err := serv.Start(ctx)
		if err != nil {
			log.Warningf(ctx, "server %d failed to start: %v", idx, err)
			select {
			case errCh <- err:

				// Don't block if we are shutting down.
			case <-ctx.Done():
			case <-serv.Stopper().ShouldQuiesce():
			case <-c.stopper.ShouldQuiesce():
			case <-timeoutCh:
			}
		}
	})
}

// waitForRPCAddrReadinessOrError waits until the given server knows its
// RPC address or fails to initialize.
func (c *transientCluster) waitForRPCAddrReadinessOrError(
	ctx context.Context,
	idx int,
	errCh chan error,
	rpcAddrReadyChs []chan struct{},
	timeoutCh <-chan time.Time,
) error {
	if idx > len(rpcAddrReadyChs) || idx > len(c.servers) {
		return errors.AssertionFailedf("programming error: server %d not created yet", idx)
	}

	select {
	case <-rpcAddrReadyChs[idx]:
		// This server knows its RPC address. Proceed with the next phase.
		return nil

		// If we are asked for an early shutdown by the cases below or a
		// server startup failure, detect it here.
	case err := <-errCh:
		return err
	case <-timeoutCh:
		return errors.Newf("demo startup timeout while waiting for server %d", idx)
	case <-ctx.Done():
		return errors.CombineErrors(ctx.Err(), errors.Newf("server %d startup aborted due to context cancellation", idx))
	case <-c.servers[idx].Stopper().ShouldQuiesce():
		return errors.Newf("server %d stopped prematurely", idx)
	case <-c.stopper.ShouldQuiesce():
		return errors.Newf("demo cluster stopped prematurely while starting server %d", idx)
	}
}

// waitForNodeIDReadiness waits until the given server reports it knows its node ID.
func (c *transientCluster) waitForNodeIDReadiness(
	ctx context.Context, idx int, errCh chan error, timeoutCh <-chan time.Time,
) error {
	retryOpts := retry.Options{InitialBackoff: 10 * time.Millisecond, MaxBackoff: time.Second}
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		log.Infof(ctx, "waiting for server %d to know its node ID", idx)
		select {
		// Errors or premature shutdown.
		case <-timeoutCh:
			return errors.Newf("initialization timeout while waiting for server %d node ID", idx)
		case err := <-errCh:
			return errors.Wrapf(err, "server %d failed to start", idx)
		case <-ctx.Done():
			return errors.CombineErrors(errors.Newf("context cancellation while waiting for server %d to have a node ID", idx), ctx.Err())
		case <-c.servers[idx].Stopper().ShouldQuiesce():
			return errors.Newf("server %s shut down prematurely", idx)
		case <-c.stopper.ShouldQuiesce():
			return errors.Newf("demo cluster shut down prematurely while waiting for server %d to have a node ID", idx)

		default:
			if c.servers[idx].NodeID() == 0 {
				log.Infof(ctx, "server %d does not know its node ID yet", idx)
				continue
			} else {
				log.Infof(ctx, "server %d: n%d", idx, c.servers[idx].NodeID())
			}
		}
		break
	}
	return nil
}

// waitForSQLReadiness waits until the given server reports it is
// healthy and ready to accept SQL clients.
func (c *transientCluster) waitForSQLReadiness(
	baseCtx context.Context, idx int, errCh chan error, timeoutCh <-chan time.Time,
) error {
	retryOpts := retry.Options{InitialBackoff: 10 * time.Millisecond, MaxBackoff: time.Second}
	for r := retry.StartWithCtx(baseCtx, retryOpts); r.Next(); {
		ctx := logtags.AddTag(baseCtx, "n", c.servers[idx].NodeID())
		log.Infof(ctx, "waiting for server %d to become ready", idx)
		select {
		// Errors or premature shutdown.
		case <-timeoutCh:
			return errors.Newf("initialization timeout while waiting for server %d readiness", idx)
		case err := <-errCh:
			return errors.Wrapf(err, "server %d failed to start", idx)
		case <-ctx.Done():
			return errors.CombineErrors(errors.Newf("context cancellation while waiting for server %d to become ready", idx), ctx.Err())
		case <-c.servers[idx].Stopper().ShouldQuiesce():
			return errors.Newf("server %s shut down prematurely", idx)
		case <-c.stopper.ShouldQuiesce():
			return errors.Newf("demo cluster shut down prematurely while waiting for server %d to become ready", idx)
		default:
			if err := c.servers[idx].Readiness(ctx); err != nil {
				log.Infof(ctx, "server %d not yet ready: %v", idx, err)
				continue
			}
		}
		break
	}
	return nil
}

// testServerArgsForTransientCluster creates the test arguments for
// a necessary server in the demo cluster.
func testServerArgsForTransientCluster(
	sock unixSocketDetails,
	nodeID roachpb.NodeID,
	joinAddr string,
	demoDir string,
	sqlBasePort, httpBasePort int,
	stickyEngineRegistry server.StickyInMemEnginesRegistry,
) base.TestServerArgs {
	// Assign a path to the store spec, to be saved.
	storeSpec := base.DefaultTestStoreSpec
	storeSpec.StickyInMemoryEngineID = fmt.Sprintf("demo-node%d", nodeID)

	args := base.TestServerArgs{
		SocketFile:              sock.filename(),
		PartOfCluster:           true,
		Stopper:                 stop.NewStopper(),
		JoinAddr:                joinAddr,
		DisableTLSForHTTP:       true,
		StoreSpecs:              []base.StoreSpec{storeSpec},
		SQLMemoryPoolSize:       demoCtx.sqlPoolMemorySize,
		CacheSize:               demoCtx.cacheSize,
		NoAutoInitializeCluster: true,
		EnableDemoLoginEndpoint: true,
		// This disables the tenant server. We could enable it but would have to
		// generate the suitable certs at the caller who wishes to do so.
		TenantAddr: new(string),
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				StickyEngineRegistry: stickyEngineRegistry,
			},
		},
	}

	if !testingForceRandomizeDemoPorts {
		// Unit tests can be run with multiple processes side-by-side with
		// `make stress`. This is bound to not work with fixed ports.
		sqlPort := sqlBasePort + int(nodeID) - 1
		if sqlBasePort == 0 {
			sqlPort = 0
		}
		httpPort := httpBasePort + int(nodeID) - 1
		if httpBasePort == 0 {
			httpPort = 0
		}
		args.SQLAddr = fmt.Sprintf(":%d", sqlPort)
		args.HTTPAddr = fmt.Sprintf(":%d", httpPort)
	}

	if demoCtx.localities != nil {
		args.Locality = demoCtx.localities[int(nodeID-1)]
	}
	if demoCtx.insecure {
		args.Insecure = true
	} else {
		args.Insecure = false
		args.SSLCertsDir = demoDir
	}

	return args
}

// testingForceRandomizeDemoPorts disables the fixed port allocation
// for demo clusters, for use in tests.
var testingForceRandomizeDemoPorts bool

func (c *transientCluster) cleanup(ctx context.Context) {
	if c.stopper != nil {
		c.stopper.Stop(ctx)
	}
	if c.demoDir != "" {
		if err := checkAndMaybeShout(os.RemoveAll(c.demoDir)); err != nil {
			// There's nothing to do here anymore if err != nil.
			_ = err
		}
	}
}

// DrainAndShutdown will gracefully attempt to drain a node in the cluster, and
// then shut it down.
func (c *transientCluster) DrainAndShutdown(nodeID roachpb.NodeID) error {
	if demoCtx.simulateLatency {
		return errors.Errorf("shutting down nodes is not supported in --%s configurations", cliflags.Global.Name)
	}
	nodeIndex := int(nodeID - 1)

	if nodeIndex < 0 || nodeIndex >= len(c.servers) {
		return errors.Errorf("node %d does not exist", nodeID)
	}
	// This is possible if we re-assign c.s and make the other nodes to the new
	// base node.
	if nodeIndex == 0 {
		return errors.Errorf("cannot shutdown node %d", nodeID)
	}
	if c.servers[nodeIndex] == nil {
		return errors.Errorf("node %d is already shut down", nodeID)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	adminClient, finish, err := getAdminClient(ctx, *(c.servers[nodeIndex].Cfg))
	if err != nil {
		return err
	}
	defer finish()

	if err := drainAndShutdown(ctx, adminClient); err != nil {
		return err
	}
	c.servers[nodeIndex] = nil
	return nil
}

// Recommission recommissions a given node.
func (c *transientCluster) Recommission(nodeID roachpb.NodeID) error {
	nodeIndex := int(nodeID - 1)

	if nodeIndex < 0 || nodeIndex >= len(c.servers) {
		return errors.Errorf("node %d does not exist", nodeID)
	}

	req := &serverpb.DecommissionRequest{
		NodeIDs:          []roachpb.NodeID{nodeID},
		TargetMembership: livenesspb.MembershipStatus_ACTIVE,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	adminClient, finish, err := getAdminClient(ctx, *(c.firstServer.Cfg))
	if err != nil {
		return err
	}

	defer finish()
	_, err = adminClient.Decommission(ctx, req)
	if err != nil {
		return errors.Wrap(err, "while trying to mark as decommissioning")
	}

	return nil
}

// Decommission decommissions a given node.
func (c *transientCluster) Decommission(nodeID roachpb.NodeID) error {
	nodeIndex := int(nodeID - 1)

	if nodeIndex < 0 || nodeIndex >= len(c.servers) {
		return errors.Errorf("node %d does not exist", nodeID)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	adminClient, finish, err := getAdminClient(ctx, *(c.firstServer.Cfg))
	if err != nil {
		return err
	}
	defer finish()

	// This (cumbersome) two step process is due to the allowed state
	// transitions for membership status. To mark a node as fully
	// decommissioned, it has to be marked as decommissioning first.
	{
		req := &serverpb.DecommissionRequest{
			NodeIDs:          []roachpb.NodeID{nodeID},
			TargetMembership: livenesspb.MembershipStatus_DECOMMISSIONING,
		}
		_, err = adminClient.Decommission(ctx, req)
		if err != nil {
			return errors.Wrap(err, "while trying to mark as decommissioning")
		}
	}

	{
		req := &serverpb.DecommissionRequest{
			NodeIDs:          []roachpb.NodeID{nodeID},
			TargetMembership: livenesspb.MembershipStatus_DECOMMISSIONED,
		}
		_, err = adminClient.Decommission(ctx, req)
		if err != nil {
			return errors.Wrap(err, "while trying to mark as decommissioned")
		}
	}

	return nil
}

// RestartNode will bring back a node in the cluster.
// The node must have been shut down beforehand.
// The node will restart, connecting to the same in memory node.
func (c *transientCluster) RestartNode(nodeID roachpb.NodeID) error {
	nodeIndex := int(nodeID - 1)

	if nodeIndex < 0 || nodeIndex >= len(c.servers) {
		return errors.Errorf("node %d does not exist", nodeID)
	}
	if c.servers[nodeIndex] != nil {
		return errors.Errorf("node %d is already running", nodeID)
	}

	// TODO(#42243): re-compute the latency mapping.
	// TODO(...): the RPC address of the first server may not be available
	// if the first server was shut down.
	if demoCtx.simulateLatency {
		return errors.Errorf("restarting nodes is not supported in --%s configurations", cliflags.Global.Name)
	}
	args := testServerArgsForTransientCluster(c.sockForServer(nodeID), nodeID,
		c.firstServer.ServingRPCAddr(), c.demoDir,
		c.sqlFirstPort, c.httpFirstPort, c.stickyEngineRegistry)
	s, err := server.TestServerFactory.New(args)
	if err != nil {
		return err
	}
	serv := s.(*server.TestServer)

	// We want to only return after the server is ready.
	readyCh := make(chan struct{})
	serv.Cfg.ReadyFn = func(bool) {
		close(readyCh)
	}

	if err := serv.Start(context.Background()); err != nil {
		return err
	}

	// Wait until the server is ready to action.
	select {
	case <-readyCh:
	case <-time.After(maxNodeInitTime):
		return errors.Newf("could not initialize node %d in time", nodeID)
	}

	c.stopper.AddCloser(stop.CloserFn(serv.Stop))
	c.servers[nodeIndex] = serv
	return nil
}

// AddNode create a new node in the cluster and start it.
// This function uses RestartNode to perform the actual node
// starting.
func (c *transientCluster) AddNode(ctx context.Context, localityString string) error {
	// TODO(#42243): re-compute the latency mapping for this to work.
	if demoCtx.simulateLatency {
		return errors.Errorf("adding nodes is not supported in --%s configurations", cliflags.Global.Name)
	}

	// '\demo add' accepts both strings that are quoted and not quoted. To properly make use of
	// quoted strings, strip off the quotes.  Before we do that though, make sure that the quotes match,
	// or that there aren't any quotes in the string.
	re := regexp.MustCompile(`".+"|'.+'|[^'"]+`)
	if localityString != re.FindString(localityString) {
		return errors.Errorf(`Invalid locality (missing " or '): %s`, localityString)
	}
	trimmedString := strings.Trim(localityString, `"'`)

	// Setup locality based on supplied string.
	var loc roachpb.Locality
	if err := loc.Set(trimmedString); err != nil {
		return err
	}

	// Ensure that the cluster is sane before we start messing around with it.
	if len(demoCtx.localities) != demoCtx.nodes || demoCtx.nodes != len(c.servers) {
		return errors.Errorf("number of localities specified (%d) must equal number of "+
			"nodes (%d) and number of servers (%d)", len(demoCtx.localities), demoCtx.nodes, len(c.servers))
	}

	// Create a new empty server element and add associated locality info.
	// When we call RestartNode below, this element will be properly initialized.
	c.servers = append(c.servers, nil)
	demoCtx.localities = append(demoCtx.localities, loc)
	demoCtx.nodes++
	newNodeID := roachpb.NodeID(demoCtx.nodes)

	return c.RestartNode(newNodeID)
}

func maybeWarnMemSize(ctx context.Context) {
	if maxMemory, err := status.GetTotalMemory(ctx); err == nil {
		requestedMem := (demoCtx.cacheSize + demoCtx.sqlPoolMemorySize) * int64(demoCtx.nodes)
		maxRecommendedMem := int64(.75 * float64(maxMemory))
		if requestedMem > maxRecommendedMem {
			log.Ops.Shoutf(
				ctx, severity.WARNING,
				`HIGH MEMORY USAGE
The sum of --max-sql-memory (%s) and --cache (%s) multiplied by the
number of nodes (%d) results in potentially high memory usage on your
device.
This server is running at increased risk of memory-related failures.`,
				demoNodeSQLMemSizeValue,
				demoNodeCacheSizeValue,
				demoCtx.nodes,
			)
		}
	}
}

// generateCerts generates some temporary certificates for cockroach demo.
func generateCerts(certsDir string) (err error) {
	caKeyPath := filepath.Join(certsDir, security.EmbeddedCAKey)
	// Create a CA-Key.
	if err := security.CreateCAPair(
		certsDir,
		caKeyPath,
		defaultKeySize,
		defaultCALifetime,
		false, /* allowKeyReuse */
		false, /*overwrite */
	); err != nil {
		return err
	}
	// Generate a certificate for the demo nodes.
	if err := security.CreateNodePair(
		certsDir,
		caKeyPath,
		defaultKeySize,
		defaultCertLifetime,
		false, /* overwrite */
		[]string{"127.0.0.1"},
	); err != nil {
		return err
	}
	// Create a certificate for the root user.
	return security.CreateClientPair(
		certsDir,
		caKeyPath,
		defaultKeySize,
		defaultCertLifetime,
		false, /* overwrite */
		security.RootUserName(),
		false, /* generatePKCS8Key */
	)
}

func (c *transientCluster) getNetworkURLForServer(
	ctx context.Context, serverIdx int, includeAppName bool,
) (*pgurl.URL, error) {
	u := pgurl.New()
	if includeAppName {
		if err := u.SetOption("application_name", catconstants.ReportableAppNamePrefix+"cockroach demo"); err != nil {
			return nil, err
		}
	}
	host, port, _ := netutil.SplitHostPort(c.servers[serverIdx].ServingSQLAddr(), "")
	u.
		WithNet(pgurl.NetTCP(host, port)).
		WithDatabase(c.defaultDB)

	// For a demo cluster we don't use client TLS certs and instead use
	// password-based authentication with the password pre-filled in the
	// URL.
	if demoCtx.insecure {
		u.WithInsecure()
	} else {
		u.
			WithUsername(c.adminUser.Normalized()).
			WithAuthn(pgurl.AuthnPassword(true, c.adminPassword)).
			WithTransport(pgurl.TransportTLS(pgurl.TLSRequire, ""))
	}
	return u, nil
}

func (c *transientCluster) setupWorkload(
	ctx context.Context, gen workload.Generator, licenseDone <-chan error,
) error {
	// If there is a load generator, create its database and load its
	// fixture.
	if gen != nil {
		db, err := gosql.Open("postgres", c.connURL)
		if err != nil {
			return err
		}
		defer db.Close()

		if _, err := db.Exec(`CREATE DATABASE ` + gen.Meta().Name); err != nil {
			return err
		}

		ctx := context.TODO()
		var l workloadsql.InsertsDataLoader
		if cliCtx.isInteractive {
			fmt.Printf("#\n# Beginning initialization of the %s dataset, please wait...\n", gen.Meta().Name)
		}
		if _, err := workloadsql.Setup(ctx, db, gen, l); err != nil {
			return err
		}
		// Perform partitioning if requested by configuration.
		if demoCtx.geoPartitionedReplicas {
			// Wait until the license has been acquired to trigger partitioning.
			if cliCtx.isInteractive {
				fmt.Println("#\n# Waiting for license acquisition to complete...")
			}
			if err := waitForLicense(licenseDone); err != nil {
				return err
			}
			if cliCtx.isInteractive {
				fmt.Println("#\n# Partitioning the demo database, please wait...")
			}

			db, err := gosql.Open("postgres", c.connURL)
			if err != nil {
				return err
			}
			defer db.Close()
			// Based on validation done in setup, we know that this workload has a partitioning step.
			if err := gen.(workload.Hookser).Hooks().Partition(db); err != nil {
				return errors.Wrapf(err, "partitioning the demo database")
			}
		}

		// Run the workload. This must occur after partitioning the database.
		if demoCtx.runWorkload {
			var sqlURLs []string
			for i := range c.servers {
				sqlURL, err := c.getNetworkURLForServer(ctx, i, true /* includeAppName */)
				if err != nil {
					return err
				}
				sqlURLs = append(sqlURLs, sqlURL.ToPQ().String())
			}
			if err := c.runWorkload(ctx, gen, sqlURLs); err != nil {
				return errors.Wrapf(err, "starting background workload")
			}
		}
	}

	return nil
}

func (c *transientCluster) runWorkload(
	ctx context.Context, gen workload.Generator, sqlUrls []string,
) error {
	opser, ok := gen.(workload.Opser)
	if !ok {
		return errors.Errorf("default dataset %s does not have a workload defined", gen.Meta().Name)
	}

	// Dummy registry to prove to the Opser.
	reg := histogram.NewRegistry(
		time.Duration(100)*time.Millisecond,
		opser.Meta().Name,
	)
	ops, err := opser.Ops(ctx, sqlUrls, reg)
	if err != nil {
		return errors.Wrap(err, "unable to create workload")
	}

	// Use a light rate limit of 25 queries per second
	limiter := rate.NewLimiter(rate.Limit(25), 1)

	// Start a goroutine to run each of the workload functions.
	for _, workerFn := range ops.WorkerFns {
		workloadFun := func(f func(context.Context) error) func(context.Context) {
			return func(ctx context.Context) {
				for {
					// Limit how quickly we can generate work.
					if err := limiter.Wait(ctx); err != nil {
						// When the limiter throws an error, panic because we don't
						// expect any errors from it.
						panic(err)
					}
					if err := f(ctx); err != nil {
						// Log an error without exiting the load generator when the workload
						// function throws an error. A single error during the workload demo
						// should not stop the demo.
						log.Warningf(ctx, "error running workload query: %+v", err)
					}
					select {
					case <-c.firstServer.Stopper().ShouldQuiesce():
						return
					case <-ctx.Done():
						log.Warningf(ctx, "workload terminating from context cancellation: %v", ctx.Err())
						return
					case <-c.stopper.ShouldQuiesce():
						log.Warningf(ctx, "demo cluster shutting down")
						return
					default:
					}
				}
			}
		}
		// As the SQL shell is tied to `c.firstServer`, this means we want to tie the workload
		// onto this as we want the workload to stop when the server dies,
		// rather than the cluster. Otherwise, interrupts on cockroach demo hangs.
		if err := c.firstServer.Stopper().RunAsyncTask(ctx, "workload", workloadFun(workerFn)); err != nil {
			return err
		}
	}

	return nil
}

// acquireDemoLicense begins an asynchronous process to obtain a
// temporary demo license from the Cockroach Labs website. It returns
// a channel that can be waited on if it is needed to wait on the
// license acquisition.
func (c *transientCluster) acquireDemoLicense(ctx context.Context) (chan error, error) {
	// Communicate information about license acquisition to services
	// that depend on it.
	licenseDone := make(chan error)
	if demoCtx.disableLicenseAcquisition {
		// If we are not supposed to acquire a license, close the channel
		// immediately so that future waiters don't hang.
		close(licenseDone)
	} else {
		// If we allow telemetry, then also try and get an enterprise license for the demo.
		// GetAndApplyLicense will be nil in the pure OSS/BSL build of cockroach.
		db, err := gosql.Open("postgres", c.connURL)
		if err != nil {
			return nil, err
		}
		go func() {
			defer db.Close()

			success, err := GetAndApplyLicense(db, c.firstServer.ClusterID(), demoOrg)
			if err != nil {
				select {
				case licenseDone <- err:

				// Avoid waiting on the license channel write if the
				// server or cluster is shutting down.
				case <-ctx.Done():
				case <-c.firstServer.Stopper().ShouldQuiesce():
				case <-c.stopper.ShouldQuiesce():
				}
				return
			}
			if !success {
				if demoCtx.geoPartitionedReplicas {
					select {
					case licenseDone <- errors.WithDetailf(
						errors.New("unable to acquire a license for this demo"),
						"Enterprise features are needed for this demo (--%s).",
						cliflags.DemoGeoPartitionedReplicas.Name):

						// Avoid waiting on the license channel write if the
						// server or cluster is shutting down.
					case <-ctx.Done():
					case <-c.firstServer.Stopper().ShouldQuiesce():
					case <-c.stopper.ShouldQuiesce():
					}
					return
				}
			}
			close(licenseDone)
		}()
	}

	return licenseDone, nil
}

// sockForServer generates the metadata for a unix socket for the given node.
// For example, node 1 gets socket /tmpdemodir/.s.PGSQL.26267,
// node 2 gets socket /tmpdemodir/.s.PGSQL.26268, etc.
func (c *transientCluster) sockForServer(nodeID roachpb.NodeID) unixSocketDetails {
	if !c.useSockets {
		return unixSocketDetails{}
	}
	port := strconv.Itoa(c.sqlFirstPort + int(nodeID) - 1)
	return unixSocketDetails{
		socketDir: c.demoDir,
		port:      port,
		u: pgurl.New().
			WithNet(pgurl.NetUnix(c.demoDir, port)).
			WithUsername(c.adminUser.Normalized()).
			WithAuthn(pgurl.AuthnPassword(true, c.adminPassword)).
			WithDatabase(c.defaultDB),
	}
}

type unixSocketDetails struct {
	socketDir string
	port      string
	u         *pgurl.URL
}

func (s unixSocketDetails) exists() bool {
	return s.socketDir != ""
}

func (s unixSocketDetails) filename() string {
	if !s.exists() {
		// No socket configured.
		return ""
	}
	return filepath.Join(s.socketDir, ".s.PGSQL."+s.port)
}

func (s unixSocketDetails) String() string {
	return s.u.ToPQ().String()
}

func (c *transientCluster) listDemoNodes(w io.Writer, justOne bool) {
	numNodesLive := 0
	for i, s := range c.servers {
		if s == nil {
			continue
		}
		numNodesLive++
		if numNodesLive > 1 && justOne {
			// Demo introduction: we just want conn parameters for one node.
			continue
		}

		nodeID := s.NodeID()
		if !justOne {
			// We skip the node ID if we're in the top level introduction of
			// the demo.
			fmt.Fprintf(w, "node %d:\n", nodeID)
		}
		serverURL := s.Cfg.AdminURL()
		if !demoCtx.insecure {
			// Print node ID and web UI URL. Embed the autologin feature inside the URL.
			// We avoid printing those when insecure, as the autologin path is not available
			// in that case.
			pwauth := url.Values{
				"username": []string{c.adminUser.Normalized()},
				"password": []string{c.adminPassword},
			}
			serverURL.Path = server.DemoLoginPath
			serverURL.RawQuery = pwauth.Encode()
		}
		fmt.Fprintln(w, "  (webui)   ", serverURL)
		// Print network URL if defined.
		netURL, err := c.getNetworkURLForServer(context.Background(), i, false /*includeAppName*/)
		if err != nil {
			fmt.Fprintln(stderr, errors.Wrap(err, "retrieving network URL"))
		} else {
			fmt.Fprintln(w, "  (sql)     ", netURL.ToPQ())
			fmt.Fprintln(w, "  (sql/jdbc)", netURL.ToJDBC())
		}
		// Print unix socket if defined.
		if c.useSockets {
			sock := c.sockForServer(nodeID)
			fmt.Fprintln(w, "  (sql/unix)", sock)
		}
		fmt.Fprintln(w)
	}
	if numNodesLive == 0 {
		fmt.Fprintln(w, "no demo nodes currently running")
	}
	if justOne && numNodesLive > 1 {
		fmt.Fprintln(w, `To display connection parameters for other nodes, use \demo ls.`)
	}
}

// genDemoPassword generates a password that prevents accidental
// misuse of the DB console started by demo shells.
// It also prevents beginner or naive programmers from scripting the
// demo shell, before they fully understand the notion of API
// stability and the lack of forward or backward compatibility in
// features of 'cockroach demo'. (What we are saying here is that
// someone needs to be "advanced enough" to script the derivation of
// the demo password, at which point we're expecting them to properly
// weigh the trade-off between working to script "demo" which may
// require non-trivial changes to their script from one version to the
// next, and starting a regular server with "start-single-node".)
func genDemoPassword(username string) string {
	mypid := os.Getpid()
	return fmt.Sprintf("%s%d", username, mypid)
}
