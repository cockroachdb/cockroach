// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package democluster

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
	"github.com/cockroachdb/cockroach/pkg/cli/clierror"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/pgurl"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"golang.org/x/time/rate"
)

type transientCluster struct {
	demoCtx *Context

	connURL       string
	demoDir       string
	useSockets    bool
	stopper       *stop.Stopper
	firstServer   *server.TestServer
	servers       []*server.TestServer
	tenantServers []serverutils.TestTenantInterface
	defaultDB     string

	httpFirstPort int
	sqlFirstPort  int

	adminPassword string
	adminUser     security.SQLUsername

	stickyEngineRegistry server.StickyInMemEnginesRegistry

	getAdminClient   func(ctx context.Context, cfg server.Config) (serverpb.AdminClient, func(), error)
	drainAndShutdown func(ctx context.Context, adminClient serverpb.AdminClient) error

	infoLog  LoggerFn
	warnLog  LoggerFn
	shoutLog ShoutLoggerFn
}

// maxNodeInitTime is the maximum amount of time to wait for nodes to
// be connected.
const maxNodeInitTime = 30 * time.Second

// demoOrg is the organization to use to request an evaluation
// license.
const demoOrg = "Cockroach Demo"

// LoggerFn is the type of a logger function to use by the
// demo cluster to report special events.
type LoggerFn = func(context.Context, string, ...interface{})

// ShoutLoggerFn is the type of a logger function to use by the demo
// cluster to report special events to both a log and the terminal.
type ShoutLoggerFn = func(context.Context, logpb.Severity, string, ...interface{})

// NewDemoCluster instantiates a demo cluster. The caller must call
// the .Close() method to clean up resources even if the
// NewDemoCluster function returns an error.
//
// The getAdminClient and drainAndShutdown function give the
// demo cluster access to the RPC client code.
// (It is provided as argument to avoid a heavyweight
// dependency on CockroachDB's idiosyncratic CLI configuration code.)
// TODO(knz): move this functionality to a sub-package with
// a clean interface and use that here.
func NewDemoCluster(
	ctx context.Context,
	demoCtx *Context,
	infoLog LoggerFn,
	warnLog LoggerFn,
	shoutLog ShoutLoggerFn,
	startStopper func(ctx context.Context) (*stop.Stopper, error),
	getAdminClient func(ctx context.Context, cfg server.Config) (serverpb.AdminClient, func(), error),
	drainAndShutdown func(ctx context.Context, s serverpb.AdminClient) error,
) (DemoCluster, error) {
	c := &transientCluster{
		demoCtx:          demoCtx,
		getAdminClient:   getAdminClient,
		drainAndShutdown: drainAndShutdown,
		infoLog:          infoLog,
		warnLog:          warnLog,
		shoutLog:         shoutLog,
	}
	// useSockets is true on unix, false on windows.
	c.useSockets = useUnixSocketsInDemo()

	// The user specified some localities for their nodes.
	if len(c.demoCtx.Localities) != 0 {
		// Error out of localities don't line up with requested node
		// count before doing any sort of setup.
		if len(c.demoCtx.Localities) != c.demoCtx.NumNodes {
			return c, errors.Errorf("number of localities specified must equal number of nodes")
		}
	} else {
		c.demoCtx.Localities = make([]roachpb.Locality, c.demoCtx.NumNodes)
		for i := 0; i < c.demoCtx.NumNodes; i++ {
			c.demoCtx.Localities[i] = defaultLocalities[i%len(defaultLocalities)]
		}
	}

	var err error
	c.stopper, err = startStopper(ctx)
	if err != nil {
		return c, err
	}
	c.maybeWarnMemSize(ctx)

	// Create a temporary directory for certificates (if secure) and
	// the unix sockets.
	// The directory is removed in the Close() method.
	if c.demoDir, err = ioutil.TempDir("", "demo"); err != nil {
		return c, err
	}

	if !c.demoCtx.Insecure {
		if err := c.demoCtx.generateCerts(c.demoDir); err != nil {
			return c, err
		}
	}

	c.httpFirstPort = c.demoCtx.HTTPPort
	c.sqlFirstPort = c.demoCtx.SQLPort
	if c.demoCtx.Multitenant {
		// This allows the first demo tenant to get the desired ports (i.e., those
		// configured by --http-port or --sql-port, or the default) without
		// conflicting with the system tenant.
		c.httpFirstPort += c.demoCtx.NumNodes
		c.sqlFirstPort += c.demoCtx.NumNodes
	}

	c.stickyEngineRegistry = server.NewStickyInMemEnginesRegistry()
	return c, nil
}

func (c *transientCluster) Start(
	ctx context.Context,
	runInitialSQL func(ctx context.Context, s *server.Server, startSingleNode bool, adminUser, adminPassword string) error,
) (err error) {
	ctx = logtags.AddTag(ctx, "start-demo-cluster", nil)

	// Initialize the connection database.
	// We can't do this earlier as this depends on which generator is used.
	c.defaultDB = catalogkeys.DefaultDatabaseName
	if c.demoCtx.WorkloadGenerator != nil {
		c.defaultDB = c.demoCtx.WorkloadGenerator.Meta().Name
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
	// 8. Start multi-tenant SQL servers if running in multi-tenant mode.
	// 9. after all nodes are initialized, initialize SQL and telemetry.

	timeoutCh := time.After(maxNodeInitTime)

	// errCh is used to catch errors when initializing servers.
	errCh := make(chan error, c.demoCtx.NumNodes)

	// rpcAddrReadyChs will be used in steps 2 and 4 below
	// to wait until the nodes know their RPC address.
	rpcAddrReadyChs := make([]chan struct{}, c.demoCtx.NumNodes)

	// latencyMapWaitChs is used to block test servers after RPC address
	// computation until the artificial latency map has been constructed.
	latencyMapWaitChs := make([]chan struct{}, c.demoCtx.NumNodes)

	// Step 1: create the first node.
	phaseCtx := logtags.AddTag(ctx, "phase", 1)
	if err := func(ctx context.Context) error {
		c.infoLog(ctx, "creating the first node")

		latencyMapWaitChs[0] = make(chan struct{})
		firstRPCAddrReadyCh, err := c.createAndAddNode(ctx, 0, latencyMapWaitChs[0], timeoutCh)
		if err != nil {
			return err
		}
		rpcAddrReadyChs[0] = firstRPCAddrReadyCh
		return nil
	}(phaseCtx); err != nil {
		return err
	}

	// Step 2: start the first node asynchronously, then wait for RPC
	// listen readiness or error.
	phaseCtx = logtags.AddTag(ctx, "phase", 2)
	if err := func(ctx context.Context) error {
		c.infoLog(ctx, "starting first node")
		if err := c.startNodeAsync(ctx, 0, errCh, timeoutCh); err != nil {
			return err
		}
		c.infoLog(ctx, "waiting for first node RPC address")
		return c.waitForRPCAddrReadinessOrError(ctx, 0, errCh, rpcAddrReadyChs, timeoutCh)
	}(phaseCtx); err != nil {
		return err
	}

	// Step 3: create the other nodes and start them asynchronously.
	phaseCtx = logtags.AddTag(ctx, "phase", 3)
	if err := func(ctx context.Context) error {
		c.infoLog(ctx, "creating other nodes")

		for i := 1; i < c.demoCtx.NumNodes; i++ {
			latencyMapWaitChs[i] = make(chan struct{})
			rpcAddrReady, err := c.createAndAddNode(ctx, i, latencyMapWaitChs[i], timeoutCh)
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
		for i := 1; i < c.demoCtx.NumNodes; i++ {
			if err := c.startNodeAsync(ctx, i, errCh, timeoutCh); err != nil {
				return err
			}
		}
		return nil
	}(phaseCtx); err != nil {
		return err
	}

	// Step 4: wait for all the nodes to know their RPC address,
	// or for an error or premature shutdown.
	phaseCtx = logtags.AddTag(ctx, "phase", 4)
	if err := func(ctx context.Context) error {
		c.infoLog(ctx, "waiting for remaining nodes to get their RPC address")

		for i := 0; i < c.demoCtx.NumNodes; i++ {
			if err := c.waitForRPCAddrReadinessOrError(ctx, i, errCh, rpcAddrReadyChs, timeoutCh); err != nil {
				return err
			}
		}
		return nil
	}(phaseCtx); err != nil {
		return err
	}

	// Step 5: optionally initialize the latency map, then let the servers
	// proceed with their initialization.
	phaseCtx = logtags.AddTag(ctx, "phase", 5)
	if err := func(ctx context.Context) error {
		// If latency simulation is requested, initialize the latency map.
		if c.demoCtx.SimulateLatency {
			// Now, all servers have been started enough to know their own RPC serving
			// addresses, but nothing else. Assemble the artificial latency map.
			c.infoLog(ctx, "initializing latency map")
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
		return nil
	}(phaseCtx); err != nil {
		return err
	}

	// Step 6: cluster initialization.
	phaseCtx = logtags.AddTag(ctx, "phase", 6)
	if err := func(ctx context.Context) error {
		for i := 0; i < c.demoCtx.NumNodes; i++ {
			c.infoLog(ctx, "letting server %d initialize", i)
			close(latencyMapWaitChs[i])
			if err := c.waitForNodeIDReadiness(ctx, i, errCh, timeoutCh); err != nil {
				return err
			}
			c.infoLog(ctx, "node n%d initialized", c.servers[i].NodeID())
		}
		return nil
	}(phaseCtx); err != nil {
		return err
	}

	// Step 7: wait for SQL to signal ready.
	phaseCtx = logtags.AddTag(ctx, "phase", 7)
	if err := func(ctx context.Context) error {
		for i := 0; i < c.demoCtx.NumNodes; i++ {
			c.infoLog(ctx, "waiting for server %d SQL readiness", i)
			if err := c.waitForSQLReadiness(ctx, i, errCh, timeoutCh); err != nil {
				return err
			}
			c.infoLog(ctx, "node n%d ready", c.servers[i].NodeID())
		}
		return nil
	}(phaseCtx); err != nil {
		return err
	}

	const demoUsername = "demo"
	demoPassword := genDemoPassword(demoUsername)

	// Step 8: initialize tenant servers, if enabled.
	phaseCtx = logtags.AddTag(ctx, "phase", 8)
	if err := func(ctx context.Context) error {
		if c.demoCtx.Multitenant {
			c.infoLog(ctx, "starting tenant nodes")

			c.tenantServers = make([]serverutils.TestTenantInterface, c.demoCtx.NumNodes)
			for i := 0; i < c.demoCtx.NumNodes; i++ {
				latencyMap := c.servers[i].Cfg.TestingKnobs.Server.(*server.TestingKnobs).ContextTestingKnobs.ArtificialLatencyMap
				c.infoLog(ctx, "starting tenant node %d", i)
				tenantStopper := stop.NewStopper()
				ts, err := c.servers[i].StartTenant(ctx, base.TestTenantArgs{
					// We set the tenant ID to i+2, since tenant 0 is not a tenant, and
					// tenant 1 is the system tenant. We also subtract 2 for the "starting"
					// SQL/HTTP ports so the first tenant ends up with the desired default
					// ports.
					TenantID:         roachpb.MakeTenantID(uint64(i + 2)),
					Stopper:          tenantStopper,
					ForceInsecure:    c.demoCtx.Insecure,
					SSLCertsDir:      c.demoDir,
					StartingSQLPort:  c.demoCtx.SQLPort - 2,
					StartingHTTPPort: c.demoCtx.HTTPPort - 2,
					Locality:         c.demoCtx.Localities[i],
					TestingKnobs: base.TestingKnobs{
						Server: &server.TestingKnobs{
							ContextTestingKnobs: rpc.ContextTestingKnobs{
								ArtificialLatencyMap: latencyMap,
							},
						},
					},
				})
				c.stopper.AddCloser(stop.CloserFn(func() {
					stopCtx := context.Background()
					if ts != nil {
						stopCtx = ts.AnnotateCtx(stopCtx)
					}
					tenantStopper.Stop(stopCtx)
				}))
				if err != nil {
					return err
				}
				c.tenantServers[i] = ts
				c.infoLog(ctx, "started tenant %d: %s", i, ts.SQLAddr())

				// Propagate the tenant server tags to the initialization
				// context, so that the initialization messages below are
				// properly annotated in traces.
				ctx = ts.AnnotateCtx(ctx)

				if !c.demoCtx.Insecure {
					// Set up the demo username and password on each tenant.
					ie := ts.DistSQLServer().(*distsql.ServerImpl).ServerConfig.Executor
					_, err = ie.Exec(ctx, "tenant-password", nil,
						fmt.Sprintf("CREATE USER %s WITH PASSWORD %s", demoUsername, demoPassword))
					if err != nil {
						return err
					}
					_, err = ie.Exec(ctx, "tenant-grant", nil, fmt.Sprintf("GRANT admin TO %s", demoUsername))
					if err != nil {
						return err
					}
				}
			}
		}
		return nil
	}(phaseCtx); err != nil {
		return err
	}

	// Step 9: run SQL initialization.
	phaseCtx = logtags.AddTag(ctx, "phase", 9)
	if err := func(ctx context.Context) error {
		// Run the SQL initialization. This takes care of setting up the
		// initial replication factor for small clusters and creating the
		// admin user.
		c.infoLog(ctx, "running initial SQL for demo cluster")
		// Propagate the server log tags to the operations below, to include node ID etc.
		server := c.firstServer.Server
		ctx = server.AnnotateCtx(ctx)

		if err := runInitialSQL(ctx, server, c.demoCtx.NumNodes < 3, demoUsername, demoPassword); err != nil {
			return err
		}
		if c.demoCtx.Insecure {
			c.adminUser = security.RootUserName()
			c.adminPassword = "unused"
		} else {
			c.adminUser = security.MakeSQLUsernameFromPreNormalizedString(demoUsername)
			c.adminPassword = demoPassword
		}

		// Prepare the URL for use by the SQL shell.
		purl, err := c.getNetworkURLForServer(ctx, 0, true /* includeAppName */, c.demoCtx.Multitenant)
		if err != nil {
			return err
		}
		c.connURL = purl.ToPQ().String()

		// Write the URL to a file if this was requested by configuration.
		if c.demoCtx.ListeningURLFile != "" {
			c.infoLog(ctx, "listening URL file: %s", c.demoCtx.ListeningURLFile)
			if err = ioutil.WriteFile(c.demoCtx.ListeningURLFile, []byte(fmt.Sprintf("%s\n", c.connURL)), 0644); err != nil {
				c.warnLog(ctx, "failed writing the URL: %v", err)
			}
		}

		// Start up the update check loop.
		// We don't do this in (*server.Server).Start() because we don't want this
		// overhead and possible interference in tests.
		if !c.demoCtx.DisableTelemetry {
			c.infoLog(ctx, "starting telemetry")
			c.firstServer.StartDiagnostics(ctx)
		}

		return nil
	}(phaseCtx); err != nil {
		return err
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
	args := c.demoCtx.testServerArgsForTransientCluster(
		c.sockForServer(nodeID, "" /* databaseNameOverride */), nodeID, joinAddr, c.demoDir,
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

	if c.demoCtx.SimulateLatency {
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
		logcrash.SetGlobalSettings(&serv.ClusterSettings().SV)
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
		// We call Start() with context.Background() because we don't want the
		// tracing span corresponding to the task started just above to leak into
		// the new server. Server's have their own Tracers, different from the one
		// used by this transientCluster, and so traces inside the Server can't be
		// combined with traces from outside it.
		ctx = logtags.WithTags(context.Background(), logtags.FromContext(ctx))
		ctx = logtags.AddTag(ctx, tag, nil)

		err := serv.Start(ctx)
		if err != nil {
			c.warnLog(ctx, "server %d failed to start: %v", idx, err)
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
	if idx >= len(rpcAddrReadyChs) || idx >= len(c.servers) {
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
		c.infoLog(ctx, "waiting for server %d to know its node ID", idx)
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
				c.infoLog(ctx, "server %d does not know its node ID yet", idx)
				continue
			} else {
				c.infoLog(ctx, "server %d: n%d", idx, c.servers[idx].NodeID())
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
		c.infoLog(ctx, "waiting for server %d to become ready", idx)
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
				c.infoLog(ctx, "server %d not yet ready: %v", idx, err)
				continue
			}
		}
		break
	}
	return nil
}

// testServerArgsForTransientCluster creates the test arguments for
// a necessary server in the demo cluster.
func (demoCtx *Context) testServerArgsForTransientCluster(
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
		SQLMemoryPoolSize:       demoCtx.SQLPoolMemorySize,
		CacheSize:               demoCtx.CacheSize,
		NoAutoInitializeCluster: true,
		EnableDemoLoginEndpoint: true,
		// This disables the tenant server. We could enable it but would have to
		// generate the suitable certs at the caller who wishes to do so.
		TenantAddr: new(string),
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				StickyEngineRegistry: stickyEngineRegistry,
			},
			JobsTestingKnobs: &jobs.TestingKnobs{
				// Allow the scheduler daemon to start earlier in demo.
				SchedulerDaemonInitialScanDelay: func() time.Duration {
					return time.Second * 15
				},
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

	if demoCtx.Localities != nil {
		args.Locality = demoCtx.Localities[int(nodeID-1)]
	}
	if demoCtx.Insecure {
		args.Insecure = true
	} else {
		args.Insecure = false
		args.SSLCertsDir = demoDir
	}

	return args
}

var testingForceRandomizeDemoPorts bool

// TestingForceRandomizeDemoPorts disables the fixed port allocation
// for demo clusters, for use in tests.
func TestingForceRandomizeDemoPorts() func() {
	testingForceRandomizeDemoPorts = true
	return func() { testingForceRandomizeDemoPorts = false }
}

func (c *transientCluster) Close(ctx context.Context) {
	if c.stopper != nil {
		c.stopper.Stop(ctx)
	}
	if c.demoDir != "" {
		if err := clierror.CheckAndMaybeLog(os.RemoveAll(c.demoDir), c.shoutLog); err != nil {
			// There's nothing to do here anymore if err != nil.
			_ = err
		}
	}
}

// DrainAndShutdown will gracefully attempt to drain a node in the cluster, and
// then shut it down.
func (c *transientCluster) DrainAndShutdown(ctx context.Context, nodeID int32) error {
	if c.demoCtx.SimulateLatency {
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

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	adminClient, finish, err := c.getAdminClient(ctx, *(c.servers[nodeIndex].Cfg))
	if err != nil {
		return err
	}
	defer finish()

	if err := c.drainAndShutdown(ctx, adminClient); err != nil {
		return err
	}
	c.servers[nodeIndex] = nil
	return nil
}

// Recommission recommissions a given node.
func (c *transientCluster) Recommission(ctx context.Context, nodeID int32) error {
	nodeIndex := int(nodeID - 1)

	if nodeIndex < 0 || nodeIndex >= len(c.servers) {
		return errors.Errorf("node %d does not exist", nodeID)
	}

	req := &serverpb.DecommissionRequest{
		NodeIDs:          []roachpb.NodeID{roachpb.NodeID(nodeID)},
		TargetMembership: livenesspb.MembershipStatus_ACTIVE,
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	adminClient, finish, err := c.getAdminClient(ctx, *(c.firstServer.Cfg))
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
func (c *transientCluster) Decommission(ctx context.Context, nodeID int32) error {
	nodeIndex := int(nodeID - 1)

	if nodeIndex < 0 || nodeIndex >= len(c.servers) {
		return errors.Errorf("node %d does not exist", nodeID)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	adminClient, finish, err := c.getAdminClient(ctx, *(c.firstServer.Cfg))
	if err != nil {
		return err
	}
	defer finish()

	// This (cumbersome) two step process is due to the allowed state
	// transitions for membership status. To mark a node as fully
	// decommissioned, it has to be marked as decommissioning first.
	{
		req := &serverpb.DecommissionRequest{
			NodeIDs:          []roachpb.NodeID{roachpb.NodeID(nodeID)},
			TargetMembership: livenesspb.MembershipStatus_DECOMMISSIONING,
		}
		_, err = adminClient.Decommission(ctx, req)
		if err != nil {
			return errors.Wrap(err, "while trying to mark as decommissioning")
		}
	}

	{
		req := &serverpb.DecommissionRequest{
			NodeIDs:          []roachpb.NodeID{roachpb.NodeID(nodeID)},
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
func (c *transientCluster) RestartNode(ctx context.Context, nodeID int32) error {
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
	if c.demoCtx.SimulateLatency {
		return errors.Errorf("restarting nodes is not supported in --%s configurations", cliflags.Global.Name)
	}
	args := c.demoCtx.testServerArgsForTransientCluster(
		c.sockForServer(roachpb.NodeID(nodeID), "" /* databaseNameOverride */),
		roachpb.NodeID(nodeID),
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

	if err := serv.Start(ctx); err != nil {
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
func (c *transientCluster) AddNode(
	ctx context.Context, localityString string,
) (newNodeID int32, err error) {
	// TODO(#42243): re-compute the latency mapping for this to work.
	if c.demoCtx.SimulateLatency {
		return 0, errors.Errorf("adding nodes is not supported in --%s configurations", cliflags.Global.Name)
	}

	// '\demo add' accepts both strings that are quoted and not quoted. To properly make use of
	// quoted strings, strip off the quotes.  Before we do that though, make sure that the quotes match,
	// or that there aren't any quotes in the string.
	re := regexp.MustCompile(`".+"|'.+'|[^'"]+`)
	if localityString != re.FindString(localityString) {
		return 0, errors.Errorf(`Invalid locality (missing " or '): %s`, localityString)
	}
	trimmedString := strings.Trim(localityString, `"'`)

	// Setup locality based on supplied string.
	var loc roachpb.Locality
	if err := loc.Set(trimmedString); err != nil {
		return 0, err
	}

	// Ensure that the cluster is sane before we start messing around with it.
	if len(c.demoCtx.Localities) != c.demoCtx.NumNodes || c.demoCtx.NumNodes != len(c.servers) {
		return 0, errors.Errorf("number of localities specified (%d) must equal number of "+
			"nodes (%d) and number of servers (%d)", len(c.demoCtx.Localities), c.demoCtx.NumNodes, len(c.servers))
	}

	// Create a new empty server element and add associated locality info.
	// When we call RestartNode below, this element will be properly initialized.
	c.servers = append(c.servers, nil)
	c.demoCtx.Localities = append(c.demoCtx.Localities, loc)
	c.demoCtx.NumNodes++
	// TODO(knz): this should start the server and then retrieve its node ID
	// instead of assuming the node ID will always be at the end.
	nodeID := int32(c.demoCtx.NumNodes)

	return nodeID, c.RestartNode(ctx, nodeID)
}

func (c *transientCluster) maybeWarnMemSize(ctx context.Context) {
	if maxMemory, err := status.GetTotalMemory(ctx); err == nil {
		requestedMem := (c.demoCtx.CacheSize + c.demoCtx.SQLPoolMemorySize) * int64(c.demoCtx.NumNodes)
		maxRecommendedMem := int64(.75 * float64(maxMemory))
		if requestedMem > maxRecommendedMem {
			c.shoutLog(
				ctx, severity.WARNING,
				`HIGH MEMORY USAGE
The sum of --max-sql-memory (%s) and --cache (%s) multiplied by the
number of nodes (%d) results in potentially high memory usage on your
device.
This server is running at increased risk of memory-related failures.`,
				humanizeutil.IBytes(c.demoCtx.SQLPoolMemorySize),
				humanizeutil.IBytes(c.demoCtx.CacheSize),
				c.demoCtx.NumNodes,
			)
		}
	}
}

// generateCerts generates some temporary certificates for cockroach demo.
func (demoCtx *Context) generateCerts(certsDir string) (err error) {
	caKeyPath := filepath.Join(certsDir, security.EmbeddedCAKey)
	// Create a CA-Key.
	if err := security.CreateCAPair(
		certsDir,
		caKeyPath,
		demoCtx.DefaultKeySize,
		demoCtx.DefaultCALifetime,
		false, /* allowKeyReuse */
		false, /*overwrite */
	); err != nil {
		return err
	}
	// Generate a certificate for the demo nodes.
	if err := security.CreateNodePair(
		certsDir,
		caKeyPath,
		demoCtx.DefaultKeySize,
		demoCtx.DefaultCertLifetime,
		false, /* overwrite */
		[]string{"127.0.0.1"},
	); err != nil {
		return err
	}
	// Create a certificate for the root user.
	if err := security.CreateClientPair(
		certsDir,
		caKeyPath,
		demoCtx.DefaultKeySize,
		demoCtx.DefaultCertLifetime,
		false, /* overwrite */
		security.RootUserName(),
		false, /* generatePKCS8Key */
	); err != nil {
		return err
	}

	if demoCtx.Multitenant {
		tenantCAKeyPath := filepath.Join(certsDir, security.EmbeddedTenantCAKey)
		// Create a CA key for the tenants.
		if err := security.CreateTenantCAPair(
			certsDir,
			tenantCAKeyPath,
			demoCtx.DefaultKeySize,
			// We choose a lifetime that is over the default cert lifetime because,
			// without doing this, the tenant connection complains that the certs
			// will expire too soon.
			demoCtx.DefaultCertLifetime+time.Hour,
			false, /* allowKeyReuse */
			false, /* ovewrite */
		); err != nil {
			return err
		}

		for i := 0; i < demoCtx.NumNodes; i++ {
			// Create a cert for each tenant.
			hostAddrs := []string{
				"127.0.0.1",
				"::1",
				"localhost",
				"*.local",
			}
			pair, err := security.CreateTenantPair(
				certsDir,
				tenantCAKeyPath,
				demoCtx.DefaultKeySize,
				demoCtx.DefaultCertLifetime,
				uint64(i+2),
				hostAddrs,
			)
			if err != nil {
				return err
			}
			if err := security.WriteTenantPair(certsDir, pair, false /* overwrite */); err != nil {
				return err
			}
			if err := security.CreateTenantSigningPair(
				certsDir, demoCtx.DefaultCertLifetime, false /* overwrite */, uint64(i+2),
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *transientCluster) getNetworkURLForServer(
	ctx context.Context, serverIdx int, includeAppName bool, isTenant bool,
) (*pgurl.URL, error) {
	u := pgurl.New()
	if includeAppName {
		if err := u.SetOption("application_name", catconstants.ReportableAppNamePrefix+"cockroach demo"); err != nil {
			return nil, err
		}
	}
	sqlAddr := c.servers[serverIdx].ServingSQLAddr()
	database := c.defaultDB
	if isTenant {
		sqlAddr = c.tenantServers[serverIdx].SQLAddr()
	}
	if !isTenant && c.demoCtx.Multitenant {
		database = catalogkeys.DefaultDatabaseName
	}
	host, port, _ := addr.SplitHostPort(sqlAddr, "")
	u.
		WithNet(pgurl.NetTCP(host, port)).
		WithDatabase(database).
		WithUsername(c.adminUser.Normalized())

	// For a demo cluster we don't use client TLS certs and instead use
	// password-based authentication with the password pre-filled in the
	// URL.
	if c.demoCtx.Insecure {
		u.WithInsecure()
	} else {
		u.
			WithAuthn(pgurl.AuthnPassword(true, c.adminPassword)).
			WithTransport(pgurl.TransportTLS(pgurl.TLSRequire, ""))
	}
	return u, nil
}

func (c *transientCluster) GetConnURL() string {
	return c.connURL
}

func (c *transientCluster) GetSQLCredentials() (
	adminUser security.SQLUsername,
	adminPassword, certsDir string,
) {
	return c.adminUser, c.adminPassword, c.demoDir
}

func (c *transientCluster) SetupWorkload(ctx context.Context, licenseDone <-chan error) error {
	gen := c.demoCtx.WorkloadGenerator
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
		if c.demoCtx.IsInteractive() {
			fmt.Printf("#\n# Beginning initialization of the %s dataset, please wait...\n", gen.Meta().Name)
		}
		if _, err := workloadsql.Setup(ctx, db, gen, l); err != nil {
			return err
		}
		// Perform partitioning if requested by configuration.
		if c.demoCtx.GeoPartitionedReplicas {
			// Wait until the license has been acquired to trigger partitioning.
			if c.demoCtx.IsInteractive() {
				fmt.Println("#\n# Waiting for license acquisition to complete...")
			}
			if err := <-licenseDone; err != nil {
				return err
			}
			if c.demoCtx.IsInteractive() {
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
		if c.demoCtx.RunWorkload {
			var sqlURLs []string
			for i := range c.servers {
				sqlURL, err := c.getNetworkURLForServer(ctx, i, true /* includeAppName */, c.demoCtx.Multitenant)
				if err != nil {
					return err
				}
				sqlURLs = append(sqlURLs, sqlURL.ToPQ().String())
			}
			if err := c.runWorkload(ctx, c.demoCtx.WorkloadGenerator, sqlURLs); err != nil {
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

	// Use a rate limit (default 25 queries per second).
	limiter := rate.NewLimiter(rate.Limit(c.demoCtx.WorkloadMaxQPS), 1)

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
						c.warnLog(ctx, "error running workload query: %+v", err)
					}
					select {
					case <-ctx.Done():
						c.warnLog(ctx, "workload terminating from context cancellation: %v", ctx.Err())
						return
					case <-c.stopper.ShouldQuiesce():
						c.warnLog(ctx, "demo cluster shutting down")
						return
					default:
					}
				}
			}
		}
		// By running under the cluster's stopper, we ensure that, on shutdown, the
		// workload is stopped before any servers or tenants are stopped.
		if err := c.stopper.RunAsyncTask(ctx, "workload", workloadFun(workerFn)); err != nil {
			return err
		}
	}

	return nil
}

// acquireDemoLicense begins an asynchronous process to obtain a
// temporary demo license from the Cockroach Labs website. It returns
// a channel that can be waited on if it is needed to wait on the
// license acquisition.
func (c *transientCluster) AcquireDemoLicense(ctx context.Context) (chan error, error) {
	// Communicate information about license acquisition to services
	// that depend on it.
	licenseDone := make(chan error)
	if c.demoCtx.DisableLicenseAcquisition {
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
				if c.demoCtx.GeoPartitionedReplicas {
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
func (c *transientCluster) sockForServer(
	nodeID roachpb.NodeID, databaseNameOverride string,
) unixSocketDetails {
	if !c.useSockets {
		return unixSocketDetails{}
	}
	port := strconv.Itoa(c.sqlFirstPort + int(nodeID) - 1)
	databaseName := c.defaultDB
	if databaseNameOverride != "" {
		databaseName = databaseNameOverride
	}
	return unixSocketDetails{
		socketDir: c.demoDir,
		port:      port,
		u: pgurl.New().
			WithNet(pgurl.NetUnix(c.demoDir, port)).
			WithUsername(c.adminUser.Normalized()).
			WithAuthn(pgurl.AuthnPassword(true, c.adminPassword)).
			WithDatabase(databaseName),
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

func (c *transientCluster) NumNodes() int {
	return len(c.servers)
}

func (c *transientCluster) GetLocality(nodeID int32) string {
	return c.demoCtx.Localities[nodeID-1].String()
}

func (c *transientCluster) ListDemoNodes(w, ew io.Writer, justOne bool) {
	numNodesLive := 0
	// First, list system tenant nodes.
	if c.demoCtx.Multitenant {
		fmt.Fprintln(w, "system tenant")
	}
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
		if !c.demoCtx.Insecure {
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
		netURL, err := c.getNetworkURLForServer(context.Background(), i,
			false /* includeAppName */, false /* isTenant */)
		if err != nil {
			fmt.Fprintln(ew, errors.Wrap(err, "retrieving network URL"))
		} else {
			fmt.Fprintln(w, "  (sql)     ", netURL.ToPQ())
			fmt.Fprintln(w, "  (sql/jdbc)", netURL.ToJDBC())
		}
		// Print unix socket if defined.
		if c.useSockets {
			databaseNameOverride := ""
			if c.demoCtx.Multitenant {
				databaseNameOverride = catalogkeys.DefaultDatabaseName
			}
			sock := c.sockForServer(nodeID, databaseNameOverride)
			fmt.Fprintln(w, "  (sql/unix)", sock)
		}
		fmt.Fprintln(w)
	}
	// Print the SQL address of each tenant if in MT mode.
	if c.demoCtx.Multitenant {
		for i := range c.servers {
			fmt.Fprintf(w, "tenant %d:\n", i+1)
			tenantURL, err := c.getNetworkURLForServer(context.Background(), i,
				false /* includeAppName */, true /* isTenant */)
			if err != nil {
				fmt.Fprintln(ew, errors.Wrap(err, "retrieving tenant network URL"))
			} else {
				fmt.Fprintln(w, "   (sql): ", tenantURL.ToPQ())
			}
			fmt.Fprintln(w)
		}
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
