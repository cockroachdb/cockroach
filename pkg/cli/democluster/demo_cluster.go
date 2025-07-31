// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package democluster

import (
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/clierror"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/certnames"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/pgurl"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlclustersettings"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils/regionlatency"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/logtags"
	"github.com/nightlyone/lockfile"
	"golang.org/x/time/rate"
)

type serverEntry struct {
	serverutils.TestServerInterface
	adminClient    serverpb.AdminClient
	nodeID         roachpb.NodeID
	decommissioned bool
}

type transientCluster struct {
	demoCtx *Context

	connURL       string
	demoDir       string
	useSockets    bool
	stopper       *stop.Stopper
	firstServer   serverutils.TestServerInterface
	servers       []serverEntry
	tenantServers []serverutils.ApplicationLayerInterface
	defaultDB     string

	adminPassword string
	adminUser     username.SQLUsername

	stickyVFSRegistry fs.StickyRegistry

	drainAndShutdown func(ctx context.Context, adminClient serverpb.AdminClient) error

	infoLog  LoggerFn
	warnLog  LoggerFn
	shoutLog ShoutLoggerFn

	// latencyEnabled controls whether simulated latency is currently enabled.
	// It is only relevant when using SimulateLatency.
	latencyEnabled atomic.Bool
}

// maxNodeInitTime is the maximum amount of time to wait for nodes to
// be connected.
const maxNodeInitTime = 60 * time.Second

// secondaryTenantID is the ID of the secondary tenant to use when
// --multitenant=true.
const secondaryTenantID = 3

// demoOrg is the organization to use to request an evaluation
// license.
const demoOrg = "Cockroach Demo"

// demoUsername is the name of the predefined non-root user.
const demoUsername = "demo"

// demoTenantName is the name of the demo tenant.
const demoTenantName = "demoapp"

// LoggerFn is the type of a logger function to use by the
// demo cluster to report special events.
type LoggerFn = func(context.Context, string, ...interface{})

// ShoutLoggerFn is the type of a logger function to use by the demo
// cluster to report special events to both a log and the terminal.
type ShoutLoggerFn = func(context.Context, logpb.Severity, string, ...interface{})

type serverSelection bool

const forSystemTenant serverSelection = false
const forSecondaryTenant serverSelection = true

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
	drainAndShutdown func(ctx context.Context, s serverpb.AdminClient) error,
) (DemoCluster, error) {
	c := &transientCluster{
		demoCtx:          demoCtx,
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
	if !testingForceRandomizeDemoPorts {
		// Common case.
		// The directory is NOT removed in the Close() method: this way,
		// we can preserve configuration across `demo` calls.
		homeDir, err := envutil.HomeDir()
		if err != nil {
			return c, err
		}
		c.demoDir = filepath.Join(homeDir, ".cockroach-demo")
	} else {
		// We are running multiple tests concurrently, and we don't want
		// them to conflict on access to the shared directory. Randomize
		// the directory in that case.
		if c.demoDir, err = os.MkdirTemp("", "demo"); err != nil {
			return c, err
		}
	}
	if err := os.Mkdir(c.demoDir, 0700); err != nil && !oserror.IsExist(err) {
		return c, err
	}

	if !c.demoCtx.Insecure {
		if err := c.generateCerts(ctx, c.demoDir); err != nil {
			return c, err
		}
	}

	c.stickyVFSRegistry = fs.NewStickyRegistry()
	return c, nil
}

func (c *transientCluster) Start(ctx context.Context) (err error) {
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
		if !c.demoCtx.SimulateLatency {
			return nil
		}
		// Now, all servers have been started enough to know their own RPC serving
		// addresses, but nothing else. Assemble the artificial latency map.
		c.infoLog(ctx, "initializing latency map")
		return localityLatencies.Apply(c)
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

	demoPassword := genDemoPassword(demoUsername)

	// Step 8: initialize tenant servers, if enabled.
	phaseCtx = logtags.AddTag(ctx, "phase", 8)
	if err := func(ctx context.Context) error {
		if c.demoCtx.Multitenant {
			c.infoLog(ctx, "starting tenant servers")

			c.tenantServers = make([]serverutils.ApplicationLayerInterface, c.demoCtx.NumNodes)
			for i := 0; i < c.demoCtx.NumNodes; i++ {
				createTenant := i == 0
				if err := c.startTenantService(ctx, i, createTenant); err != nil {
					return err
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
		srv := c.firstServer
		ctx = srv.AnnotateCtx(ctx)

		if err := srv.RunInitialSQL(ctx, c.demoCtx.NumNodes < 3, demoUsername, demoPassword); err != nil {
			return err
		}
		if c.demoCtx.Insecure {
			c.adminUser = username.RootUserName()
			c.adminPassword = "unused"
		} else {
			c.adminUser = username.MakeSQLUsernameFromPreNormalizedString(demoUsername)
			c.adminPassword = demoPassword
		}

		if c.demoCtx.Multitenant {
			if !c.demoCtx.Insecure {
				// Also create the user/password for the secondary tenant.
				ts := c.tenantServers[0]
				tctx := ts.AnnotateCtx(ctx)
				ieTenant := ts.InternalExecutor().(isql.Executor)
				_, err = ieTenant.Exec(tctx, "tenant-password", nil,
					fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s'", demoUsername, demoPassword))
				if err != nil {
					return err
				}
				_, err = ieTenant.Exec(tctx, "tenant-grant", nil, fmt.Sprintf("GRANT admin TO %s", demoUsername))
				if err != nil {
					return err
				}
			}

			ie := c.firstServer.InternalExecutor().(isql.Executor)

			// Grant full capabilities.
			_, err = ie.Exec(ctx, "tenant-grant-capabilities", nil, fmt.Sprintf("ALTER VIRTUAL CLUSTER %s GRANT ALL CAPABILITIES", demoTenantName))
			if err != nil {
				return err
			}

			if !c.demoCtx.DisableServerController {
				// Select the default tenant.
				// Choose the tenant to use when no tenant is specified on a
				// connection or web URL.
				if _, err := ie.Exec(ctx, "default-tenant", nil,
					`SET CLUSTER SETTING `+multitenant.DefaultClusterSelectSettingName+` = $1`,
					demoTenantName); err != nil {
					return err
				}
			}

			for _, s := range []string{
				string(sqlclustersettings.RestrictAccessToSystemInterface.Name()),
				string(sql.TipUserAboutSystemInterface.Name()),
			} {
				if _, err := ie.Exec(ctx, "restrict-system-interface", nil, fmt.Sprintf(`SET CLUSTER SETTING %s = true`, s)); err != nil {
					return err
				}
			}
		}

		// Prepare the URL for use by the SQL shell.
		targetServer := forSystemTenant
		if c.demoCtx.Multitenant {
			targetServer = forSecondaryTenant
		}
		purl, err := c.getNetworkURLForServer(ctx, 0, true /* includeAppName */, targetServer)
		if err != nil {
			return err
		}
		c.connURL = purl.ToPQ().String()

		// Write the URL to a file if this was requested by configuration.
		if c.demoCtx.ListeningURLFile != "" {
			c.infoLog(ctx, "listening URL file: %s", c.demoCtx.ListeningURLFile)
			if err = os.WriteFile(c.demoCtx.ListeningURLFile, []byte(fmt.Sprintf("%s\n", c.connURL)), 0644); err != nil {
				c.warnLog(ctx, "failed writing the URL: %v", err)
			}
		}

		return nil
	}(phaseCtx); err != nil {
		return err
	}

	// Step 10: restore web sessions.
	phaseCtx = logtags.AddTag(ctx, "phase", 10)
	if err := func(ctx context.Context) error {
		if err := c.restoreWebSessions(ctx); err != nil {
			c.warnLog(ctx, "unable to restore web sessions: %v", err)
		}
		return nil
	}(phaseCtx); err != nil {
		return err
	}

	return nil
}

// startTenantServer starts the server for the demo secondary tenant.
func (c *transientCluster) startTenantService(
	ctx context.Context, serverIdx int, createTenant bool,
) (resErr error) {
	var tenantStopper *stop.Stopper
	defer func() {
		if resErr != nil && tenantStopper != nil {
			tenantStopper.Stop(context.Background())
		}
	}()

	var latencyMap rpc.InjectedLatencyOracle
	if knobs := c.servers[serverIdx].TestingKnobs().Server; knobs != nil {
		latencyMap = knobs.(*server.TestingKnobs).ContextTestingKnobs.InjectedLatencyOracle
	}
	c.infoLog(ctx, "starting tenant node %d", serverIdx)

	var ts serverutils.ApplicationLayerInterface
	if c.demoCtx.DisableServerController {
		tenantStopper = stop.NewStopper()
		args := base.TestTenantArgs{
			DisableCreateTenant:     !createTenant,
			TenantName:              demoTenantName,
			TenantID:                roachpb.MustMakeTenantID(secondaryTenantID),
			Stopper:                 tenantStopper,
			ForceInsecure:           c.demoCtx.Insecure,
			SSLCertsDir:             c.demoDir,
			DisableTLSForHTTP:       true,
			EnableDemoLoginEndpoint: true,
			StartingRPCAndSQLPort:   c.demoCtx.sqlPort(serverIdx, true) - secondaryTenantID,
			StartingHTTPPort:        c.demoCtx.httpPort(serverIdx, true) - secondaryTenantID,
			Locality:                c.demoCtx.Localities[serverIdx],
			TestingKnobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					ContextTestingKnobs: rpc.ContextTestingKnobs{
						InjectedLatencyOracle:  latencyMap,
						InjectedLatencyEnabled: c.latencyEnabled.Load,
					},
				},
			},
		}

		var err error
		ts, err = c.servers[serverIdx].TenantController().StartTenant(ctx, args)
		if err != nil {
			return err
		}
		c.stopper.AddCloser(stop.CloserFn(func() {
			stopCtx := context.Background()
			if ts != nil {
				stopCtx = ts.AnnotateCtx(stopCtx)
			}
			tenantStopper.Stop(stopCtx)
		}))
	} else {
		var err error
		ts, _, err = c.servers[serverIdx].TenantController().StartSharedProcessTenant(ctx,
			base.TestSharedProcessTenantArgs{
				TenantID:   roachpb.MustMakeTenantID(secondaryTenantID),
				TenantName: demoTenantName,
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						ContextTestingKnobs: rpc.ContextTestingKnobs{
							InjectedLatencyOracle:  latencyMap,
							InjectedLatencyEnabled: c.latencyEnabled.Load,
						},
					},
				},
			})
		if err != nil {
			return err
		}
	}

	c.tenantServers[serverIdx] = ts
	c.infoLog(ctx, "started tenant server %d: %s", serverIdx, ts.SQLAddr())
	return nil
}

// SetSimulatedLatency enables or disable the simulated latency and then
// clears the remote clock tracking. If the remote clocks were not cleared,
// bad routing decisions would be made as soon as latency is turned on.
func (c *transientCluster) SetSimulatedLatency(on bool) {
	c.latencyEnabled.Store(on)
	for _, s := range c.servers {
		s.RPCContext().RemoteClocks.TestingResetLatencyInfos()
	}
	for _, s := range c.tenantServers {
		s.RPCContext().RemoteClocks.TestingResetLatencyInfos()
	}
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
		joinAddr = c.firstServer.AdvRPCAddr()
	}
	socketDetails, err := c.sockForServer(idx, forSystemTenant)
	if err != nil {
		return nil, err
	}
	args := c.demoCtx.testServerArgsForTransientCluster(
		socketDetails, idx, joinAddr, c.demoDir,
		c.stickyVFSRegistry,
	)
	if idx == 0 {
		// The first node also auto-inits the cluster.
		args.NoAutoInitializeCluster = false
		// The first node also runs diagnostics (unless disabled by cluster.TelemetryOptOut).
		args.StartDiagnosticsReporting = true
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
			InjectedLatencyOracle:  regionlatency.MakeAddrMap(),
			InjectedLatencyEnabled: c.latencyEnabled.Load,
		}
	}

	// Create the server instance. This also registers the in-memory store
	// into the sticky engine registry.
	srv, err := server.TestServerFactory.New(args)
	if err != nil {
		return nil, err
	}
	s := &wrap{srv.(serverutils.TestServerInterfaceRaw)}

	// Ensure that this server gets stopped when the top level demo
	// stopper instructs the cluster to stop.
	c.stopper.AddCloser(stop.CloserFn(func() { s.Stop(context.Background()) }))

	if idx == 0 {
		// Remember the first server for later use by other APIs on
		// transientCluster.
		c.firstServer = s
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
		logcrash.SetGlobalSettings(&s.ClusterSettings().SV)
	}

	// Remember this server for the stop/restart primitives in the SQL
	// shell.
	c.servers = append(c.servers, serverEntry{TestServerInterface: s, nodeID: s.NodeID()})

	return rpcAddrReadyCh, nil
}

// startNodeAsync starts the node initialization asynchronously.
func (c *transientCluster) startNodeAsync(
	ctx context.Context, idx int, errCh chan error, timeoutCh <-chan time.Time,
) error {
	if idx > len(c.servers) {
		return errors.AssertionFailedf("programming error: server %d not created yet", idx)
	}

	s := c.servers[idx]
	tag := fmt.Sprintf("start-n%d", idx+1)
	return c.stopper.RunAsyncTask(ctx, tag, func(ctx context.Context) {
		// We call Start() with context.Background() because we don't want the
		// tracing span corresponding to the task started just above to leak into
		// the new server. Server's have their own Tracers, different from the one
		// used by this transientCluster, and so traces inside the Server can't be
		// combined with traces from outside it.
		ctx = logtags.WithTags(context.Background(), logtags.FromContext(ctx))
		ctx = logtags.AddTag(ctx, tag, nil)

		err := s.Start(ctx)
		if err != nil {
			c.warnLog(ctx, "server %d failed to start: %v", idx, err)
			select {
			case errCh <- err:

				// Don't block if we are shutting down.
			case <-ctx.Done():
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
		select {
		case err := <-errCh:
			return err
		case <-time.After(30 * time.Second):
		}
		return errors.Newf("server %d stopped prematurely without returning error", idx)
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
			nodeID := c.servers[idx].NodeID()
			if nodeID == 0 {
				c.infoLog(ctx, "server %d does not know its node ID yet", idx)
				continue
			} else {
				c.infoLog(ctx, "server %d: n%d", idx, nodeID)
				c.servers[idx].nodeID = nodeID
			}
			// We can open a RPC admin connection now.
			conn, err := c.servers[idx].RPCClientConnE(username.RootUserName())
			if err != nil {
				return err
			}
			c.servers[idx].adminClient = serverpb.NewAdminClient(conn)

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
			return errors.Newf("server %d shut down prematurely", idx)
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

func (demoCtx *Context) sqlPort(serverIdx int, target serverSelection) int {
	if demoCtx.SQLPort == 0 || testingForceRandomizeDemoPorts {
		return 0
	}
	if !demoCtx.Multitenant {
		// No multitenancy: just one port per node.
		return demoCtx.SQLPort + serverIdx
	}
	if target == forSecondaryTenant {
		// The port number of the secondary tenant is always
		// the "base" port number.
		return demoCtx.SQLPort + serverIdx
	}
	// System tenant.
	if !demoCtx.DisableServerController {
		// Using server controller: same port for app and system tenant.
		return demoCtx.SQLPort + serverIdx
	}
	// Currently using a separate SQL listener. System tenant uses port number
	// offset by NumNodes.
	return demoCtx.SQLPort + serverIdx + demoCtx.NumNodes
}

func (demoCtx *Context) httpPort(serverIdx int, target serverSelection) int {
	if demoCtx.HTTPPort == 0 || testingForceRandomizeDemoPorts {
		return 0
	}
	if !demoCtx.Multitenant {
		// No multitenancy: just one port per node.
		return demoCtx.HTTPPort + serverIdx
	}
	if target == forSecondaryTenant {
		// The port number of the secondary tenant is always
		// the "base" port number.
		return demoCtx.HTTPPort + serverIdx
	}
	// System tenant.
	if !demoCtx.DisableServerController {
		// Using server controller: same port for app and system tenant.
		return demoCtx.HTTPPort + serverIdx
	}
	// Not using server controller: http port is offset by number of nodes.
	return demoCtx.HTTPPort + serverIdx + demoCtx.NumNodes
}

func (demoCtx *Context) rpcPort(serverIdx int, target serverSelection) int {
	if demoCtx.SQLPort == 0 || testingForceRandomizeDemoPorts {
		return 0
	}
	// +100 is the offset we've chosen to recommend to separate the SQL
	// port from the RPC port when we deprecated the use of merged
	// ports.
	if !demoCtx.Multitenant {
		return demoCtx.SQLPort + serverIdx + 100
	}
	if target == forSecondaryTenant {
		return demoCtx.SQLPort + serverIdx + 100
	}
	// System tenant.
	return demoCtx.SQLPort + serverIdx + demoCtx.NumNodes + 100
}

// testServerArgsForTransientCluster creates the test arguments for
// a necessary server in the demo cluster.
func (demoCtx *Context) testServerArgsForTransientCluster(
	sock unixSocketDetails,
	serverIdx int,
	joinAddr string,
	demoDir string,
	stickyVFSRegistry fs.StickyRegistry,
) base.TestServerArgs {
	// Assign a path to the store spec, to be saved.
	storeSpec := base.DefaultTestStoreSpec
	storeSpec.StickyVFSID = fmt.Sprintf("demo-server%d", serverIdx)

	args := base.TestServerArgs{
		SocketFile:              sock.filename(),
		PartOfCluster:           true,
		Stopper:                 stop.NewStopper(),
		JoinAddr:                joinAddr,
		DisableTLSForHTTP:       true,
		StoreSpecs:              []base.StoreSpec{storeSpec},
		ExternalIODir:           filepath.Join(demoDir, "nodelocal", fmt.Sprintf("n%d", serverIdx+1)),
		SQLMemoryPoolSize:       demoCtx.SQLPoolMemorySize,
		CacheSize:               demoCtx.CacheSize,
		NoAutoInitializeCluster: true,
		EnableDemoLoginEndpoint: true,
		// Demo clusters by default will create their own tenants, so we
		// don't need to create them here.
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		DefaultTenantName: roachpb.TenantName(demoTenantName),

		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				StickyVFSRegistry: stickyVFSRegistry,
			},
			JobsTestingKnobs: &jobs.TestingKnobs{
				// Allow the scheduler daemon to start earlier in demo.
				SchedulerDaemonInitialScanDelay: func() time.Duration {
					return time.Second * 15
				},
			},
		},
	}

	// Unit tests can be run with multiple processes side-by-side with
	// `make stress`. This is bound to not work with fixed ports.
	// So by default we use :0 to auto-allocate ports.
	args.Addr = "127.0.0.1:0"
	if sqlPort := demoCtx.sqlPort(serverIdx, forSystemTenant); sqlPort != 0 {
		rpcPort := demoCtx.rpcPort(serverIdx, false)
		args.Addr = fmt.Sprintf("127.0.0.1:%d", rpcPort)
		args.SQLAddr = fmt.Sprintf("127.0.0.1:%d", sqlPort)
		if !demoCtx.DisableServerController {
			// The code in NewDemoCluster put the KV ports higher so
			// we need to subtract the number of nodes to get back
			// to the "good" ports.
			//
			// We reduce lower bound of the port range by 1 because
			// the server controller uses 1-based indexing for
			// servers.
			args.ApplicationInternalRPCPortMin = rpcPort - (demoCtx.NumNodes + 1)
			args.ApplicationInternalRPCPortMax = args.ApplicationInternalRPCPortMin + 1024
		}
	}
	if httpPort := demoCtx.httpPort(serverIdx, forSystemTenant); httpPort != 0 {
		args.HTTPAddr = fmt.Sprintf("127.0.0.1:%d", httpPort)
	}

	if len(demoCtx.Localities) > serverIdx {
		args.Locality = demoCtx.Localities[serverIdx]
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
	if err := timeutil.RunWithTimeout(ctx, "save-web-sessions", 10*time.Second, func(ctx context.Context) error {
		if err := c.saveWebSessions(ctx); err != nil {
			return err
		}
		return nil
	}); err != nil {
		c.warnLog(ctx, "unable to save web sessions: %v", err)
	}
	if c.stopper != nil {
		if r := recover(); r != nil {
			// A panic here means some of the async tasks may still be
			// blocked, so the stopper Stop call would block. Just initiate
			// it asynchronously in that case. It may or may not catch up
			// with the subsequent panic propagation.
			go c.stopper.Stop(ctx)
			panic(r)
		}
		c.stopper.Stop(ctx)
	}
	if testingForceRandomizeDemoPorts {
		// Note: unless we're running in unit tests, c.demoDir is NOT
		// removed in the Close() method: this way, we can preserve
		// configuration across `demo` calls.
		if err := clierror.CheckAndMaybeLog(os.RemoveAll(c.demoDir), c.shoutLog); err != nil {
			// There's nothing to do here anymore if err != nil.
			_ = err
		}
	}
}

// findServer looks for the index of the server with the given node
// ID. We need to do this search because the mapping between server
// index and node ID is not guaranteed.
func (c *transientCluster) findServer(nodeID roachpb.NodeID) (int, error) {
	serverIdx := -1
	for i, s := range c.servers {
		if s.nodeID == nodeID {
			if s.decommissioned {
				return -1, errors.Newf("node %d is permanently decommissioned", nodeID)
			}
			serverIdx = i
			break
		}
	}
	if serverIdx == -1 {
		return -1, errors.Newf("node %d does not exist", nodeID)
	}
	return serverIdx, nil
}

// DrainAndShutdown will gracefully attempt to drain a node in the cluster, and
// then shut it down.
func (c *transientCluster) DrainAndShutdown(ctx context.Context, nodeID int32) error {
	if c.demoCtx.SimulateLatency {
		return errors.Errorf("shutting down nodes is not supported in --%s configurations", cliflags.Global.Name)
	}

	// Find which server has the requested node ID.
	serverIdx, err := c.findServer(roachpb.NodeID(nodeID))
	if err != nil {
		return err
	}
	if c.servers[serverIdx].TestServerInterface == nil {
		return errors.Errorf("node %d is already shut down", nodeID)
	}
	// This is possible if we re-assign c.s and make the other nodes to the new
	// base node.
	if serverIdx == 0 {
		return errors.Errorf("cannot shutdown node %d", nodeID)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := c.drainAndShutdown(ctx, c.servers[serverIdx].adminClient); err != nil {
		return err
	}

	select {
	case <-c.servers[serverIdx].Stopper().IsStopped():
	case <-time.After(10 * time.Second):
		return errors.Errorf("server stopper not stopped after 10 seconds")
	}

	c.servers[serverIdx].TestServerInterface = nil
	c.servers[serverIdx].adminClient = nil
	if c.demoCtx.Multitenant {
		c.tenantServers[serverIdx] = nil
	}
	return nil
}

// findOtherServer returns an admin RPC client to another
// server than the one referred to by the node ID.
func (c *transientCluster) findOtherServer(
	ctx context.Context, nodeID int32, op string,
) (serverpb.AdminClient, error) {
	// Find a node to use as the sender.
	var adminClient serverpb.AdminClient
	for _, s := range c.servers {
		if s.adminClient != nil && s.nodeID != roachpb.NodeID(nodeID) {
			adminClient = s.adminClient
			break
		}
	}
	if adminClient == nil {
		return nil, errors.Newf("no other nodes available to send a %s request", op)
	}
	return adminClient, nil
}

// Decommission decommissions a given node.
func (c *transientCluster) Decommission(ctx context.Context, nodeID int32) error {
	// It's important to drain the node before decommissioning it;
	// otherwise it's possible for other nodes (and the front-end SQL
	// shell) to experience weird errors and timeouts while still trying
	// to use the decommissioned node.
	if err := c.DrainAndShutdown(ctx, nodeID); err != nil {
		return err
	}

	// Mark the node ID as permanently unavailable.
	// We do not need to check the error return of findServer()
	// because we know DrainAndShutdown above succeeded and it
	// already checked findServer().
	serverIdx, _ := c.findServer(roachpb.NodeID(nodeID))
	c.servers[serverIdx].decommissioned = true

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Find a node to use as the sender.
	adminClient, err := c.findOtherServer(ctx, nodeID, "decommission")
	if err != nil {
		return err
	}

	// This (cumbersome) two step process is due to the allowed state
	// transitions for membership status. To mark a node as fully
	// decommissioned, it has to be marked as decommissioning first.
	{
		req := &serverpb.DecommissionRequest{
			NodeIDs:          []roachpb.NodeID{roachpb.NodeID(nodeID)},
			TargetMembership: livenesspb.MembershipStatus_DECOMMISSIONING,
		}
		if _, err := adminClient.Decommission(ctx, req); err != nil {
			return errors.Wrap(err, "while trying to mark as decommissioning")
		}
	}

	{
		req := &serverpb.DecommissionRequest{
			NodeIDs:          []roachpb.NodeID{roachpb.NodeID(nodeID)},
			TargetMembership: livenesspb.MembershipStatus_DECOMMISSIONED,
		}
		if _, err := adminClient.Decommission(ctx, req); err != nil {
			return errors.Wrap(err, "while trying to mark as decommissioned")
		}
	}

	return nil
}

// RestartNode will bring back a node in the cluster.
// The node must have been shut down beforehand.
// The node will restart, connecting to the same in memory node.
func (c *transientCluster) RestartNode(ctx context.Context, nodeID int32) error {
	// Find which server has the requested node ID.
	serverIdx, err := c.findServer(roachpb.NodeID(nodeID))
	if err != nil {
		return err
	}
	if c.servers[serverIdx].TestServerInterface != nil {
		return errors.Errorf("node %d is already running", nodeID)
	}

	_, err = c.startServerInternal(ctx, serverIdx)
	return err
}

func (c *transientCluster) startServerInternal(
	ctx context.Context, serverIdx int,
) (newNodeID int32, err error) {
	// TODO(#42243): re-compute the latency mapping.
	if c.demoCtx.SimulateLatency {
		return 0, errors.Errorf("restarting nodes is not supported in --%s configurations", cliflags.Global.Name)
	}
	// TODO(...): the RPC address of the first server may not be available
	// if the first server was shut down.
	socketDetails, err := c.sockForServer(serverIdx, forSystemTenant)
	if err != nil {
		return 0, err
	}
	args := c.demoCtx.testServerArgsForTransientCluster(
		socketDetails,
		serverIdx,
		c.firstServer.AdvRPCAddr(), c.demoDir,
		c.stickyVFSRegistry)
	srv, err := server.TestServerFactory.New(args)
	if err != nil {
		return 0, err
	}
	s := &wrap{srv.(serverutils.TestServerInterfaceRaw)}

	// We want to only return after the server is ready.
	readyCh := make(chan struct{})
	s.SetReadyFn(func(bool) {
		close(readyCh)
	})

	if err := s.Start(ctx); err != nil {
		return 0, err
	}

	// Wait until the server is ready to action.
	select {
	case <-readyCh:
	case <-time.After(maxNodeInitTime):
		return 0, errors.Newf("could not initialize server %d in time", serverIdx)
	}

	c.stopper.AddCloser(stop.CloserFn(func() { s.Stop(context.Background()) }))
	nodeID := s.NodeID()

	conn, err := s.RPCClientConnE(username.RootUserName())
	if err != nil {
		s.Stopper().Stop(ctx)
		return 0, err
	}

	c.servers[serverIdx] = serverEntry{
		TestServerInterface: s,
		adminClient:         serverpb.NewAdminClient(conn),
		nodeID:              nodeID,
	}

	if c.demoCtx.Multitenant {
		if err := c.startTenantService(ctx, serverIdx, false /* createTenant */); err != nil {
			s.Stopper().Stop(ctx)
			c.servers[serverIdx].TestServerInterface = nil
			c.servers[serverIdx].adminClient = nil
			c.tenantServers[serverIdx] = nil
			return 0, err
		}
	}

	return int32(nodeID), nil
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
	// When we call startServerInternal below, this element will be properly initialized.
	c.servers = append(c.servers, serverEntry{})
	c.tenantServers = append(c.tenantServers, nil)
	c.demoCtx.Localities = append(c.demoCtx.Localities, loc)
	c.demoCtx.NumNodes++

	return c.startServerInternal(ctx, c.demoCtx.NumNodes-1)
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
func (c *transientCluster) generateCerts(ctx context.Context, certsDir string) (err error) {
	// Prevent concurrent access while we write the certificates.
	cleanup, err := c.lockDir(ctx, certsDir, "certgen")
	if err != nil {
		return err
	}
	defer cleanup()

	// Make a CA key/cert pair.
	caKeyPath := filepath.Join(certsDir, certnames.CAKeyFilename())
	caKeyExists, err := fileExists(caKeyPath)
	if err != nil {
		return err
	}
	caCertPath := filepath.Join(certsDir, certnames.CACertFilename())
	caCertExists, err := fileExists(caCertPath)
	if err != nil {
		return err
	}
	nodeKeyPath := filepath.Join(certsDir, certnames.NodeKeyFilename())
	nodeKeyExists, err := fileExists(nodeKeyPath)
	if err != nil {
		return err
	}
	nodeCertPath := filepath.Join(certsDir, certnames.NodeCertFilename())
	nodeCertExists, err := fileExists(nodeCertPath)
	if err != nil {
		return err
	}
	tenantKeyPath := filepath.Join(certsDir, certnames.TenantKeyFilename("2"))
	tenantKeyExists, err := fileExists(tenantKeyPath)
	if err != nil {
		return err
	}
	tenantCertPath := filepath.Join(certsDir, certnames.TenantCertFilename("2"))
	tenantCertExists, err := fileExists(tenantCertPath)
	if err != nil {
		return err
	}
	rootClientCertPath := filepath.Join(certsDir, certnames.ClientCertFilename(username.RootUserName()))
	rootClientCertExists, err := fileExists(rootClientCertPath)
	if err != nil {
		return err
	}
	rootClientKeyPath := filepath.Join(certsDir, certnames.ClientKeyFilename(username.RootUserName()))
	rootClientKeyExists, err := fileExists(rootClientKeyPath)
	if err != nil {
		return err
	}
	demoUser := username.MakeSQLUsernameFromPreNormalizedString(demoUsername)
	demoClientCertPath := filepath.Join(certsDir, certnames.ClientCertFilename(demoUser))
	demoClientCertExists, err := fileExists(demoClientCertPath)
	if err != nil {
		return err
	}
	demoClientKeyPath := filepath.Join(certsDir, certnames.ClientKeyFilename(demoUser))
	demoClientKeyExists, err := fileExists(demoClientKeyPath)
	if err != nil {
		return err
	}
	tenantSigningCertPath := filepath.Join(certsDir, certnames.TenantSigningCertFilename("2"))
	tenantSigningCertExists, err := fileExists(tenantSigningCertPath)
	if err != nil {
		return err
	}
	tenantSigningKeyPath := filepath.Join(certsDir, certnames.TenantSigningKeyFilename("2"))
	tenantSigningKeyExists, err := fileExists(tenantSigningKeyPath)
	if err != nil {
		return err
	}

	// Do we have everything we need to run a cluster?
	// NB: we do not need the CA key in this test - it's not needed to
	// _run_ a cluster, only to generate new certs. See below for that.
	if caCertExists &&
		nodeKeyExists && nodeCertExists &&
		rootClientKeyExists && rootClientCertExists &&
		demoClientKeyExists && demoClientCertExists &&
		(!c.demoCtx.Multitenant || (tenantSigningKeyExists && tenantSigningCertExists &&
			(c.demoCtx.DisableServerController || (tenantKeyExists && tenantCertExists)))) {
		// All good.
		return nil
	}

	allFilenamesCreatedFromCA := []string{
		nodeKeyPath, nodeCertPath,
		tenantKeyPath, tenantCertPath,
		rootClientKeyPath, rootClientKeyPath + ".pk8", rootClientCertPath,
		demoClientKeyPath, demoClientKeyPath + ".pk8", demoClientCertPath,
	}

	// Here we will need to create _some_ certs. Do we have a CA key to go with?
	if !caKeyExists || !caCertExists {
		// If there's some certificate left over from a previous session, we
		// can't exclude that a demo session is still running and is using
		// them; and we can't regenerate them piecemeal without invalidating
		// the others. Bail out in that case.
		//
		// Note: the tenant signing key/cert pair is independent from the CA
		// so we do not need to check this here.
		if caCertExists ||
			nodeKeyExists || nodeCertExists ||
			tenantKeyExists || tenantCertExists ||
			rootClientKeyExists || rootClientCertExists ||
			demoClientKeyExists || demoClientCertExists {
			err := errors.Newf("cannot create demo TLS certs: stray certificates left over in %q", certsDir)
			err = errors.WithHint(err, "Remove them manually before trying again.")
			for _, f := range allFilenamesCreatedFromCA {
				if exists, _ := fileExists(f); exists {
					err = errors.WithDetailf(err, "Stray file: %q", f)
				}
			}
			return err
		}

		if !(caKeyExists && caCertExists) {
			// We need to regenerate CA cert/key pair. This will invalidate
			// any pre-existing cert in the directory, so we will need to
			// regenerate them below.
			for _, f := range allFilenamesCreatedFromCA {
				err := os.Remove(f)
				if err != nil && !oserror.IsNotExist(err) {
					return err
				}
				if err == nil {
					c.infoLog(ctx, "removed stray file: %q", f)
				}
			}
			nodeKeyExists = false
			nodeCertExists = false
			tenantKeyExists = false
			tenantCertExists = false
			rootClientKeyExists = false
			rootClientCertExists = false
			demoClientKeyExists = false
			demoClientCertExists = false
			// Actually create the key/cert pair.
			c.infoLog(ctx, "generating CA key/cert pair in %q", certsDir)
			if err := security.CreateCAPair(
				certsDir,
				caKeyPath,
				c.demoCtx.DefaultKeySize,
				c.demoCtx.DefaultCALifetime,
				true,  /* allowKeyReuse */
				false, /*overwrite */
			); err != nil {
				return err
			}
		}
	}

	// At this point we have a CA cert/key pair, and we need to re-generate
	// some of the certs. Do it now.

	tlsServerNames := []string{
		"127.0.0.1", "*.local", "::1", "localhost",
	}

	if !(nodeKeyExists && nodeCertExists) {
		// Generate a certificate for the demo nodes.
		c.infoLog(ctx, "generating node key/cert pair in %q", certsDir)
		if err := security.CreateNodePair(
			certsDir,
			caKeyPath,
			c.demoCtx.DefaultKeySize,
			c.demoCtx.DefaultCertLifetime,
			true, /* overwrite */
			tlsServerNames,
		); err != nil {
			return err
		}
	}

	if !(rootClientKeyExists && rootClientCertExists) {
		c.infoLog(ctx, "generating root client key/cert pair in %q", certsDir)
		if err := security.CreateClientPair(
			certsDir,
			caKeyPath,
			c.demoCtx.DefaultKeySize,
			c.demoCtx.DefaultCertLifetime,
			true, /* overwrite */
			username.RootUserName(),
			nil,  /* tenantIDs - this makes it valid for all tenants */
			nil,  /* tenantNames - this makes it valid for all tenants */
			true, /* generatePKCS8Key */
		); err != nil {
			return err
		}
	}

	if !(demoClientKeyExists && demoClientCertExists) {
		c.infoLog(ctx, "generating demo client key/cert pair in %q", certsDir)
		if err := security.CreateClientPair(
			certsDir,
			caKeyPath,
			c.demoCtx.DefaultKeySize,
			c.demoCtx.DefaultCertLifetime,
			true, /* overwrite */
			demoUser,
			nil,  /* tenantIDs - this makes it valid for all tenants */
			nil,  /* tenantNames - this makes it valid for all tenants */
			true, /* generatePKCS8Key */
		); err != nil {
			return err
		}
	}

	if c.demoCtx.Multitenant {
		if !(tenantSigningKeyExists && tenantSigningCertExists) {
			c.infoLog(ctx, "generating tenant signing key/cert pair in %q", certsDir)
			if err := security.CreateTenantSigningPair(
				certsDir,
				c.demoCtx.DefaultCertLifetime,
				true, /* overwrite */
				2,
			); err != nil {
				return err
			}
		}

		if c.demoCtx.DisableServerController {
			if !(tenantKeyExists && tenantCertExists) {
				c.infoLog(ctx, "generating tenant server key/cert pair in %q", certsDir)
				pair, err := security.CreateTenantPair(
					certsDir,
					caKeyPath,
					c.demoCtx.DefaultKeySize,
					c.demoCtx.DefaultCertLifetime,
					2,
					tlsServerNames,
				)
				if err != nil {
					return err
				}
				if err := security.WriteTenantPair(certsDir, pair, true /* overwrite */); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (c *transientCluster) getNetworkURLForServer(
	ctx context.Context, serverIdx int, includeAppName bool, target serverSelection,
) (*pgurl.URL, error) {
	u := pgurl.New()
	if includeAppName {
		if err := u.SetOption("application_name", catconstants.ReportableAppNamePrefix+"cockroach demo"); err != nil {
			return nil, err
		}
	}
	sqlAddr := c.servers[serverIdx].AdvSQLAddr()
	database := c.defaultDB
	if target == forSecondaryTenant {
		sqlAddr = c.tenantServers[serverIdx].SQLAddr()
	}
	if (target == forSystemTenant) && c.demoCtx.Multitenant {
		database = catalogkeys.DefaultDatabaseName
	}

	if err := c.extendURLWithTargetCluster(u, target); err != nil {
		return nil, err
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
		caCert := certnames.CACertFilename()
		u.
			WithAuthn(pgurl.AuthnPassword(true, c.adminPassword)).
			WithTransport(pgurl.TransportTLS(
				pgurl.TLSRequire,
				filepath.Join(c.demoDir, caCert),
			))
	}
	return u, nil
}

func (c *transientCluster) extendURLWithTargetCluster(u *pgurl.URL, target serverSelection) error {
	if c.demoCtx.Multitenant && !c.demoCtx.DisableServerController {
		if target == forSecondaryTenant {
			if err := u.SetOption("options", "-ccluster="+demoTenantName); err != nil {
				return err
			}
		} else {
			if err := u.SetOption("options", "-ccluster="+catconstants.SystemTenantName); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *transientCluster) GetConnURL() string {
	return c.connURL
}

func (c *transientCluster) GetSQLCredentials() (
	adminUser username.SQLUsername,
	adminPassword, certsDir string,
) {
	return c.adminUser, c.adminPassword, c.demoDir
}

func (c *transientCluster) maybeEnableMultiTenantMultiRegion(ctx context.Context) error {
	if !c.demoCtx.Multitenant {
		return nil
	}

	storageURL, err := c.getNetworkURLForServer(ctx, 0, false /* includeAppName */, forSystemTenant)
	if err != nil {
		return err
	}
	db, err := gosql.Open("postgres", storageURL.ToPQ().String())
	if err != nil {
		return err
	}
	defer db.Close()

	if _, err = db.Exec(`ALTER TENANT ALL SET CLUSTER SETTING ` +
		sql.SecondaryTenantsMultiRegionAbstractionsEnabledSettingName + " = true"); err != nil {
		return err
	}

	return nil
}

func (c *transientCluster) SetClusterSetting(
	ctx context.Context, setting string, value interface{},
) error {
	storageURL, err := c.getNetworkURLForServer(ctx, 0, false /* includeAppName */, forSystemTenant)
	if err != nil {
		return err
	}
	db, err := gosql.Open("postgres", storageURL.ToPQ().String())
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec(fmt.Sprintf("SET CLUSTER SETTING %s = '%v'", setting, value))
	if err != nil {
		return err
	}
	if c.demoCtx.Multitenant {
		_, err = db.Exec(fmt.Sprintf("ALTER TENANT ALL SET CLUSTER SETTING %s = '%v'", setting, value))
	}
	return err
}

func (c *transientCluster) SetupWorkload(ctx context.Context) error {
	if err := c.maybeEnableMultiTenantMultiRegion(ctx); err != nil {
		return err
	}

	// If there is a load generator, create its database and load its
	// fixture.
	gen := c.demoCtx.WorkloadGenerator
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
			targetServer := forSystemTenant
			if c.demoCtx.Multitenant {
				targetServer = forSecondaryTenant
			}
			var sqlURLs []string
			for i := range c.servers {
				sqlURL, err := c.getNetworkURLForServer(ctx, i, true /* includeAppName */, targetServer)
				if err != nil {
					return err
				}
				sqlURLs = append(sqlURLs, sqlURL.WithDatabase(gen.Meta().Name).ToPQ().String())
			}
			if err := c.runWorkload(ctx, c.demoCtx.WorkloadGenerator, sqlURLs); err != nil {
				return errors.Wrapf(err, "starting background workload")
			}
		}

		if c.demoCtx.ExpandSchema > 0 {
			if err := c.expandSchema(ctx, gen); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *transientCluster) expandSchema(ctx context.Context, gen workload.Generator) error {
	return c.stopper.RunAsyncTask(ctx, "expand-schema", func(ctx context.Context) {
		ctx, cancel := c.stopper.WithCancelOnQuiesce(ctx)
		defer cancel()
		ctx = logtags.AddTag(ctx, "expand-schemas", nil)

		db, err := gosql.Open("postgres", c.connURL)
		if err != nil {
			c.warnLog(ctx, "unable to connect: %v", err)
			return
		}
		defer db.Close()

		// How to generate names.
		nameGenOpts := "{}"
		if c.demoCtx.NameGenOptions != "" {
			nameGenOpts = c.demoCtx.NameGenOptions
		}

		// Don't spam too many warnings if the schema expansion cannot
		// make progress.
		every := log.Every(10 * time.Second)

		for num := 0; num < c.demoCtx.ExpandSchema; {
			// Pace the expansion in proportion to the target
			// number (more desired descriptors = grow faster).
			var dbPerRound, tbPerDbPerRound int
			switch {
			case c.demoCtx.ExpandSchema <= 100:
				dbPerRound = 1
				tbPerDbPerRound = 10
			case c.demoCtx.ExpandSchema <= 50000:
				dbPerRound = 10
				tbPerDbPerRound = 100
			default:
				dbPerRound = 20
				tbPerDbPerRound = 450
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
			if _, err := db.ExecContext(ctx, `SELECT crdb_internal.generate_test_objects(
json_build_object(
  'names', quote_ident($1) || '._',
  'counts', array[$2,0,$3],
  'table_templates', array[quote_ident($1) || '.*'],
  'name_gen', $4::JSONB
))`, gen.Meta().Name, dbPerRound, tbPerDbPerRound, nameGenOpts); err != nil {
				if every.ShouldLog() {
					c.warnLog(ctx, "while expanding the schema: %v", err)
				}
				// Try again - it may succeed later.
				continue
			}
			num += dbPerRound*2 + (dbPerRound * tbPerDbPerRound)
		}
	})
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

// EnableEnterprise enables enterprise features if available in this build.
func (c *transientCluster) EnableEnterprise(ctx context.Context) (func(), error) {
	purl, err := c.getNetworkURLForServer(ctx, 0, true /* includeAppName */, forSystemTenant)
	if err != nil {
		return nil, err
	}
	connURL := purl.ToPQ().String()
	db, err := gosql.Open("postgres", connURL)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	return EnableEnterprise(db, demoOrg)
}

// sockForServer generates the metadata for a unix socket for the given node.
// For example, node 1 gets socket /tmpdemodir/.s.PGSQL.26267,
// node 2 gets socket /tmpdemodir/.s.PGSQL.26268, etc.
func (c *transientCluster) sockForServer(
	serverIdx int, target serverSelection,
) (unixSocketDetails, error) {
	if !c.useSockets {
		return unixSocketDetails{}, nil
	}
	port := strconv.Itoa(c.demoCtx.sqlPort(serverIdx, false))
	databaseName := c.defaultDB
	if c.demoCtx.Multitenant {
		// TODO(knz): for now, we only define the unix socket for the
		// system tenant, which is not where the workload database is
		// running.
		// NB: This logic can be simplified once we use a single
		// SQL listener for all tenants.
		databaseName = catalogkeys.DefaultDatabaseName
	}
	u := pgurl.New().
		WithNet(pgurl.NetUnix(c.demoDir, port)).
		WithUsername(c.adminUser.Normalized()).
		WithAuthn(pgurl.AuthnPassword(true, c.adminPassword)).
		WithDatabase(databaseName)
	if err := c.extendURLWithTargetCluster(u, target); err != nil {
		return unixSocketDetails{}, err
	}
	return unixSocketDetails{
		socketDir: c.demoDir,
		port:      port,
		u:         u,
	}, nil
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
	numNodes := 0
	for _, s := range c.servers {
		if s.TestServerInterface != nil {
			numNodes++
		}
	}
	return numNodes
}

func (c *transientCluster) NumServers() int {
	return len(c.servers)
}

func (c *transientCluster) Server(i int) serverutils.TestServerInterface {
	return c.servers[i].TestServerInterface
}

func (c *transientCluster) GetLocality(nodeID int32) string {
	serverIdx, err := c.findServer(roachpb.NodeID(nodeID))
	if err != nil {
		return fmt.Sprintf("(%v)", err)
	}
	return c.demoCtx.Localities[serverIdx].String()
}

func (c *transientCluster) ListDemoNodes(w, ew io.Writer, justOne, verbose bool) {
	numNodesLive := 0
	// First, list system tenant nodes.
	for i, s := range c.servers {
		if s.TestServerInterface == nil {
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
		if c.demoCtx.Multitenant {
			if !c.demoCtx.DisableServerController {
				// When using the server controller, we have a single web UI
				// URL for both tenants. The demologin link does an
				// auto-login for both.
				uiURL := c.addDemoLoginToURL(s.AdminURL().URL, false /* includeTenantName */)
				fmt.Fprintln(w, "   (webui)   ", uiURL)
			}
			if verbose {
				fmt.Fprintln(w)
				fmt.Fprintln(w, "  Application tenant:")
			}

			rpcAddr := c.tenantServers[i].RPCAddr()
			tenantUiURL := c.tenantServers[i].AdminURL()
			tenantSqlURL, err := c.getNetworkURLForServer(context.Background(), i,
				false /* includeAppName */, forSecondaryTenant)
			if err != nil {
				fmt.Fprintln(ew, errors.Wrap(err, "retrieving network URL for tenant server"))
			} else {
				// Only include a separate HTTP URL if there's no server
				// controller.
				includeHTTP := !c.demoCtx.Multitenant || c.demoCtx.DisableServerController

				socketDetails, err := c.sockForServer(i, forSecondaryTenant)
				if err != nil {
					fmt.Fprintln(ew, errors.Wrap(err, "retrieving socket URL for tenant server"))
				}
				c.printURLs(w, ew, tenantSqlURL, tenantUiURL.URL, socketDetails, rpcAddr, verbose, includeHTTP)
			}
			fmt.Fprintln(w)
			if verbose {
				fmt.Fprintln(w, "  System tenant:")
			}
		}
		if !c.demoCtx.Multitenant || verbose {
			// Connection parameters for the system tenant follow.
			uiURL := s.AdminURL().URL
			if q := uiURL.Query(); c.demoCtx.Multitenant && !c.demoCtx.DisableServerController && !q.Has(server.ClusterNameParamInQueryURL) {
				q.Add(server.ClusterNameParamInQueryURL, catconstants.SystemTenantName)
				uiURL.RawQuery = q.Encode()
			}

			sqlURL, err := c.getNetworkURLForServer(context.Background(), i,
				false /* includeAppName */, forSystemTenant)
			if err != nil {
				fmt.Fprintln(ew, errors.Wrap(err, "retrieving network URL"))
			} else {
				// Only include a separate HTTP URL if there's no server
				// controller.
				includeHTTP := !c.demoCtx.Multitenant || c.demoCtx.DisableServerController
				socketDetails, err := c.sockForServer(i, forSystemTenant)
				if err != nil {
					fmt.Fprintln(ew, errors.Wrap(err, "retrieving socket URL for system tenant server"))
				}
				c.printURLs(w, ew, sqlURL, uiURL, socketDetails, s.AdvRPCAddr(), verbose, includeHTTP)
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

func (c *transientCluster) ExpandShortDemoURLs(s string) string {
	if !strings.Contains(s, "demo://") {
		return s
	}
	u, err := c.getNetworkURLForServer(context.Background(), 0, false, forSystemTenant)
	if err != nil {
		return s
	}
	return regexp.MustCompile(`demo://([[:alnum:]]+)`).ReplaceAllString(s, strings.ReplaceAll(u.String(), "-ccluster%3Dsystem", "-ccluster%3D$1"))
}

func (c *transientCluster) printURLs(
	w, ew io.Writer,
	sqlURL *pgurl.URL,
	uiURL *url.URL,
	socket unixSocketDetails,
	rpcAddr string,
	verbose, includeHTTP bool,
) {
	if includeHTTP {
		uiURL = c.addDemoLoginToURL(uiURL, true /* includeTenantName */)
		fmt.Fprintln(w, "   (webui)   ", uiURL)
	}

	_, _, port := sqlURL.GetNetworking()
	portArg := ""
	if port != fmt.Sprint(base.DefaultPort) {
		portArg = " -p " + port
	}
	secArgs := " --certs-dir=" + c.demoDir + " -u " + demoUsername
	if c.demoCtx.Insecure {
		secArgs = " --insecure"
	}
	db := sqlURL.GetDatabase()
	if opt := sqlURL.GetOption("options"); strings.HasPrefix(opt, "-ccluster=") {
		db = "cluster:" + opt[10:] + "/" + db
	}
	fmt.Fprintln(w, "   (cli)     ", "cockroach sql"+secArgs+portArg+" -d "+db)

	fmt.Fprintln(w, "   (sql)     ", sqlURL.ToPQ())
	if verbose {
		fmt.Fprintln(w, "   (sql/jdbc)", sqlURL.ToJDBC())
		if socket.exists() {
			fmt.Fprintln(w, "   (sql/unix)", socket)
		}
		fmt.Fprintln(w, "   (rpc)     ", rpcAddr)
	}
}

// addDemoLoginToURL generates an "autologin" URL from the
// server-generated URL.
func (c *transientCluster) addDemoLoginToURL(uiURL *url.URL, includeTenantName bool) *url.URL {
	q := uiURL.Query()
	if !c.demoCtx.Insecure {
		// Print web UI URL. Embed the autologin feature inside the URL.
		// We avoid printing those when insecure, as the autologin path is not available
		// in that case.
		q.Add("username", c.adminUser.Normalized())
		q.Add("password", c.adminPassword)
		uiURL.Path = authserver.DemoLoginPath
	}

	if !includeTenantName {
		q.Del(server.ClusterNameParamInQueryURL)
	}

	uiURL.RawQuery = q.Encode()
	return uiURL
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
//
// The password can be overridden via the env var
// COCKROACH_DEMO_PASSWORD for the benefit of test automation.
func genDemoPassword(username string) string {
	mypid := os.Getpid()
	candidatePassword := fmt.Sprintf("%s%d", username, mypid)
	return envutil.EnvOrDefaultString("COCKROACH_DEMO_PASSWORD", candidatePassword)
}

// lockDir uses a file lock to prevent concurrent writes to the
// demo directory.
func (c *transientCluster) lockDir(
	ctx context.Context, dir, operation string,
) (cleanup func(), err error) {
	defer func() {
		if err != nil && cleanup != nil {
			cleanup()
			cleanup = nil
		}
	}()

	// Prevent concurrent generation.
	lockPath := filepath.Join(dir, operation+".lock")
	// The lockfile package wants an absolute path.
	absLockPath, err := filepath.Abs(lockPath)
	if err != nil {
		return cleanup, errors.Wrap(err, "computing absolute path")
	}
	tlsLock, err := lockfile.New(absLockPath)
	if err != nil {
		// This should only fail on non-absolute paths, but we
		// just made it absolute above.
		return cleanup, errors.NewAssertionErrorWithWrappedErrf(err, "creating lock")
	}
	cleanup = func() {
		if err := tlsLock.Unlock(); err != nil {
			c.warnLog(ctx, "removing lock: %v", err)
		}
	}

	every := log.Every(1 * time.Second)
	for retry := retry.StartWithCtx(ctx, retry.Options{MaxRetries: 20}); retry.Next(); {
		if err := tlsLock.TryLock(); err != nil {
			if errors.Is(err, lockfile.ErrBusy) {
				if every.ShouldLog() {
					c.warnLog(ctx, "lock busy; waiting to try again: %q", lockPath)
				}
				// Retry later.
				continue
			}
			return cleanup, errors.Wrap(err, "acquiring lock")
		}
		// Lock succeeded.
		return cleanup, nil
	}
	return cleanup, errors.Newf("timeout while acquiring lock: %q", lockPath)
}

func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	exists := err == nil
	if oserror.IsNotExist(err) {
		err = nil
	}
	return exists, err
}

func (c *transientCluster) TenantName() string {
	if c.demoCtx.Multitenant {
		return demoTenantName
	}
	return catconstants.SystemTenantName
}

type wrap struct {
	serverutils.TestServerInterfaceRaw
}

var _ serverutils.TestServerInterface = (*wrap)(nil)

func (w *wrap) ApplicationLayer() serverutils.ApplicationLayerInterface {
	return w.TestServerInterfaceRaw
}
func (w *wrap) SystemLayer() serverutils.ApplicationLayerInterface   { return w.TestServerInterfaceRaw }
func (w *wrap) TenantController() serverutils.TenantControlInterface { return w.TestServerInterfaceRaw }
func (w *wrap) StorageLayer() serverutils.StorageLayerInterface      { return w.TestServerInterfaceRaw }
