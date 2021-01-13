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
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"golang.org/x/time/rate"
)

type transientCluster struct {
	connURL    string
	demoDir    string
	useSockets bool
	stopper    *stop.Stopper
	s          *server.TestServer
	servers    []*server.TestServer

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
	serverFactory := server.TestServerFactory
	var servers []*server.TestServer

	// latencyMapWaitCh is used to block test servers after RPC address computation until the artificial
	// latency map has been constructed.
	latencyMapWaitCh := make(chan struct{})

	// errCh is used to catch all errors when initializing servers.
	// Sending a nil on this channel indicates success.
	errCh := make(chan error, demoCtx.nodes)

	for i := 0; i < demoCtx.nodes; i++ {
		// All the nodes connect to the address of the first server created.
		var joinAddr string
		if i != 0 {
			joinAddr = c.s.ServingRPCAddr()
		}
		nodeID := roachpb.NodeID(i + 1)
		args := testServerArgsForTransientCluster(
			c.sockForServer(nodeID), nodeID, joinAddr, c.demoDir,
			c.sqlFirstPort,
			c.httpFirstPort,
			c.stickyEngineRegistry,
		)
		if i == 0 {
			// The first node also auto-inits the cluster.
			args.NoAutoInitializeCluster = false
		}

		// servRPCReadyCh is used if latency simulation is requested to notify that a test server has
		// successfully computed its RPC address.
		servRPCReadyCh := make(chan struct{})

		if demoCtx.simulateLatency {
			serverKnobs := args.Knobs.Server.(*server.TestingKnobs)
			serverKnobs.PauseAfterGettingRPCAddress = latencyMapWaitCh
			serverKnobs.SignalAfterGettingRPCAddress = servRPCReadyCh
			serverKnobs.ContextTestingKnobs = rpc.ContextTestingKnobs{
				ArtificialLatencyMap: make(map[string]int),
			}
		}

		s, err := serverFactory.New(args)
		if err != nil {
			return err
		}
		serv := s.(*server.TestServer)
		c.stopper.AddCloser(stop.CloserFn(serv.Stop))
		if i == 0 {
			c.s = serv
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
		servers = append(servers, serv)

		// We force a wait for all servers until they are ready.
		servReadyFnCh := make(chan struct{})
		serv.Cfg.ReadyFn = func(bool) {
			close(servReadyFnCh)
		}

		// If latency simulation is requested, start the servers in a background thread. We do this because
		// the start routine needs to wait for the latency map construction after their RPC address has been computed.
		if demoCtx.simulateLatency {
			go func(i int) {
				if err := serv.Start(); err != nil {
					errCh <- err
				} else {
					// Block until the ReadyFn has been called before continuing.
					<-servReadyFnCh
					errCh <- nil
				}
			}(i)
			<-servRPCReadyCh
		} else {
			if err := serv.Start(); err != nil {
				return err
			}
			// Block until the ReadyFn has been called before continuing.
			<-servReadyFnCh
			errCh <- nil
		}
	}

	// Ensure we close all sticky stores we've created.
	c.stopper.AddCloser(stop.CloserFn(func() {
		c.stickyEngineRegistry.CloseAllStickyInMemEngines()
	}))
	c.servers = servers

	if demoCtx.simulateLatency {
		// Now, all servers have been started enough to know their own RPC serving
		// addresses, but nothing else. Assemble the artificial latency map.
		for i, src := range servers {
			latencyMap := src.Cfg.TestingKnobs.Server.(*server.TestingKnobs).ContextTestingKnobs.ArtificialLatencyMap
			srcLocality, ok := src.Cfg.Locality.Find("region")
			if !ok {
				continue
			}
			srcLocalityMap, ok := regionToRegionToLatency[srcLocality]
			if !ok {
				continue
			}
			for j, dst := range servers {
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

	// We've assembled our latency maps and are ready for all servers to proceed
	// through bootstrapping.
	close(latencyMapWaitCh)

	// Wait for all servers to respond.
	{
		timeRemaining := maxNodeInitTime
		lastUpdateTime := timeutil.Now()
		var err error
		for i := 0; i < demoCtx.nodes; i++ {
			select {
			case e := <-errCh:
				err = errors.CombineErrors(err, e)
			case <-time.After(timeRemaining):
				return errors.New("failed to setup transientCluster in time")
			}
			updateTime := timeutil.Now()
			timeRemaining -= updateTime.Sub(lastUpdateTime)
			lastUpdateTime = updateTime
		}
		if err != nil {
			return err
		}
	}

	// Run the SQL initialization. This takes care of setting up the
	// initial replication factor for small clusters and creating the
	// admin user.
	const demoUsername = "demo"
	demoPassword := genDemoPassword(demoUsername)
	if err := runInitialSQL(ctx, c.s.Server, demoCtx.nodes < 3, demoUsername, demoPassword); err != nil {
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
	c.connURL, err = c.getNetworkURLForServer(0, gen, true /* includeAppName */)
	if err != nil {
		return err
	}

	// Start up the update check loop.
	// We don't do this in (*server.Server).Start() because we don't want it
	// in tests.
	if !demoCtx.disableTelemetry {
		c.s.PeriodicallyCheckForUpdates(ctx)
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
		httpPort := httpBasePort + int(nodeID) - 1
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

	adminClient, finish, err := getAdminClient(ctx, *(c.s.Cfg))
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

	adminClient, finish, err := getAdminClient(ctx, *(c.s.Cfg))
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
	args := testServerArgsForTransientCluster(c.sockForServer(nodeID), nodeID, c.s.ServingRPCAddr(), c.demoDir,
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

	if err := serv.Start(); err != nil {
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
func (c *transientCluster) AddNode(localityString string) error {
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
	serverIdx int, gen workload.Generator, includeAppName bool,
) (string, error) {
	options := url.Values{}
	if includeAppName {
		options.Add("application_name", catconstants.ReportableAppNamePrefix+"cockroach demo")
	}
	sqlURL := url.URL{
		Scheme: "postgres",
		Host:   c.servers[serverIdx].ServingSQLAddr(),
	}
	if gen != nil {
		// The generator wants a particular database name to be
		// pre-filled.
		sqlURL.Path = gen.Meta().Name
	}
	// For a demo cluster we don't use client TLS certs and instead use
	// password-based authentication with the password pre-filled in the
	// URL.
	if demoCtx.insecure {
		sqlURL.User = url.User(security.RootUser)
		options.Add("sslmode", "disable")
	} else {
		sqlURL.User = url.UserPassword(c.adminUser.Normalized(), c.adminPassword)
		options.Add("sslmode", "require")
	}
	sqlURL.RawQuery = options.Encode()
	return sqlURL.String(), nil
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
				sqlURL, err := c.getNetworkURLForServer(i, gen, true /* includeAppName */)
				if err != nil {
					return err
				}
				sqlURLs = append(sqlURLs, sqlURL)
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
	reg := histogram.NewRegistry(time.Duration(100) * time.Millisecond)
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
					case <-c.s.Stopper().ShouldStop():
						return
					default:
					}
				}
			}
		}
		// As the SQL shell is tied to `c.s`, this means we want to tie the workload
		// onto this as we want the workload to stop when the server dies,
		// rather than the cluster. Otherwise, interrupts on cockroach demo hangs.
		c.s.Stopper().RunWorker(ctx, workloadFun(workerFn))
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

			success, err := GetAndApplyLicense(db, c.s.ClusterID(), demoOrg)
			if err != nil {
				licenseDone <- err
				return
			}
			if !success {
				if demoCtx.geoPartitionedReplicas {
					licenseDone <- errors.WithDetailf(
						errors.New("unable to acquire a license for this demo"),
						"Enterprise features are needed for this demo (--%s).",
						cliflags.DemoGeoPartitionedReplicas.Name)
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
	return unixSocketDetails{
		socketDir:  c.demoDir,
		portNumber: c.sqlFirstPort + int(nodeID) - 1,
		username:   c.adminUser,
		password:   c.adminPassword,
	}
}

type unixSocketDetails struct {
	socketDir  string
	portNumber int
	username   security.SQLUsername
	password   string
}

func (s unixSocketDetails) exists() bool {
	return s.socketDir != ""
}

func (s unixSocketDetails) filename() string {
	if !s.exists() {
		// No socket configured.
		return ""
	}
	return filepath.Join(s.socketDir, fmt.Sprintf(".s.PGSQL.%d", s.portNumber))
}

func (s unixSocketDetails) String() string {
	options := url.Values{}
	options.Add("host", s.socketDir)
	options.Add("port", strconv.Itoa(s.portNumber))

	// Node: in the generated unix socket URL, a password is always
	// included even in insecure mode This is OK because in insecure
	// mode the password is not checked on the server.
	sqlURL := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(s.username.Normalized(), s.password),
		RawQuery: options.Encode(),
	}
	return sqlURL.String()
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
			// Print node ID and console URL. Embed the autologin feature inside the URL.
			// We avoid printing those when insecure, as the autologin path is not available
			// in that case.
			pwauth := url.Values{
				"username": []string{c.adminUser.Normalized()},
				"password": []string{c.adminPassword},
			}
			serverURL.Path = server.DemoLoginPath
			serverURL.RawQuery = pwauth.Encode()
		}
		fmt.Fprintf(w, "  (console) %s\n", serverURL)
		// Print unix socket if defined.
		if c.useSockets {
			sock := c.sockForServer(nodeID)
			fmt.Fprintln(w, "  (sql)    ", sock)
		}
		// Print network URL if defined.
		netURL, err := c.getNetworkURLForServer(i, nil, false /*includeAppName*/)
		if err != nil {
			fmt.Fprintln(stderr, errors.Wrap(err, "retrieving network URL"))
		} else {
			fmt.Fprintln(w, "  (sql/tcp)", netURL)
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
