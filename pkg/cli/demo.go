// Copyright 2018 The Cockroach Authors.
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
	"net/url"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logflags"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"golang.org/x/time/rate"
)

var demoCmd = &cobra.Command{
	Use:   "demo",
	Short: "open a demo sql shell",
	Long: `
Start an in-memory, standalone, single-node CockroachDB instance, and open an
interactive SQL prompt to it. Various datasets are available to be preloaded as
subcommands: e.g. "cockroach demo startrek". See --help for a full list.

By default, the 'movr' dataset is pre-loaded. You can also use --empty
to avoid pre-loading a dataset.

cockroach demo attempts to connect to a Cockroach Labs server to obtain a
temporary enterprise license for demoing enterprise features and enable
telemetry back to Cockroach Labs. In order to disable this behavior, set the
environment variable "COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING" to true.
`,
	Example: `  cockroach demo`,
	Args:    cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(func(cmd *cobra.Command, _ []string) error {
		return runDemo(cmd, nil /* gen */)
	}),
}

const demoOrg = "Cockroach Demo"

const defaultGeneratorName = "movr"

var defaultGenerator workload.Generator

// maxNodeInitTime is the maximum amount of time to wait for a single demo node
// to start up.
const maxNodeInitTime = 30 * time.Second

var defaultLocalities = demoLocalityList{
	// Default localities for a 3 node cluster
	{Tiers: []roachpb.Tier{{Key: "region", Value: "us-east1"}, {Key: "az", Value: "b"}}},
	{Tiers: []roachpb.Tier{{Key: "region", Value: "us-east1"}, {Key: "az", Value: "c"}}},
	{Tiers: []roachpb.Tier{{Key: "region", Value: "us-east1"}, {Key: "az", Value: "d"}}},
	// Default localities for a 6 node cluster
	{Tiers: []roachpb.Tier{{Key: "region", Value: "us-west1"}, {Key: "az", Value: "a"}}},
	{Tiers: []roachpb.Tier{{Key: "region", Value: "us-west1"}, {Key: "az", Value: "b"}}},
	{Tiers: []roachpb.Tier{{Key: "region", Value: "us-west1"}, {Key: "az", Value: "c"}}},
	// Default localities for a 9 node cluster
	{Tiers: []roachpb.Tier{{Key: "region", Value: "europe-west1"}, {Key: "az", Value: "b"}}},
	{Tiers: []roachpb.Tier{{Key: "region", Value: "europe-west1"}, {Key: "az", Value: "c"}}},
	{Tiers: []roachpb.Tier{{Key: "region", Value: "europe-west1"}, {Key: "az", Value: "d"}}},
}

type regionPair struct {
	regionA string
	regionB string
}

var regionToRegionToLatency map[string]map[string]int

func insertPair(pair regionPair, latency int) {
	regionToLatency, ok := regionToRegionToLatency[pair.regionA]
	if !ok {
		regionToLatency = make(map[string]int)
		regionToRegionToLatency[pair.regionA] = regionToLatency
	}
	regionToLatency[pair.regionB] = latency
}

func init() {
	regionToRegionToLatency = make(map[string]map[string]int)
	// Latencies collected from http://cloudping.co on 2019-09-11.
	for pair, latency := range map[regionPair]int{
		{regionA: "us-east1", regionB: "us-west1"}:     66,
		{regionA: "us-east1", regionB: "europe-west1"}: 64,
		{regionA: "us-west1", regionB: "europe-west1"}: 146,
	} {
		insertPair(pair, latency)
		insertPair(regionPair{
			regionA: pair.regionB,
			regionB: pair.regionA,
		}, latency)
	}
}

func init() {
	for _, meta := range workload.Registered() {
		gen := meta.New()

		if meta.Name == defaultGeneratorName {
			// Save the default for use in the top-level 'demo' command
			// without argument.
			defaultGenerator = gen
		}

		var genFlags *pflag.FlagSet
		if f, ok := gen.(workload.Flagser); ok {
			genFlags = f.Flags().FlagSet
		}

		genDemoCmd := &cobra.Command{
			Use:   meta.Name,
			Short: meta.Description,
			Args:  cobra.ArbitraryArgs,
			RunE: MaybeDecorateGRPCError(func(cmd *cobra.Command, _ []string) error {
				return runDemo(cmd, gen)
			}),
		}
		demoCmd.AddCommand(genDemoCmd)
		genDemoCmd.Flags().AddFlagSet(genFlags)
	}
}

// GetAndApplyLicense is not implemented in order to keep OSS/BSL builds successful.
// The cliccl package sets this function if enterprise features are available to demo.
var GetAndApplyLicense func(dbConn *gosql.DB, clusterID uuid.UUID, org string) (bool, error)

type transientCluster struct {
	connURL string
	stopper *stop.Stopper
	s       *server.TestServer
	servers []transientServerCtx
	cleanup func()
}

type transientServerCtx struct {
	s       *server.TestServer
	running bool
	// rpcAddrReadyCh is closed when the RPC address is ready (but RPC
	// is not being listened to yet).
	rpcAddrReadyCh chan struct{}
	// startCh is closed when Start() returns.
	startCh chan error
	// readyCh is closed when the server is ready to accept connections.
	readyCh chan struct{}
	// contextTestingKnobs is set when latency simulation is requested.
	contextTestingKnobs *rpc.ContextTestingKnobs
}

func (c *transientCluster) findServer(nodeID roachpb.NodeID) (int, *server.TestServer, bool) {
	for i, s := range c.servers {
		if s.s.NodeID() == nodeID {
			return i, s.s, true
		}
	}
	return -1, nil, false
}

// DrainNode will gracefully attempt to drain a node in the cluster.
func (c *transientCluster) DrainNode(nodeID roachpb.NodeID) error {
	nodeIndex, serv, ok := c.findServer(nodeID)
	if !ok {
		return errors.Errorf("node %d does not exist", nodeID)
	}
	if !c.servers[nodeIndex].running {
		return errors.Errorf("node %d is not running", nodeID)
	}
	if nodeIndex == 0 {
		// This is possible if we re-assign c.s and make the other nodes
		// to the new base node.
		return errors.Newf("cannot shutdown node %d", nodeID)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	adminClient, finish, err := getAdminClient(ctx, *(serv.Cfg))
	if err != nil {
		return err
	}
	defer finish()

	onModes := make([]int32, len(server.GracefulDrainModes))
	for i, m := range server.GracefulDrainModes {
		onModes[i] = int32(m)
	}

	if err := doShutdown(ctx, adminClient, onModes); err != nil {
		return err
	}
	c.servers[nodeIndex].running = false
	return nil
}

// CallDecommission calls the Decommission RPC on a node.
func (c *transientCluster) CallDecommission(nodeID roachpb.NodeID, decommissioning bool) error {
	_, serv, ok := c.findServer(nodeID)
	if !ok {
		return errors.Errorf("node %d does not exist", nodeID)
	}

	req := &serverpb.DecommissionRequest{
		NodeIDs:         []roachpb.NodeID{nodeID},
		Decommissioning: decommissioning,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	adminClient, finish, err := getAdminClient(ctx, *(serv.Cfg))
	if err != nil {
		return err
	}
	defer finish()

	_, err = adminClient.Decommission(ctx, req)
	return errors.Wrap(err, "while trying to mark as decommissioning")
}

// RestartNode will bring back a node in the cluster.
// The node must have been shut down beforehand.
// The node will restart, connecting to the same in memory node.
func (c *transientCluster) RestartNode(nodeID roachpb.NodeID) error {
	nodeIndex, _, ok := c.findServer(nodeID)
	if !ok {
		return errors.Errorf("node %d does not exist", nodeID)
	}
	if c.servers[nodeIndex].running {
		return errors.Errorf("node %d is already running", nodeID)
	}

	// TODO(#42243): re-compute the latency mapping.
	args := testServerArgsForTransientCluster(nodeID, c.s.ServingRPCAddr())
	serv := server.TestServerFactory.New(args).(*server.TestServer)

	// We want to only return after the server is ready.
	readyCh := make(chan struct{})
	serv.Cfg.ReadyFn = func(_ bool) { close(readyCh) }

	if err := serv.Start(args); err != nil {
		return err
	}
	c.stopper.AddCloser(stop.CloserFn(serv.Stop))

	// Wait until the server is ready to action.
	select {
	case <-readyCh:
	case <-time.After(maxNodeInitTime):
		return errors.Newf("could not initialize node %d in time", nodeID)
	}

	c.servers[nodeIndex].s = serv
	c.servers[nodeIndex].running = true
	return nil
}

// testServerArgsForTransientCluster creates the test arguments for
// a necessary server in the demo cluster.
func testServerArgsForTransientCluster(nodeID roachpb.NodeID, joinAddr string) base.TestServerArgs {
	args := base.TestServerArgs{
		PartOfCluster: true,
		Insecure:      true,
		Stopper: initBacktrace(
			fmt.Sprintf("%s/demo-node%d", startCtx.backtraceOutputDir, nodeID),
		),
	}

	if demoCtx.localities != nil {
		args.Locality = demoCtx.localities[int(nodeID-1)]
	}

	// Assign a path to the store spec, to be saved.
	storeSpec := base.DefaultTestStoreSpec
	storeSpec.StickyInMemoryEngineID = fmt.Sprintf("demo-node%d", nodeID)
	args.StoreSpecs = []base.StoreSpec{storeSpec}
	args.JoinAddr = joinAddr

	return args
}

func setupTransientCluster(
	ctx context.Context, cmd *cobra.Command, gen workload.Generator,
) (c transientCluster, err error) {
	c.cleanup = func() {}

	// The user specified some localities for their nodes.
	if len(demoCtx.localities) != 0 {
		// Error out of localities don't line up with requested node
		// count before doing any sort of setup.
		if len(demoCtx.localities) != demoCtx.nodes {
			return c, errors.Errorf("number of localities specified must equal number of nodes")
		}
	} else {
		demoCtx.localities = make([]roachpb.Locality, demoCtx.nodes)
		for i := 0; i < demoCtx.nodes; i++ {
			demoCtx.localities[i] = defaultLocalities[i%len(defaultLocalities)]
		}
	}

	// Set up logging. For demo/transient server we use non-standard
	// behavior where we avoid file creation if possible.
	fl := flagSetForCmd(cmd)
	df := fl.Lookup(cliflags.LogDir.Name)
	sf := fl.Lookup(logflags.LogToStderrName)
	if !df.Changed && !sf.Changed {
		// User did not request logging flags; shut down all logging.
		// Otherwise, the demo command would cause a cockroach-data
		// directory to appear in the current directory just for logs.
		_ = df.Value.Set("")
		df.Changed = true
		_ = sf.Value.Set(log.Severity_NONE.String())
		sf.Changed = true
	}
	c.stopper, err = setupAndInitializeLoggingAndProfiling(ctx, cmd)
	if err != nil {
		return c, err
	}
	c.cleanup = func() {
		c.stopper.Stop(ctx)
	}

	serverFactory := server.TestServerFactory

	// We'll stop startup if it takes too long.
	timeoutDuration := time.Duration(demoCtx.nodes) * maxNodeInitTime
	timeoutCh := time.After(timeoutDuration)

	// latencyMapWaitCh is used to block test servers after RPC address computation until the artificial
	// latency map has been constructed.
	latencyMapWaitCh := make(chan struct{})

	servers := make([]transientServerCtx, demoCtx.nodes)
	for i := 0; i < demoCtx.nodes; i++ {
		log.Infof(ctx, "starting demo node %d/%d...\n", i+1, demoCtx.nodes)

		// All the nodes connect to the address of the first server created.
		var joinAddr string
		if c.s != nil {
			joinAddr = c.s.ServingRPCAddr()
		}
		args := testServerArgsForTransientCluster(roachpb.NodeID(i+1), joinAddr)

		// We wait on this below before creating the next node.
		servers[i].rpcAddrReadyCh = make(chan struct{})
		serverKnobs := server.TestingKnobs{
			SignalAfterGettingRPCAddress: servers[i].rpcAddrReadyCh,
		}
		args.Knobs = base.TestingKnobs{Server: &serverKnobs}

		if demoCtx.simulateLatency {
			// If artificial latencies are requested, we're going to want
			// each server to pause bootstrapping.
			serverKnobs.PauseAfterGettingRPCAddress = latencyMapWaitCh
			// And we'll inject the latencies to other nodes in a node-specific map.
			serverKnobs.ContextTestingKnobs = rpc.ContextTestingKnobs{
				ArtificialLatencyMap: make(map[string]int),
			}
			servers[i].contextTestingKnobs = &serverKnobs.ContextTestingKnobs
		}

		serv := serverFactory.New(args).(*server.TestServer)
		servers[i].s = serv
		if i == 0 {
			// Remember the head node to connect the SQL shell to.
			c.s = serv
		}

		thisCtx := &servers[i]

		// We'll want to know when the server is ready to accept connections.
		servers[i].readyCh = make(chan struct{})
		serv.Cfg.ReadyFn = func(_ bool) { close(thisCtx.readyCh) }

		// Start the servers in a background thread. We do this to enable
		// potentially faster startup, and specifically because we need to
		// pause bootstrapping across all nodes to initialize the latency
		// map if requested.
		thisCtx.startCh = make(chan error, 1)
		go func(i int) {
			err := serv.Start(args)
			log.Infof(ctx, "demo node at i = %d completed startup: %v", i, err)
			thisCtx.startCh <- err
			close(thisCtx.startCh)
		}(i)

		// Before we go on to create the next node (or proceed below) we
		// must wait for the RPC listener to be ready. However, it's also
		// possible for Start() to fail early instead, so we need to deal
		// with that too.
		log.Infof(ctx, "waiting for demo node at i = %d to have a RPC address", i)
		select {
		case <-timeoutCh:
			return c, errors.Newf("demo cluster failed to initialize after %s", timeoutDuration)
		case err := <-thisCtx.startCh:
			// Start() completed early, with an error. CombineErrors()
			// returns the existing error if the other argument is `nil`.
			if err != nil {
				log.Errorf(ctx, "demo node at i = %d failed: %v", i, err)
				return c, err
			}
		case <-thisCtx.rpcAddrReadyCh:
			// Done waiting on RPC address for this server.
		}

		// Each node has its own stopper. However when the demo session
		// stops we also want to shut down everything. Add this node
		// to the cluster-wide stopper.
		c.stopper.AddCloser(stop.CloserFn(serv.Stop))
		// Ditto, ensure we close all sticky stores we've created when
		// we shut down the entire cluster.
		for _, store := range args.StoreSpecs {
			if store.StickyInMemoryEngineID != "" {
				engineID := store.StickyInMemoryEngineID
				c.stopper.AddCloser(stop.CloserFn(func() {
					if err := server.CloseStickyInMemEngine(engineID); err != nil {
						// Something else may have already closed the sticky store.
						// Since we are closer, it doesn't really matter.
						log.Warningf(
							ctx,
							"could not close sticky in-memory store %s: %+v",
							engineID,
							err,
						)
					}
				}))
			}
		}
	}
	c.servers = servers

	// The next stage is to inject server latencies if requested.
	if demoCtx.simulateLatency {
		// Assemble the artificial latency map.
		for i := range servers {
			latencyMap := servers[i].contextTestingKnobs.ArtificialLatencyMap
			if srcLocality, ok := servers[i].s.Cfg.Locality.Find("region"); ok {
				if srcLocalityMap, ok := regionToRegionToLatency[srcLocality]; ok {
					for j := range servers {
						if i != j { // No injected latency from a node to itself.
							dst := servers[j].s
							if dstLocality, ok := dst.Cfg.Locality.Find("region"); ok {
								latency := srcLocalityMap[dstLocality]
								latencyMap[dst.ServingRPCAddr()] = latency
							}
						}
					}
				}
			}
		}
		// We've assembled our latency maps and are ready for all servers to proceed
		// through bootstrapping.
		close(latencyMapWaitCh)
	}

	// Wait until the Start() function returns.
	err = nil
	for i := range servers {
		log.Infof(ctx, "waiting for demo node to start %d/%d...\n", i, demoCtx.nodes)
		select {
		case <-timeoutCh:
			return c, errors.CombineErrors(
				errors.Newf("demo cluster failed to initialize after %s", timeoutDuration), err)
		case sErr := <-servers[i].startCh:
			// Start() and an error occurred.
			err = errors.CombineErrors(err,
				errors.Wrapf(sErr, "demo node at i = %d failed"))
		}
	}
	if err != nil {
		return c, err
	}
	// Wait until servers are ready to accept RPC and SQL connections.
	//
	// This needs to happen *after* the `Start()` function has returned
	// (i.e. we can't fuse with the `select` above) because they signal
	// different conditions and we need both conditions to proceed:
	// - Start signals the node is ready to accept RPC connections
	// - readyCh signals the node has completed bootstrapping
	for i := range servers {
		log.Infof(ctx, "waiting for demo node readiness %d/%d...\n", i, demoCtx.nodes)
		select {
		case <-timeoutCh:
			return c, errors.CombineErrors(
				errors.Newf("demo cluster failed to become ready after %s", timeoutDuration), err)
		case <-servers[i].readyCh:
			// Done waiting for readiness.
			log.Infof(ctx, "demo node at i = %d is ready", i)
			servers[i].running = true
		}
	}
	if err != nil {
		return c, err
	}

	log.Infof(ctx, "demo cluster ready")
	if demoCtx.nodes < 3 {
		log.Infof(ctx, "disabling replication on demo cluster")
		// Set up the default zone configuration. We are using an in-memory store
		// so we really want to disable replication.
		if err := cliDisableReplication(ctx, c.s.Server); err != nil {
			return c, errors.Wrap(err, "disabling replication")
		}
	}

	c.setupSQL(ctx, gen)
	return c, nil
}

func (c *transientCluster) setupSQL(ctx context.Context, gen workload.Generator) {
	// Prepare the URL for use by the SQL shell.
	// TODO (rohany): there should be a way that the user can request a specific node
	//  to connect to to see the effects of the artificial latency.
	options := url.Values{}
	options.Add("sslmode", "disable")
	options.Add("application_name", sqlbase.ReportableAppNamePrefix+"cockroach demo")
	sqlURL := url.URL{
		Scheme:   "postgres",
		User:     url.User(security.RootUser),
		Host:     c.s.ServingSQLAddr(),
		RawQuery: options.Encode(),
	}
	if gen != nil {
		sqlURL.Path = gen.Meta().Name
	}

	c.connURL = sqlURL.String()
	log.Infof(ctx, "connecting demo SQL session to: %s", c.connURL)

	// Start up the update check loop.
	// We don't do this in (*server.Server).Start() because we don't want it
	// in tests.
	if !demoCtx.disableTelemetry {
		c.s.PeriodicallyCheckForUpdates(ctx)
	}
}

func (c *transientCluster) setupWorkload(ctx context.Context, gen workload.Generator) error {
	// Communicate information about license acquisition to services
	// that depend on it.
	licenseDone := make(chan struct{})
	if demoCtx.disableLicenseAcquisition {
		close(licenseDone)
	} else {
		// If we allow telemetry, then also try and get an enterprise license for the demo.
		// GetAndApplyLicense will be nil in the pure OSS/BSL build of cockroach.
		db, err := gosql.Open("postgres", c.connURL)
		if err != nil {
			return err
		}
		// Perform license acquisition asynchronously to avoid delay in cli startup.
		go func() {
			defer db.Close()
			success, err := GetAndApplyLicense(db, c.s.ClusterID(), demoOrg)
			if err != nil {
				exitWithError("demo", err)
			}
			if !success {
				if demoCtx.geoPartitionedReplicas {
					log.Shout(ctx, log.Severity_ERROR,
						"license acquisition was unsuccessful.\nNote: enterprise features are needed for --geo-partitioned-replicas.")
					exitWithError("demo", errors.New("unable to acquire a license for this demo"))
				}

				const msg = "\nwarning: unable to acquire demo license - enterprise features are not enabled."
				fmt.Fprintln(stderr, msg)
			}
			close(licenseDone)
		}()
	}

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
		if _, err := workloadsql.Setup(ctx, db, gen, l); err != nil {
			return err
		}

		// Perform partitioning if requested by configuration.
		if demoCtx.geoPartitionedReplicas {
			// Wait until the license has been acquired to trigger partitioning.
			fmt.Println("#\n# Waiting for license acquisition to complete...")
			<-licenseDone

			fmt.Println("#\n# Partitioning the demo database, please wait...")

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
			if err := c.runWorkload(ctx, gen, []string{c.connURL}); err != nil {
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
	ops, err := opser.Ops(sqlUrls, reg)
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
						// Only log an error and return when the workload function throws
						// an error, because errors these errors should be ignored, and
						// should not interrupt the rest of the demo.
						log.Warningf(ctx, "Error running workload query: %+v\n", err)
						return
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

func incrementTelemetryCounters(cmd *cobra.Command) {
	if flagSetForCmd(cmd).Lookup(cliflags.DemoNodes.Name).Changed {
		incrementDemoCounter(nodes)
	}
	if demoCtx.localities != nil {
		incrementDemoCounter(demoLocality)
	}
	if demoCtx.runWorkload {
		incrementDemoCounter(withLoad)
	}
	if demoCtx.geoPartitionedReplicas {
		incrementDemoCounter(geoPartitionedReplicas)
	}
}

func checkDemoConfiguration(
	cmd *cobra.Command, gen workload.Generator,
) (workload.Generator, error) {
	if gen == nil && !demoCtx.useEmptyDatabase {
		// Use a default dataset unless prevented by --empty.
		gen = defaultGenerator
	}

	// Make sure that the user didn't request a workload and an empty database.
	if demoCtx.runWorkload && demoCtx.useEmptyDatabase {
		return nil, errors.New("cannot run a workload against an empty database")
	}

	// Make sure the number of nodes is valid.
	if demoCtx.nodes <= 0 {
		return nil, errors.Newf("--nodes has invalid value (expected positive, got %d)", demoCtx.nodes)
	}

	// If artificial latencies were requested, then the user cannot supply their own localities.
	if demoCtx.simulateLatency && demoCtx.localities != nil {
		return nil, errors.New("--global cannot be used with --demo-locality")
	}

	demoCtx.disableTelemetry = cluster.TelemetryOptOut()
	demoCtx.disableLicenseAcquisition = demoCtx.disableTelemetry || (GetAndApplyLicense == nil)

	if demoCtx.geoPartitionedReplicas {
		if demoCtx.disableLicenseAcquisition {
			return nil, errors.New("enterprise features are needed for this demo (--geo-partitioning-replicas)")
		}

		// Make sure that the user didn't request to have a topology and an empty database.
		if demoCtx.useEmptyDatabase {
			return nil, errors.New("cannot setup geo-partitioned replicas topology on an empty database")
		}

		// Make sure that the Movr database is selected when automatically partitioning.
		if gen == nil || gen.Meta().Name != "movr" {
			return nil, errors.New("--geo-partitioned-replicas must be used with the Movr dataset")
		}

		// If the geo-partitioned replicas flag was given and the demo localities have changed, throw an error.
		if demoCtx.localities != nil {
			return nil, errors.New("--demo-locality cannot be used with --geo-partitioned-replicas")
		}

		// If the geo-partitioned replicas flag was given and the nodes have changed, throw an error.
		if flagSetForCmd(cmd).Lookup(cliflags.DemoNodes.Name).Changed {
			if demoCtx.nodes != 9 {
				return nil, errors.New("--nodes with a value different from 9 cannot be used with --geo-partitioned-replicas")
			}
		} else {
			const msg = `#
# --geo-partitioned replicas operates on a 9 node cluster.
# The cluster size has been changed from the default to 9 nodes.`
			fmt.Println(msg)
			demoCtx.nodes = 9
		}

		// If geo-partition-replicas is requested, make sure the workload has a Partitioning step.
		configErr := errors.New(fmt.Sprintf("workload %s is not configured to have a partitioning step", gen.Meta().Name))
		hookser, ok := gen.(workload.Hookser)
		if !ok {
			return nil, configErr
		}
		if hookser.Hooks().Partition == nil {
			return nil, configErr
		}
	}

	return gen, nil
}

func runDemo(cmd *cobra.Command, gen workload.Generator) (err error) {
	if gen, err = checkDemoConfiguration(cmd, gen); err != nil {
		return err
	}
	// Record some telemetry about what flags are being used.
	incrementTelemetryCounters(cmd)

	ctx := context.Background()

	c, err := setupTransientCluster(ctx, cmd, gen)
	defer c.cleanup()
	if err != nil {
		return checkAndMaybeShout(err)
	}
	demoCtx.transientCluster = &c

	checkInteractive()

	if cliCtx.isInteractive {
		fmt.Printf(`#
# Welcome to the CockroachDB demo database!
#
# You are connected to a temporary, in-memory CockroachDB cluster of %d node%s.
`, demoCtx.nodes, util.Pluralize(int64(demoCtx.nodes)))

		if demoCtx.disableTelemetry {
			fmt.Println("#\n# Telemetry and automatic license acquisition disabled by configuration.")
		} else if demoCtx.disableLicenseAcquisition {
			fmt.Println("#\n# Enterprise features disabled by OSS-only build.")
		} else {
			fmt.Println("#\n# This demo session will attempt to enable enterprise features\n" +
				"# by acquiring a temporary license from Cockroach Labs in the background.\n" +
				"# To disable this behavior, set the environment variable\n" +
				"# COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING=true.")
		}
	}

	if err := c.setupWorkload(ctx, gen); err != nil {
		return err
	}

	if cliCtx.isInteractive {
		if gen != nil {
			fmt.Printf("#\n# The cluster has been preloaded with the %q dataset\n# (%s).\n",
				gen.Meta().Name, gen.Meta().Description)
		}

		fmt.Printf(`#
# Reminder: your changes to data stored in the demo session will not be saved!
#
# Web UI: %s
#
`, c.s.AdminURL())
	}

	checkTzDatabaseAvailability(ctx)

	conn := makeSQLConn(c.connURL)
	defer conn.Close()

	return runClient(cmd, conn)
}
