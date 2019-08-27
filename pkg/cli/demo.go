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
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
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
	"github.com/pkg/errors"
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

func setupTransientServers(
	cmd *cobra.Command, gen workload.Generator,
) (connURL string, adminURL string, cleanup func(), err error) {
	cleanup = func() {}
	ctx := context.Background()

	if demoCtx.nodes <= 0 {
		return "", "", cleanup, errors.Errorf("must have a positive number of nodes")
	}

	// The user specified some localities for their nodes.
	if len(demoCtx.localities) != 0 {
		// Error out of localities don't line up with requested node
		// count before doing any sort of setup.
		if len(demoCtx.localities) != demoCtx.nodes {
			return "", "", cleanup, errors.Errorf("number of localities specified must equal number of nodes")
		}
	} else {
		demoCtx.localities = make([]roachpb.Locality, demoCtx.nodes)
		for i := 0; i < demoCtx.nodes; i++ {
			demoCtx.localities[i] = defaultLocalities[i%len(defaultLocalities)]
		}
	}

	// Set up logging. For demo/transient server we use non-standard
	// behavior where we avoid file creation if possible.
	df := cmd.Flags().Lookup(cliflags.LogDir.Name)
	sf := cmd.Flags().Lookup(logflags.LogToStderrName)
	if !df.Changed && !sf.Changed {
		// User did not request logging flags; shut down all logging.
		// Otherwise, the demo command would cause a cockroach-data
		// directory to appear in the current directory just for logs.
		_ = df.Value.Set("")
		df.Changed = true
		_ = sf.Value.Set(log.Severity_NONE.String())
		sf.Changed = true
	}
	stopper, err := setupAndInitializeLoggingAndProfiling(ctx, cmd)
	if err != nil {
		return connURL, adminURL, cleanup, err
	}
	cleanup = func() { stopper.Stop(ctx) }

	// Create the first transient server. The others will join this one.
	args := base.TestServerArgs{
		PartOfCluster: true,
		Insecure:      true,
		Stopper:       stopper,
	}

	serverFactory := server.TestServerFactory
	var s *server.TestServer
	for i := 0; i < demoCtx.nodes; i++ {
		// All the nodes connect to the address of the first server created.
		if s != nil {
			args.JoinAddr = s.ServingRPCAddr()
		}
		if demoCtx.localities != nil {
			args.Locality = demoCtx.localities[i]
		}
		serv := serverFactory.New(args).(*server.TestServer)
		if err := serv.Start(args); err != nil {
			return connURL, adminURL, cleanup, err
		}
		// Remember the first server created.
		if i == 0 {
			s = serv
		}
	}

	if demoCtx.nodes < 3 {
		// Set up the default zone configuration. We are using an in-memory store
		// so we really want to disable replication.
		if err := cliDisableReplication(ctx, s.Server); err != nil {
			return ``, ``, cleanup, err
		}
	}

	// Prepare the URL for use by the SQL shell.
	options := url.Values{}
	options.Add("sslmode", "disable")
	options.Add("application_name", sqlbase.ReportableAppNamePrefix+"cockroach demo")
	url := url.URL{
		Scheme:   "postgres",
		User:     url.User(security.RootUser),
		Host:     s.ServingSQLAddr(),
		RawQuery: options.Encode(),
	}
	if gen != nil {
		url.Path = gen.Meta().Name
	}
	urlStr := url.String()

	// Communicate information about license acquisition to services
	// that depend on it.
	licenseSuccess := make(chan bool, 1)

	// Start up the update check loop.
	// We don't do this in (*server.Server).Start() because we don't want it
	// in tests.
	if !cluster.TelemetryOptOut() {
		s.PeriodicallyCheckForUpdates(ctx)
		// If we allow telemetry, then also try and get an enterprise license for the demo.
		// GetAndApplyLicense will be nil in the pure OSS/BSL build of cockroach.
		if GetAndApplyLicense != nil {
			db, err := gosql.Open("postgres", urlStr)
			if err != nil {
				return ``, ``, cleanup, err
			}
			// Perform license acquisition asynchronously to avoid delay in cli startup.
			go func() {
				defer db.Close()
				success, err := GetAndApplyLicense(db, s.ClusterID(), demoOrg)
				if err != nil {
					exitWithError("demo", err)
				}
				if !success {
					const msg = "Unable to acquire demo license. Enterprise features are not enabled in this session.\n"
					fmt.Fprint(stderr, msg)
				}
				licenseSuccess <- success
			}()
		}
	} else {
		// If we aren't supposed to check for a license, then automatically
		// notify failure.
		licenseSuccess <- false
	}

	// If there is a load generator, create its database and load its
	// fixture.
	if gen != nil {
		db, err := gosql.Open("postgres", urlStr)
		if err != nil {
			return ``, ``, cleanup, err
		}
		defer db.Close()

		if _, err := db.Exec(`CREATE DATABASE ` + gen.Meta().Name); err != nil {
			return ``, ``, cleanup, err
		}

		ctx := context.TODO()
		var l workloadsql.InsertsDataLoader
		if _, err := workloadsql.Setup(ctx, db, gen, l); err != nil {
			return ``, ``, cleanup, err
		}

		partitioningComplete := make(chan struct{}, 1)
		// If we are requested to prepartition our data spawn a goroutine to do the partitioning.
		if demoCtx.geoPartitionedReplicas {
			go func() {
				success := <-licenseSuccess
				// Only try partitioning if license acquisition was successful.
				if success {
					db, err := gosql.Open("postgres", urlStr)
					if err != nil {
						exitWithError("demo", err)
					}
					defer db.Close()
					// Based on validation done in setup, we know that this workload has a partitioning step.
					if err := gen.(workload.Hookser).Hooks().Partition(db); err != nil {
						exitWithError("demo", err)
					}
					partitioningComplete <- struct{}{}
				} else {
					const msg = "license acquisition was unsuccessful. Enterprise features are needed to partition data"
					exitWithError("demo", errors.New(msg))
				}
			}()
		}

		if demoCtx.runWorkload {
			go func() {
				// If partitioning was requested, wait for that to complete before running the workload.
				if demoCtx.geoPartitionedReplicas {
					<-partitioningComplete
				}
				if err := runWorkload(ctx, gen, urlStr, stopper); err != nil {
					exitWithError("demo", err)
				}
			}()
		}
	}

	return urlStr, s.AdminURL(), cleanup, nil
}

func runWorkload(
	ctx context.Context, gen workload.Generator, dbURL string, stopper *stop.Stopper,
) error {
	opser, ok := gen.(workload.Opser)
	if !ok {
		return errors.Errorf("default dataset %s does not have a workload defined", gen.Meta().Name)
	}

	// Dummy registry to prove to the Opser.
	reg := histogram.NewRegistry(time.Duration(100) * time.Millisecond)
	ops, err := opser.Ops([]string{dbURL}, reg)
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
		stopper.RunWorker(ctx, workloadFun(workerFn))
	}

	return nil
}

func runDemo(cmd *cobra.Command, gen workload.Generator) error {
	if gen == nil && !demoCtx.useEmptyDatabase {
		// Use a default dataset unless prevented by --empty.
		gen = defaultGenerator
	}

	// Make sure that the user didn't request a workload and an empty database.
	if demoCtx.runWorkload && demoCtx.useEmptyDatabase {
		return errors.New("cannot run a workload against an empty database")
	}

	// Make sure that the user didn't request to have a topology and an empty database.
	if demoCtx.geoPartitionedReplicas && demoCtx.useEmptyDatabase {
		return errors.New("cannot setup geo-partitioned replicas topology on an empty database")
	}

	// Make sure that the Movr database is selected when automatically partitioning.
	if demoCtx.geoPartitionedReplicas && (gen == nil || gen.Meta().Name != "movr") {
		return errors.New("--geo-partitioned-replicas must be used with the Movr dataset")
	}

	// If the geo-partitioned replicas flag was given and the demo localities have changed, throw an error.
	if demoCtx.geoPartitionedReplicas && demoCtx.localities != nil {
		return errors.New("--demo-locality cannot be used with --geo-partitioned-replicas")
	}

	// If the geo-partitioned replicas flag was given and the nodes have changed, throw an error.
	if demoCtx.geoPartitionedReplicas && cmd.Flags().Lookup(cliflags.DemoNodes.Name).Changed {
		return errors.New("--nodes cannot be used with --geo-partitioned-replicas")
	}

	// If geo-partition-replicas is requested, make sure the workload has a Partitioning step.
	if demoCtx.geoPartitionedReplicas {
		configErr := errors.New(fmt.Sprintf("workload %s is not configured to have a partitioning step", gen.Meta().Name))
		hookser, ok := gen.(workload.Hookser)
		if !ok {
			return configErr
		}
		if hookser.Hooks().Partition == nil {
			return configErr
		}
	}

	// Th geo-partitioned replicas demo only works on a 9 node cluster, so set the node count as such.
	// Ignore input user localities so that the nodes have the same attributes/localities as expected.
	if demoCtx.geoPartitionedReplicas {
		const msg = `#
# --geo-partitioned replicas operates on a 9 node cluster.
# The cluster size has been changed from the default to 9 nodes.`
		fmt.Println(msg)
		demoCtx.nodes = 9
		demoCtx.localities = nil
	}

	connURL, adminURL, cleanup, err := setupTransientServers(cmd, gen)
	defer cleanup()
	if err != nil {
		return checkAndMaybeShout(err)
	}

	checkInteractive()

	if cliCtx.isInteractive {
		fmt.Printf(`#
# Welcome to the CockroachDB demo database!
#
# You are connected to a temporary, in-memory CockroachDB cluster of %d node%s.
`, demoCtx.nodes, util.Pluralize(int64(demoCtx.nodes)))

		if gen != nil {
			fmt.Printf("# The cluster has been preloaded with the %q dataset\n# (%s).\n",
				gen.Meta().Name, gen.Meta().Description)
		}

		fmt.Printf(`#
# Your changes will not be saved!
#
# Web UI: %s
#
`, adminURL)
	}

	checkTzDatabaseAvailability(context.Background())

	conn := makeSQLConn(connURL)
	defer conn.Close()

	return runClient(cmd, conn)
}
