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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var demoCmd = &cobra.Command{
	Use:   "demo",
	Short: "open a demo sql shell",
	Long: `
Start an in-memory, standalone, single-node CockroachDB instance, and open an
interactive SQL prompt to it. Various datasets are available to be preloaded as
subcommands: e.g. "cockroach demo startrek". See --help for a full list.

By default, the 'movr' dataset is pre-loaded. You can also use --no-example-database
to avoid pre-loading a dataset.

cockroach demo attempts to connect to a Cockroach Labs server to obtain a
temporary enterprise license for demoing enterprise features and enable
telemetry back to Cockroach Labs. In order to disable this behavior, set the
environment variable "COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING" to true.
`,
	Example: `  cockroach demo`,
	Args:    cobra.NoArgs,
	// Note: RunE is set in the init() function below to avoid an
	// initialization cycle.
}

func init() {
	demoCmd.RunE = MaybeDecorateGRPCError(func(cmd *cobra.Command, _ []string) error {
		return runDemo(cmd, nil /* gen */)
	})
}

const demoOrg = "Cockroach Demo"

const defaultGeneratorName = "movr"

var defaultGenerator workload.Generator

// maxNodeInitTime is the maximum amount of time to wait for nodes to be connected.
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

var demoNodeCacheSizeValue = newBytesOrPercentageValue(
	&demoCtx.cacheSize,
	memoryPercentResolver,
)
var demoNodeSQLMemSizeValue = newBytesOrPercentageValue(
	&demoCtx.sqlPoolMemorySize,
	memoryPercentResolver,
)

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

// Round-trip latencies collected from http://cloudping.co on 2019-09-11.
var regionRoundTripLatencies = map[regionPair]int{
	{regionA: "us-east1", regionB: "us-west1"}:     66,
	{regionA: "us-east1", regionB: "europe-west1"}: 64,
	{regionA: "us-west1", regionB: "europe-west1"}: 146,
}

var regionOneWayLatencies = make(map[regionPair]int)

func init() {
	// We record one-way latencies next, because the logic in our delayingConn
	// and delayingListener is in terms of one-way network delays.
	for pair, latency := range regionRoundTripLatencies {
		regionOneWayLatencies[pair] = latency / 2
	}
	regionToRegionToLatency = make(map[string]map[string]int)
	for pair, latency := range regionOneWayLatencies {
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
		if !meta.PublicFacing {
			genDemoCmd.Hidden = true
		}
		demoCmd.AddCommand(genDemoCmd)
		genDemoCmd.Flags().AddFlagSet(genFlags)
	}
}

// GetAndApplyLicense is not implemented in order to keep OSS/BSL builds successful.
// The cliccl package sets this function if enterprise features are available to demo.
var GetAndApplyLicense func(dbConn *gosql.DB, clusterID uuid.UUID, org string) (bool, error)

func incrementTelemetryCounters(cmd *cobra.Command) {
	incrementDemoCounter(demo)
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
	if gen == nil && !demoCtx.noExampleDatabase {
		// Use a default dataset unless prevented by --no-example-database.
		gen = defaultGenerator
	}

	// Make sure that the user didn't request a workload and an empty database.
	if demoCtx.runWorkload && demoCtx.noExampleDatabase {
		return nil, errors.New("cannot run a workload when generation of the example database is disabled")
	}

	// Make sure the number of nodes is valid.
	if demoCtx.nodes <= 0 {
		return nil, errors.Newf("--%s has invalid value (expected positive, got %d)", cliflags.DemoNodes.Name, demoCtx.nodes)
	}

	// If artificial latencies were requested, then the user cannot supply their own localities.
	if demoCtx.simulateLatency && demoCtx.localities != nil {
		return nil, errors.Newf("--%s cannot be used with --%s", cliflags.Global.Name, cliflags.DemoNodeLocality.Name)
	}

	demoCtx.disableTelemetry = cluster.TelemetryOptOut()
	// disableLicenseAcquisition can also be set by the user as an
	// input flag, so make sure it include it when considering the final
	// value of disableLicenseAcquisition.
	demoCtx.disableLicenseAcquisition =
		demoCtx.disableTelemetry || (GetAndApplyLicense == nil) || demoCtx.disableLicenseAcquisition

	if demoCtx.geoPartitionedReplicas {
		geoFlag := "--" + cliflags.DemoGeoPartitionedReplicas.Name
		if demoCtx.disableLicenseAcquisition {
			return nil, errors.Newf("enterprise features are needed for this demo (%s)", geoFlag)
		}

		// Make sure that the user didn't request to have a topology and disable the example database.
		if demoCtx.noExampleDatabase {
			return nil, errors.New("cannot setup geo-partitioned replicas topology without generating an example database")
		}

		// Make sure that the Movr database is selected when automatically partitioning.
		if gen == nil || gen.Meta().Name != "movr" {
			return nil, errors.Newf("%s must be used with the Movr dataset", geoFlag)
		}

		// If the geo-partitioned replicas flag was given and the demo localities have changed, throw an error.
		if demoCtx.localities != nil {
			return nil, errors.Newf("--demo-locality cannot be used with %s", geoFlag)
		}

		// If the geo-partitioned replicas flag was given and the nodes have changed, throw an error.
		if flagSetForCmd(cmd).Lookup(cliflags.DemoNodes.Name).Changed {
			if demoCtx.nodes != 9 {
				return nil, errors.Newf("--nodes with a value different from 9 cannot be used with %s", geoFlag)
			}
		} else {
			demoCtx.nodes = 9
			printlnUnlessEmbedded(
				// Only explain how the configuration was interpreted if the
				// user has control over it.
				`#
# --geo-partitioned replicas operates on a 9 node cluster.
# The cluster size has been changed from the default to 9 nodes.`)
		}

		// If geo-partition-replicas is requested, make sure the workload has a Partitioning step.
		configErr := errors.Newf(
			"workload %s is not configured to have a partitioning step", gen.Meta().Name)
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
	cmdIn, closeFn, err := getInputFile()
	if err != nil {
		return err
	}
	defer closeFn()

	if gen, err = checkDemoConfiguration(cmd, gen); err != nil {
		return err
	}
	// Record some telemetry about what flags are being used.
	incrementTelemetryCounters(cmd)

	ctx := context.Background()

	var c transientCluster
	if err := c.checkConfigAndSetupLogging(ctx, cmd); err != nil {
		return err
	}
	defer c.cleanup(ctx)

	initGEOS(ctx)

	if err := c.start(ctx, cmd, gen); err != nil {
		return checkAndMaybeShout(err)
	}
	demoCtx.transientCluster = &c

	checkInteractive(cmdIn)

	if cliCtx.isInteractive {
		printfUnlessEmbedded(`#
# Welcome to the CockroachDB demo database!
#
# You are connected to a temporary, in-memory CockroachDB cluster of %d node%s.
`, demoCtx.nodes, util.Pluralize(int64(demoCtx.nodes)))

		if demoCtx.simulateLatency {
			printfUnlessEmbedded(
				`# Communication between nodes will simulate real world latencies.
#
# WARNING: the use of --%s is experimental. Some features may not work as expected.
`,
				cliflags.Global.Name,
			)
		}

		// Only print details about the telemetry configuration if the
		// user has control over it.
		if demoCtx.disableTelemetry {
			printlnUnlessEmbedded("#\n# Telemetry and automatic license acquisition disabled by configuration.")
		} else if demoCtx.disableLicenseAcquisition {
			printlnUnlessEmbedded("#\n# Enterprise features disabled by OSS-only build.")
		} else {
			printlnUnlessEmbedded("#\n# This demo session will attempt to enable enterprise features\n" +
				"# by acquiring a temporary license from Cockroach Labs in the background.\n" +
				"# To disable this behavior, set the environment variable\n" +
				"# COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING=true.")
		}
	}

	// Start license acquisition in the background.
	licenseDone, err := c.acquireDemoLicense(ctx)
	if err != nil {
		return checkAndMaybeShout(err)
	}

	// Initialize the workload, if requested.
	if err := c.setupWorkload(ctx, gen, licenseDone); err != nil {
		return checkAndMaybeShout(err)
	}

	if cliCtx.isInteractive {
		if gen != nil {
			fmt.Printf("#\n# The cluster has been preloaded with the %q dataset\n# (%s).\n",
				gen.Meta().Name, gen.Meta().Description)
		}

		fmt.Println(`#
# Reminder: your changes to data stored in the demo session will not be saved!`)

		var nodeList strings.Builder
		c.listDemoNodes(&nodeList, true /* justOne */)
		printlnUnlessEmbedded(
			// Only print the server details when the shell is not embedded;
			// if embedded, the embedding platform owns the network
			// configuration.
			`#
# If you wish to access this demo cluster using another tool, you will need
# the following details:
#
#   - Connection parameters:
#  `,
			strings.ReplaceAll(strings.TrimSuffix(nodeList.String(), "\n"), "\n", "\n#   "))

		if !demoCtx.insecure {
			fmt.Printf(`#   - Username: %q, password: %q
#   - Directory with certificate files (for certain SQL drivers/tools): %s
#
`,
				c.adminUser,
				c.adminPassword,
				c.demoDir,
			)
		}

		// It's ok to do this twice (if workload setup already waited) because
		// then the error return is guaranteed to be nil.
		go func() {
			if err := waitForLicense(licenseDone); err != nil {
				_ = checkAndMaybeShout(err)
			}
		}()
	} else {
		// If we are not running an interactive shell, we need to wait to ensure
		// that license acquisition is successful. If license acquisition is
		// disabled, then a read on this channel will return immediately.
		if err := waitForLicense(licenseDone); err != nil {
			return checkAndMaybeShout(err)
		}
	}

	conn := makeSQLConn(c.connURL)
	defer conn.Close()

	return runClient(cmd, conn, cmdIn)
}

func waitForLicense(licenseDone <-chan error) error {
	err := <-licenseDone
	return err
}
