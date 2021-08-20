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
	"fmt"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/cli/democluster"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
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
	demoCmd.RunE = clierrorplus.MaybeDecorateError(func(cmd *cobra.Command, _ []string) error {
		return runDemo(cmd, nil /* gen */)
	})
}

const defaultGeneratorName = "movr"

var defaultGenerator workload.Generator

var demoNodeCacheSizeValue = newBytesOrPercentageValue(
	&demoCtx.CacheSize,
	memoryPercentResolver,
)
var demoNodeSQLMemSizeValue = newBytesOrPercentageValue(
	&demoCtx.SQLPoolMemorySize,
	memoryPercentResolver,
)

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
			RunE: clierrorplus.MaybeDecorateError(func(cmd *cobra.Command, _ []string) error {
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

func incrementTelemetryCounters(cmd *cobra.Command) {
	incrementDemoCounter(demo)
	if flagSetForCmd(cmd).Lookup(cliflags.DemoNodes.Name).Changed {
		incrementDemoCounter(nodes)
	}
	if demoCtx.Localities != nil {
		incrementDemoCounter(demoLocality)
	}
	if demoCtx.RunWorkload {
		incrementDemoCounter(withLoad)
	}
	if demoCtx.GeoPartitionedReplicas {
		incrementDemoCounter(geoPartitionedReplicas)
	}
}

func checkDemoConfiguration(
	cmd *cobra.Command, gen workload.Generator,
) (workload.Generator, error) {
	if gen == nil && !demoCtx.NoExampleDatabase {
		// Use a default dataset unless prevented by --no-example-database.
		gen = defaultGenerator
	}

	// Make sure that the user didn't request a workload and an empty database.
	if demoCtx.RunWorkload && demoCtx.NoExampleDatabase {
		return nil, errors.New("cannot run a workload when generation of the example database is disabled")
	}

	// Make sure the number of nodes is valid.
	if demoCtx.NumNodes <= 0 {
		return nil, errors.Newf("--%s has invalid value (expected positive, got %d)", cliflags.DemoNodes.Name, demoCtx.NumNodes)
	}

	// If artificial latencies were requested, then the user cannot supply their own localities.
	if demoCtx.SimulateLatency && demoCtx.Localities != nil {
		return nil, errors.Newf("--%s cannot be used with --%s", cliflags.Global.Name, cliflags.DemoNodeLocality.Name)
	}

	demoCtx.DisableTelemetry = cluster.TelemetryOptOut()
	// disableLicenseAcquisition can also be set by the user as an
	// input flag, so make sure it include it when considering the final
	// value of disableLicenseAcquisition.
	demoCtx.DisableLicenseAcquisition =
		demoCtx.DisableTelemetry || (democluster.GetAndApplyLicense == nil) || demoCtx.DisableLicenseAcquisition

	if demoCtx.GeoPartitionedReplicas {
		geoFlag := "--" + cliflags.DemoGeoPartitionedReplicas.Name
		if demoCtx.DisableLicenseAcquisition {
			return nil, errors.Newf("enterprise features are needed for this demo (%s)", geoFlag)
		}

		// Make sure that the user didn't request to have a topology and disable the example database.
		if demoCtx.NoExampleDatabase {
			return nil, errors.New("cannot setup geo-partitioned replicas topology without generating an example database")
		}

		// Make sure that the Movr database is selected when automatically partitioning.
		if gen == nil || gen.Meta().Name != "movr" {
			return nil, errors.Newf("%s must be used with the Movr dataset", geoFlag)
		}

		// If the geo-partitioned replicas flag was given and the demo localities have changed, throw an error.
		if demoCtx.Localities != nil {
			return nil, errors.Newf("--demo-locality cannot be used with %s", geoFlag)
		}

		// If the geo-partitioned replicas flag was given and the nodes have changed, throw an error.
		if flagSetForCmd(cmd).Lookup(cliflags.DemoNodes.Name).Changed {
			if demoCtx.NumNodes != 9 {
				return nil, errors.Newf("--nodes with a value different from 9 cannot be used with %s", geoFlag)
			}
		} else {
			demoCtx.NumNodes = 9
			cliCtx.PrintlnUnlessEmbedded(
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

func runDemo(cmd *cobra.Command, gen workload.Generator) (resErr error) {
	closeFn, err := sqlCtx.Open(os.Stdin)
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

	demoCtx.WorkloadGenerator = gen

	c, err := democluster.NewDemoCluster(ctx, &demoCtx,
		log.Infof,
		log.Warningf,
		log.Ops.Shoutf,
		func(ctx context.Context) (*stop.Stopper, error) {
			// Override the default server store spec.
			//
			// This is needed because the logging setup code peeks into this to
			// decide how to enable logging.
			serverCfg.Stores.Specs = nil
			return setupAndInitializeLoggingAndProfiling(ctx, cmd, false /* isServerCmd */)
		},
		getAdminClient,
		drainAndShutdown,
	)
	if err != nil {
		c.Close(ctx)
		return err
	}
	defer c.Close(ctx)

	initGEOS(ctx)

	if err := c.Start(ctx, runInitialSQL); err != nil {
		return clierrorplus.CheckAndMaybeShout(err)
	}
	sqlCtx.ShellCtx.DemoCluster = c

	if cliCtx.IsInteractive {
		cliCtx.PrintfUnlessEmbedded(`#
# Welcome to the CockroachDB demo database!
#
# You are connected to a temporary, in-memory CockroachDB cluster of %d node%s.
`, demoCtx.NumNodes, util.Pluralize(int64(demoCtx.NumNodes)))

		if demoCtx.SimulateLatency {
			cliCtx.PrintfUnlessEmbedded(
				`# Communication between nodes will simulate real world latencies.
#
# WARNING: the use of --%s is experimental. Some features may not work as expected.
`,
				cliflags.Global.Name,
			)
		}

		// Only print details about the telemetry configuration if the
		// user has control over it.
		if demoCtx.DisableTelemetry {
			cliCtx.PrintlnUnlessEmbedded("#\n# Telemetry and automatic license acquisition disabled by configuration.")
		} else if demoCtx.DisableLicenseAcquisition {
			cliCtx.PrintlnUnlessEmbedded("#\n# Enterprise features disabled by OSS-only build.")
		} else {
			cliCtx.PrintlnUnlessEmbedded("#\n# This demo session will attempt to enable enterprise features\n" +
				"# by acquiring a temporary license from Cockroach Labs in the background.\n" +
				"# To disable this behavior, set the environment variable\n" +
				"# COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING=true.")
		}
	}

	// Start license acquisition in the background.
	licenseDone, err := c.AcquireDemoLicense(ctx)
	if err != nil {
		return clierrorplus.CheckAndMaybeShout(err)
	}

	// Initialize the workload, if requested.
	if err := c.SetupWorkload(ctx, licenseDone); err != nil {
		return clierrorplus.CheckAndMaybeShout(err)
	}

	if cliCtx.IsInteractive {
		if gen != nil {
			fmt.Printf("#\n# The cluster has been preloaded with the %q dataset\n# (%s).\n",
				gen.Meta().Name, gen.Meta().Description)
		}

		fmt.Println(`#
# Reminder: your changes to data stored in the demo session will not be saved!`)

		var nodeList strings.Builder
		c.ListDemoNodes(&nodeList, stderr, true /* justOne */)
		cliCtx.PrintlnUnlessEmbedded(
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

		if !demoCtx.Insecure {
			adminUser, adminPassword, certsDir := c.GetSQLCredentials()

			fmt.Printf(`#   - Username: %q, password: %q
#   - Directory with certificate files (for certain SQL drivers/tools): %s
#
`,
				adminUser,
				adminPassword,
				certsDir,
			)
		}

		// It's ok to do this twice (if workload setup already waited) because
		// then the error return is guaranteed to be nil.
		go func() {
			if err := waitForLicense(licenseDone); err != nil {
				_ = clierrorplus.CheckAndMaybeShout(err)
			}
		}()
	} else {
		// If we are not running an interactive shell, we need to wait to ensure
		// that license acquisition is successful. If license acquisition is
		// disabled, then a read on this channel will return immediately.
		if err := waitForLicense(licenseDone); err != nil {
			return clierrorplus.CheckAndMaybeShout(err)
		}
	}

	conn, err := sqlCtx.MakeConn(c.GetConnURL())
	if err != nil {
		return err
	}
	defer func() { resErr = errors.CombineErrors(resErr, conn.Close()) }()

	sqlCtx.ShellCtx.ParseURL = makeURLParser(cmd)
	return sqlCtx.Run(conn)
}

func waitForLicense(licenseDone <-chan error) error {
	err := <-licenseDone
	return err
}
